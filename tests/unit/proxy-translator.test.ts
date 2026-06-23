import { describe, expect, it } from "bun:test";
import {
  claudeToOpenAIRequest,
  createTranslatorState,
  isOpenAIModel,
  openAIEventToClaudeSSE,
} from "@/proxy";

describe("M7 proxy/translator — claudeToOpenAIRequest", () => {
  it("maps system → instructions and sets stream/store/include defaults", () => {
    const out = claudeToOpenAIRequest({ system: "Be terse.", messages: [] }, "gpt-5-codex");
    expect(out.model).toBe("gpt-5-codex");
    expect(out.instructions).toBe("Be terse.");
    expect(out.stream).toBe(true);
    expect(out.store).toBe(false);
    expect(out.include).toEqual(["reasoning.encrypted_content"]);
  });

  it("joins structured system text blocks", () => {
    const out = claudeToOpenAIRequest({
      system: [{ type: "text", text: "Line 1" }, { type: "text", text: "Line 2" }],
      messages: [],
    }, "gpt-5");
    expect(out.instructions).toBe("Line 1\nLine 2");
  });

  it("maps string + text content to input messages with the right role/type", () => {
    const out = claudeToOpenAIRequest({
      messages: [
        { role: "user", content: "hello" },
        { role: "assistant", content: [{ type: "text", text: "hi back" }] },
      ],
    }, "gpt-5");
    expect(out.input).toEqual([
      { type: "message", role: "user", content: [{ type: "input_text", text: "hello" }] },
      { type: "message", role: "assistant", content: [{ type: "output_text", text: "hi back" }] },
    ]);
  });

  it("maps tool_use → function_call and tool_result → function_call_output", () => {
    const out = claudeToOpenAIRequest({
      messages: [
        { role: "assistant", content: [{ type: "tool_use", id: "toolu_1", name: "get_weather", input: { city: "SF" } }] },
        { role: "user", content: [{ type: "tool_result", tool_use_id: "toolu_1", content: "72F" }] },
      ],
    }, "gpt-5");
    expect(out.input[0]).toEqual({ type: "function_call", call_id: "toolu_1", name: "get_weather", arguments: JSON.stringify({ city: "SF" }) });
    expect(out.input[1]).toEqual({ type: "function_call_output", call_id: "toolu_1", output: "72F" });
  });

  it("maps image blocks to input_image data URLs", () => {
    const out = claudeToOpenAIRequest({
      messages: [{ role: "user", content: [{ type: "image", source: { type: "base64", media_type: "image/png", data: "AAAA" } }] }],
    }, "gpt-5");
    expect(out.input[0]).toEqual({ type: "message", role: "user", content: [{ type: "input_image", image_url: "data:image/png;base64,AAAA" }] });
  });

  it("maps tools to OpenAI function tools and derives reasoning effort from thinking", () => {
    const out = claudeToOpenAIRequest({
      messages: [],
      tools: [{ name: "search", description: "Search", input_schema: { type: "object", properties: { q: { type: "string" } } } }],
      thinking: { type: "enabled", budget_tokens: 512 },
    }, "gpt-5");
    expect(out.tools?.[0]).toEqual({ type: "function", name: "search", parameters: { type: "object", properties: { q: { type: "string" } } }, strict: false, description: "Search" });
    expect(out.reasoning).toEqual({ effort: "low", summary: "auto" });
  });
});

describe("M7 proxy/translator — openAIEventToClaudeSSE", () => {
  it("translates response.created → message_start", () => {
    const state = createTranslatorState();
    const lines = openAIEventToClaudeSSE(JSON.stringify({ type: "response.created", response: { id: "resp_1", model: "gpt-5" } }), state);
    expect(lines).toHaveLength(1);
    expect(lines[0]).toContain("event: message_start");
    expect(lines[0]).toContain("\"id\":\"resp_1\"");
  });

  it("translates a text streaming sequence", () => {
    const state = createTranslatorState();
    const start = openAIEventToClaudeSSE(JSON.stringify({ type: "response.content_part.added" }), state);
    const delta = openAIEventToClaudeSSE(JSON.stringify({ type: "response.output_text.delta", delta: "Hello" }), state);
    const stop = openAIEventToClaudeSSE(JSON.stringify({ type: "response.content_part.done" }), state);
    expect(start[0]).toContain("content_block_start");
    expect(delta[0]).toContain("text_delta");
    expect(delta[0]).toContain("Hello");
    expect(stop[0]).toContain("content_block_stop");
    expect(state.blockIndex).toBe(1);
  });

  it("translates a function call, restoring the original tool name and flagging tool_use", () => {
    const state = createTranslatorState();
    const originalTools = [{ name: "get_weather" }];
    const added = openAIEventToClaudeSSE(JSON.stringify({ type: "response.output_item.added", item: { type: "function_call", call_id: "toolu_9", name: "get_weather" } }), state, originalTools);
    expect(added[0]).toContain("tool_use");
    expect(added[0]).toContain("get_weather");
    expect(added[0]).toContain("toolu_9");
    expect(state.hasToolCall).toBe(true);
    const argd = openAIEventToClaudeSSE(JSON.stringify({ type: "response.function_call_arguments.delta", delta: "{\"city\"" }), state);
    expect(argd[0]).toContain("input_json_delta");
  });

  it("translates response.completed → message_delta(stop_reason) + message_stop", () => {
    const state = createTranslatorState();
    const lines = openAIEventToClaudeSSE(JSON.stringify({
      type: "response.completed",
      response: { usage: { input_tokens: 100, output_tokens: 20, input_tokens_details: { cached_tokens: 30 } } },
    }), state);
    expect(lines).toHaveLength(2);
    expect(lines[0]).toContain("message_delta");
    expect(lines[0]).toContain("\"stop_reason\":\"end_turn\"");
    expect(lines[0]).toContain("\"input_tokens\":70"); // 100 - 30 cached
    expect(lines[0]).toContain("\"cache_read_input_tokens\":30");
    expect(lines[1]).toContain("message_stop");
  });

  it("reports tool_use stop_reason after a function call", () => {
    const state = createTranslatorState();
    openAIEventToClaudeSSE(JSON.stringify({ type: "response.output_item.added", item: { type: "function_call", call_id: "c", name: "n" } }), state);
    const lines = openAIEventToClaudeSSE(JSON.stringify({ type: "response.completed", response: {} }), state);
    expect(lines[0]).toContain("\"stop_reason\":\"tool_use\"");
  });

  it("returns [] on unparseable / unknown events", () => {
    const state = createTranslatorState();
    expect(openAIEventToClaudeSSE("not json", state)).toEqual([]);
    expect(openAIEventToClaudeSSE(JSON.stringify({ type: "response.unknown" }), state)).toEqual([]);
  });

  it("isOpenAIModel detects model families", () => {
    expect(isOpenAIModel("gpt-5")).toBe(true);
    expect(isOpenAIModel("o3-mini")).toBe(true);
    expect(isOpenAIModel("claude-sonnet-4-6")).toBe(false);
  });
});
