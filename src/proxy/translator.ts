// M7 — protocol translator: Claude Messages API ↔ OpenAI Responses API.
//
// Pure functions, no I/O — the heart of the Claude→OpenAI bridge and fully
// unit-testable in isolation. Ported from the v1 proxy (backup/pre-hostv2
// src/proxy/openai-translator.ts, itself ported from CLIProxyAPI), tightened for
// the v2 strict-TS settings (no `any`, exactOptionalPropertyTypes,
// noUncheckedIndexedAccess).
//
//   Request:  Claude Messages → OpenAI Responses (sent over WS to chatgpt.com)
//   Response: OpenAI Responses SSE → Claude Messages SSE (back to the engine)

// ─── Request translation: Claude Messages → OpenAI Responses ───

interface ClaudeContentBlock {
  readonly type: string;
  readonly [key: string]: unknown;
}

export interface ClaudeRequest {
  readonly model?: string;
  readonly max_tokens?: number;
  readonly system?: string | ReadonlyArray<{ type: string; text: string }>;
  readonly messages: ReadonlyArray<{
    readonly role: string;
    readonly content: string | ReadonlyArray<ClaudeContentBlock>;
  }>;
  readonly tools?: ReadonlyArray<Record<string, unknown>>;
  readonly thinking?: { type: string; budget_tokens?: number };
  readonly [key: string]: unknown;
}

export interface OpenAIResponsesRequest {
  model: string;
  input: unknown[];
  stream: boolean;
  store: boolean;
  parallel_tool_calls: boolean;
  include: string[];
  instructions?: string;
  tools?: unknown[];
  reasoning?: { effort: string; summary?: string };
  [key: string]: unknown;
}

/** Translate a Claude Messages request body into an OpenAI Responses request. */
export function claudeToOpenAIRequest(body: ClaudeRequest, targetModel: string): OpenAIResponsesRequest {
  const out: OpenAIResponsesRequest = {
    model: targetModel,
    input: [],
    stream: true,
    store: false,
    parallel_tool_calls: true,
    include: ["reasoning.encrypted_content"],
  };

  // System → instructions (required by the chatgpt.com WS).
  if (body.system) {
    const systemText = typeof body.system === "string"
      ? body.system
      : body.system.filter(s => s.type === "text").map(s => s.text).join("\n");
    if (systemText)
      out.instructions = systemText;
  }

  // Messages → input items.
  for (const msg of body.messages) {
    const role = msg.role === "assistant" ? "assistant" : "user";
    if (typeof msg.content === "string") {
      const textType = msg.role === "assistant" ? "output_text" : "input_text";
      out.input.push({ type: "message", role, content: [{ type: textType, text: msg.content }] });
      continue;
    }

    for (const block of msg.content) {
      switch (block.type) {
        case "text": {
          const textType = msg.role === "assistant" ? "output_text" : "input_text";
          out.input.push({ type: "message", role, content: [{ type: textType, text: String(block.text ?? "") }] });
          break;
        }
        case "tool_use": {
          out.input.push({
            type: "function_call",
            call_id: String(block.id ?? ""),
            name: shortenName(String(block.name ?? "")),
            arguments: JSON.stringify(block.input ?? {}),
          });
          break;
        }
        case "tool_result": {
          const content = block.content;
          const outputText = typeof content === "string" ? content : JSON.stringify(content ?? "");
          out.input.push({
            type: "function_call_output",
            call_id: String(block.tool_use_id ?? ""),
            output: outputText,
          });
          break;
        }
        case "image": {
          const source = block.source as { data?: string; media_type?: string } | undefined;
          if (source?.data) {
            out.input.push({
              type: "message",
              role: "user",
              content: [{ type: "input_image", image_url: `data:${source.media_type ?? "image/png"};base64,${source.data}` }],
            });
          }
          break;
        }
        // "thinking" blocks are context only — reasoning is set separately.
        default:
          break;
      }
    }
  }

  // Tools → OpenAI function format.
  if (body.tools?.length) {
    out.tools = body.tools.map((tool) => {
      const name = shortenName(String((tool as { name?: unknown }).name ?? ""));
      const description = (tool as { description?: unknown }).description;
      const parameters = (tool as { input_schema?: unknown }).input_schema ?? { type: "object", properties: {} };
      const fn: Record<string, unknown> = { type: "function", name, parameters, strict: false };
      if (typeof description === "string")
        fn.description = description;
      return fn;
    });
  }

  // Thinking → reasoning effort.
  out.reasoning = { effort: body.thinking ? thinkingToEffort(body.thinking) : "medium", summary: "auto" };

  return out;
}

// ─── Response translation: OpenAI Responses SSE → Claude Messages SSE ───

export interface TranslatorState {
  blockIndex: number;
  hasToolCall: boolean;
  hasReceivedArgsDelta: boolean;
  toolIdCounter: number;
}

export function createTranslatorState(): TranslatorState {
  return { blockIndex: 0, hasToolCall: false, hasReceivedArgsDelta: false, toolIdCounter: 0 };
}

/**
 * Translate one OpenAI Responses SSE event (raw JSON string) into zero or more
 * Claude Messages SSE lines (each an `event: <name>\ndata: <json>\n`). Unknown
 * or unparseable events yield `[]`.
 */
export function openAIEventToClaudeSSE(
  data: string,
  state: TranslatorState,
  originalTools?: ReadonlyArray<Record<string, unknown>>,
): string[] {
  let parsed: Record<string, unknown>;
  try {
    parsed = JSON.parse(data) as Record<string, unknown>;
  }
  catch {
    return [];
  }

  const type = parsed.type as string | undefined;
  const lines: string[] = [];
  const emit = (event: string, payload: Record<string, unknown>) => {
    lines.push(`event: ${event}\ndata: ${JSON.stringify(payload)}\n`);
  };

  switch (type) {
    case "response.created": {
      const resp = (parsed.response ?? {}) as Record<string, unknown>;
      emit("message_start", {
        type: "message_start",
        message: {
          id: resp.id ?? "",
          type: "message",
          role: "assistant",
          model: (resp.model as string) ?? "",
          stop_sequence: null,
          usage: { input_tokens: 0, output_tokens: 0 },
          content: [],
          stop_reason: null,
        },
      });
      break;
    }
    case "response.reasoning_summary_part.added":
      emit("content_block_start", { type: "content_block_start", index: state.blockIndex, content_block: { type: "thinking", thinking: "" } });
      break;
    case "response.reasoning_summary_text.delta":
      emit("content_block_delta", { type: "content_block_delta", index: state.blockIndex, delta: { type: "thinking_delta", thinking: parsed.delta ?? "" } });
      break;
    case "response.reasoning_summary_part.done":
      emit("content_block_stop", { type: "content_block_stop", index: state.blockIndex });
      state.blockIndex++;
      break;
    case "response.content_part.added":
      emit("content_block_start", { type: "content_block_start", index: state.blockIndex, content_block: { type: "text", text: "" } });
      break;
    case "response.output_text.delta":
      emit("content_block_delta", { type: "content_block_delta", index: state.blockIndex, delta: { type: "text_delta", text: parsed.delta ?? "" } });
      break;
    case "response.content_part.done":
      emit("content_block_stop", { type: "content_block_stop", index: state.blockIndex });
      state.blockIndex++;
      break;
    case "response.output_item.added": {
      const item = (parsed.item ?? {}) as Record<string, unknown>;
      if (item.type === "function_call") {
        state.hasToolCall = true;
        state.hasReceivedArgsDelta = false;
        emit("content_block_start", {
          type: "content_block_start",
          index: state.blockIndex,
          content_block: {
            type: "tool_use",
            id: sanitizeToolId(item.call_id as string | undefined, state),
            name: restoreToolName(String(item.name ?? ""), originalTools),
            input: {},
          },
        });
        emit("content_block_delta", { type: "content_block_delta", index: state.blockIndex, delta: { type: "input_json_delta", partial_json: "" } });
      }
      break;
    }
    case "response.function_call_arguments.delta":
      state.hasReceivedArgsDelta = true;
      emit("content_block_delta", { type: "content_block_delta", index: state.blockIndex, delta: { type: "input_json_delta", partial_json: parsed.delta ?? "" } });
      break;
    case "response.function_call_arguments.done":
      if (!state.hasReceivedArgsDelta && parsed.arguments)
        emit("content_block_delta", { type: "content_block_delta", index: state.blockIndex, delta: { type: "input_json_delta", partial_json: parsed.arguments as string } });
      break;
    case "response.output_item.done": {
      const item = (parsed.item ?? {}) as Record<string, unknown>;
      if (item.type === "function_call") {
        emit("content_block_stop", { type: "content_block_stop", index: state.blockIndex });
        state.blockIndex++;
      }
      break;
    }
    case "response.completed": {
      const resp = (parsed.response ?? {}) as Record<string, unknown>;
      const usage = (resp.usage ?? {}) as Record<string, unknown>;
      const inputTokens = (usage.input_tokens as number) ?? 0;
      const outputTokens = (usage.output_tokens as number) ?? 0;
      const cachedTokens = ((usage.input_tokens_details as Record<string, unknown> | undefined)?.cached_tokens as number) ?? 0;
      const stopReason = state.hasToolCall
        ? "tool_use"
        : (resp.stop_reason === "max_tokens" ? "max_tokens" : "end_turn");
      const usageOut: Record<string, unknown> = {
        input_tokens: Math.max(0, inputTokens - cachedTokens),
        output_tokens: outputTokens,
      };
      if (cachedTokens > 0)
        usageOut.cache_read_input_tokens = cachedTokens;
      emit("message_delta", { type: "message_delta", delta: { stop_reason: stopReason, stop_sequence: null }, usage: usageOut });
      emit("message_stop", { type: "message_stop" });
      break;
    }
    default:
      break;
  }

  return lines;
}

// ─── Helpers ───

const NAME_LIMIT = 64;

function shortenName(name: string): string {
  if (name.length <= NAME_LIMIT)
    return name;
  if (name.startsWith("mcp__")) {
    const idx = name.lastIndexOf("__");
    if (idx > 0) {
      const cand = `mcp__${name.slice(idx + 2)}`;
      return cand.length > NAME_LIMIT ? cand.slice(0, NAME_LIMIT) : cand;
    }
  }
  return name.slice(0, NAME_LIMIT);
}

function restoreToolName(shortName: string, originalTools?: ReadonlyArray<Record<string, unknown>>): string {
  if (!originalTools)
    return shortName;
  for (const tool of originalTools) {
    const name = (tool as { name?: unknown }).name;
    if (typeof name === "string" && shortenName(name) === shortName)
      return name;
  }
  return shortName;
}

function sanitizeToolId(id: string | undefined, state: TranslatorState): string {
  if (!id)
    return `toolu_${state.toolIdCounter++}`;
  return id; // Claude accepts the upstream call_id as-is
}

function thinkingToEffort(thinking: { type: string; budget_tokens?: number }): string {
  if (thinking.type === "disabled")
    return "none";
  if (thinking.type === "adaptive" || thinking.type === "auto")
    return "high";
  if (thinking.type === "enabled" && thinking.budget_tokens) {
    const b = thinking.budget_tokens;
    if (b <= 1024)
      return "low";
    if (b <= 4096)
      return "medium";
    return "high";
  }
  return "medium";
}

const RE_OPENAI_MODEL_PREFIX = /^(?:gpt-|o[134]|chatgpt-)/;

/** True for an OpenAI/Codex model name (prefix heuristic). */
export function isOpenAIModel(model: string): boolean {
  return RE_OPENAI_MODEL_PREFIX.test(model);
}
