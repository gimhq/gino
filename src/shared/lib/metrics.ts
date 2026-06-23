/**
 * Tiny in-process metrics registry. Avoids pulling in `prom-client` /
 * `@opentelemetry/*` for what is, today, a single-instance product.
 * Emits prometheus exposition format on demand. Operators that outgrow
 * this can swap the implementation without changing call sites.
 */

interface Counter {
  readonly type: "counter";
  readonly name: string;
  readonly help: string;
  readonly values: Map<string, number>;
}

interface Gauge {
  readonly type: "gauge";
  readonly name: string;
  readonly help: string;
  readonly values: Map<string, number>;
}

interface Histogram {
  readonly type: "histogram";
  readonly name: string;
  readonly help: string;
  readonly buckets: readonly number[];
  // Map<labels, { counts: number[]; sum: number; count: number }>
  readonly values: Map<string, { counts: number[]; sum: number; count: number }>;
}

type Metric = Counter | Gauge | Histogram;

const registry = new Map<string, Metric>();

function labelKey(labels: Record<string, string | number> | undefined): string {
  if (!labels)
    return "";
  const entries = Object.entries(labels)
    .filter(([, v]) => v !== undefined && v !== null)
    .map(([k, v]) => `${k}="${escapeLabelValue(String(v))}"`)
    .sort();
  return entries.join(",");
}

const RE_LABEL_ESCAPE = /[\\\n"]/g;
function escapeLabelValue(value: string): string {
  return value.replace(RE_LABEL_ESCAPE, (m) => {
    if (m === "\\")
      return "\\\\";
    if (m === "\n")
      return "\\n";
    return "\\\"";
  });
}

export function counterAdd(name: string, help: string, value = 1, labels?: Record<string, string | number>): void {
  let metric = registry.get(name);
  if (!metric) {
    metric = { type: "counter", name, help, values: new Map() };
    registry.set(name, metric);
  }
  if (metric.type !== "counter")
    return;
  const key = labelKey(labels);
  metric.values.set(key, (metric.values.get(key) ?? 0) + value);
}

export function gaugeSet(name: string, help: string, value: number, labels?: Record<string, string | number>): void {
  let metric = registry.get(name);
  if (!metric) {
    metric = { type: "gauge", name, help, values: new Map() };
    registry.set(name, metric);
  }
  if (metric.type !== "gauge")
    return;
  metric.values.set(labelKey(labels), value);
}

const DEFAULT_HISTOGRAM_BUCKETS = [
  0.005,
  0.01,
  0.025,
  0.05,
  0.1,
  0.25,
  0.5,
  1,
  2.5,
  5,
  10,
] as const;

export function histogramObserve(name: string, help: string, value: number, labels?: Record<string, string | number>): void {
  let metric = registry.get(name);
  if (!metric) {
    metric = {
      type: "histogram",
      name,
      help,
      buckets: DEFAULT_HISTOGRAM_BUCKETS,
      values: new Map(),
    };
    registry.set(name, metric);
  }
  if (metric.type !== "histogram")
    return;
  const key = labelKey(labels);
  let entry = metric.values.get(key);
  if (!entry) {
    entry = { counts: Array.from({ length: metric.buckets.length }).fill(0) as number[], sum: 0, count: 0 };
    metric.values.set(key, entry);
  }
  for (let i = 0; i < metric.buckets.length; i++) {
    if (value <= metric.buckets[i]!)
      entry.counts[i] = (entry.counts[i] ?? 0) + 1;
  }
  entry.sum += value;
  entry.count += 1;
}

function formatLine(name: string, key: string, value: number): string {
  return key ? `${name}{${key}} ${value}\n` : `${name} ${value}\n`;
}

export function renderPrometheus(): string {
  const lines: string[] = [];
  for (const metric of registry.values()) {
    lines.push(`# HELP ${metric.name} ${metric.help}`);
    lines.push(`# TYPE ${metric.name} ${metric.type}`);
    if (metric.type === "histogram") {
      for (const [key, entry] of metric.values) {
        for (let i = 0; i < metric.buckets.length; i++) {
          const labels = key
            ? `${key},le="${metric.buckets[i]}"`
            : `le="${metric.buckets[i]}"`;
          lines.push(`${metric.name}_bucket{${labels}} ${entry.counts[i]}`);
        }
        const inf = key ? `${key},le="+Inf"` : `le="+Inf"`;
        lines.push(`${metric.name}_bucket{${inf}} ${entry.count}`);
        lines.push(formatLine(`${metric.name}_sum`, key, entry.sum).trimEnd());
        lines.push(formatLine(`${metric.name}_count`, key, entry.count).trimEnd());
      }
    }
    else {
      for (const [key, value] of metric.values) {
        lines.push(formatLine(metric.name, key, value).trimEnd());
      }
    }
  }
  return `${lines.join("\n")}\n`;
}

/** Test-only: drop the registry between specs. */
export function __resetMetricsForTests(): void {
  registry.clear();
}
