// HOST — metrics sink (the domain's counter seam).
//
// Domain modules never touch the app's Prometheus registry directly: they
// report through the `Metrics` sink injected via `HostContext`. The standalone
// app adapts its in-process registry (`registryMetrics`); an embedding host
// can inject its own, or leave the default no-op.

/**
 * The counter sink core reports through (inbound frame outcomes today).
 * `name` is the metric, `help` its description, `value` the increment
 * (default `1`) and `labels` the dimension set. Implement it over whatever
 * registry the embedder already runs, or leave the `noopMetrics` default.
 */
export interface Metrics {
  counterAdd: (name: string, help: string, value?: number, labels?: Record<string, string | number>) => void;
}

/** Default sink: counts nothing. */
export const noopMetrics: Metrics = { counterAdd: () => {} };
