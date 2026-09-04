// HOST — metrics sink (the domain's counter seam).
//
// Domain modules never touch the app's Prometheus registry directly: they
// report through the `Metrics` sink injected via `HostContext`. The standalone
// app adapts its in-process registry (`registryMetrics`); an embedding host
// can inject its own, or leave the default no-op.

export interface Metrics {
  counterAdd: (name: string, help: string, value?: number, labels?: Record<string, string | number>) => void;
}

/** Default sink: counts nothing. */
export const noopMetrics: Metrics = { counterAdd: () => {} };
