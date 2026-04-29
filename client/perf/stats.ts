export const round = (n: number, digits = 2): number => {
  const factor = 10 ** digits;
  return Math.round(n * factor) / factor;
};

export const percentile = (values: number[], p: number): number => {
  if (values.length === 0) {
    return 0;
  }
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.floor(sorted.length * p)));
  return sorted[idx] ?? 0;
};

export const latencySummary = (values: number[]) => {
  if (values.length === 0) {
    return {
      min: 0,
      p50: 0,
      p95: 0,
      p99: 0,
      max: 0,
      avg: 0,
    };
  }

  const sorted = [...values].sort((a, b) => a - b);
  const sum = sorted.reduce((acc, current) => acc + current, 0);

  return {
    min: round(sorted[0] ?? 0),
    p50: round(percentile(sorted, 0.5)),
    p95: round(percentile(sorted, 0.95)),
    p99: round(percentile(sorted, 0.99)),
    max: round(sorted[sorted.length - 1] ?? 0),
    avg: round(sum / sorted.length),
  };
};

export const average = (values: number[]): number => {
  if (values.length === 0) {
    return 0;
  }
  return values.reduce((acc, current) => acc + current, 0) / values.length;
};
