import type { PerfProfileName, ProfileConfig } from "./types";

const shared = {
  serverFlags: {
    queryTimeoutMs: 10000,
    namespaceIdleTimeoutMs: 300000,
    maxOpenNamespaces: 1024,
  },
};

const fast: ProfileConfig = {
  name: "fast",
  seed: 20260302,
  namespaces: {
    controlPlane: 10,
    workload: 10,
  },
  rows: {
    basePerNamespace: 12000,
    hotNamespaceRows: 50000,
    batchSize: 500,
  },
  timing: {
    repeats: 2,
    warmupMs: 8_000,
    measureMs: 18_000,
  },
  sse: {
    subscribers: [1, 10, 100],
  },
  noisyNeighbor: {
    neighbors: 5,
    writerConcurrency: 8,
  },
  pprof: {
    enabled: false,
    captureThresholdP95Ms: 200,
    captureThresholdErrorRate: 0.01,
    profileSeconds: 10,
  },
  ...shared,
};

const deep: ProfileConfig = {
  name: "deep",
  seed: 20260303,
  namespaces: {
    controlPlane: 30,
    workload: 30,
  },
  rows: {
    basePerNamespace: 100000,
    hotNamespaceRows: 1000000,
    batchSize: 1000,
  },
  timing: {
    repeats: 5,
    warmupMs: 60_000,
    measureMs: 90_000,
  },
  sse: {
    subscribers: [1, 10, 100],
  },
  noisyNeighbor: {
    neighbors: 10,
    writerConcurrency: 16,
  },
  pprof: {
    enabled: true,
    captureThresholdP95Ms: 250,
    captureThresholdErrorRate: 0.005,
    profileSeconds: 15,
  },
  ...shared,
};

export const getProfile = (name: PerfProfileName): ProfileConfig => {
  const template = name === "deep" ? deep : fast;
  return {
    ...template,
    namespaces: { ...template.namespaces },
    rows: { ...template.rows },
    timing: { ...template.timing },
    sse: { subscribers: [...template.sse.subscribers] },
    noisyNeighbor: { ...template.noisyNeighbor },
    pprof: { ...template.pprof },
    serverFlags: { ...template.serverFlags },
  };
};
