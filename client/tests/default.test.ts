import { describe, expect, test } from "bun:test";

describe("default client env behavior", () => {
  test("env is read lazily on first access", async () => {
    const oldUrl = process.env.RSQL_URL;
    const oldToken = process.env.RSQL_API_TOKEN;

    process.env.RSQL_URL = "";
    process.env.RSQL_API_TOKEN = "";

    const mod = await import(`../src/default.ts?lazy=${Date.now()}`);
    expect(typeof mod.rsql).toBe("object");

    let thrown = false;
    try {
      // first property access should trigger env evaluation
      void mod.rsql.namespaces;
    } catch (err) {
      thrown = true;
      expect(String(err)).toContain("RSQL_URL");
    }
    expect(thrown).toBe(true);

    process.env.RSQL_URL = oldUrl;
    process.env.RSQL_API_TOKEN = oldToken;
  });
});
