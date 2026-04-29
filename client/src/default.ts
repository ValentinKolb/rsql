import { createRsqlClient } from "./client";
import type { RsqlClient } from "./types";

let defaultClient: RsqlClient | null = null;

const readEnv = (key: string): string | undefined => {
  const maybeProcess = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process;
  return maybeProcess?.env?.[key];
};

const createDefaultClient = (): RsqlClient => {
  const url = readEnv("RSQL_URL");
  const token = readEnv("RSQL_API_TOKEN");

  if (!url || !token) {
    throw new Error(
      "RSQL_URL and RSQL_API_TOKEN environment variables are required.\n" +
        "You can also create an explicit client:\n" +
        "  createRsqlClient({ url: \"http://127.0.0.1:8080\", token: \"secret\" })",
    );
  }

  return createRsqlClient({ url, token });
};

export const rsql: RsqlClient = new Proxy({} as RsqlClient, {
  get(_target, prop, receiver) {
    if (defaultClient === null) {
      defaultClient = createDefaultClient();
    }

    const value = Reflect.get(defaultClient, prop, receiver);
    if (typeof value === "function") {
      return value.bind(defaultClient);
    }
    return value;
  },
});
