import type { HttpClient } from "../http";
import type {
  NamespaceModule,
  QueryRequest,
  QueryStatement,
  SSESubscribeOptions,
  TableCreateRequest,
  TableUpdateRequest,
} from "../types";
import { createTableModule } from "./table";
import { createSSESubscription } from "./sse";

const version = "v1";
const jsonHeaders: HeadersInit = {
  "Content-Type": "application/json",
};

export const createNamespaceModule = (http: HttpClient, namespace: string): NamespaceModule => {
  const encoded = encodeURIComponent(namespace);
  const base = `/${version}/${encoded}`;

  return {
    name: namespace,
    tables: {
      list() {
        return http.json(`${base}/tables`);
      },
      create(request: TableCreateRequest) {
        return http.json(`${base}/tables`, {
          method: "POST",
          headers: jsonHeaders,
          body: JSON.stringify(request),
        });
      },
      get(name: string) {
        return http.json(`${base}/tables/${encodeURIComponent(name)}`);
      },
      update(name: string, request: TableUpdateRequest) {
        return http.json(`${base}/tables/${encodeURIComponent(name)}`, {
          method: "PUT",
          headers: jsonHeaders,
          body: JSON.stringify(request),
        });
      },
      delete(name: string, meta?: Record<string, unknown>) {
        if (!meta) {
          return http.empty(`${base}/tables/${encodeURIComponent(name)}`, {
            method: "DELETE",
          });
        }
        return http.empty(`${base}/tables/${encodeURIComponent(name)}`, {
          method: "DELETE",
          headers: jsonHeaders,
          body: JSON.stringify({ _meta: meta }),
        });
      },
    },

    table<Row extends Record<string, unknown> = Record<string, unknown>>(name: string) {
      return createTableModule<Row>(http, namespace, name);
    },

    query: {
      run(request: QueryRequest) {
        return http.json(`${base}/query`, {
          method: "POST",
          headers: jsonHeaders,
          body: JSON.stringify(request),
        });
      },
      batch(statements: QueryStatement[]) {
        return http.json(`${base}/query`, {
          method: "POST",
          headers: jsonHeaders,
          body: JSON.stringify({ statements }),
        });
      },
    },

    changelog: {
      list(options?: { table?: string; limit?: number; offset?: number }) {
        const path = http.withQuery(`${base}/changelog`, {
          table: options?.table,
          limit: options?.limit,
          offset: options?.offset,
        });
        return http.json(path);
      },
    },

    stats: {
      get() {
        return http.json(`${base}/stats`);
      },
    },

    events: {
      async subscribe(options?: SSESubscribeOptions) {
        const controller = new AbortController();
        const externalSignal = options?.signal;

        if (externalSignal) {
          if (externalSignal.aborted) {
            controller.abort(externalSignal.reason);
          } else {
            externalSignal.addEventListener(
              "abort",
              () => {
                controller.abort(externalSignal.reason);
              },
              { once: true },
            );
          }
        }

        const path = http.withQuery(`${base}/subscribe`, {
          tables: options?.tables?.join(","),
        });

        const res = await http.raw(path, {
          method: "GET",
          signal: controller.signal,
          headers: {
            Accept: "text/event-stream",
          },
        });

        if (!res.ok) {
          return res;
        }

        try {
          const subscription = createSSESubscription(res.data, controller);
          return {
            ok: true,
            data: subscription,
            status: res.status,
            headers: res.headers,
          };
        } catch (err) {
          controller.abort();
          return {
            ok: false,
            error: {
              error: "invalid_sse_stream",
              message: err instanceof Error ? err.message : "SSE stream is not available",
            },
            status: 500,
            headers: res.headers,
          };
        }
      },
    },
  };
};
