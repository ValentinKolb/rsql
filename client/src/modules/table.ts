import type { HttpClient } from "../http";
import type {
  IndexCreateRequest,
  MutateOptions,
  QueryInput,
  RowsModule,
  RsqlResult,
  TableExportOptions,
  TableModule,
  TableUpdateRequest,
} from "../types";

interface Context {
  http: HttpClient;
  namespace: string;
  table: string;
}

export const createTableModule = <Row extends Record<string, unknown>>(
  http: HttpClient,
  namespace: string,
  table: string,
): TableModule<Row> => {
  const ctx: Context = {
    http,
    namespace: encodeURIComponent(namespace),
    table: encodeURIComponent(table),
  };

  return {
    rows: createRowsModule<Row>(ctx),
    indexes: {
      create(request: IndexCreateRequest): Promise<RsqlResult<{ created: boolean }>> {
        return ctx.http.json(`/${version}/${ctx.namespace}/tables/${ctx.table}/indexes`, {
          method: "POST",
          headers: jsonHeaders,
          body: JSON.stringify(request),
        });
      },
      delete(indexName: string, meta?: Record<string, unknown>): Promise<RsqlResult<void>> {
        return deleteWithOptionalMeta(
          ctx.http,
          `/${version}/${ctx.namespace}/tables/${ctx.table}/indexes/${encodeURIComponent(indexName)}`,
          meta,
        );
      },
    },
    schema: {
      get(): Promise<RsqlResult<Record<string, unknown>>> {
        return ctx.http.json(`/${version}/${ctx.namespace}/tables/${ctx.table}`);
      },
      update(request: TableUpdateRequest): Promise<RsqlResult<{ updated: boolean }>> {
        return ctx.http.json(`/${version}/${ctx.namespace}/tables/${ctx.table}`, {
          method: "PUT",
          headers: jsonHeaders,
          body: JSON.stringify(request),
        });
      },
      delete(meta?: Record<string, unknown>): Promise<RsqlResult<void>> {
        return deleteWithOptionalMeta(ctx.http, `/${version}/${ctx.namespace}/tables/${ctx.table}`, meta);
      },
    },
    export(options: TableExportOptions): Promise<RsqlResult<Response>> {
      // The endpoint accepts the same filter grammar as rows.list. We add
      // the required `format` (and optional `bom`) on top of the caller's
      // own filters.
      const query: Record<string, unknown> = { format: options.format };
      if (options.bom) {
        query.bom = "true";
      }
      const merged = mergeQuery(options.query, query);
      const path = ctx.http.withQuery(
        `/${version}/${ctx.namespace}/tables/${ctx.table}/export`,
        merged,
      );
      return ctx.http.raw(path, { method: "GET" });
    },
  };
};

const mergeQuery = (base: QueryInput | undefined, extras: Record<string, unknown>): QueryInput => {
  const params = new URLSearchParams();
  if (base instanceof URLSearchParams) {
    for (const [k, v] of base.entries()) params.append(k, v);
  } else if (base) {
    for (const [k, raw] of Object.entries(base)) {
      const values = Array.isArray(raw) ? raw : [raw];
      for (const v of values) {
        if (v === undefined) continue;
        params.append(k, v === null ? "null" : String(v));
      }
    }
  }
  for (const [k, v] of Object.entries(extras)) {
    if (v === undefined || v === null) continue;
    params.set(k, String(v));
  }
  return params;
};

const createRowsModule = <Row extends Record<string, unknown>>(ctx: Context): RowsModule<Row> => {
  const basePath = `/${version}/${ctx.namespace}/tables/${ctx.table}/rows`;

  return {
    list(query?: QueryInput) {
      return ctx.http.json(ctx.http.withQuery(basePath, query));
    },

    insert(rows: Partial<Row> | Array<Partial<Row>>, options?: MutateOptions) {
      const body = Array.isArray(rows) ? { rows } : { ...rows };
      if (options?.meta) {
        (body as Record<string, unknown>)._meta = options.meta;
      }
      return ctx.http.json(basePath, {
        method: "POST",
        headers: withPrefer(options?.prefer),
        body: JSON.stringify(body),
      });
    },

    get(id: number) {
      return ctx.http.json(`${basePath}/${id}`);
    },

    update(id: number, payload: Record<string, unknown>, options?: MutateOptions) {
      return ctx.http.json(`${basePath}/${id}`, {
        method: "PUT",
        headers: withPrefer(options?.prefer),
        body: JSON.stringify(withMeta(payload, options?.meta)),
      });
    },

    delete(id: number, options?: MutateOptions) {
      return deleteWithPreferAndMeta<Row>(ctx.http, `${basePath}/${id}`, options);
    },

    bulkUpdate(query: QueryInput, payload: Record<string, unknown>, options?: MutateOptions) {
      return ctx.http.json(ctx.http.withQuery(basePath, query), {
        method: "PATCH",
        headers: withPrefer(options?.prefer),
        body: JSON.stringify(withMeta(payload, options?.meta)),
      });
    },

    bulkDelete(query: QueryInput, options?: MutateOptions) {
      return deleteWithPreferAndMeta<Row>(ctx.http, ctx.http.withQuery(basePath, query), options);
    },
  };
};

const deleteWithOptionalMeta = (http: HttpClient, path: string, meta?: Record<string, unknown>): Promise<RsqlResult<void>> => {
  if (!meta) {
    return http.empty(path, { method: "DELETE" });
  }
  return http.empty(path, {
    method: "DELETE",
    headers: jsonHeaders,
    body: JSON.stringify({ _meta: meta }),
  });
};

const deleteWithPreferAndMeta = <Row extends Record<string, unknown>>(
  http: HttpClient,
  path: string,
  options?: MutateOptions,
): Promise<RsqlResult<{ data: Row[] } | { deleted: number } | void>> => {
  const headers = withPrefer(options?.prefer);
  const body = options?.meta ? JSON.stringify({ _meta: options.meta }) : undefined;

  if (options?.prefer === "return=representation") {
    return http.json<{ data: Row[] }>(path, {
      method: "DELETE",
      headers,
      body,
    });
  }

  return http.json<void>(path, {
    method: "DELETE",
    headers,
    body,
  });
};

const withMeta = (payload: Record<string, unknown>, meta?: Record<string, unknown>): Record<string, unknown> => {
  if (!meta) {
    return payload;
  }
  return {
    ...payload,
    _meta: meta,
  };
};

const withPrefer = (prefer?: string): HeadersInit => {
  const headers = new Headers(jsonHeaders);
  if (prefer) {
    headers.set("Prefer", prefer);
  }
  return headers;
};

const version = "v1";
const jsonHeaders: HeadersInit = {
  "Content-Type": "application/json",
};
