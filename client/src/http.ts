import type { ApiErrorBody, CreateRsqlClientOptions, QueryInput, QueryValue, RsqlResult } from "./types";

export interface HttpClient {
  json<T>(path: string, init?: RequestInit): Promise<RsqlResult<T>>;
  binary(path: string, init?: RequestInit): Promise<RsqlResult<ArrayBuffer>>;
  empty(path: string, init?: RequestInit): Promise<RsqlResult<void>>;
  raw(path: string, init?: RequestInit): Promise<RsqlResult<Response>>;
  withQuery(path: string, query?: QueryInput): string;
}

const defaultHeaders = (token: string): HeadersInit => ({
  Authorization: `Bearer ${token}`,
});

export const createHttpClient = (options: CreateRsqlClientOptions): HttpClient => {
  const baseUrl = options.url.replace(/\/$/, "");
  const doFetch = options.fetch ?? fetch;

  const execute = async (path: string, init?: RequestInit): Promise<Response> => {
    const headers = new Headers(defaultHeaders(options.token));

    if (init?.headers) {
      const incoming = new Headers(init.headers);
      for (const [k, v] of incoming.entries()) {
        headers.set(k, v);
      }
    }

    return doFetch(`${baseUrl}${path}`, {
      ...init,
      headers,
    });
  };

  return {
    async json<T>(path: string, init?: RequestInit) {
      const res = await execute(path, init);
      if (!res.ok) {
        return errorResult(res);
      }
      if (res.status === 204) {
        return {
          ok: true,
          data: undefined as T,
          status: res.status,
          headers: res.headers,
        };
      }
      const data = (await res.json()) as T;
      return {
        ok: true,
        data,
        status: res.status,
        headers: res.headers,
      };
    },

    async binary(path: string, init?: RequestInit) {
      const res = await execute(path, init);
      if (!res.ok) {
        return errorResult(res);
      }
      const data = await res.arrayBuffer();
      return {
        ok: true,
        data,
        status: res.status,
        headers: res.headers,
      };
    },

    async empty(path: string, init?: RequestInit) {
      const res = await execute(path, init);
      if (!res.ok) {
        return errorResult(res);
      }
      return {
        ok: true,
        data: undefined,
        status: res.status,
        headers: res.headers,
      };
    },

    async raw(path: string, init?: RequestInit) {
      const res = await execute(path, init);
      if (!res.ok) {
        return errorResult(res);
      }
      return {
        ok: true,
        data: res,
        status: res.status,
        headers: res.headers,
      };
    },

    withQuery(path, query) {
      if (!query) {
        return path;
      }
      const search = toSearchParams(query);
      const qs = search.toString();
      if (!qs) {
        return path;
      }
      return `${path}?${qs}`;
    },
  };
};

const errorResult = async <T>(res: Response): Promise<RsqlResult<T>> => {
  let body: ApiErrorBody = {
    error: "unknown_error",
    message: "Unknown error",
  };

  try {
    const maybeJson = (await res.json()) as Partial<ApiErrorBody>;
    body = {
      error: maybeJson.error ?? "unknown_error",
      message: maybeJson.message ?? "Unknown error",
    };
  } catch {
    try {
      const text = await res.text();
      if (text.trim() !== "") {
        body = {
          error: "unknown_error",
          message: text,
        };
      }
    } catch {
      // Keep fallback body
    }
  }

  return {
    ok: false,
    error: body,
    status: res.status,
    headers: res.headers,
  };
};

const toSearchParams = (input: QueryInput): URLSearchParams => {
  if (input instanceof URLSearchParams) {
    return input;
  }

  const params = new URLSearchParams();
  for (const [key, raw] of Object.entries(input)) {
    if (Array.isArray(raw)) {
      for (const v of raw) {
        appendQueryValue(params, key, v);
      }
      continue;
    }
    appendQueryValue(params, key, raw);
  }
  return params;
};

const appendQueryValue = (params: URLSearchParams, key: string, value: QueryValue): void => {
  if (value === undefined) {
    return;
  }
  if (value === null) {
    params.append(key, "null");
    return;
  }
  params.append(key, String(value));
};
