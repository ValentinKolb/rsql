export interface ApiErrorBody {
  error: string;
  message: string;
}

export type RsqlResult<T> =
  | { ok: true; data: T; status: number; headers: Headers }
  | { ok: false; error: ApiErrorBody; status: number; headers: Headers };

export interface CreateRsqlClientOptions {
  url: string;
  token: string;
  fetch?: typeof fetch;
}

export interface NamespaceConfig {
  journal_mode: string;
  busy_timeout: number;
  max_db_size?: number;
  query_timeout: number;
  foreign_keys: boolean;
  read_only?: boolean;
}

export interface NamespaceDefinition {
  name: string;
  config?: NamespaceConfig;
}

export interface ColumnDefinition {
  name: string;
  type: string;
  not_null?: boolean;
  unique?: boolean;
  default?: unknown;
  index?: boolean;
  pattern?: string;
  max_length?: number;
  min?: number;
  max?: number;
  auto?: boolean;
  options?: string[];
  formula?: string;
  metadata?: Record<string, unknown>;
  primary_key?: boolean;
  read_only?: boolean;
}

export interface TableCreateRequest {
  type: "table" | "view";
  name: string;
  metadata?: Record<string, unknown>;
  columns?: ColumnDefinition[];
  sql?: string;
  meta?: Record<string, unknown>;
}

export interface TableUpdateRequest {
  rename?: string;
  add_columns?: ColumnDefinition[];
  drop_columns?: string[];
  rename_columns?: Record<string, string>;
  sql?: string;
  metadata?: Record<string, unknown>;
  meta?: Record<string, unknown>;
}

export interface IndexCreateRequest {
  type: "index" | "unique" | "fts";
  name?: string;
  columns: string[];
  meta?: Record<string, unknown>;
}

export interface ListMeta {
  total_count: number;
  filter_count: number;
  limit: number;
  offset: number;
}

export interface ListResponse<T> {
  data: T[];
  meta: ListMeta;
}

export interface QueryStatement {
  sql: string;
  params?: unknown[];
}

export interface QueryRequest {
  sql?: string;
  params?: unknown[];
  statements?: QueryStatement[];
}

export interface ChangelogEntry {
  id: number;
  timestamp: string;
  action: string;
  table: string;
  detail: Record<string, unknown>;
  meta?: Record<string, unknown>;
}

export interface SSEEvent {
  namespace?: string;
  table: string;
  action: string;
  source_table?: string;
  source_action?: string;
  row?: Record<string, unknown>;
  row_count?: number;
  row_ids?: Array<string | number>;
  detail?: Record<string, unknown>;
  meta?: Record<string, unknown>;
  timestamp: string;
}

export interface SSESubscribeOptions {
  tables?: string[];
  signal?: AbortSignal;
}

export interface SSESubscription {
  stream: AsyncIterable<SSEEvent>;
  response: Response;
  close: () => void;
}

export type QueryInput = URLSearchParams | Record<string, QueryValue | QueryValue[]>;
export type QueryValue = string | number | boolean | null | undefined;

export interface MutateOptions {
  prefer?: string;
  meta?: Record<string, unknown>;
}

export interface ImportFile {
  filename: string;
  content: Blob | ArrayBuffer | Uint8Array | string;
  contentType?: string;
}

export interface NamespaceRecord {
  name: string;
  created_at?: string;
  db_path?: string;
  config?: NamespaceConfig;
}

export interface NamespacesModule {
  create(request: NamespaceDefinition): Promise<RsqlResult<NamespaceRecord>>;
  list(): Promise<RsqlResult<NamespaceRecord[]>>;
  get(name: string): Promise<RsqlResult<NamespaceRecord>>;
  update(name: string, config: NamespaceConfig): Promise<RsqlResult<NamespaceRecord>>;
  delete(name: string): Promise<RsqlResult<void>>;
  duplicate(source: string, target: string): Promise<RsqlResult<{ source: string; target: string }>>;
  exportDb(name: string): Promise<RsqlResult<ArrayBuffer>>;
  importDb(name: string, file: ImportFile): Promise<RsqlResult<{ imported: boolean; type: string }>>;
  importCsv(name: string, table: string, file: ImportFile): Promise<RsqlResult<{ inserted: number }>>;
}

export interface TablesModule {
  list(): Promise<RsqlResult<Array<Record<string, unknown>>>>;
  create(request: TableCreateRequest): Promise<RsqlResult<{ created: string; type: string }>>;
  get(name: string): Promise<RsqlResult<Record<string, unknown>>>;
  update(name: string, request: TableUpdateRequest): Promise<RsqlResult<{ updated: boolean }>>;
  delete(name: string, meta?: Record<string, unknown>): Promise<RsqlResult<void>>;
}

export interface RowsModule<Row extends Record<string, unknown>> {
  list(query?: QueryInput): Promise<RsqlResult<ListResponse<Row> | { data: Row[] }>>;
  insert(
    rows: Partial<Row> | Array<Partial<Row>>,
    options?: MutateOptions,
  ): Promise<RsqlResult<{ data: Row[] } | { inserted: number }>>;
  get(id: number): Promise<RsqlResult<Row>>;
  update(id: number, payload: Record<string, unknown>, options?: MutateOptions): Promise<RsqlResult<{ data: Row[] } | { updated: number }>>;
  delete(id: number, options?: MutateOptions): Promise<RsqlResult<{ data: Row[] } | { deleted: number } | void>>;
  bulkUpdate(
    query: QueryInput,
    payload: Record<string, unknown>,
    options?: MutateOptions,
  ): Promise<RsqlResult<{ data: Row[] } | { updated: number }>>;
  bulkDelete(query: QueryInput, options?: MutateOptions): Promise<RsqlResult<{ data: Row[] } | { deleted: number } | void>>;
}

export interface TableModule<Row extends Record<string, unknown>> {
  rows: RowsModule<Row>;
  indexes: {
    create(request: IndexCreateRequest): Promise<RsqlResult<{ created: boolean }>>;
    delete(indexName: string, meta?: Record<string, unknown>): Promise<RsqlResult<void>>;
  };
  schema: {
    get(): Promise<RsqlResult<Record<string, unknown>>>;
    update(request: TableUpdateRequest): Promise<RsqlResult<{ updated: boolean }>>;
    delete(meta?: Record<string, unknown>): Promise<RsqlResult<void>>;
  };
}

export interface NamespaceModule {
  readonly name: string;
  tables: TablesModule;
  table<Row extends Record<string, unknown> = Record<string, unknown>>(name: string): TableModule<Row>;
  query: {
    run(request: QueryRequest): Promise<RsqlResult<Record<string, unknown>>>;
    batch(statements: QueryStatement[]): Promise<RsqlResult<Record<string, unknown>>>;
  };
  changelog: {
    list(options?: { table?: string; limit?: number; offset?: number }): Promise<RsqlResult<ChangelogEntry[]>>;
  };
  stats: {
    get(): Promise<RsqlResult<Record<string, unknown>>>;
  };
  events: {
    subscribe(options?: SSESubscribeOptions): Promise<RsqlResult<SSESubscription>>;
  };
}

export interface RsqlClient {
  namespaces: NamespacesModule;
  ns(name: string): NamespaceModule;
}
