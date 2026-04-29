import { createHttpClient } from "./http";
import { createNamespaceModule } from "./modules/namespace";
import { createNamespacesModule } from "./modules/namespaces";
import type { CreateRsqlClientOptions, RsqlClient } from "./types";

export const createRsqlClient = (options: CreateRsqlClientOptions): RsqlClient => {
  const http = createHttpClient(options);

  return {
    namespaces: createNamespacesModule(http),
    ns(name: string) {
      return createNamespaceModule(http, name);
    },
  };
};
