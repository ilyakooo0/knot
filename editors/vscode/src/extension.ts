import * as path from "path";
import * as fs from "fs";
import {
  workspace,
  ExtensionContext,
  window,
} from "vscode";
import {
  LanguageClient,
  LanguageClientOptions,
  ServerOptions,
} from "vscode-languageclient/node";

let client: LanguageClient | undefined;

function findServer(context: ExtensionContext): string | undefined {
  // 1. Explicit setting
  const configPath = workspace
    .getConfiguration("knot")
    .get<string>("server.path");
  if (configPath && fs.existsSync(configPath)) {
    return configPath;
  }

  // 2. Workspace cargo target directories (debug then release)
  const workspaceFolders = workspace.workspaceFolders;
  if (workspaceFolders) {
    for (const folder of workspaceFolders) {
      for (const profile of ["debug", "release"]) {
        const candidate = path.join(
          folder.uri.fsPath,
          "target",
          profile,
          "knot-lsp"
        );
        if (fs.existsSync(candidate)) {
          return candidate;
        }
      }
    }
  }

  // 3. Next to the extension (for bundled distribution)
  const bundled = path.join(context.extensionPath, "knot-lsp");
  if (fs.existsSync(bundled)) {
    return bundled;
  }

  // 4. Fall back to PATH
  return "knot-lsp";
}

export function activate(context: ExtensionContext) {
  const serverPath = findServer(context);
  if (!serverPath) {
    window.showErrorMessage(
      "Could not find knot-lsp. Set knot.server.path or build with `cargo build -p knot-lsp`."
    );
    return;
  }

  const extraArgs = workspace
    .getConfiguration("knot")
    .get<string[]>("server.extraArgs") ?? [];

  const serverOptions: ServerOptions = {
    command: serverPath,
    args: extraArgs,
  };

  const clientOptions: LanguageClientOptions = {
    documentSelector: [{ scheme: "file", language: "knot" }],
    synchronize: {
      fileEvents: workspace.createFileSystemWatcher("**/*.knot"),
    },
  };

  client = new LanguageClient(
    "knot-lsp",
    "Knot Language Server",
    serverOptions,
    clientOptions
  );

  client.start();
}

export function deactivate(): Thenable<void> | undefined {
  return client?.stop();
}
