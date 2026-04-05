import * as path from "path";
import * as fs from "fs";
import { execFileSync } from "child_process";
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
  if (configPath) {
    if (fs.existsSync(configPath)) {
      return configPath;
    }
    window.showWarningMessage(
      `knot.server.path "${configPath}" does not exist, searching elsewhere...`
    );
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

  // 4. Check if knot-lsp is on PATH
  try {
    const resolved = execFileSync("which", ["knot-lsp"], {
      encoding: "utf-8",
      timeout: 3000,
    }).trim();
    if (resolved) {
      return resolved;
    }
  } catch {
    // not on PATH
  }

  return undefined;
}

export function activate(context: ExtensionContext) {
  const outputChannel = window.createOutputChannel("Knot Language Server");

  const serverPath = findServer(context);
  if (!serverPath) {
    window.showErrorMessage(
      "Could not find knot-lsp. Build it with `cargo build -p knot-lsp` or set `knot.server.path`."
    );
    return;
  }

  outputChannel.appendLine(`Using knot-lsp: ${serverPath}`);

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
    outputChannel,
    traceOutputChannel: outputChannel,
  };

  client = new LanguageClient(
    "knot-lsp",
    "Knot Language Server",
    serverOptions,
    clientOptions
  );

  client.start().catch((err) => {
    window.showErrorMessage(
      `Failed to start knot-lsp: ${err.message}`
    );
    outputChannel.appendLine(`Error: ${err.message}`);
  });
}

export function deactivate(): Thenable<void> | undefined {
  return client?.stop();
}
