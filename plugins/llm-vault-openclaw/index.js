import { execFile } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const DEFAULT_TIMEOUT_MS = 120_000;
const MAX_SAFE_TOP_K = 10;
const SOURCE_CHOICES = new Set(["all", "docs", "photos", "mail"]);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "..", "..");
const VAULT_AGENT = path.join(REPO_ROOT, "vault-agent");

function usage() {
  return [
    "Usage:",
    "/vault status",
    "/vault search <query> [--source all|docs|photos|mail] [--top-k 1-10]",
    "/vault search-redacted <query> [--source all|docs|photos|mail] [--top-k 1-10]",
    "",
    "This plugin only exposes the agent-safe vault-agent surface.",
    "Raw vault-ops update/repair/full-clearance workflows remain operator-only.",
  ].join("\n");
}

function normalize(text) {
  return String(text ?? "").trim();
}

function tokenizeArgs(raw) {
  const text = normalize(raw);
  if (!text) {
    return [];
  }

  const tokens = [];
  let current = "";
  let quote = null;
  let escaped = false;

  for (const char of text) {
    if (escaped) {
      current += char;
      escaped = false;
      continue;
    }
    if (char === "\\") {
      escaped = true;
      continue;
    }
    if (quote) {
      if (char === quote) {
        quote = null;
      } else {
        current += char;
      }
      continue;
    }
    if (char === "'" || char === '"') {
      quote = char;
      continue;
    }
    if (/\s/.test(char)) {
      if (current) {
        tokens.push(current);
        current = "";
      }
      continue;
    }
    current += char;
  }

  if (escaped || quote) {
    throw new Error("Unterminated quoted argument.");
  }
  if (current) {
    tokens.push(current);
  }
  return tokens;
}

function parsePositiveInt(raw) {
  const parsed = Number.parseInt(String(raw), 10);
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > MAX_SAFE_TOP_K) {
    throw new Error(`top-k must be an integer between 1 and ${MAX_SAFE_TOP_K}.`);
  }
  return parsed;
}

function parseSearchArgs(tokens) {
  const args = [];
  let source = "all";
  let topK = 5;
  const queryTokens = [];

  for (let index = 0; index < tokens.length; index += 1) {
    const token = tokens[index];
    if (token === "--source") {
      const value = tokens[index + 1];
      if (!SOURCE_CHOICES.has(value)) {
        throw new Error("source must be one of all, docs, photos, or mail.");
      }
      source = value;
      index += 1;
      continue;
    }
    if (token === "--top-k") {
      const value = tokens[index + 1];
      topK = parsePositiveInt(value);
      index += 1;
      continue;
    }
    queryTokens.push(token);
  }

  const query = queryTokens.join(" ").trim();
  if (!query) {
    throw new Error("search requires a query.");
  }

  args.push("search-redacted", query, "--source", source, "--top-k", String(topK));
  return args;
}

async function runVaultAgent(args) {
  const { stdout, stderr } = await execFileAsync(VAULT_AGENT, args, {
    cwd: REPO_ROOT,
    timeout: DEFAULT_TIMEOUT_MS,
    maxBuffer: 1024 * 1024,
    shell: false,
  });
  return [stdout?.trim(), stderr?.trim()].filter(Boolean).join("\n").trim() || "(no output)";
}

async function handleVaultCommand(rawArgs) {
  const tokens = tokenizeArgs(rawArgs);
  if (tokens.length === 0 || tokens[0] === "help") {
    return usage();
  }

  const [command, ...rest] = tokens;
  if (command === "status") {
    if (rest.length > 0) {
      throw new Error("status does not accept extra arguments.");
    }
    return runVaultAgent(["status"]);
  }

  if (command === "search" || command === "search-redacted") {
    return runVaultAgent(parseSearchArgs(rest));
  }

  throw new Error(`Unsupported vault command: ${command}`);
}

const plugin = {
  id: "llm-vault",
  name: "llm-vault",
  description: "Safe llm-vault OpenClaw plugin scaffold backed by vault-agent.",
  configSchema: {
    type: "object",
    additionalProperties: false,
    properties: {},
  },
  register(api) {
    api.registerCommand({
      name: "vault",
      description: "Run safe llm-vault status and redacted search commands.",
      acceptsArgs: true,
      handler: async (ctx) => {
        try {
          return { text: await handleVaultCommand(ctx?.args) };
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          return { text: `${message}\n\n${usage()}` };
        }
      },
    });
  },
};

export { handleVaultCommand, parseSearchArgs, tokenizeArgs, usage };
export default plugin;
