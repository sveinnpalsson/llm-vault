import { execFile } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const DEFAULT_TIMEOUT_SECONDS = 120;
const MAX_TIMEOUT_SECONDS = 300;
const PROCESS_TIMEOUT_GRACE_MS = 1_000;
const MAX_SAFE_TOP_K = 10;
const SOURCE_CHOICES = new Set(["all", "docs", "photos", "mail"]);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "..", "..");
const DEFAULT_VAULT_AGENT_PATH = "./vault-agent";
const PLUGIN_CONFIG_KEYS = new Set(["repoRoot", "vaultAgentPath", "timeoutSeconds"]);
const SAFE_SURFACE = Object.freeze([
  {
    name: "status",
    usage: "/vault status",
  },
  {
    name: "search",
    usage:
      "/vault search <query> [--source all|docs|photos|mail] [--top-k 1-10] [--from-date YYYY-MM-DD] [--to-date YYYY-MM-DD] [--taxonomy <value>] [--category-primary <value>]",
  },
  {
    name: "search-redacted",
    usage:
      "/vault search-redacted <query> [--source all|docs|photos|mail] [--top-k 1-10] [--from-date YYYY-MM-DD] [--to-date YYYY-MM-DD] [--taxonomy <value>] [--category-primary <value>]",
  },
]);
const SAFE_BOUNDARY_LINES = Object.freeze([
  "This plugin only exposes the agent-safe vault-agent surface.",
  "Raw vault-ops update/repair/full-clearance workflows remain operator-only.",
]);

function usage() {
  return [
    "Usage:",
    ...SAFE_SURFACE.map((command) => command.usage),
    "",
    ...SAFE_BOUNDARY_LINES,
  ].join("\n");
}

function normalize(text) {
  return String(text ?? "").trim();
}

function optionalConfigString(raw, keyName) {
  if (raw === null || raw === undefined) {
    return "";
  }
  if (typeof raw !== "string") {
    throw new Error(`${keyName} must be a string when provided.`);
  }
  const value = normalize(raw);
  if (!value) {
    throw new Error(`${keyName} must be a non-empty string when provided.`);
  }
  return value;
}

function ensureOptionValue(tokens, index, optionName) {
  const value = tokens[index + 1];
  if (!normalize(value)) {
    throw new Error(`${optionName} requires a value.`);
  }
  return value;
}

function parseIsoDate(raw, optionName) {
  const value = normalize(raw);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    throw new Error(`${optionName} must be YYYY-MM-DD.`);
  }
  const [year, month, day] = value.split("-").map((part) => Number.parseInt(part, 10));
  const parsed = new Date(Date.UTC(year, month - 1, day));
  if (
    parsed.getUTCFullYear() !== year
    || parsed.getUTCMonth() !== month - 1
    || parsed.getUTCDate() !== day
  ) {
    throw new Error(`${optionName} must be YYYY-MM-DD.`);
  }
  return value;
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
  let fromDate = null;
  let toDate = null;
  let taxonomy = null;
  let categoryPrimary = null;
  const queryTokens = [];

  for (let index = 0; index < tokens.length; index += 1) {
    const token = tokens[index];
    if (token === "--source") {
      const value = ensureOptionValue(tokens, index, "--source");
      if (!SOURCE_CHOICES.has(value)) {
        throw new Error("source must be one of all, docs, photos, or mail.");
      }
      source = value;
      index += 1;
      continue;
    }
    if (token === "--top-k") {
      const value = ensureOptionValue(tokens, index, "--top-k");
      topK = parsePositiveInt(value);
      index += 1;
      continue;
    }
    if (token === "--from-date") {
      fromDate = parseIsoDate(ensureOptionValue(tokens, index, "--from-date"), "--from-date");
      index += 1;
      continue;
    }
    if (token === "--to-date") {
      toDate = parseIsoDate(ensureOptionValue(tokens, index, "--to-date"), "--to-date");
      index += 1;
      continue;
    }
    if (token === "--taxonomy") {
      taxonomy = normalize(ensureOptionValue(tokens, index, "--taxonomy"));
      index += 1;
      continue;
    }
    if (token === "--category-primary") {
      categoryPrimary = normalize(ensureOptionValue(tokens, index, "--category-primary"));
      index += 1;
      continue;
    }
    if (token.startsWith("--")) {
      throw new Error(`Unsupported search option: ${token}`);
    }
    queryTokens.push(token);
  }

  const query = queryTokens.join(" ").trim();
  if (!query) {
    throw new Error("search requires a query.");
  }

  args.push("search-redacted", query, "--source", source, "--top-k", String(topK));
  if (fromDate) {
    args.push("--from-date", fromDate);
  }
  if (toDate) {
    args.push("--to-date", toDate);
  }
  if (taxonomy) {
    args.push("--taxonomy", taxonomy);
  }
  if (categoryPrimary) {
    args.push("--category-primary", categoryPrimary);
  }
  return args;
}

function parseTimeoutSeconds(raw) {
  const parsed = Number.parseInt(String(raw), 10);
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > MAX_TIMEOUT_SECONDS) {
    throw new Error(`timeoutSeconds must be an integer between 1 and ${MAX_TIMEOUT_SECONDS}.`);
  }
  return parsed;
}

function resolvePluginConfig(rawConfig = {}) {
  if (rawConfig === null || rawConfig === undefined) {
    rawConfig = {};
  }
  if (typeof rawConfig !== "object" || Array.isArray(rawConfig)) {
    throw new Error("Plugin config must be an object.");
  }

  for (const key of Object.keys(rawConfig)) {
    if (!PLUGIN_CONFIG_KEYS.has(key)) {
      throw new Error(`Unsupported plugin config key: ${key}`);
    }
  }

  const repoRootValue = optionalConfigString(rawConfig.repoRoot, "repoRoot");
  const repoRoot = repoRootValue ? path.resolve(REPO_ROOT, repoRootValue) : REPO_ROOT;
  const vaultAgentPathValue = optionalConfigString(rawConfig.vaultAgentPath, "vaultAgentPath") || DEFAULT_VAULT_AGENT_PATH;
  const vaultAgentPath = path.isAbsolute(vaultAgentPathValue)
    ? vaultAgentPathValue
    : path.resolve(repoRoot, vaultAgentPathValue);
  const timeoutSeconds = parseTimeoutSeconds(rawConfig.timeoutSeconds ?? DEFAULT_TIMEOUT_SECONDS);

  return {
    repoRoot,
    vaultAgentPath,
    timeoutSeconds,
  };
}

function buildVaultAgentInvocation(args, rawConfig) {
  const config = resolvePluginConfig(rawConfig);
  return {
    file: config.vaultAgentPath,
    args: ["--timeout-seconds", String(config.timeoutSeconds), ...args],
    cwd: config.repoRoot,
    timeoutMs: config.timeoutSeconds * 1000 + PROCESS_TIMEOUT_GRACE_MS,
  };
}

async function runVaultAgent(args, rawConfig) {
  const invocation = buildVaultAgentInvocation(args, rawConfig);
  const { stdout, stderr } = await execFileAsync(invocation.file, invocation.args, {
    cwd: invocation.cwd,
    timeout: invocation.timeoutMs,
    maxBuffer: 1024 * 1024,
    shell: false,
  });
  return [stdout?.trim(), stderr?.trim()].filter(Boolean).join("\n").trim() || "(no output)";
}

async function handleVaultCommand(rawArgs, rawConfig) {
  const tokens = tokenizeArgs(rawArgs);
  if (tokens.length === 0 || tokens[0] === "help") {
    return usage();
  }

  const [command, ...rest] = tokens;
  if (command === "status") {
    if (rest.length > 0) {
      throw new Error("status does not accept extra arguments.");
    }
    return runVaultAgent(["status"], rawConfig);
  }

  if (command === "search" || command === "search-redacted") {
    return runVaultAgent(parseSearchArgs(rest), rawConfig);
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
    properties: {
      repoRoot: {
        type: "string",
        minLength: 1,
        description: "Absolute path to the llm-vault checkout. Defaults to this plugin checkout root.",
      },
      vaultAgentPath: {
        type: "string",
        minLength: 1,
        description: "Path to vault-agent. Relative paths resolve from repoRoot and default to ./vault-agent.",
      },
      timeoutSeconds: {
        type: "integer",
        minimum: 1,
        maximum: MAX_TIMEOUT_SECONDS,
        default: DEFAULT_TIMEOUT_SECONDS,
        description: "Timeout passed to vault-agent and enforced by the plugin wrapper.",
      },
    },
  },
  register(api) {
    api.registerCommand({
      name: "vault",
      description: "Run safe llm-vault status and redacted search commands.",
      acceptsArgs: true,
      handler: async (ctx) => {
        try {
          return { text: await handleVaultCommand(ctx?.args, ctx?.config) };
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          return { text: `${message}\n\n${usage()}` };
        }
      },
    });
  },
};

export { buildVaultAgentInvocation, handleVaultCommand, parseSearchArgs, resolvePluginConfig, tokenizeArgs, usage };
export default plugin;
