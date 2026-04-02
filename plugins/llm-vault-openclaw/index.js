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
const PLUGIN_ID = "llm-vault";
const PLUGIN_NAME = "llm-vault";
const PLUGIN_DESCRIPTION = "Safe llm-vault OpenClaw plugin scaffold backed by vault-agent.";
const COMMAND_NAME = "vault";
const COMMAND_DESCRIPTION = "Run safe llm-vault status and redacted search commands.";
const TOOL_STATUS_NAME = "llm_vault_status";
const TOOL_SEARCH_NAME = "llm_vault_search";
const TOOL_STATUS_DESCRIPTION = "Return llm-vault agent-safe status from vault-agent.";
const TOOL_SEARCH_DESCRIPTION =
  "Run llm-vault redacted search through vault-agent with narrow safe filters.";
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
const STATUS_TOOL_PARAMETERS = Object.freeze({
  type: "object",
  additionalProperties: false,
  properties: {},
});
const SEARCH_TOOL_PARAMETERS = Object.freeze({
  type: "object",
  additionalProperties: false,
  required: ["query"],
  properties: {
    query: {
      type: "string",
      minLength: 1,
      description: "Redacted search query text.",
    },
    source: {
      type: "string",
      enum: ["all", "docs", "photos", "mail"],
      description: "Restrict results to one indexed source family.",
    },
    topK: {
      type: "integer",
      minimum: 1,
      maximum: MAX_SAFE_TOP_K,
      description: `Maximum result count from 1 to ${MAX_SAFE_TOP_K}.`,
    },
    fromDate: {
      type: "string",
      pattern: "^\\d{4}-\\d{2}-\\d{2}$",
      description: "Lower inclusive date bound in YYYY-MM-DD format.",
    },
    toDate: {
      type: "string",
      pattern: "^\\d{4}-\\d{2}-\\d{2}$",
      description: "Upper inclusive date bound in YYYY-MM-DD format.",
    },
    taxonomy: {
      type: "string",
      minLength: 1,
      description: "Optional taxonomy filter forwarded to vault-agent.",
    },
    categoryPrimary: {
      type: "string",
      minLength: 1,
      description: "Optional primary category filter forwarded to vault-agent.",
    },
  },
});
const CONFIG_SCHEMA = Object.freeze({
  type: "object",
  additionalProperties: false,
  properties: {
    repoRoot: {
      type: "string",
      minLength: 1,
      description:
        "Path to the llm-vault checkout. Defaults to the repo root that contains this plugin; relative paths resolve from that default root.",
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
      description: "Timeout enforced by the plugin wrapper around vault-agent execution.",
    },
  },
});

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

function isPlainObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
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

function parseSearchFilters(tokens) {
  const filters = {
    source: "all",
    topK: 5,
    fromDate: null,
    toDate: null,
    taxonomy: null,
    categoryPrimary: null,
  };
  const queryTokens = [];

  for (let index = 0; index < tokens.length; index += 1) {
    const token = tokens[index];
    if (token === "--source") {
      const value = ensureOptionValue(tokens, index, "--source");
      if (!SOURCE_CHOICES.has(value)) {
        throw new Error("source must be one of all, docs, photos, or mail.");
      }
      filters.source = value;
      index += 1;
      continue;
    }
    if (token === "--top-k") {
      const value = ensureOptionValue(tokens, index, "--top-k");
      filters.topK = parsePositiveInt(value);
      index += 1;
      continue;
    }
    if (token === "--from-date") {
      filters.fromDate = parseIsoDate(ensureOptionValue(tokens, index, "--from-date"), "--from-date");
      index += 1;
      continue;
    }
    if (token === "--to-date") {
      filters.toDate = parseIsoDate(ensureOptionValue(tokens, index, "--to-date"), "--to-date");
      index += 1;
      continue;
    }
    if (token === "--taxonomy") {
      filters.taxonomy = normalize(ensureOptionValue(tokens, index, "--taxonomy"));
      index += 1;
      continue;
    }
    if (token === "--category-primary") {
      filters.categoryPrimary = normalize(ensureOptionValue(tokens, index, "--category-primary"));
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

  return {
    query,
    ...filters,
  };
}

function buildSearchRedactedArgs(options) {
  const query = optionalConfigString(options?.query, "query");
  if (!query) {
    throw new Error("query must be a non-empty string.");
  }

  const source = normalize(options?.source || "all");
  if (!SOURCE_CHOICES.has(source)) {
    throw new Error("source must be one of all, docs, photos, or mail.");
  }

  const topK = parsePositiveInt(options?.topK ?? 5);
  const fromDate = options?.fromDate === null || options?.fromDate === undefined
    ? null
    : parseIsoDate(options.fromDate, "fromDate");
  const toDate = options?.toDate === null || options?.toDate === undefined
    ? null
    : parseIsoDate(options.toDate, "toDate");
  const taxonomy = options?.taxonomy === null || options?.taxonomy === undefined
    ? null
    : optionalConfigString(options.taxonomy, "taxonomy");
  const categoryPrimary = options?.categoryPrimary === null || options?.categoryPrimary === undefined
    ? null
    : optionalConfigString(options.categoryPrimary, "categoryPrimary");

  const args = ["search-redacted", query, "--source", source, "--top-k", String(topK)];
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

function parseSearchArgs(tokens) {
  return buildSearchRedactedArgs(parseSearchFilters(tokens));
}

function parseTimeoutSeconds(raw) {
  const parsed = Number.parseInt(String(raw), 10);
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > MAX_TIMEOUT_SECONDS) {
    throw new Error(`timeoutSeconds must be an integer between 1 and ${MAX_TIMEOUT_SECONDS}.`);
  }
  return parsed;
}

function hasPluginConfigKeys(value) {
  return isPlainObject(value) && Object.keys(value).some((key) => PLUGIN_CONFIG_KEYS.has(key));
}

function extractPluginConfigValues(
  rawConfig,
  { isRoot = true, insideConfigWrapper = false, isStandaloneConfigWrapper = false } = {},
) {
  if (!isPlainObject(rawConfig)) {
    return {
      foundConfigKeys: false,
      foundNestedObject: false,
      values: {},
    };
  }

  const values = {};
  let foundConfigKeys = false;
  let foundNestedObject = false;
  const unknownScalarKeys = [];
  const objectKeys = Object.keys(rawConfig);
  const hasDirectConfigKeys = hasPluginConfigKeys(rawConfig);
  const hasObjectChildren = objectKeys.some((key) => isPlainObject(rawConfig[key]));
  const validateUnknownKeys =
    insideConfigWrapper || (isRoot && hasDirectConfigKeys && !hasObjectChildren);

  for (const key of objectKeys) {
    const value = rawConfig[key];

    if (PLUGIN_CONFIG_KEYS.has(key)) {
      values[key] = value;
      foundConfigKeys = true;
      continue;
    }

    if (key === "config" && value !== null && value !== undefined && !isPlainObject(value)) {
      throw new Error("Plugin config wrapper key `config` must be an object when provided.");
    }

    if (validateUnknownKeys) {
      unknownScalarKeys.push(key);
      continue;
    }

    if (isPlainObject(value)) {
      foundNestedObject = true;
      const nested = extractPluginConfigValues(value, {
        isRoot: false,
        insideConfigWrapper: key === "config",
        isStandaloneConfigWrapper: isRoot && key === "config" && objectKeys.length === 1,
      });
      foundConfigKeys = foundConfigKeys || nested.foundConfigKeys;
      foundNestedObject = foundNestedObject || nested.foundNestedObject;
      Object.assign(values, nested.values);
      continue;
    }
  }

  if (unknownScalarKeys.length > 0) {
    throw new Error(`Unsupported plugin config key: ${unknownScalarKeys[0]}`);
  }

  if (
    (isRoot || isStandaloneConfigWrapper)
    && !foundConfigKeys
    && !foundNestedObject
    && objectKeys.length > 0
  ) {
    throw new Error(`Unsupported plugin config key: ${objectKeys[0]}`);
  }

  return {
    foundConfigKeys,
    foundNestedObject,
    values,
  };
}

function resolvePluginConfig(rawConfig = {}, baseConfig = null) {
  if (rawConfig === null || rawConfig === undefined) {
    rawConfig = {};
  }
  if (!isPlainObject(rawConfig)) {
    throw new Error("Plugin config must be an object.");
  }

  const extractedConfig = extractPluginConfigValues(rawConfig).values;
  const base = baseConfig && isPlainObject(baseConfig)
    ? {
      repoRoot: optionalConfigString(baseConfig.repoRoot, "repoRoot") || REPO_ROOT,
      vaultAgentPath:
          optionalConfigString(baseConfig.vaultAgentPath, "vaultAgentPath")
          || path.resolve(
            optionalConfigString(baseConfig.repoRoot, "repoRoot") || REPO_ROOT,
            DEFAULT_VAULT_AGENT_PATH,
          ),
      timeoutSeconds: parseTimeoutSeconds(baseConfig.timeoutSeconds ?? DEFAULT_TIMEOUT_SECONDS),
    }
    : {
      repoRoot: REPO_ROOT,
      vaultAgentPath: path.resolve(REPO_ROOT, DEFAULT_VAULT_AGENT_PATH),
      timeoutSeconds: DEFAULT_TIMEOUT_SECONDS,
    };

  const repoRootValue = optionalConfigString(extractedConfig.repoRoot, "repoRoot");
  const repoRoot = repoRootValue ? path.resolve(REPO_ROOT, repoRootValue) : base.repoRoot;
  const vaultAgentPathValue =
    optionalConfigString(extractedConfig.vaultAgentPath, "vaultAgentPath");
  const vaultAgentPath = path.isAbsolute(vaultAgentPathValue)
    ? vaultAgentPathValue
    : vaultAgentPathValue
      ? path.resolve(repoRoot, vaultAgentPathValue)
      : base.vaultAgentPath;
  const timeoutSeconds = parseTimeoutSeconds(extractedConfig.timeoutSeconds ?? base.timeoutSeconds);

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
    args: [...args],
    cwd: config.repoRoot,
    timeoutMs: config.timeoutSeconds * 1000 + PROCESS_TIMEOUT_GRACE_MS,
  };
}

async function runVaultAgent(args, rawConfig) {
  const invocation = buildVaultAgentInvocation(args, rawConfig);
  const { stdout, stderr } = await execFileAsync(invocation.file, invocation.args, {
    cwd: invocation.cwd,
    encoding: "utf8",
    timeout: invocation.timeoutMs,
    maxBuffer: 1024 * 1024,
    shell: false,
  });
  return [stdout?.trim(), stderr?.trim()].filter(Boolean).join("\n").trim() || "(no output)";
}

function formatToolResult(text, details = {}) {
  return {
    content: [
      {
        type: "text",
        text,
      },
    ],
    details,
  };
}

async function runStatus(rawConfig) {
  return runVaultAgent(["status"], rawConfig);
}

async function runSearchRedacted(options, rawConfig) {
  return runVaultAgent(buildSearchRedactedArgs(options), rawConfig);
}

function createStatusTool(rawConfig) {
  return {
    name: TOOL_STATUS_NAME,
    label: "Vault Status",
    description: TOOL_STATUS_DESCRIPTION,
    parameters: STATUS_TOOL_PARAMETERS,
    async execute() {
      const text = await runStatus(rawConfig);
      return formatToolResult(text, { backendCommand: "status" });
    },
  };
}

function createSearchTool(rawConfig) {
  return {
    name: TOOL_SEARCH_NAME,
    label: "Vault Search",
    description: TOOL_SEARCH_DESCRIPTION,
    parameters: SEARCH_TOOL_PARAMETERS,
    async execute(_toolCallId, params) {
      const text = await runSearchRedacted(params, rawConfig);
      return formatToolResult(text, {
        backendCommand: "search-redacted",
        forwarded: buildSearchRedactedArgs(params),
      });
    },
  };
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
    return runStatus(rawConfig);
  }

  if (command === "search" || command === "search-redacted") {
    return runVaultAgent(parseSearchArgs(rest), rawConfig);
  }

  throw new Error(`Unsupported vault command: ${command}`);
}

const plugin = {
  id: PLUGIN_ID,
  name: PLUGIN_NAME,
  description: PLUGIN_DESCRIPTION,
  configSchema: CONFIG_SCHEMA,
  register(api) {
    const pluginConfig = resolvePluginConfig(api.pluginConfig);

    api.registerCommand({
      name: COMMAND_NAME,
      description: COMMAND_DESCRIPTION,
      acceptsArgs: true,
      handler: async (ctx) => {
        try {
          const runtimeConfig = resolvePluginConfig(ctx?.config ?? {}, pluginConfig);
          return { text: await handleVaultCommand(ctx?.args, runtimeConfig) };
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          return { text: `${message}\n\n${usage()}` };
        }
      },
    });

    api.registerTool(createStatusTool(pluginConfig), { name: TOOL_STATUS_NAME });
    api.registerTool(createSearchTool(pluginConfig), { name: TOOL_SEARCH_NAME });
  },
};

export {
  COMMAND_DESCRIPTION,
  COMMAND_NAME,
  CONFIG_SCHEMA,
  PLUGIN_DESCRIPTION,
  PLUGIN_ID,
  PLUGIN_NAME,
  SAFE_BOUNDARY_LINES,
  SAFE_SURFACE,
  SEARCH_TOOL_PARAMETERS,
  STATUS_TOOL_PARAMETERS,
  TOOL_SEARCH_DESCRIPTION,
  TOOL_SEARCH_NAME,
  TOOL_STATUS_DESCRIPTION,
  TOOL_STATUS_NAME,
  buildVaultAgentInvocation,
  buildSearchRedactedArgs,
  createSearchTool,
  createStatusTool,
  extractPluginConfigValues,
  formatToolResult,
  handleVaultCommand,
  hasPluginConfigKeys,
  parseSearchArgs,
  parseSearchFilters,
  runSearchRedacted,
  runStatus,
  resolvePluginConfig,
  tokenizeArgs,
  usage,
};
export default plugin;
