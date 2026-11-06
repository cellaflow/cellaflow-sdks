// Where the API key comes from, what may be sent, and where. Not re-exported by
// the package index: `client.ts` re-exports the public names, and the checks
// are shared with `taskLease`, which runs them when it is created.
import { isIP } from "node:net";

/**
 * Where an API key is read from when none is passed, and the only variable read:
 * the same one the engine reads its token from. The engine also accepts older
 * names for its token (`CELLAFLOW_AUTH_TOKEN`, and the generic `AUTH_TOKEN` and
 * `API_KEY`); the SDK reads none of them, because a variable as generic as
 * `API_KEY` often holds another service's secret, which would then be sent to
 * the engine.
 */
export const API_KEY_ENV_VAR = "CELLAFLOW_API_KEY";

/**
 * Set to `true` (or `1`, `yes`) to allow sending a key without TLS to another
 * host, on a network you trust. Without it, that is refused, as the engine
 * refuses to serve without a token unless told to.
 */
export const ALLOW_PLAINTEXT_KEY_ENV_VAR = "CELLAFLOW_ALLOW_PLAINTEXT_KEY";

/**
 * The key to send and where it came from: `apiKey` if given, else
 * `CELLAFLOW_API_KEY`. A blank value means no key.
 */
export function apiKeyAndSource(apiKey?: string): [string | undefined, string] {
  const [value, source] =
    apiKey !== undefined
      ? [apiKey, "the apiKey option"]
      : [process.env[API_KEY_ENV_VAR], API_KEY_ENV_VAR];
  return [value?.trim() ? value : undefined, source];
}

/** The API key to send: `apiKey` if given, else `CELLAFLOW_API_KEY`. */
export function resolveApiKey(apiKey?: string): string | undefined {
  return apiKeyAndSource(apiKey)[0];
}

/**
 * Refuses a key that cannot be sent as a header, naming where it came from but
 * never what it is. The same rule as the Python SDK's `_check_api_key`.
 */
export function checkApiKey(apiKey: string, source: string): void {
  if (apiKey !== apiKey.trim() || !/^[\x20-\x7e]*$/.test(apiKey)) {
    throw new Error(
      `The Cellaflow API key from ${source} cannot be sent in a header: it has ` +
        "surrounding whitespace or a character outside printable ASCII (a " +
        "trailing newline from a secret file, say).",
    );
  }
}

/**
 * Whether `host` is this machine: `localhost`, any of 127.0.0.0/8, `::1`, or a
 * 127 address mapped into IPv6 (`::ffff:127.0.0.1`), as the Python SDK decides.
 */
function hostIsLocal(host: string): boolean {
  const name = host.toLowerCase().replace(/\.+$/, "");
  if (name === "localhost") return true;
  if (isIP(name) === 4) return name.startsWith("127.");
  if (isIP(name) !== 6) return false;
  // The URL parser writes every spelling of an IPv6 address one way: `[::1]`,
  // and `[::ffff:7fxx:xxxx]` for a mapped 127 address.
  const normalised = new URL(`http://[${name}]`).hostname;
  return normalised === "[::1]" || /^\[::ffff:7f[0-9a-f]{2}:[0-9a-f]{1,4}\]$/.test(normalised);
}

/**
 * Whether `target` names this machine. Parsed exactly as the transport parses
 * it, as the host of `http://<target>`, so URL syntax such as
 * `localhost:50051@evil.com` is judged by the host the key would go to.
 */
function isLocal(target: string): boolean {
  let host: string;
  try {
    host = new URL(`http://${target}`).hostname;
  } catch {
    return false; // the transport refuses it too
  }
  return hostIsLocal(host.startsWith("[") ? host.slice(1, -1) : host);
}

function plaintextKeyAllowed(allowPlaintextKey?: boolean): boolean {
  if (allowPlaintextKey !== undefined) return allowPlaintextKey;
  const value = (process.env[ALLOW_PLAINTEXT_KEY_ENV_VAR] ?? "").trim().toLowerCase();
  return value === "1" || value === "true" || value === "yes";
}

const warnedPlaintext = new Set<string>();

/**
 * Refuses to send a key without TLS to anything but this machine, unless
 * allowed, so forgetting TLS cannot be how a key crosses the network. When
 * allowed, warns once per target.
 */
export function checkPlaintext(target: string, allowPlaintextKey?: boolean): void {
  if (isLocal(target)) return;
  if (!plaintextKeyAllowed(allowPlaintextKey)) {
    throw new Error(
      `Refusing to send the Cellaflow API key to ${target} without TLS, where ` +
        "anyone on the path could read it. Pass secure: true, or, on a network " +
        `you trust, set ${ALLOW_PLAINTEXT_KEY_ENV_VAR}=true or pass ` +
        "allowPlaintextKey: true to CellaflowClient.",
    );
  }
  if (warnedPlaintext.has(target)) return;
  warnedPlaintext.add(target);
  console.warn(
    `Cellaflow: sending an API key to ${target} without TLS, as allowed, so anyone ` +
      "on the path can read it.",
  );
}

/**
 * Refuses an `apiKey` option that a client would refuse, for wrappers that
 * build their client later, so a bad key fails when the wrapper is made.
 */
export function checkApiKeyOption(options: {
  apiKey?: string;
  target?: string;
  secure?: boolean;
}): void {
  if (options.apiKey === undefined) return;
  const [apiKey, source] = apiKeyAndSource(options.apiKey);
  if (apiKey === undefined) return;
  checkApiKey(apiKey, source);
  if (!options.secure) checkPlaintext(options.target ?? "localhost:50051");
}
