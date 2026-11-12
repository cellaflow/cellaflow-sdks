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
export function apiKeyAndSource(apiKey?: string | null): [string | undefined, string] {
  // `null` reads the variable too, as `None` does in Python: a caller passing
  // `config.apiKey ?? null` means "no key of my own", not "no key at all".
  const [value, source] =
    apiKey != null
      ? [apiKey, "the apiKey option"]
      : [process.env[API_KEY_ENV_VAR], API_KEY_ENV_VAR];
  return [value?.trim() ? value : undefined, source];
}

/** The API key to send: `apiKey` if given, else `CELLAFLOW_API_KEY`. */
export function resolveApiKey(apiKey?: string | null): string | undefined {
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
 * Whether `target` names this machine: `localhost`, any of 127.0.0.0/8, `::1`,
 * or a 127 address mapped into IPv6 (`::ffff:127.0.0.1`), as the Python SDK
 * decides. Parsed once, exactly as the transport parses it, as the host of
 * `http://<target>`, so URL syntax such as `localhost:50051@evil.com` is judged
 * by the host the key would go to. The parser lowercases the host, writes
 * every IPv4 spelling as four decimals, and every IPv6 one in brackets the same
 * way (`[::1]`, `[::ffff:7fxx:xxxx]`). Node's HTTP/2 client uses no proxy, so
 * the host is where the connection goes.
 */
function isLocal(target: string): boolean {
  let host: string;
  try {
    host = new URL(`http://${target}`).hostname.replace(/\.+$/, "");
  } catch {
    return false; // the transport refuses it too
  }
  if (host === "localhost") return true;
  if (isIP(host) === 4) return host.startsWith("127.");
  return host === "[::1]" || /^\[::ffff:7f[0-9a-f]{2}:[0-9a-f]{1,4}\]$/.test(host);
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
        `you trust, set ${ALLOW_PLAINTEXT_KEY_ENV_VAR}=true (or, building a ` +
        "CellaflowClient yourself, pass allowPlaintextKey: true).",
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
 * The key to send, refusing one that cannot be a header: the one place both
 * rules are applied, for clients and wrappers alike.
 */
export function checkedApiKey(apiKey?: string | null): string | undefined {
  const [value, source] = apiKeyAndSource(apiKey);
  if (value !== undefined) checkApiKey(value, source);
  return value;
}

/**
 * Refuses an `apiKey` option that cannot be a header, for wrappers that build
 * their client later, so a bad key fails when the wrapper is made. Whether a
 * key may go without TLS depends on the environment when the client is built,
 * so that is checked then.
 */
export function checkApiKeyOption(options: { apiKey?: string | null }): void {
  if (options.apiKey != null) checkedApiKey(options.apiKey);
}
