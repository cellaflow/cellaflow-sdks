# CellaFlow SDKs

Official client SDKs and protocol definitions for the [CellaFlow Engine](https://www.cellaflow.com) — a high-performance, deterministic execution runtime and cognitive state machine for autonomous AI agents and swarms.

[![CI](https://img.shields.io/badge/CI-passing-brightgreen.svg)](https://github.com/cellaflow/cellaflow-sdks/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/badge/coverage-98%25-brightgreen.svg)](https://github.com/cellaflow/cellaflow-sdks/actions/workflows/ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/cellaflow.svg?color=blue)](https://pypi.org/project/cellaflow/)
[![License](https://img.shields.io/badge/license-Apache%202.0%20%2F%20MIT-blue.svg)](https://github.com/cellaflow/cellaflow-sdks/blob/main/LICENSE)

---

## 📦 Available SDKs

| Language | Package | Status | Documentation |
| :--- | :--- | :--- | :--- |
| **Python** | [`cellaflow`](https://pypi.org/project/cellaflow/) | ✅ **v0.1.0** (Released) | [Python SDK Guide](python/README.md) |
| **TypeScript** | `@cellaflow/sdk` | 🚧 *In Development* | Coming Soon |

---

## 🏗️ Repository Structure

```
cellaflow-sdks/
├── proto/              # Shared Protocol Buffer definitions (v1)
│   └── cellaflow/v1/   # Core gRPC services (WorkflowEngineService, Idempotency, etc.)
├── python/             # Official Python SDK (pip install cellaflow)
│   ├── src/cellaflow/  # Decorators, contextvars isolation, gRPC client, MessagePack
│   ├── tests/          # Comprehensive test suite (pytest)
│   └── README.md       # Python SDK documentation & quickstarts
├── LICENSE             # Project license (Apache 2.0 or MIT)
└── buf.gen.yaml        # Buf code generation configuration
```

---

## 🚀 Getting Started

To get started with the Python SDK:

```bash
pip install cellaflow
```

Check out the full [Python SDK Quickstart & Architecture Guide](python/README.md) to learn about:
- Zero-friction `@workflow`, `@step`, and `@tool` decorators.
- Transparent replay recovery from engine crashes.
- Cross-agent idempotency key derivation with RFC 8785 Canonical JSON and SHA-256.
- Background lease management with fencing tokens.

---

## 📜 License

This repository is licensed under the Apache 2.0 or MIT License. See [LICENSE](LICENSE) for details.
