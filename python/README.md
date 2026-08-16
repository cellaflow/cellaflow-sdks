# Cellaflow Python SDK

The official Python SDK for the Cellaflow Engine.
## Development Setup

To get up and running with the Python SDK for development, follow these steps:

1. **Create and activate a virtual environment**:
   ```bash
   cd python
   python3 -m venv venv
   source venv/bin/activate
   ```

2. **Install the SDK in editable mode with development dependencies**:
   ```bash
   pip install -e ".[dev]"
   ```

## Generating Protobufs and Typing Stubs (`.pyi`)

We use `grpcio-tools` and `mypy-protobuf` to generate Python code and typing stubs from the `.proto` files. This ensures your IDE (e.g. VS Code with Pylance or mypy) will properly resolve gRPC stubs and message attributes.

To regenerate the protobuf definitions and their corresponding `.pyi` typing stubs, run the generation script from the `python` directory:

```bash
./scripts/generate_protos.sh
```

This will automatically compile the protos from `../proto/cellaflow/v1/*.proto` and output both the Python code (`*_pb2.py`, `*_pb2_grpc.py`) and typing stubs (`*.pyi`) into `src/cellaflow/v1/`.

## Running Tests and Linters

You can validate your changes using the following tools:

- **Tests**: `pytest`
- **Formatting**: `black src tests`
- **Linting**: `flake8 src tests`
- **Type Checking**: `mypy src tests`
