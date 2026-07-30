# Contributing to AgenticMQ

Thank you for your interest in contributing to AgenticMQ! We welcome community contributions to help improve the message broker.

## How to Contribute

1. **Report Bugs & Suggest Features:** Please open a GitHub issue with a clear description and steps to reproduce.
2. **Propose Changes:** Fork the repository, create a descriptive branch, make your changes, and open a Pull Request.

## Running Tests

Before submitting a Pull Request, please ensure all tests compile and pass.

- **Rust Core Broker Tests:**
  Run the test suite using Cargo:
  ```bash
  cargo test --manifest-path agenticmq-core/Cargo.toml
  ```

- **Python SDK Tests:**
  Run the unittest suite from the project root:
  ```bash
  python3 -m unittest agenticmq-python/tests/test_sdk.py
  ```
