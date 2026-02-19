# Contributing to SQL Identity Resolution

Thank you for considering contributing to SQL Identity Resolution! This guide will help you get started.

## Getting Started

### 1. Fork and Clone

```bash
git clone https://github.com/YOUR_USERNAME/sql-identity-resolution.git
cd sql-identity-resolution
```

### 2. Set Up Development Environment

```bash
# Install as editable with dev dependencies
pip install -e ".[dev,duckdb,mcp,api]"

# Install pre-commit hooks
pre-commit install

# Verify setup
idr quickstart
```

### 3. Create a Branch

```bash
git checkout -b feature/your-feature-name
```

## Development Workflow

### Running Tests

```bash
# Run all tests
make test

# Or directly with pytest
python -m pytest tests/ --ignore=tests/legacy -v
```

### Code Style

We use **Ruff** for linting and formatting.

```bash
# Run checks
ruff check .
ruff format .

# Or run via pre-commit (recommended)
pre-commit run --all-files
```

### Documentation

- Update docs if you change behavior
- Add docstrings to new functions
- Update README if adding features

```bash
# Preview docs locally
make docs
```

## Types of Contributions

### 🐛 Bug Reports

1. Check existing issues first
2. Include reproduction steps
3. Include platform/version info
4. Attach relevant logs

### ✨ Feature Requests

1. Describe the use case
2. Explain expected behavior
3. Consider backward compatibility

### 🔧 Pull Requests

1. Reference related issues
2. Include tests for new features
3. Update documentation
4. Keep changes focused

## Pull Request Process

1. **Create PR** with clear description
2. **Pass CI** - all tests must pass
3. **Review** - address feedback
4. **Merge** - maintainer will merge

### PR Checklist

- [ ] Tests pass locally (`make test`)
- [ ] Lint passes (`pre-commit run --all-files`)
- [ ] Documentation updated (if needed)
- [ ] Commit messages are clear
- [ ] PR description explains changes

## Code Structure

```
sql-identity-resolution/
├── idr_core/         # Core Python package (Resolution Engine)
│   ├── adapters/     # Database adapters (DuckDB, Snowflake, BigQuery, Databricks)
│   ├── stages/       # Pipeline stage classes
│   └── runner.py     # IDRRunner orchestrator
├── idr_api/          # FastAPI backend & Dashboard API
├── idr_ui/           # React Frontend (Dashboard & Wizard)
├── idr_mcp/          # Model Context Protocol (AI Agent Server)
├── tests/            # pytest test suite
├── tools/            # Utilities (benchmarks, deployment)
├── docs/             # MkDocs documentation
└── examples/         # Example templates and data
```

## Platform-Specific Guidelines

### Adding a New Platform

1. Create a new adapter in `idr_core/adapters/<platform>.py`
2. Register dialect config in `idr_core/adapters/base.py`
3. Add tests
4. Add documentation

### Modifying Core Logic

When changing the matching/clustering algorithm:
1. Update all platform implementations
2. Ensure determinism is maintained
3. Add tests for edge cases
4. Document the change

## Questions?

- Open a [GitHub Issue](https://github.com/anilkulkarni87/sql-identity-resolution/issues)
- Check existing [documentation](https://anilkulkarni87.github.io/sql-identity-resolution/)

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.
