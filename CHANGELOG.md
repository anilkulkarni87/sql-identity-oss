# Changelog

All notable changes to SQL Identity Resolution will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Quickstart Command**: `idr quickstart` for one-click demo setup and execution.
- **MCP Server**: Native Model Context Protocol support for AI agents (`idr mcp`).
- **Web UI**: React-based dashboard and configuration wizard (`idr serve`).
- **Pre-commit Hooks**: Automated linting and formatting on commit.
- **Project Structure**: Refactored into `idr_core`, `idr_api`, `idr_ui`, `idr_mcp`.

### Changed
- **Linting**: Replaced `flake8`/`black` with `ruff` for faster, unified linting.
- **Docker**: Improved Docker Compose setup with healthchecks and volume persistence.
- **Docs**: Comprehensive documentation overhaul to reflect new features.

## [0.5.1] - 2026-01-16

### Helper Features
- **User Documentation**: Added comprehensive guides for CLI, UI, and DBT modes in `docs/guides/`.
- **Initialization UX**: explicitly distinguished "Connection" vs "Initialization" in Setup Wizard.
- **Read-Only Mode**: UI now correctly handles permission warnings (e.g. read-only service accounts), preventing invalid save attempts.
- **Existing Connection Detection**: Setup Wizard now detects and allows re-initializing existing connections.

### Fixed
- **Snowflake Adapter**: Fixed parameter style issue (`pyformat` vs `qmark`) preventing connection in some environments.
- **BigQuery Schema**: Added adaptability for `updated_at` vs `updated_ts` column naming conventions.
- **Databricks Adapter**: Fixed SQL escaping for table checks and schema validation.
- **UI Logs**: Clarified in documentation that logs are CLI-based, effectively correcting misleading UI expectations.

## [0.5.0] - 2026-01-13

### Added
- **Multi-Platform Support**: Full validation for Databricks, Snowflake, BigQuery, and DuckDB.
- **YAML Configuration**: New `tools/generate_config.py` to generate SQL setup from a simple `config.yaml`.
- **Unified `idr_core`**: Consolidated Python logic handling dialect abstractions.
- **Databricks Support**: Added `MERGE INTO` syntax support and `TIMESTAMP` casting fixes.
- **Snowflake Support**: Fixed deployment scripts for read-only filesystem constraints.
- **dbt Package**: `dbt_idr` package verified for dbt Hub listing.
- **Fuzzy Matching**: Implemented fuzzy matching with Jaro-Winkler similarity across all platforms.
  - Dynamic pivoting for composite blocking keys (e.g., `SOUNDEX(last_name) + first_initial`).
  - Platform-specific UDF polyfills for consistent 0.0-1.0 scoring.
  - Configurable thresholds and blocking expressions via `idr_meta.fuzzy_rule`.
- **Dialect Configuration**: Added `int_type` to all platform dialect configs.

### Changed
- **Repository Structure**: Consolidated all DDL into `sql/ddl/` and deprecated per-platform `core` directories.
- **Documentation**: Major refactor of `README.md` and Guides to reflect new unified architecture.
- **Validation**: Added end-to-end benchmark results for 10M rows across all platforms.
- **BigQuery SQL Generation**: Fixed string escaping (using `\'` instead of `''`) for composite expressions.

### Fixed
- **BigQuery Fuzzy Matching Performance**: Fixed 25+ minute query times for fuzzy matching on BigQuery.
  - Root cause: BigQuery lacks native Jaro-Winkler and JavaScript UDFs have significant overhead.
  - Solution: Use native functions via `score_expr` in `fuzzy_rule` table (e.g., normalized Levenshtein).
  - All platforms now use `score_expr` from the table - fully configurable, no hardcoding.
  - Block size limits (max 500 per block) applied to all platforms to prevent O(n²) explosion.

### Removed
- Legacy platform-specific runner scripts (`sql/*/core/idr_run.py`) in favor of centralized `runners/`.

## [0.4.0] - 2025-12-01

### Added
- Initial implementation of `idr_core` package.
- Recursive CTE support for Snowflake and Databricks.
- Iterative label propagation for BigQuery.
- Base `IDRRunner` class with abstract methods.

### Changed
- Refactored monolithic SQL scripts into modular `sql/common` templates.

## [0.1.0] - 2024-01-01

### Added
- Initial release.
- DuckDB prototype.
