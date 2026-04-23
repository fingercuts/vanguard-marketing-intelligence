# Changelog

All notable changes to the Vanguard Marketing Intelligence Platform will be documented in this file.

## [1.1.0] - 2024-04-21
### Added
- Enterprise-grade GOVERNANCE.md detailing data quality and PII standards.
- Dark-themed premium CSS overrides for the executive dashboard.
- Advanced anomaly detection for spent-revenue outliers.

### Changed
- Rebranded platform to Vanguard Marketing Intelligence.
- Optimized silver-layer SQL transformations with business-aligned CTEs.
- Standardized color palette for cross-channel reporting (Navy/Maroon).

### Fixed
- Handling of scientific notation for low-revenue TikTok API extracts.
- Resolved race conditions in DuckDB connection timeouts during backfill operations.

## [1.0.0] - 2024-04-12
### Added
- Initial Medallion Architecture implementation (Bronze, Silver, Gold).
- Vectorized data simulator for multi-channel performance tracking.
- Automated data quality framework with Z-score outlier detection.
- Interactive Streamlit executive overview.

## [0.5.0] - 2024-03-25
### Added
- Bronze and Silver layer transformation modules.
- Basic DuckDB schema definitions.
- Initial test suite for pipeline validation.

## [0.1.0] - 2024-03-01
### Added
- Project initialization and infrastructure scoping.
- Source data extraction proofs of concept.
