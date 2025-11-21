# Changelog

All notable changes to the FEC Campaign Finance Analysis Tool will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.2.0] - 2025-01-27

### Added
- Implementation Status Matrix in README
- Roadmap section with planned features
- Makefile for unified development experience
- Backend test suite with health and candidate API tests
- Frontend test infrastructure with Vitest and React Testing Library
- GitHub Actions CI workflow for automated testing
- Unified logging utility with JSON formatting support
- Database schema documentation (backend/db/SCHEMA.md)
- Prometheus metrics endpoint (guarded by METRICS_ENABLED config)
- API type generation script for frontend
- CONTRIBUTING.md with development guidelines
- CHANGELOG.md for version tracking

### Changed
- Updated README with implementation status and roadmap
- Refactored logging calls in route files to use unified logging utility
- Enhanced test coverage for health endpoints

### Fixed
- Improved error handling in test setup

## [v0.1.0] - 2024-XX-XX

### Added
- Initial release
- Candidate search and analysis
- Contribution tracking and analysis
- Money flow visualization
- Fraud detection algorithms
- Bulk data import functionality
- Export capabilities (PDF, DOCX, CSV, Excel, Markdown)
- Committee management
- Independent expenditure tracking
- Saved searches
- Trend analysis
- Settings management

[Unreleased]: https://github.com/yourusername/FEC_Query/compare/v0.2.0...HEAD
[v0.2.0]: https://github.com/yourusername/FEC_Query/compare/v0.1.0...v0.2.0
[v0.1.0]: https://github.com/yourusername/FEC_Query/releases/tag/v0.1.0

