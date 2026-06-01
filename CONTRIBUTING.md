# Contributing to Healthy Stellar Backend

## Security CI Requirements

This repository enforces security checks in CI for every pull request and push to `main`/`develop`.

The security workflow includes:

- `npm audit --audit-level=high` to fail on high or critical npm vulnerabilities
- `Trivy` filesystem scanning for dependency/OS package vulnerabilities
- `Semgrep` SAST scanning with `p/security-audit`
- `Gitleaks` secret detection against repository history

If you add new package dependencies or update CI workflows, ensure these scans still run successfully.

## Pagination and API safety

All list-style endpoints should use the shared `PaginationDto` and enforce a maximum `pageSize` of `100` with a default of `20` when query parameters are omitted.

Requests that omit pagination must not return unbounded result sets.
