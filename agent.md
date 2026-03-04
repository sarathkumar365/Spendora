# Agent Rules for This Repository

These rules apply to any coding agent working in this repository.

## Quality Gates (Must Pass)
1. All module tests must pass before considering work complete.
2. All repository tests must pass before considering work complete.
3. Build must pass for changed components before considering work complete.
4. If any test or build fails, fix the issue (or clearly document blocker if external).

## Engineering Standards
1. Follow repository structure and conventions. Do not introduce ad-hoc layouts.
2. Follow language-specific best practices for Rust, TypeScript, and React.
3. Keep code DRY: avoid duplication; extract reusable logic where appropriate.
4. Apply SOLID principles where relevant (especially service boundaries and abstractions).
5. Prefer clear, maintainable code over clever shortcuts.
6. Add tests for new logic and regression tests for bug fixes.
7. Keep functions small, explicit, and single-purpose.
8. Use meaningful names and avoid ambiguous identifiers.
9. Handle errors explicitly and return actionable messages.
10. Keep public interfaces stable and versioned where applicable.

## Change Discipline
1. Make focused changes with minimal blast radius.
2. Do not break existing workflows, scripts, or contracts without explicit migration notes.
3. Update docs when behavior, startup flow, or developer workflow changes.
4. Validate end-to-end behavior for touched features, not just unit scope.

## Repository Expectations
1. Respect existing architecture decisions in `docs/desktop-rust-plan.md`.
2. Use `tests/step1` and root test scripts as baseline validation entry points.
3. Prefer incremental improvements that keep the project in a releasable state.
