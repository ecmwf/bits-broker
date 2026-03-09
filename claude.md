* Read the design.md and design_config.yaml to understand the system we are building.
* Run tests with `cargo test` to ensure everything is working as expected.
* Be critical of suggested code changes. If you see something that doesn't make sense, question it and ask for clarification.
* Always try to flatten to remove Arcs and other wrappers when unnecessary


## Commenting standards
- Comments must add information not obvious from the code.
- Prefer *why* / *invariants* / *edge cases* over narrating *what* the code does.
- Keep comments short (1–3 lines). No verbose doc essays.
- Update/remove stale comments during refactors; never leave TODOs without context.