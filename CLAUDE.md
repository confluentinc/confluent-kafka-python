# Commentary & Documentation Style
- NEVER write obvious comments (e.g., do not explain what standard language syntax does).
- Avoid AI cliches and puffery (e.g., ban terms like "robust," "seamlessly," "pivotal," "serves to").
- Write comments from the perspective of a practical, slightly tired human developer.
- Focus exclusively on *why* a piece of code exists, not *what* it is doing (the code should speak for itself).
- Embrace normal human coding traits: note technical debt casually (e.g., `// TODO: clean this up when API v2 drops` or `// Hacky workaround for Safari bug`).
- Keep docstrings direct, plain-text, and concise. No verbose summaries.

# Semaphore CI/CD

See [.semaphore/MCP.md](.semaphore/MCP.md) for Semaphore tool usage.

Key rules:
- Cache org/project IDs in `.semaphore/config.json`
- Download test results once (signed URLs expire)
- Use `mode="summary"` to reduce response size
