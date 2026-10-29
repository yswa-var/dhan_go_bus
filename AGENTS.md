You are working on an existing Go + Dhan + Redpanda quantitative research project.

Rules:
1. Inspect the repository before making changes. Do not assume paths or existing abstractions.
2. Preserve existing Dhan authentication, contract resolution, Kafka publisher and Docker Compose.
3. Implement only the requested quest. Avoid speculative features.
4. Prefer maintained open-source libraries and standard Go packages. No custom message bus, Flink, Kubernetes or orchestration framework.
5. Keep components small, independently testable and configured through environment variables.
6. Use event timestamps for market calculations; define missing, late and duplicate-event behavior.
7. Use Kafka partition ordering deliberately. Do not claim global ordering or exactly-once execution without implementing and verifying it.
8. Never silently discard ticks, data errors, or failed writes.
9. Use bounded memory, context cancellation, graceful shutdown and structured logs.
10. Add deterministic tests and run go test ./... and go vet ./... after changes.
11. Summarize changed files, commands, test results, limitations and next manual verification step.
12. Do not implement subsequent quests until requested.
