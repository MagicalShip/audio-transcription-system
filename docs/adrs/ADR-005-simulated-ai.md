# ADR-005: Use simulated AI by default with optional local model adapters

## Status

Accepted

## Context

The examiners must start the system with one command and without paid API credits. Real Whisper and translation models are large, download slowly, and may run slower than the available laptop CPU. The group still needs to evaluate actual transcription and translation before claiming model quality.

This decision must be reopened if the assessment environment guarantees downloaded models and sufficient CPU/RAM, or if the demo is required to judge output accuracy rather than the distributed architecture.

## Decision

Cao, Mingshu will make deterministic simulated providers the default Compose configuration. The same service interfaces also provide optional local `faster-whisper` and Ollama adapters, selected by environment variables without changing events or service boundaries.

## Consequences

- One-command startup stays offline after container images are available and uses zero paid credits, directly satisfying two baseline constraints.
- Failure, ordering, idempotency and metrics tests are deterministic and repeatable on ordinary laptops.
- Simulated output proves no AI accuracy. The group must run a small public or synthetic dataset through local models and clearly separate architecture evidence from quality evidence in the report.
- Supporting two providers adds about one adapter class per AI service, but avoids duplicating the distributed pipeline.
