# Architecture report working outline (target about 5,000 words)

1. Requirements and constraints (about 500)
2. Architecture and service boundaries, with C4 diagrams (about 800)
3. Data ownership, messages and consistency (about 650)
4. Resilience and failure-injection evidence (about 700)
5. AI components and their architectural implications (about 600)
   - model/prompt versioning
   - non-determinism
   - token/compute cost control
   - prompt injection defence
6. Trade-off analyses using the Week 2/10 method (about 700)
7. Implementation, observability and test evidence (about 550)
8. Cost and operations (about 250)
9. Evaluation and limitations (about 250)

Append outside the word count: five one-page ADRs, specific AI disclosure, and the signed one-page contribution statement.

Do not fill this report with generic descriptions. Add measured evidence: queue size under concurrent uploads, processing seconds per audio minute, retry timelines, recovery after worker kill, failure screenshots, and local hardware/model versions.
