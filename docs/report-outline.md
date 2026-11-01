# Architecture report working outline (maximum 8 pages including diagrams)

1. Requirements and constraints (about 0.5 page)
2. Architecture and service boundaries, with C4 diagrams (about 1.5 pages)
3. Data ownership, messages and consistency (about 1 page)
4. Resilience and failure-injection evidence (about 1 page)
5. AI components and their architectural implications (about 1 page)
   - model/prompt versioning
   - non-determinism
   - token/compute cost control
   - prompt injection defence
6. Trade-off analyses using the Week 2/10 method (about 1 page)
7. Implementation, observability and test evidence (about 1 page)
8. Cost, operations, evaluation and limitations (about 1 page)

After the report, outside the 8-page limit, append in this order:

1. Five ADRs, one page each
2. AI disclosure appendix
3. Signed one-page individual contribution statement

Do not fill this report with generic descriptions. Add measured evidence: queue size under concurrent uploads, processing seconds per audio minute, retry timelines, recovery after worker kill, failure screenshots, and local hardware/model versions.
