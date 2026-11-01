# ADR-002: Return a marked partial transcript after a permanent segment failure

## Status

Accepted

## Context

A 30-minute file creates about 60 segments. One segment can still fail after three attempts while the other 59 are useful. Users must not mistake missing speech for a complete result, and retrying forever would prevent a job from reaching a terminal state.

This decision must be reopened if user evaluation shows that one missing passage makes the entire result unsafe or useless, or if the required success threshold becomes 100% of segments.

## Decision

Cao, Mingshu will make Job Service store one outcome per segment, order outcomes by segment number, and insert `[TRANSCRIPTION FAILED start-end]` for a permanent failure. If at least one segment succeeds, processing continues and the final state is `PARTLY_DONE`; if all fail, it is `FAILED`.

## Consequences

- A single failure no longer discards up to 29.5 minutes of valid work, supporting the useful-result constraint.
- Missing material is explicitly timestamped, satisfying the requirement that degraded output be labelled.
- Translation may paraphrase a marker when using a real model. The prompt asks it to translate only content, so this must be included in acceptance testing before the demo.
- Job Service must wait for one final outcome per segment. With a maximum of 60 segments and bounded retries, memory and database growth remain bounded.
