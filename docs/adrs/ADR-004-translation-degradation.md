# ADR-004: Keep the transcript when translation is unavailable

## Status

Accepted

## Context

The local translation model may be slow, stopped or temporarily unreachable. The transcript is independently useful and must not disappear because the last stage fails. Unlimited retries would occupy the queue and make recovery hard to observe.

This decision must be reopened if Chinese output becomes legally required for every completed job, or if measurements show normal translation regularly exceeds the 180-second timeout.

## Decision

Zhang, Zhexi will time out a translation attempt after 180 seconds, retry twice with 1- and 2-second backoff, and open a circuit for 30 seconds after three consecutive failures. Job Service will keep the English transcript, label the job `PARTLY_DONE`, and expose a manual translation retry.

## Consequences

- Users receive the completed transcript even during a model outage, meeting the graceful-degradation requirement.
- Three attempts bound queue occupancy per job, while the 30-second circuit prevents immediate repeated calls during an outage.
- A temporary outage longer than the retry window needs a user retry. This adds one API action but avoids a background retry loop the team would have to operate.
- A request thread may continue inside the HTTP library after the outer timeout; the one-message prefetch limits the Translation Service to one such request at a time, acting as a bulkhead on the laptop.
