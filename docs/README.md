# DeltaZulu.LocalStream — documentation

Estate-wide architectural policy, Constraints, the current target and planning are maintained in [DeltaZulu-OU/docs](https://github.com/DeltaZulu-OU/docs). Local documents describe source, interfaces and historical reasoning; they do not override Accepted Decisions.

| Looking for | Go to |
|---|---|
| Current target | [NRT pipeline](https://github.com/DeltaZulu-OU/docs/blob/master/architecture/NRT-PIPELINE.md) |
| Governing Decisions | [Decision index](https://github.com/DeltaZulu-OU/docs/blob/master/architecture/GOVERNING-DECISIONS.md) |
| Constraints and qualifications | [Constraints](https://github.com/DeltaZulu-OU/docs/tree/master/constraints) |
| Active work | [Consolidated roadmap](https://github.com/DeltaZulu-OU/docs/blob/master/roadmaps/CONSISTENCY-CORRECTNESS-ROADMAP.md) (C05–C07, C14) |
| Historical architecture | [Pipeline import](https://github.com/DeltaZulu-OU/docs/blob/master/architecture/PIPELINE.md) with [errata](https://github.com/DeltaZulu-OU/docs/blob/master/architecture/PIPELINE-ERRATA.md) |
| Historical ADRs | [Archive](https://github.com/DeltaZulu-OU/docs/tree/master/archive/DeltaZulu.LocalStream) |
| Dated verification evidence | [Reports](https://github.com/DeltaZulu-OU/docs/tree/master/reports) |

## Local scope

The core owns payload-opaque node-local logs, partitions, offsets, independent subscriptions and retention. Query and Query.LiteDB are separate packages for already-bound operators and optional durable state/results. No Kusto binding belongs in the core; DEC-0033 is the current estate target. Historical version observations must be rechecked before publishing.

Only tested behavior is verified; a target architecture or historical report is not proof of current runtime conformance.
