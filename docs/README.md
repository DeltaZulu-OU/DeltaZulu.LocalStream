# DeltaZulu.LocalStream — documentation

Architecture, Decisions, Constraints and roadmaps for the DeltaZulu estate live
in **[`DeltaZulu-OU/docs`](https://github.com/DeltaZulu-OU/docs)**, not here.

| Looking for | Go to |
|---|---|
| Decisions governing this repository | [`architecture/GOVERNING-DECISIONS.md`](https://github.com/DeltaZulu-OU/docs/blob/main/architecture/GOVERNING-DECISIONS.md) |
| The estate-wide pipeline architecture | [`architecture/PIPELINE.md`](https://github.com/DeltaZulu-OU/docs/blob/main/architecture/PIPELINE.md) — read with `PIPELINE-ERRATA.md` |
| Facts the estate does not control | [`constraints/`](https://github.com/DeltaZulu-OU/docs/tree/main/constraints) |
| This repository's historical ADRs | [`archive/DeltaZulu.LocalStream/`](https://github.com/DeltaZulu-OU/docs/tree/main/archive/DeltaZulu.LocalStream) |
| Roadmaps | [`roadmaps/`](https://github.com/DeltaZulu-OU/docs/tree/main/roadmaps) |
| Verification evidence | [`reports/`](https://github.com/DeltaZulu-OU/docs/tree/main/reports) |

Decisions are numbered globally across the estate. The per-repository scheme this
replaced produced collisions that citations could not resolve — `DeltaZulu.Agent`
ADR 0014 and `DeltaZulu.Platform` ADR 0014 decide opposite things, and the Agent
carried two different ADR 0003 documents, so "ADR 0003" did not resolve even
within one repository.

## What remains here

- `LOCAL_STREAM_ARCHITECTURE.md`.

This repository is **payload-opaque** and must stay so: it carries
`byte[] PayloadJson` verbatim and never interprets it (`CON-0013`). Never add
`DeltaZulu.Kql` or a `Kusto.Language` pin here — that opacity is what keeps this
library out of the type contract's version span entirely.

Note also that the declared version is currently *below* the published one; see
`reports/2026-08-15-version-pin-reconciliation.md` in the docs repository before
the next release.
