# DeltaZulu estate context

## Type contract
KQL is the type system for all typed libraries. `DeltaZulu.Kql` owns it and
wraps `Microsoft.Azure.Kusto.Language`.

The type is DECLARED, never INSPECTED. Never derive a KQL type by examining
a CLR value's runtime type. If you find code doing that, it is a defect.

## Version spans — DO NOT UNIFY
Rx.Kql 3.5.3 pins Kusto.Language 9.2.0. Platform runs 12.4.1.
- DeltaZulu.Kql compiles against 9.2.0, MINIMUM not exact.
- Agent pins [9.2.0] exact. Platform pins [12.4.1] exact.
- Parse, LogCluster, Forward: minimum 9.2.0, no exact pin.
Use only API present in 9.2.0 and unchanged in 12.4.1.

## Hard constraints
- Kusto.Language contains NO CLR type mapping.
- KQL `datetime` is UTC-only. Carrier is `System.DateTime`, Kind=Utc.
  NEVER `DateTimeOffset`.
- KQL `decimal` maps to `System.Decimal` here (narrowing; KI-002).
- `TryNormalize` NEVER throws and NEVER rounds. Widen or reject per FIELD.
  This refines D3 reject-not-coerce; it does not reverse it.
- Two enums, never merged: KqlNullReason (collection, catalogue-owned,
  specified — do not modify) and KqlLossReason (conversion, Kql-owned).
- Canonical timespan unit is TICKS (100ns). The registry's microseconds
  default is a factor-of-ten defect.
- No `_ =>` fallthrough over a closed DeltaZulu enum.
- Do not re-expose anything `ScalarSymbol` already answers.
- LocalStream and DurableBuffer are PAYLOAD-OPAQUE. Never add
  DeltaZulu.Kql or a Kusto.Language pin to them.

## Repos
Contract: DeltaZulu.Kql        Typed: Parse, LogCluster, Forward
Opaque:   LocalStream, DurableBuffer
Apps:     Agent, Platform
Docs:     DeltaZulu-OU/docs — all ADRs, architecture, roadmaps

## Rule
Deleting a project, public type, or line of work requires either a status
note on the governing Decision or a new Decision with status `Rejected`.
