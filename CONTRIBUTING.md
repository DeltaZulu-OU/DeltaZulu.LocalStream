# Contributing

## The deletion rule

**Deleting a project, a public type, or a line of work requires either a status
note on the governing Decision, or a new Decision with status `Rejected`.**

Deleting the code is the easy half. Recording why it stopped being the plan is
the half that keeps the next reader from rebuilding it, or from concluding that
something was abandoned when it was never started.

Both failure modes have already happened here. `DeltaZulu.Normalize` was deleted
with three public types and no Decision recording it, and a semantic view layer
that was merely *reserved* has repeatedly been mistaken for one that was removed.
`archive/RECOVERY.md` in [`DeltaZulu-OU/docs`](https://github.com/DeltaZulu-OU/docs) exists to hold exactly this
kind of record.

The rule applies to a project, a public type, or a line of work — not to a
private helper or a refactor that preserves behaviour.

## Governance

Decisions (`DEC-NNNN`) and Constraints (`CON-NNNN`) live in
[`DeltaZulu-OU/docs`](https://github.com/DeltaZulu-OU/docs), not in this repository. See `docs/README.md` for
the ones that govern this codebase.

A **Constraint** is a fact about the world that this estate does not control, and
is immutable. A **Decision** is a choice the estate made, and carries both the
alternatives it rejected and a revisit trigger — the named condition under which
it should be reopened.

`governs-check` in the docs repository fails when an `Accepted` Decision names a
symbol or path in this repository that no longer exists. If it fails against your
change, either the Decision needs updating or the deletion needs recording. Do
not silence it by trimming the Decision's `governs:` block.
