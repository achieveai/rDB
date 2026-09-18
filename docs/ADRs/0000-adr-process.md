# ADR-0000: ADR process and template

**Status:** Accepted  
**Date:** 2026-09-17

## Context

`docs/DesignSpec-01.md` is the approved target architecture. Implementation will make many
concrete choices the spec leaves open (encodings, crate APIs, test harnesses) and must record
why, so future changes (watches, snapshots, dynamic membership) do not silently violate them.

## Decision

- Every non-obvious, hard-to-reverse decision gets an ADR in `docs/ADRs/NNNN-kebab-title.md`.
- Template sections: Status, Date, Context, Decision, Consequences, Verification (how the
  decision is proven by tests or checks), References.
- Accepted ADRs are immutable in their Decision section. Change = new ADR that supersedes.
- Every ADR names the spec section(s) it implements and, where relevant, the tests that gate it.

## Consequences

- Reviewers can check code against ADRs rather than re-deriving intent from the spec.
- Slight overhead per decision; kept small by the lite template.

## Verification

`docs/ADRs/README.md` index lists every ADR; each file has the template sections.
