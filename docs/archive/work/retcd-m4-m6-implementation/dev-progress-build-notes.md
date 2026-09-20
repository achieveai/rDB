# dev-progress-build notes

Task: split docs/progress/index.html into small JSON/mmd pieces plus
docs/progress/build.mjs (zero-dep Node assembler). See src/SCHEMA.md
(read-only) for the format this was built against.

## Files delivered
- docs/progress/build.mjs (48,273 bytes) — CLI: default, --out, --check,
  --touch, --src (test override).
- docs/progress/src/{now,milestones,work,risks,parts,meta}.json
- docs/progress/src/diagrams/01..05-*.mmd
- docs/progress/src/reference.html
- docs/progress/vendor/mermaid.min.js untouched (pre-existing, 11.17.2)

## Size (target < 60 KB src JSON)
- src JSON combined: 27,757 bytes. Under target.
- .mmd combined: ~2,862 bytes. reference.html: 10,829 bytes.
- Assembled page (mermaid.min.js inlined): 3,627,814 bytes. Dominated by
  the inlined mermaid library, not by content.

## Verified
- node --check build.mjs: OK.
- build.mjs --check on real src: "OK: all pieces in docs/progress/src are valid."
- Two --out builds byte-identical (determinism confirmed twice, before and
  after the cross-field rules were added).
- 6 negative tests (v1 unknown chip, v2 four status entries, v3 missing
  evidence, v4 long sentence, v5 finished-but-not-proven mark, v6
  built-too-early stage) each correctly fail --check with clear
  file+path messages, exit 1.
- --touch correctly skips content validation and still writes the page,
  even with a known-bad milestones.json.
- Default (no --out) mode correctly advances meta.json (updated,
  ledger.line, part_states) and appends one line to the refresh log,
  tested against a disposable scratchpad copy only, never the real
  docs/progress/src.
- docs/progress/index.html confirmed never modified (git diff/status
  empty for it throughout).

## Cross-field rules added mid-task (from main)
Implemented 6 of 7 requested rules in validateCrossField(). Rule 7 (risk
severity "blocker" implies an at-risk milestone chip) was skipped: SCHEMA
risk severity vocab is low|medium|high (no "blocker") and chip vocab has
no "at-risk" value, so there is no field pair to check. Left a code
comment explaining this.
One migrated fact tripped a new rule: M0 had chip "Done"/gate_commit
unset, which rule 1 requires together. Fixed by adding
gate_commit: "7014701" to M0, found via `git log --all --oneline | grep -i m0`.
This is a real fact, not fabricated; it was simply missing from the
original page text.

## Known gaps / deviations (flagged, not hidden)
- Full in-browser Mermaid rendering was NOT verified. file:// pages over
  ~900KB fail to open in the Claude_Browser tool (the assembled page is
  3.6MB because of the inlined mermaid.min.js); externally referenced
  <script src> files do not execute under file:// snapshot mode either;
  and starting a local static server was denied by the permission
  classifier ("Expose Local Services"). Partial evidence only: a
  screenshot of the current/original index.html for baseline comparison,
  and confirmation that the non-Mermaid HTML/CSS/JS (Now box, stale
  banner, milestone strip) renders and computes correctly when tested
  via an external-script slimmed page. The Mermaid diagram *source* text
  was confirmed well-formed (correct accTitle/accDescr/flowchart/classDef
  structure) but its rendered SVG output was never visually confirmed.
- Mermaid theming uses classDef strings referencing CSS var(--token)
  directly, rather than a client-side matchMedia hex-swap script. Works
  because Mermaid emits raw CSS into an inline <style> inside the SVG,
  which resolves against the page's :root tokens at paint time. Simpler
  and more robust than the literal instruction; documented here as an
  intentional deviation.
- accTitle/accDescr Mermaid directives used in place of a literal
  aria-label attribute (Mermaid owns the generated <svg> and does not
  expose a way to set aria-label directly).
- Swim lanes only show agents with a recorded `start` timestamp. Most
  migrated finished agents have no start/end in the original page data;
  fabricating timestamps would misrepresent evidence, so they are
  omitted with a caption explaining why.
- work.json role inference for agents not covered by the given
  prefix rules: planner-* -> Testers, adr-*/research-* -> Lead. These
  are reasonable extensions of the stated Developers/Testers/Critics/
  Docs/Lead prefix mapping, flagged as assumptions, not certainties.

## Recommended status
COMPLETED_WITH_RISKS. All validator, determinism, and CLI-mode evidence
passes. The one open risk is the unverified in-browser Mermaid render,
blocked by tooling/permissions outside this task's control, not by any
known defect in the transform.
