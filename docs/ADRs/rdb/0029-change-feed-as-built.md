# ADR-rdb-0029: The change feed as built — supersedes ADR-rdb-0020's Decision

**Status:** Accepted (issue #87; recorded on PR #170, review r5, 2026-11-21)
**Date:** 2026-11-21
**Spec:** `docs/rdb/design-specification.md` §4.1, §5.1–§5.3, §8.1, §10.1
**Supersedes:** [ADR-rdb-0020](0020-change-feed-for-external-consumers.md)'s Decision section.
ADR-rdb-0020 is Accepted, so its Decision section is not edited: it keeps the 2026-10-23 options
and leaning as the reasoning, and this ADR records what replaced them. ADR-rdb-0020's Context,
and its dated amendments, still hold; they give the detail of each point below.
**Issue:** #87, on PR #170 (review r5, thread 4655340447).

## Context

- ADR-rdb-0020 was written while Proposed. Its Decision section set out options A to C and a
  leaning, and said nothing was decided.
- Issue #87 chose option A on 2026-11-19 and built it in two slices (in process, then the wire).
  The built feed differs from the leaning in its cursor, its loss marker and who may call it.
- PR #170 first wrote that replacement into ADR-rdb-0020's Decision section as a note. The ADR
  series rule (`docs/ADRs/rdb/README.md`) keeps an Accepted ADR's Decision section unchanged, so
  the replacement is recorded here instead.

## Decision

1. **Pull, per partition.** An app reads one partition's committed `History` with
   `Db::changes(partition, from, max, source)`, or `WireCall::Changes` over the links. There is no
   order across partitions and no push.
2. **Capped at what is safe to show.** The owner serves up to its published position. A regular
   secondary serves up to what it has applied, and only while its own read of `partitions/{p}` is
   fresh (started within the grant, naming its generation and pinning it regular).
3. **The cursor is `(generation, seq, digest)`,** not `(generation, seq)`: the record's header
   stamp, its seq and its `record_digest`. A cursor the serving copy still holds at the same stamp
   and digest is served.
4. **Loss is `Rewound { to }`,** not a marker that carries `base`. A copy that no longer holds the
   cursor's record answers `Trimmed { floor }`. `Trimmed` is defined now; retention, which would
   trim `History`, is deferred (#61).
5. **Physical writes.** A page carries the stored writes, format 1. No before-images and no
   previous version.
6. **Scoped to the caller's tenant.** Only `User` writes under the caller's tenant are returned.
   A record with none is skipped, and `next` moves past it.
7. **Who may call it: fail closed for apps.** Over the links, a node serves `Changes` only to a
   node (`Label::Node`) or to an app on its `admin_apps` (`rdb_node --admin-apps`). Any other app
   is refused `INVALID_ARGUMENT` with `no_mutation`. A `Db` in the process that runs the nodes is
   trusted, as for every other call. This replaces "any authenticated app may call it", and holds
   until a certificate binds an app to its tenants (#151).

## Consequences

- `History`'s encoding is a public contract. The feed carries its own format number; a page in
  another format lands as `INCOMPATIBLE_VERSION`.
- A fault from stored feed damage names a seq, never a digest or a value, and any
  authenticated app may read a node's fault through `Inspect`/`Status`. So it shows how many
  records a partition holds, other tenants' too, and no content. This stands under the owner's
  R-Q3 ruling on #87, with this warning, until #154 scopes apps to their certificates.
- A plain app cannot read the feed until #151 lands. An operator who wants one to read it puts
  that app on `admin_apps`, which trusts it with every tenant.
- Retention (#61) and long-poll (slice 4) are still open. The prefix filter (slice 3) is dropped:
  shared config stays in rEtcd.

## Verification

- `docs/ADRs/rdb/README.md` lists this file, and ADR-rdb-0020's row says its Decision is
  superseded by it.
- ADR-rdb-0020's Decision section matches its text before PR #170
  (`git diff origin/main -- docs/ADRs/rdb/0020-change-feed-for-external-consumers.md` changes
  nothing between `## Decision` and `## Consequences`).
- The built feed's rows are named in PRs #87 and #170: the `f87_` rows in `rdb-api`, the feed
  access rows among them in `rdb-api`'s `net::tests`.

## References

- [ADR-rdb-0020](0020-change-feed-for-external-consumers.md): the ask, the options, and the dated
  amendments.
- `crates/rdb-api/src/feed.rs`: the answers, and `feed::resume`.
- `docs/rdb/embed.md`: `Db::changes` for an embedding app.
