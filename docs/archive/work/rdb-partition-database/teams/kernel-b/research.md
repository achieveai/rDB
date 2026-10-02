# kernel-b research — prefix agreement, ancestry, and loss-accepting election

Architect, team kernel-b, 2026-09-20. Sources fetched 2026-09-20 by a research worker; PDFs were
extracted with `pdftotext` after WebFetch could not parse the binary, and every quotation below was
read from that extracted text. Claims I could not verify are flagged as such — team-rules.md §19:
the `evidence/*.md` packets the spec cites do not exist, so everything here is re-derived.

Question this file answers: **why is the spec's §8 rule "longest *compatible* prefix, by hash
ancestry" the right rule, and what exactly goes wrong if the word "compatible" is dropped?**

---

## 1. Raft — ancestry is a token, not a length

Source: Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus Algorithm*
(extended version), <https://raft.github.io/raft.pdf>.

### 1.1 Log Matching (§5.3)

Raft states it as two properties: entries in different logs with the same index and term store the
same command, and — the load-bearing half — the logs are then "identical in all preceding entries"
(§5.3). Figure 3 compresses it to: if two logs share an entry at the same index and term, "the logs
are identical" up to that point.

Raft earns this inductively, not by construction. §5.3 calls the AppendEntries check "an induction
step": the empty log satisfies the property, and every extension preserves it. The check itself is
`prevLogIndex` + `prevLogTerm` sent with each AppendEntries; if the follower has no entry with that
index and term, "it refuses the new entries" (§5.3). On success "the leader knows that the
follower's log is identical to its own log up through the new entries."

**What rDB does differently, and why it is stronger.** rDB's `record_digest` chains `prev_digest`
(design.md §1.1). The digest of entry *n* is a hash commitment over the entire prefix ending at *n*,
so equal digest at equal seq implies equal prefix directly — no induction over a pairwise RPC
history is needed, and no term needs to be carried inside the entry to disambiguate. Raft needs
`(index, term)` because an index alone names a slot; rDB's `(seq, digest)` names a *history*.
Consequence for F1: one matching `(seq, digest)` pair from a sparse ladder proves the whole shared
prefix, which is why survivor inventories can be small (design.md §5.2).

### 1.2 The election restriction, and where the real argument lives (§5.4.1, §5.4.2, §5.4.3)

§5.4.1 gives the comparison rule verbatim: compare the last entries' index and term; "If the logs
have last entries with different terms, then the log with the later term is more up-to-date. If the
logs end with the same term, then whichever log is longer is more up-to-date." A voter "denies its
vote if its own log is more up-to-date than that of the candidate."

**Correction to a common paraphrase, and to my own brief.** §5.4.1 contains no prose argument that
comparing length alone is insufficient. It states the rule and stops. The rigorous argument is in
two other places, and those are what this team cites:

- **§5.4.3, safety proof steps 6 and 7.** The proof splits on exactly the two branches of the rule.
  Step 6 covers equal last terms: the candidate's log "must have been at least as long as the
  voter's, so its log contained every entry in the voter's log" — length decides, and it is *sound*
  here. Step 7 covers different last terms: the candidate's last term "must have been larger," so
  the earlier leader that created that entry already contained the committed entry, "and by the Log
  Matching Property" the candidate's log contains it too — **ancestry decides and length carries no
  information at all.**
- **Figure 8 / §5.4.2**, the concrete counterexample (below).

So the precise statement, which is the one that transfers to rDB: *length is a valid tiebreaker only
once ancestry is already established as equal.* Used before that check, it is not a weaker heuristic
— it is meaningless.

### 1.3 Figure 8 — a longer, majority-replicated log that is still wrong (§5.4.2)

Figure 8's sequence: S1 partially replicates index 2 in term 2; S1 crashes; S5 wins term 3 and
writes a *different* entry at index 2; S5 crashes; S1 is re-elected and replicates the term-2 entry
to a majority — at which point, in the paper's words, that entry "has been replicated on a majority
of the servers, but it is not committed"; S1 crashes; S5 is elected again and overwrites it.

Two logs here are of comparable length and are *incomparable* — different commands at index 2 under
different terms. Neither is an extension of the other. A "longest wins" selector would confidently
pick one and silently destroy the other's acknowledged writes.

Hence §5.4.2's rule, verbatim: "Raft never commits log entries from previous terms by counting
replicas." Only current-term entries commit by counting; prior entries commit indirectly through
Log Matching. §5.4.2 names the root cause in terms that map straight onto rDB: "log entries retain
their original term numbers when a leader replicates entries from previous terms."

**Transfer to rDB §8.** rDB's structural defence against Figure 8 is different and, for our narrow
case, sufficient: §8.1 restricts comparison to survivors *within the same prior authoritative epoch*
— "no new owner has yet written, so compatible contiguous histories can be compared by hash
ancestry." Recovery never compares across an epoch boundary, because a new epoch only exists after
a new lineage root is committed, and that root cites its predecessor cutoff. Figure 8's hazard is
two writers under two terms; rDB forbids the comparison that would be needed to fall into it, and
F1's `verify_ancestry` rejects any inventory whose `lineage_root_seen` differs (design.md §5.3 rule
1) before length is ever read.

**Residual risk, stated plainly.** That defence holds only if fencing holds. If a fencing violation
lets two owners write under one generation, rDB gets divergent digests at the same seq. The spec's
answer (§8.1) is the right one and is not a tie-break: "Different digests at the same lineage
position are corruption or a fencing violation, not a normal tie. Quarantine and block automatic
promotion." F1 implements exactly that and has no merge function to call (design.md §5.4).

---

## 2. Chain replication — where length *is* ancestry

Source: Robbert van Renesse and Fred B. Schneider, *Chain Replication for Supporting High Throughput
and Availability*, OSDI 2004, <https://www.cs.cornell.edu/home/rvr/papers/OSDI04.pdf>. All citations
are to §3, whose subsections are bold run-in headings, not numbered.

### 2.1 The two invariants

**Update Propagation Invariant** (§3, verbatim): "For servers labeled i and j such that i ⪯ j holds
(i.e., i is a predecessor of j in the chain) then: Hist_objID^j ⪯ Hist_objID^i." The successor's
history is a prefix of the predecessor's.

**Inprocess Requests Invariant** (§3, verbatim): "If i ⪯ j then Hist_objID^i = Hist_objID^j ⊕
Sent_i." The difference between any two servers is *exactly* the list of requests forwarded but not
yet acknowledged by the tail — an in-flight suffix, never a fork.

> Flag: the prose sentence introducing the first invariant reads, in the extracted text, "the
> sequence of updates received by each server is a prefix of those received by its successor,"
> which is backwards relative to the invariant immediately below it and relative to the protocol.
> The USENIX HTML mirror returned 403, so it could not be cross-checked. **Cite the invariant, not
> that sentence.**

### 2.2 Recovery is "send the suffix", never "truncate and overwrite"

- Head failure (§3): the master removes the head and promotes its successor; requests the head
  accepted but never forwarded are simply dropped from `Pending`.
- Tail failure (§3): promoting the predecessor "potentially increases the set of requests completed
  by the tail" — because the old tail's history was a prefix of the new one, promotion can only
  commit *more*, never discard.
- Middle failure (§3): the predecessor `S−` "first forwards the sequence of requests in Sent_{S−} to
  S+", and "only after those have been sent may S− process and forward requests that it receives
  subsequent to assuming its new chain position." An explicit ordering constraint on repair.
- The catch-up is a suffix computed from a reported sequence number: the new successor reports "the
  sequence number sn of the last update request S+ has received", and `S−` computes "the suffix of
  Sent_{S−} to send to S+". The paper notes truncating the already-present prefix is an
  optimisation, not a correctness requirement.

### 2.3 Why the analogy holds only halfway

**Inference, not a quotation** (the paper does not use this phrase): because the chain is a total
order on servers and the Update Propagation Invariant holds for *every* pair i ⪯ j, any two
servers' histories are prefix-comparable, and the longer one is always a strict extension of the
shorter. There is one writer (the head) and one sequencer, and a history can only be extended,
never rewritten. **In chain replication, length *is* ancestry** — so "longest wins" is safe there,
and it is safe for a reason that has nothing to do with the rule being intuitive.

rDB's §8.2 argument is structurally the same argument: the primary assigns one sequence at a time,
and a secondary accepts *n* only holding *n−1* with the exact expected digest, so if B acknowledged
103 it necessarily holds 102. Survivors differ only by suffix length. That is chain replication's
Inprocess Requests Invariant re-derived for a fan-out topology.

**But rDB does not get it for free.** Chain replication earns prefix-comparability from the chain
topology plus a single head. rDB fans out to two secondaries concurrently (§5.2) and changes
primaries on failure, so the topology guarantee is gone. rDB reintroduces the missing ancestry the
same way Raft does — with an explicit token — except rDB's token is a hash chain rather than a term.
The practical consequence is the one rule this team must never soften: `verify_ancestry` runs before
any length comparison, and `select_prefix` cannot be called with anything but a `VerifiedInventory`
(design.md §5.3).

---

## 3. Kafka — the loss-accepting analogue of spec §8.4

Sources: <https://kafka.apache.org/43/design/design/#replication> and
<https://kafka.apache.org/43/configuration/broker-configs/> (anchor
`#unclean.leader.election.enable`).

> Flag: the anchor named in my brief, `kafka.apache.org/documentation/#design_uncleanleader`, is
> dead. The site was restructured; `/documentation/` is now a redirect shell with no
> `#design_uncleanleader` entry, and `/40/design.html` returns 404. The two URLs above are the
> working ones and are what this file cites.

### 3.1 ISR is a membership certificate, not a length comparison

Kafka "dynamically maintains a set of in-sync replicas (ISR) that are caught-up to the leader."
Membership requires an active controller session and not falling significantly behind; failing
either evicts the replica. The commit rule: "a write to a Kafka partition is not considered
committed until all in-sync replicas have received the write."

**This is the point worth stealing.** Kafka never compares log lengths to elect a leader. It elects
from a set that is *by construction* known to hold the full committed prefix. Raft uses a term;
chain replication uses topology; Kafka uses a membership certificate. All three refuse to let length
stand in for ancestry, and all three pay for it with an explicit mechanism.

rDB's equivalent certificate is the pinned config's required-copy set (design.md §3.5): the ACK
predicate is computed from the *configuration's* roles, not from what a replica claims about itself.
That is why shadows can never qualify and why `min_regular_acks` never drops below 1.

### 3.2 The dial, and the default

With every replica for a partition dead, the docs frame exactly two options, verbatim: (1) "Wait for
a replica in the ISR to come back to life and choose this replica as the leader (hopefully it still
has all its data)"; (2) "Choose the first replica (not necessarily in the ISR) that comes back to
life as the leader."

Availability against durability, stated without euphemism. The config entry's own description is the
honest version: enabling it lets replicas not in the ISR "be elected as leader as a last resort,
even though doing so may result in data loss". Type `boolean`, **default `false`**, importance
`high`, update mode `cluster-wide`. The design page adds: "By default from version 0.11.0.0, Kafka
chooses the first strategy and favor waiting for a consistent replica."

### 3.3 How rDB §8.4 differs, and where it is better

rDB's majority-loss recovery is loss-accepting in the same family — D6 and §8.1 say so: "An isolated
holder may remain unavailable, so D6 permits loss-accepting recovery from the surviving prefix."
Three differences are worth recording, because they are design decisions this team must not erode:

| | Kafka unclean election | rDB §8.4 |
|---|---|---|
| What is adopted | whatever the returning replica has | a **validated whole-transaction prefix** with checked ancestry against the committed root |
| What callers learn | nothing in-band; loss is silent to producers | a **new recovery generation**; `GENERATION_CHANGED` forces explicit client reconciliation (§5.3) |
| What happens to writes | resume immediately | **read-only** until three copies fsync the same prefix (§8.4 steps 4–6) |
| What happens to the divergent data | overwritten | **quarantined**, 7-day retention, deletion needs policy approval (§8.4) |
| Gate | one boolean, off by default | not a toggle: the mode is a consequence of how many eligible survivors hold the barrier |

The generation bump is the substantive improvement. Kafka's unclean election is silent to the
producer that received an ack; rDB makes the loss *nameable* — the caller's `expected_generation` no
longer matches, so it cannot silently retry a lost-generation transaction as a new effect. The cost
is that callers must implement reconciliation, which is exactly what §5.3 demands and what the API
error table encodes.

The thing rDB must resist is the Kafka-shaped shortcut under pressure: an operator toggle that says
"promote whoever is up." rDB has no such toggle, and F1 should never acquire one. The mode is
derived from the survivor count holding a durable barrier, not configured.

---

## 4. What this research changed in the design

1. `record_digest` **must** include `prev_digest` — stated as a hard requirement on C0
   (design.md §1.1), with a known-answer chaining vector. Without it, F1 has no ancestry token and
   is Figure 8 waiting to happen.
2. `select_prefix` takes `&[VerifiedInventory]` and never a raw sequence number (design.md §5.3).
   Raft §5.4.3 steps 6–7 are the reason: length is only meaningful *after* ancestry is equal, so the
   type system should not permit the other order.
3. Recovery compares only within one prior authoritative epoch, and a differing `lineage_root_seen`
   is an eligibility failure checked before length (design.md §5.3 rule 1).
4. The ACK-qualifying set is derived from the pinned configuration, Kafka-ISR style, never from a
   replica's self-declared role (design.md §3.4 rule 5, §3.5).
5. Catch-up sends a suffix and never truncates a divergent copy — chain replication's repair
   discipline, with the added step that a `prev_digest` mismatch at the requested point is
   divergence, not a reason to overwrite (design.md §3.6 step 1).

---

## 5. Not verified

- No post from `decentralizedthoughts.github.io` was fetched. Nothing is cited from it.
- The chain-replication prose sentence noted in §2.1 could not be cross-checked against a second
  rendering; only the invariant is cited.
- rDB's own `evidence/*.md` packets do not exist (team-rules.md). Nothing here cites them.
- No measurement of any kind was performed. Everything above is a reading of published designs, not
  evidence about rDB's behaviour.
