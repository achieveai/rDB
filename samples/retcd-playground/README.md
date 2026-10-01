# rEtcd playground

Put, get, list and watch values in a real local 3-node cluster.

## Start

    # repo root, Git Bash. First run builds the server (minutes).
    ./scripts/local-cluster.sh up --dir /c/rdb_test_data/local-cluster
    cd samples/retcd-playground && npm install
    alias kv="node kv.mjs"        # then: kv help, kv status

## Try it

    kv put greeting "hello"       # prints a revision
    kv get greeting               # value + create/mod revision
    kv ls --limit 2               # pages of 2, follows all pages
    kv rm greeting
    kv putfile ./photo.png        # files up to 1 MiB, key files/photo.png
    kv getfile files/photo.png ./copy.png    # checks sha256

Browse keys like folders (patterns, `ls` and `watch` only):

    kv ls 'docs/*'                # keys here + one DIR row per sub-folder
    kv ls 'docs/**/*.md'          # every depth.  ?  one char.  [a-z]  a set
    kv watch 'docs/*.md'          # only matching changes

Always quote the pattern, or the shell expands it against local files.
The server lists prefixes only, so matching is done in `kv`.
The text before the first `*` `?` `[` is the prefix the server scans (none = all keys).

Compare-and-set (`--if-rev N` = only if the key is still at revision N):

    kv put counter 1              # say it prints revision 7
    kv put counter 2 --if-rev 7   # ok
    kv put counter 3 --if-rev 7   # REFUSED, shows the current revision

Watch in a second terminal, write in the first:

    kv watch demo/                # Ctrl-C to stop
    kv put demo/a 1

Failover:

    ./node.sh stop 1              # stop node 1, often the leader
    kv status                     # a new leader appears in seconds
    kv put still "working"        # kv finds a live node by itself
    ./node.sh start 1             # node 1 rejoins; kv status: 3 ready

Speed: `kv bench 200` (one client, one put at a time).

## Stop
    ./scripts/local-cluster.sh clean --dir /c/rdb_test_data/local-cluster   # stops, then deletes

## What this is not

- It is rEtcd, a config store. It is not rDB.
- No secondary indexes. `meta/files/...` is a hand-made "index": a JSON
  record `putfile` writes beside each file. Not a database feature.
- No multi-key transactions. A file and its meta record are two writes.
- Values up to 1 MiB. Dev cluster: plaintext, no auth, localhost only.
