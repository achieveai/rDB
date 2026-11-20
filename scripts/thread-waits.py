#!/usr/bin/env python3
"""Show where a test binary's threads wait in the kernel (Linux only).

For a "slow step" red (`host_slow_step`, a missed deadline): run this beside the test binary,
then match its lines to the slow steps by time and node. Issue #142 found this way that a node
thread's 666 ms step was 665 ms in `xfs_ilock` with ~1 ms on CPU, while the same node's syncer
(the thread id next to it) was in its fsync's journal force (`xlog_wait_on_iclog`). See #152.

Every --interval it reads /proc/<pid>/task/*/{comm,stat,wchan,schedstat} for threads whose name
starts with --threads. A thread is busy while it is runnable or in a kernel wait other than
the idle ones (futex, epoll, nanosleep). Two kinds of line, UTC times:

  <time> <pid> <thread>/<tid> busy <ms> cpu <ms> runq <ms> {wait: samples}
      a busy period of at least --min-ms ended. cpu and runq are from schedstat: on-CPU time,
      and time runnable but waiting for a CPU. busy - cpu - runq is time blocked in the
      kernel, in the waits listed (R = runnable).
  <time> <pid> CONTEXT <thread>/<tid>:<wait> ...
      a thread matching --context-on was in that wait (default xfs_ilock): every busy thread
      of the process (any name in --context-threads) at that sample, by thread id.

Usage, beside a running test binary (it re-finds the process when it restarts):
  python3 -I scripts/thread-waits.py --pattern '^/path/to/target/debug/deps/rdb_api-' \\
      --out /tmp/waits.log --seconds 600
The process is found with `pgrep -f`; anchor --pattern with ^ so it never matches this script.
"""

import argparse
import collections
import datetime
import os
import subprocess
import time

IDLE = {"futex_wait_queue", "ep_poll", "hrtimer_nanosleep", "do_epoll_wait", "0", ""}


def utc():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S.%f")[:-3]


def find(pattern):
    found = subprocess.run(["pgrep", "-f", pattern], capture_output=True, text=True)
    pids = [p for p in found.stdout.split() if int(p) != os.getpid()]
    return pids[0] if pids else None


def sample(pid, prefixes):
    """(tid, comm, state, wchan, run_ns, wait_ns) for each thread whose name has a prefix."""
    rows = []
    for tid in os.listdir(f"/proc/{pid}/task"):
        base = f"/proc/{pid}/task/{tid}"
        try:
            comm = open(f"{base}/comm").read().strip()
            if not comm.startswith(prefixes):
                continue
            state = open(f"{base}/stat").read().rsplit(")", 1)[1].split()[0]
            wchan = open(f"{base}/wchan").read().strip()
            run, wait, _ = map(int, open(f"{base}/schedstat").read().split())
        except (OSError, ValueError):
            continue
        rows.append((tid, comm, state, wchan, run, wait))
    return rows


def main():
    args = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    args.add_argument("--pattern", required=True, help="pgrep -f pattern of the test binary")
    args.add_argument("--out", required=True, help="file the lines are appended to")
    args.add_argument("--seconds", type=float, default=600)
    args.add_argument("--interval", type=float, default=0.02)
    args.add_argument("--min-ms", type=float, default=200)
    args.add_argument("--threads", default="rdb-node", help="comma-separated name prefixes")
    args.add_argument("--context-on", default="xfs_ilock", help="comma-separated waits")
    args.add_argument("--context-threads", default="rdb-")
    opts = args.parse_args()
    watched = tuple(opts.threads.split(","))
    context_on = set(opts.context_on.split(",")) if opts.context_on else set()
    context = tuple(opts.context_threads.split(","))
    out = open(opts.out, "a", buffering=1)
    busy = {}
    end = time.time() + opts.seconds
    while time.time() < end:
        pid = find(opts.pattern)
        if pid is None:
            time.sleep(0.5)
            continue
        while time.time() < end:
            now = time.monotonic()
            try:
                rows = sample(pid, watched + context)
            except FileNotFoundError:
                break
            waits = []
            for tid, comm, state, wchan, run, wait in rows:
                active = state == "R" or wchan not in IDLE
                if active and comm.startswith(context):
                    waits.append((int(tid), comm, "R" if state == "R" else wchan))
                if not comm.startswith(watched):
                    continue
                key = (pid, tid)
                if active:
                    period = busy.setdefault(key, [now, run, wait, collections.Counter(), comm])
                    period[3]["R" if state == "R" else wchan] += 1
                elif key in busy:
                    start, run0, wait0, seen, name = busy.pop(key)
                    length = (now - start) * 1000
                    if length >= opts.min_ms:
                        out.write(
                            f"{utc()} {pid} {name}/{tid} busy {length:.0f}ms "
                            f"cpu {(run - run0) / 1e6:.0f}ms runq {(wait - wait0) / 1e6:.0f}ms "
                            f"{dict(seen)}\n"
                        )
            if any(c.startswith(watched) and w in context_on for _, c, w in waits):
                listed = " ".join(f"{c}/{t}:{w}" for t, c, w in sorted(waits))
                out.write(f"{utc()} {pid} CONTEXT {listed}\n")
            time.sleep(opts.interval)


if __name__ == "__main__":
    main()
