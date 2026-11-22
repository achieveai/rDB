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
No match yet is not an error: the sampler keeps looking until --seconds ends. Any other pgrep
failure (a bad regex, say), a missing pgrep, or an --out that cannot be opened ends it with a
message on stderr and status 1.

Tests: python3 -I scripts/thread-waits-test.py
"""

import argparse
import collections
import datetime
import os
import subprocess
import sys
import time

IDLE = {"futex_wait_queue", "ep_poll", "hrtimer_nanosleep", "do_epoll_wait", "0", ""}


class SamplerError(Exception):
    """A failure that ends the sampler with a message, not a traceback."""


def utc():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S.%f")[:-3]


def find(pattern, pgrep="pgrep"):
    """The first process other than this one whose command line matches, or None.

    pgrep exits 1 when nothing matches, which is not an error: the process may not have started
    yet. Every other nonzero status (2 for a bad regex, 3 for a fatal error) is one.
    """
    try:
        found = subprocess.run([pgrep, "-f", pattern], capture_output=True, text=True)
    except OSError as e:
        raise SamplerError(f"cannot run {pgrep}: {e}") from e
    if found.returncode == 1:
        return None
    if found.returncode != 0:
        raise SamplerError(
            f"{pgrep} -f {pattern!r} failed with status {found.returncode}: "
            f"{found.stderr.strip()}"
        )
    pids = [p for p in found.stdout.split() if int(p) != os.getpid()]
    return pids[0] if pids else None


def read(path):
    with open(path) as f:
        return f.read()


def sample(pid, prefixes, proc="/proc"):
    """(tid, comm, state, wchan, run_ns, wait_ns) for each thread whose name has a prefix.

    Raises FileNotFoundError when the process is gone. A thread that exits between the listing
    and its reads is skipped.
    """
    rows = []
    for tid in os.listdir(f"{proc}/{pid}/task"):
        base = f"{proc}/{pid}/task/{tid}"
        try:
            comm = read(f"{base}/comm").strip()
            if not comm.startswith(prefixes):
                continue
            # The state follows the last ")": a thread name may hold ")" itself.
            state = read(f"{base}/stat").rsplit(")", 1)[1].split()[0]
            wchan = read(f"{base}/wchan").strip()
            run, wait, _ = map(int, read(f"{base}/schedstat").split())
        except (OSError, ValueError, IndexError):
            continue
        rows.append((tid, comm, state, wchan, run, wait))
    return rows


class Tracker:
    """Turns successive samples into busy-period summaries and CONTEXT lines."""

    def __init__(self, watched, context_on, context, min_ms, clock=utc):
        self.watched = watched
        self.context_on = context_on
        self.context = context
        self.min_ms = min_ms
        self.clock = clock
        # (pid, tid) -> [start, run0, wait0, waits seen, name]. Keyed by pid too, so a thread
        # id reused by a later process never extends a period of an earlier one.
        self.busy = {}

    def prefixes(self):
        return self.watched + self.context

    def observe(self, pid, rows, now):
        """The lines one sample at monotonic time `now` (seconds) produces."""
        lines = []
        waits = []
        for tid, comm, state, wchan, run, wait in rows:
            active = state == "R" or wchan not in IDLE
            if active and comm.startswith(self.context):
                waits.append((int(tid), comm, "R" if state == "R" else wchan))
            if not comm.startswith(self.watched):
                continue
            key = (pid, tid)
            if active:
                period = self.busy.setdefault(key, [now, run, wait, collections.Counter(), comm])
                period[3]["R" if state == "R" else wchan] += 1
            elif key in self.busy:
                start, run0, wait0, seen, name = self.busy.pop(key)
                length = (now - start) * 1000
                if length >= self.min_ms:
                    lines.append(
                        f"{self.clock()} {pid} {name}/{tid} busy {length:.0f}ms "
                        f"cpu {(run - run0) / 1e6:.0f}ms runq {(wait - wait0) / 1e6:.0f}ms "
                        f"{dict(seen)}\n"
                    )
        if any(c.startswith(self.watched) and w in self.context_on for _, c, w in waits):
            listed = " ".join(f"{c}/{t}:{w}" for t, c, w in sorted(waits))
            lines.append(f"{self.clock()} {pid} CONTEXT {listed}\n")
        return lines


def parse(argv):
    args = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    args.add_argument("--pattern", required=True, help="pgrep -f pattern of the test binary")
    args.add_argument("--out", required=True, help="file the lines are appended to")
    args.add_argument("--seconds", type=float, default=600)
    args.add_argument("--interval", type=float, default=0.02)
    args.add_argument("--min-ms", type=float, default=200)
    args.add_argument("--threads", default="rdb-node", help="comma-separated name prefixes")
    args.add_argument("--context-on", default="xfs_ilock", help="comma-separated waits")
    args.add_argument("--context-threads", default="rdb-")
    return args.parse_args(argv)


def tracker(opts):
    return Tracker(
        watched=tuple(opts.threads.split(",")),
        context_on=set(opts.context_on.split(",")) if opts.context_on else set(),
        context=tuple(opts.context_threads.split(",")),
        min_ms=opts.min_ms,
    )


def run(opts, out, track, find=find, sample=sample, clock=time.time, monotonic=time.monotonic,
        sleep=time.sleep):
    """Sample until --seconds have passed, re-finding the process whenever it is gone."""
    end = clock() + opts.seconds
    while clock() < end:
        pid = find(opts.pattern)
        if pid is None:
            sleep(0.5)
            continue
        while clock() < end:
            try:
                rows = sample(pid, track.prefixes())
            except FileNotFoundError:
                break
            for line in track.observe(pid, rows, monotonic()):
                out.write(line)
            sleep(opts.interval)


def main(argv=None):
    opts = parse(argv)
    try:
        try:
            out = open(opts.out, "a", buffering=1)
        except OSError as e:
            raise SamplerError(f"cannot open --out {opts.out}: {e}") from e
        with out:
            run(opts, out, tracker(opts))
    except SamplerError as e:
        print(f"thread-waits: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
