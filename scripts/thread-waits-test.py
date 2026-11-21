#!/usr/bin/env python3
"""Tests for scripts/thread-waits.py. Linux only. Run: python3 -I scripts/thread-waits-test.py"""

import importlib.util
import os
import pathlib
import subprocess
import sys
import tempfile
import time
import unittest

HERE = pathlib.Path(__file__).resolve().parent
SCRIPT = HERE / "thread-waits.py"
sys.dont_write_bytecode = True  # no __pycache__ beside the scripts
spec = importlib.util.spec_from_file_location("thread_waits", SCRIPT)
tw = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tw)


def cli(*args, env=None, timeout=30):
    return subprocess.run(
        [sys.executable, "-I", str(SCRIPT), *args],
        capture_output=True,
        text=True,
        env=env,
        timeout=timeout,
    )


def fake_pgrep(directory, status, stdout="", stderr=""):
    """An executable named pgrep in `directory` that prints and exits as told."""
    path = pathlib.Path(directory) / "pgrep"
    path.write_text(
        "#!/bin/sh\n"
        f"printf '%s' '{stdout}'\n"
        f"printf '%s\\n' '{stderr}' >&2\n"
        f"exit {status}\n"
    )
    path.chmod(0o755)
    return str(path)


class FakeProc:
    """A /proc-shaped tree holding one process's task directory."""

    def __init__(self, root, pid=4242):
        self.root = pathlib.Path(root)
        self.pid = pid
        self.tasks = self.root / str(pid) / "task"
        self.tasks.mkdir(parents=True)

    def task(self, tid, comm, state="S", wchan="futex_wait_queue", run=0, wait=0, skip=()):
        base = self.tasks / str(tid)
        base.mkdir()
        files = {
            "comm": f"{comm}\n",
            "stat": f"{tid} ({comm}) {state} 1 1 1 0 -1\n",
            "wchan": wchan,
            "schedstat": f"{run} {wait} 7\n",
        }
        for name, text in files.items():
            if name not in skip:
                (base / name).write_text(text)

    def sample(self, prefixes):
        return tw.sample(self.pid, prefixes, proc=str(self.root))


def track(threads="rdb-node", context_on="xfs_ilock", context_threads="rdb-", min_ms=200):
    opts = tw.parse(
        [
            "--pattern", "x", "--out", "x", "--threads", threads,
            "--context-on", context_on, "--context-threads", context_threads,
            "--min-ms", str(min_ms),
        ]
    )
    t = tw.tracker(opts)
    t.clock = lambda: "T"
    return t


def row(tid, comm, state="S", wchan="futex_wait_queue", run=0, wait=0):
    return (str(tid), comm, state, wchan, run, wait)


class FindTests(unittest.TestCase):
    def test_status_0_an_anchored_live_match_returns_its_pid(self):
        marker = "37.123456"
        child = subprocess.Popen(["sleep", marker])
        try:
            self.assertEqual(tw.find(rf"^sleep {marker}$"), str(child.pid))
        finally:
            child.kill()
            child.wait()

    def test_status_0_excludes_this_sampler(self):
        with tempfile.TemporaryDirectory() as d:
            pgrep = fake_pgrep(d, 0, stdout=f"{os.getpid()}\n999999\n")
            self.assertEqual(tw.find("^x$", pgrep=pgrep), "999999")
            only_self = fake_pgrep(d, 0, stdout=f"{os.getpid()}\n")
            self.assertIsNone(tw.find("^x$", pgrep=only_self))

    def test_status_1_is_no_match(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertIsNone(tw.find("^x$", pgrep=fake_pgrep(d, 1)))

    def test_status_2_and_3_raise_with_stderr(self):
        for status in (2, 3):
            with tempfile.TemporaryDirectory() as d:
                pgrep = fake_pgrep(d, status, stderr=f"pgrep: boom {status}")
                with self.assertRaises(tw.SamplerError) as raised:
                    tw.find("^x$", pgrep=pgrep)
                self.assertIn(f"status {status}", str(raised.exception))
                self.assertIn(f"pgrep: boom {status}", str(raised.exception))


class CliTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.out = os.path.join(self.dir.name, "waits.log")

    def tearDown(self):
        self.dir.cleanup()

    def env_with_pgrep(self, status, stderr):
        bin_dir = os.path.join(self.dir.name, "bin")
        os.mkdir(bin_dir)
        fake_pgrep(bin_dir, status, stderr=stderr)
        return {"PATH": bin_dir}

    def test_status_1_keeps_polling_and_exits_0(self):
        started = time.monotonic()
        done = cli("--pattern", "^no-such-process-thread-waits$", "--out", self.out,
                   "--seconds", "0.6")
        self.assertEqual(done.returncode, 0, done.stderr)
        # Polled again after the 0.5 s retry, rather than returning on the first miss.
        self.assertGreaterEqual(time.monotonic() - started, 0.6)
        self.assertEqual(pathlib.Path(self.out).read_text(), "")

    def test_status_2_and_3_print_stderr_and_exit_nonzero(self):
        for status in (2, 3):
            with self.subTest(status=status):
                self.tearDown()
                self.setUp()
                done = cli("--pattern", "^x$", "--out", self.out, "--seconds", "5",
                           env=self.env_with_pgrep(status, f"pgrep: fake failure {status}"))
                self.assertEqual(done.returncode, 1)
                self.assertIn(f"failed with status {status}", done.stderr)
                self.assertIn(f"pgrep: fake failure {status}", done.stderr)
                self.assertNotIn("Traceback", done.stderr)

    def test_a_bad_regex_fails_with_pgreps_diagnostic(self):
        done = cli("--pattern", "[", "--out", self.out, "--seconds", "5")
        self.assertEqual(done.returncode, 1)
        self.assertIn("regex", done.stderr)
        self.assertNotIn("Traceback", done.stderr)

    def test_a_missing_pgrep_exits_nonzero(self):
        empty = os.path.join(self.dir.name, "empty")
        os.mkdir(empty)
        done = cli("--pattern", "^x$", "--out", self.out, "--seconds", "5", env={"PATH": empty})
        self.assertEqual(done.returncode, 1)
        self.assertIn("cannot run pgrep", done.stderr)
        self.assertNotIn("Traceback", done.stderr)

    def test_an_out_that_cannot_be_opened_exits_nonzero(self):
        bad = os.path.join(self.dir.name, "no-such-dir", "waits.log")
        done = cli("--pattern", "^x$", "--out", bad, "--seconds", "5")
        self.assertEqual(done.returncode, 1)
        self.assertIn("cannot open --out", done.stderr)
        self.assertNotIn("Traceback", done.stderr)

    def test_missing_required_arguments_exit_2(self):
        for args in ([], ["--pattern", "^x$"], ["--out", self.out]):
            with self.subTest(args=args):
                self.assertEqual(cli(*args).returncode, 2)


class TrackerTests(unittest.TestCase):
    def test_active_then_idle_above_min_ms_emits_one_summary(self):
        t = track()
        self.assertEqual(t.observe("1", [row(5, "rdb-node-1", "D", "io_schedule", 0, 0)], 0.0), [])
        self.assertEqual(t.observe("1", [row(5, "rdb-node-1", "R", "0", 2e6, 3e6)], 0.2), [])
        lines = t.observe("1", [row(5, "rdb-node-1", "S", "futex_wait_queue", 4e6, 5e6)], 0.3)
        self.assertEqual(
            lines, ["T 1 rdb-node-1/5 busy 300ms cpu 4ms runq 5ms {'io_schedule': 1, 'R': 1}\n"]
        )
        self.assertEqual(t.observe("1", [row(5, "rdb-node-1")], 0.4), [])

    def test_active_then_idle_below_min_ms_emits_nothing(self):
        t = track(min_ms=200)
        t.observe("1", [row(5, "rdb-node-1", "R", "0")], 0.0)
        self.assertEqual(t.observe("1", [row(5, "rdb-node-1")], 0.1), [])
        self.assertEqual(t.busy, {})

    def test_an_active_period_at_window_end_gets_no_summary(self):
        with tempfile.TemporaryDirectory() as d:
            out = pathlib.Path(d) / "out"
            opts = tw.parse(["--pattern", "x", "--out", str(out), "--seconds", "1"])
            now = [0.0]

            def clock():
                now[0] += 0.1
                return now[0]

            with open(out, "w") as f:
                tw.run(opts, f, track(), find=lambda _: "1",
                       sample=lambda pid, p: [row(5, "rdb-node-1", "R", "0")],
                       clock=clock, monotonic=clock, sleep=lambda _: None)
            self.assertEqual(out.read_text(), "")

    def test_a_vanished_active_task_gets_no_summary(self):
        t = track()
        t.observe("1", [row(5, "rdb-node-1", "R", "0")], 0.0)
        self.assertEqual(t.observe("1", [], 1.0), [])
        self.assertEqual(t.observe("1", [row(6, "rdb-node-2")], 2.0), [])

    def test_counters_never_combine_across_processes(self):
        t = track()
        t.observe("1", [row(5, "rdb-node-1", "R", "0", 1e6, 1e6)], 0.0)
        # The same thread id in a re-found process, idle: it closes nothing of process 1.
        self.assertEqual(t.observe("2", [row(5, "rdb-node-1", "S", "futex_wait_queue", 0, 0)], 1.0), [])
        self.assertIn(("1", "5"), t.busy)
        t.observe("2", [row(5, "rdb-node-1", "R", "0", 0, 0)], 2.0)
        lines = t.observe("2", [row(5, "rdb-node-1", "S", "futex_wait_queue", 9e6, 0)], 2.5)
        self.assertEqual(lines, ["T 2 rdb-node-1/5 busy 500ms cpu 9ms runq 0ms {'R': 1}\n"])

    def test_default_xfs_context_lists_the_node_and_its_syncer(self):
        t = track()
        lines = t.observe("1", [
            row(5, "rdb-node-1", "D", "xfs_ilock"),
            row(6, "rdb-sync-1", "D", "xlog_wait_on_iclog"),
            row(7, "tokio-runtime-w", "D", "xfs_ilock"),
        ], 0.0)
        self.assertEqual(
            lines, ["T 1 CONTEXT rdb-node-1/5:xfs_ilock rdb-sync-1/6:xlog_wait_on_iclog\n"]
        )

    def test_empty_context_on_emits_no_context_but_keeps_summaries(self):
        t = track(context_on="")
        self.assertEqual(t.observe("1", [row(5, "rdb-node-1", "D", "xfs_ilock")], 0.0), [])
        lines = t.observe("1", [row(5, "rdb-node-1")], 0.5)
        self.assertEqual(len(lines), 1)
        self.assertIn("busy 500ms", lines[0])
        self.assertNotIn("CONTEXT", lines[0])

    def test_comma_separated_prefixes_select_both_name_families(self):
        t = track(threads="tokio,rdb-")
        t.observe("1", [row(5, "tokio-runtime-w", "R", "0"), row(6, "rdb-sync-1", "R", "0")], 0.0)
        lines = t.observe("1", [row(5, "tokio-runtime-w"), row(6, "rdb-sync-1")], 0.5)
        self.assertEqual(len(lines), 2)
        self.assertIn("tokio-runtime-w/5 busy", lines[0])
        self.assertIn("rdb-sync-1/6 busy", lines[1])

    def test_a_prefix_nothing_has_emits_no_summary(self):
        t = track(threads="no-such-thread")
        t.observe("1", [row(5, "rdb-node-1", "R", "0")], 0.0)
        self.assertEqual(t.observe("1", [row(5, "rdb-node-1")], 1.0), [])


class SampleTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.proc = FakeProc(self.dir.name)

    def tearDown(self):
        self.dir.cleanup()

    def test_state_is_read_after_the_last_paren(self):
        self.proc.task(5, "rdb-) node(x)", state="D", wchan="xfs_ilock", run=11, wait=22)
        self.assertEqual(
            self.proc.sample(("rdb-",)), [("5", "rdb-) node(x)", "D", "xfs_ilock", 11, 22)]
        )

    def test_a_vanished_file_skips_only_that_task(self):
        for missing in ("comm", "stat", "wchan", "schedstat"):
            with self.subTest(missing=missing):
                self.tearDown()
                self.setUp()
                self.proc.task(5, "rdb-node-1", skip=(missing,))
                self.proc.task(6, "rdb-node-2")
                self.assertEqual([r[0] for r in self.proc.sample(("rdb-",))], ["6"])

    def test_an_empty_task_directory_returns_no_rows(self):
        self.assertEqual(self.proc.sample(("rdb-",)), [])

    def test_ten_thousand_nonmatching_tasks_return_no_rows(self):
        for tid in range(10_000):
            base = self.proc.tasks / str(tid)
            base.mkdir()
            (base / "comm").write_text("other\n")
        self.assertEqual(self.proc.sample(("rdb-",)), [])

    def test_a_gone_task_directory_resumes_discovery_without_traceback(self):
        with self.assertRaises(FileNotFoundError):
            tw.sample(4243, ("rdb-",), proc=self.dir.name)
        opts = tw.parse(["--pattern", "x", "--out", "x", "--seconds", "1"])
        now = [0.0]
        found = []

        def clock():
            now[0] += 0.1
            return now[0]

        def find(_):
            found.append(now[0])
            return "4243"

        with tempfile.TemporaryFile("w+") as out:
            tw.run(opts, out, track(), find=find,
                   sample=lambda pid, p: tw.sample(pid, p, proc=self.dir.name),
                   clock=clock, monotonic=clock, sleep=lambda _: None)
        self.assertGreater(len(found), 1, "the sampler went back to pgrep after the process went")

    def test_reads_real_proc_task_files(self):
        rows = tw.sample(os.getpid(), ("",))
        tids = {r[0] for r in rows}
        self.assertIn(str(os.getpid()), tids, rows)
        for tid, comm, state, wchan, run, wait in rows:
            self.assertTrue(comm)
            self.assertIn(state, "RSDTtZXIPWK")
            self.assertIsInstance(run, int)
            self.assertIsInstance(wait, int)


if __name__ == "__main__":
    unittest.main()
