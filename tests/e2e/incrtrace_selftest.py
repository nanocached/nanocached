"""Self-test of incrtrace.py on hand-made logs. Run: python3 incrtrace_selftest.py

A node's TRACE lines go through a queue to a writer thread, so the lines still
queued when it is SIGKILLed are lost with no `TRACE dropped` marker. The
increment a replica then serves starts from a value no traced increment
produced, which incrtrace.py must not report as a value from nowhere when the
run killed a node and the loader acknowledged increments that have no trace
line. It must still report one when nothing accounts for the missing lines.
"""
import datetime
import json
import os
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
T0 = datetime.datetime(2026, 11, 10, 10, 0, 0, tzinfo=datetime.timezone.utc).timestamp()


def stamp(offset):
    t = datetime.datetime.fromtimestamp(T0 + offset, datetime.timezone.utc)
    return t.strftime("%Y-%m-%dT%H:%M:%S.%f") + "000Z"


def incr(offset, old, new):
    return f"{stamp(offset)} TRACE incr key=is0 old={old} new={new} delta=1\n"


class IncrTrace(unittest.TestCase):
    def run_incrtrace(self, n1, n2, acked, kills=None):
        """Logs of two nodes, a loader state of one serial key, and the summary."""
        with tempfile.TemporaryDirectory() as d:
            for name, lines in (("nch-n1", n1), ("nch-n2", n2)):
                with open(os.path.join(d, name + ".log"), "w") as f:
                    f.writelines(lines)
            state = os.path.join(d, "incrstate.json")
            with open(state, "w") as f:
                json.dump({"n": 1, "acked": {"s": [acked]}, "maybe": {"s": [0]}, "hist_s": [[]]}, f)
            cmd = [sys.executable, os.path.join(HERE, "incrtrace.py"), "--state", state, "--logs", d, "--nodes", "nch-n"]
            if kills is not None:
                path = os.path.join(d, "killed.txt")
                with open(path, "w") as f:
                    f.writelines(f"{name} {T0 + offset}\n" for name, offset in kills)
                cmd += ["--killed", path]
            out = subprocess.run(cmd, capture_output=True, text=True, check=True).stdout
            return json.loads(out.strip().splitlines()[-1])

    def lost_tail(self):
        # n1 applied 1..5 and was SIGKILLed at +10 s; the line of its sixth
        # increment (5 -> 6), acknowledged to the loader, never reached the
        # log. n2, the replica that took over, applies 6 -> 7.
        return [incr(i, i, i + 1) for i in range(5)], [incr(20, 6, 7)]

    def test_without_a_kill_record_an_old_value_nothing_produced_is_reported(self):
        n1, n2 = self.lost_tail()
        summary = self.run_incrtrace(n1, n2, acked=7)
        self.assertEqual(summary["value_from_nowhere"], 1)

    def test_a_kill_and_an_acknowledged_increment_without_a_line_excuse_it(self):
        n1, n2 = self.lost_tail()
        summary = self.run_incrtrace(n1, n2, acked=7, kills=[("nch-n1", 10)])
        self.assertEqual(summary["value_from_nowhere"], 0)
        self.assertEqual(summary["value_from_nowhere_after_kill"], 1)

    def test_a_kill_does_not_excuse_a_jump_nothing_acknowledged_accounts_for(self):
        # Every acknowledged increment has its line (6 of 6), so the 5 -> 6 step
        # was no increment: a copy ahead of the counter, the phantom the check hunts.
        n1, n2 = self.lost_tail()
        summary = self.run_incrtrace(n1, n2, acked=6, kills=[("nch-n1", 10)])
        self.assertEqual(summary["value_from_nowhere"], 1)
        self.assertEqual(summary["value_from_nowhere_after_kill"], 0)

    def test_a_kill_after_the_increment_does_not_excuse_it(self):
        n1, n2 = self.lost_tail()
        summary = self.run_incrtrace(n1, n2, acked=7, kills=[("nch-n1", 60)])
        self.assertEqual(summary["value_from_nowhere"], 1)

    def test_a_jump_larger_than_the_missing_lines_is_not_excused(self):
        # Two increments (5 -> 6 -> 7) are missing but only one acknowledged
        # increment lacks a line.
        n1, _ = self.lost_tail()
        summary = self.run_incrtrace(n1, [incr(20, 7, 8)], acked=7, kills=[("nch-n1", 10)])
        self.assertEqual(summary["value_from_nowhere"], 1)


if __name__ == "__main__":
    unittest.main()
