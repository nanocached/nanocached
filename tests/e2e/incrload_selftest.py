"""Self-test of incrload.py's serial-key classification. Run: python3 incrload_selftest.py

A lagging forward can rewind a serial counter, and a later forward carrying
the newer value can put it back before the next increment, so the answers go
up, down and up again: 4, 2, 5. The return is above the previous answer by
more than one but not above the highest answer so far, so it is an
is_restore, not an is_phantom. An answer above the highest answer plus the
increments that failed since that answer is still a phantom.

The sequences below are the answers two chaos cells (run 45934086635, cells 7
and 16) recorded, which the previous classification, measured against the
previous answer alone, counted as phantoms.
"""
import ast
import os
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def load_classify():
    """`classify_serial` alone: incrload.py parses its arguments when imported."""
    with open(os.path.join(HERE, "incrload.py")) as f:
        tree = ast.parse(f.read())
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "classify_serial")
    scope = {}
    exec(compile(ast.Module(body=[fn], type_ignores=[]), "incrload.py", "exec"), scope)
    return scope["classify_serial"]


classify_serial = load_classify()


def replay(events):
    """The anomalies a serial key's answers produce, kept the way is_turn keeps
    them: events are answers (int) or "err" for a failed increment."""
    prev, mark, maybes, best, out = None, 0, 0, None, []
    for e in events:
        if e == "err":
            maybes += 1
            continue
        allowed, mark = maybes - mark, maybes
        top = best
        if top is None or e > top[0]:
            best = (e, maybes)
        if prev is not None:
            kind = classify_serial(prev, e, allowed, top, maybes)
            if kind:
                out.append((kind, prev, e))
        prev = e
    return out


class SerialClassification(unittest.TestCase):
    def test_the_expected_sequence_has_no_anomaly(self):
        self.assertEqual(replay([1, 2, 3, 4, 5, 6]), [])

    def test_a_rewind_then_its_return_is_a_restore_not_a_phantom(self):
        # cell 7, is934: a forward rewound the primary to 1, the next one put 4 back
        self.assertEqual(replay([1, 2, 3, 4, 2, 5]), [("is_rewind", 4, 2), ("is_restore", 2, 5)])
        # cell 16, is1504: forwards from two old owners landing out of order
        self.assertEqual(replay([1, 2, 3, 3, 4, 5, 5, 6, 7, 6, 8]), [
            ("is_rewind", 3, 3), ("is_rewind", 5, 5), ("is_rewind", 7, 6), ("is_restore", 6, 8)])

    def test_an_answer_above_the_highest_answer_is_still_a_phantom(self):
        self.assertEqual(replay([1, 2, 3, 5]), [("is_phantom", 3, 5)])
        # after a rewind, the bound is the highest answer, not the previous one
        self.assertEqual(replay([1, 2, 3, 4, 2, 6]), [("is_rewind", 4, 2), ("is_phantom", 2, 6)])

    def test_failed_increments_since_the_highest_answer_allow_that_many_more(self):
        self.assertEqual(replay([1, 2, "err", 4]), [("is_skip", 2, 4)])
        self.assertEqual(replay([1, 2, 3, 4, 2, "err", 6]), [("is_rewind", 4, 2), ("is_restore", 2, 6)])
        self.assertEqual(replay([1, 2, 3, 4, 2, "err", 7]), [("is_rewind", 4, 2), ("is_phantom", 2, 7)])


if __name__ == "__main__":
    unittest.main()
