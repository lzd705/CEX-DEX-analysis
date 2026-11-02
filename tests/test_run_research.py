import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class RunResearchTests(unittest.TestCase):
    def module(self):
        path = ROOT/'scripts/run_research.py'
        self.assertTrue(path.is_file(), 'research dispatcher is not implemented')
        spec = importlib.util.spec_from_file_location('hub_dispatch', path)
        module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
        return module

    def fixture(self, folder, failing=False):
        root = Path(folder)/'project'
        tests = root/'research/v3_exact/tests'; tests.mkdir(parents=True)
        scripts = root/'research/v3_exact/scripts'; scripts.mkdir()
        (scripts/'marker.py').write_text('VALUE = 42\n')
        (tests/'test_uniswap_v3_math.py').write_text(
            'import unittest\nfrom scripts.marker import VALUE\n'
            'class TestIsolation(unittest.TestCase):\n'
            f'    def test_value(self): self.assertEqual(VALUE, {0 if failing else 42})\n')
        return root

    def test_list_requires_no_modules_or_child_process(self):
        module = self.module()
        with tempfile.TemporaryDirectory() as folder, contextlib.redirect_stdout(io.StringIO()) as out:
            self.assertEqual(module.main(['list'], project_root=Path(folder)), 0)
        self.assertEqual(set(json.loads(out.getvalue())['commands']),
                         {'v3-math','shib-replay','uni-v3-replay','historical-demo'})

    def test_unknown_command_is_rejected(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as caught:
            self.module().main(['shell'])
        self.assertEqual(caught.exception.code, 2)

    def test_v3_uses_its_own_cwd_and_ignores_poisoned_pythonpath(self):
        module = self.module()
        with tempfile.TemporaryDirectory() as folder:
            root = self.fixture(folder)
            poison = Path(folder)/'poison'; poison.mkdir()
            (poison/'scripts.py').write_text('raise RuntimeError("wrong source tree")')
            old = os.environ.get('PYTHONPATH'); os.environ['PYTHONPATH'] = str(poison)
            try:
                self.assertEqual(module.main(['v3-math'], project_root=root), 0)
            finally:
                if old is None: os.environ.pop('PYTHONPATH', None)
                else: os.environ['PYTHONPATH'] = old

    def test_child_failure_is_propagated(self):
        module = self.module()
        with tempfile.TemporaryDirectory() as folder:
            self.assertEqual(module.main(['v3-math'], project_root=self.fixture(folder, True)), 1)

    def test_shib_replay_reproduces_retained_snapshot(self):
        module = self.module()
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder)/'result'
            self.assertEqual(module.main(['shib-replay','--output-dir',str(output)]), 0)
            expected = ROOT/'research/shib_v2v2/data/public/research/shib-v2v2/latest.json'
            self.assertEqual((output/'snapshot.json').read_bytes(), expected.read_bytes())
