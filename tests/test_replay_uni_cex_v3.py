import csv
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import socket
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
EXPERIMENT = ROOT/'experiments/uni_cex_v3_20261102'
CAPTURE = EXPERIMENT/'capture'
REFERENCE = EXPERIMENT/'results'


class ReplayUniCexV3Tests(unittest.TestCase):
    def replay(self, capture, reference, output):
        source = ROOT/'scripts/replay_uni_cex_v3.py'
        self.assertTrue(source.is_file(), 'offline replay is not implemented')
        spec = importlib.util.spec_from_file_location('uni_offline_replay', source)
        module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
        return module.replay_capture(capture, reference, output)

    def hashes(self, folder):
        return {str(p.relative_to(folder)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in folder.rglob('*') if p.is_file()}

    def test_existing_output_is_rejected_before_any_write(self):
        with tempfile.TemporaryDirectory() as folder:
            output=Path(folder); (output/'keep').write_text('unchanged')
            with self.assertRaisesRegex(ValueError,'output'):
                self.replay(CAPTURE,REFERENCE,output)
            self.assertEqual((output/'keep').read_text(),'unchanged')
            self.assertEqual(len(list(output.iterdir())),1)

    def test_capture_results_source_and_parent_outputs_are_rejected(self):
        for output in (CAPTURE/'generated',REFERENCE/'generated',ROOT/'generated',ROOT.parent):
            with self.subTest(output=output), self.assertRaisesRegex(ValueError,'output'):
                self.replay(CAPTURE,REFERENCE,output)
            if output.name=='generated': self.assertFalse(output.exists())

    def test_symlink_alias_to_capture_is_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            alias=Path(folder)/'alias'; alias.symlink_to(CAPTURE,target_is_directory=True)
            with self.assertRaisesRegex(ValueError,'output'):
                self.replay(CAPTURE,REFERENCE,alias/'generated')
            self.assertFalse((CAPTURE/'generated').exists())

    def test_tampered_samples_create_no_output(self):
        with tempfile.TemporaryDirectory() as folder:
            capture=Path(folder)/'capture'; capture.mkdir()
            shutil.copy2(CAPTURE/'manifest.json',capture/'manifest.json')
            (capture/'samples.jsonl').write_text('tampered')
            output=Path(folder)/'result'
            with self.assertRaisesRegex(ValueError,'hash'):
                self.replay(capture,REFERENCE,output)
            self.assertFalse(output.exists())

    def test_missing_raw_member_creates_no_output(self):
        with tempfile.TemporaryDirectory() as folder:
            capture=Path(folder)/'capture'; shutil.copytree(CAPTURE,capture)
            (capture/'raw/start-binance-rules.json').unlink()
            output=Path(folder)/'result'
            with self.assertRaises(ValueError):
                self.replay(capture,REFERENCE,output)
            self.assertFalse(output.exists())

    def test_complete_replay_is_offline_exact_and_preserves_inputs(self):
        before=(self.hashes(CAPTURE),self.hashes(REFERENCE))
        with tempfile.TemporaryDirectory() as folder:
            output=Path(folder)/'result'
            with patch.object(socket.socket,'connect',side_effect=AssertionError('offline replay attempted a connection')):
                result=self.replay(CAPTURE,REFERENCE,output)
            self.assertEqual(result['polls'],60)
            self.assertEqual(result['accepted_blocks'],51)
            self.assertEqual(result['dex_quotes'],510)
            self.assertEqual(result['fee_gas_rows'],21420)
            self.assertEqual(result['delay_rows'],13770)
            self.assertEqual(result['positive_fee_gas_rows'],0)
            self.assertTrue(result['reference_byte_identical'])
            for path in REFERENCE.glob('*'):
                self.assertEqual((output/path.name).read_bytes(),path.read_bytes())
        self.assertEqual((self.hashes(CAPTURE),self.hashes(REFERENCE)),before)

    def test_reference_difference_is_reported(self):
        with tempfile.TemporaryDirectory() as folder:
            reference=Path(folder)/'reference'; shutil.copytree(REFERENCE,reference)
            summary=json.loads((reference/'summary.json').read_text()); summary['accepted_blocks']=999
            (reference/'summary.json').write_text(json.dumps(summary))
            with self.assertRaisesRegex(ValueError,'reference'):
                self.replay(CAPTURE,reference,Path(folder)/'result')
