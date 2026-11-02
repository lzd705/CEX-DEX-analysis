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

    def test_contradictory_declared_identity_is_rejected_without_output(self):
        with tempfile.TemporaryDirectory() as folder:
            capture=Path(folder)/'capture';shutil.copytree(CAPTURE,capture)
            path=capture/'manifest.json';manifest=json.loads(path.read_text())
            manifest.update(chain_id=137,quoter='0x'+'0'*40)
            path.write_text(json.dumps(manifest));output=Path(folder)/'result'
            with self.assertRaisesRegex(ValueError,'identity'):
                self.replay(capture,REFERENCE,output)
            self.assertFalse(output.exists())

    def test_schema_pool_fee_and_boolean_chain_id_are_rejected(self):
        from scripts.analyze_uni_cex_v3_experiment import verify_capture
        with tempfile.TemporaryDirectory() as folder:
            capture=Path(folder)/'capture';shutil.copytree(CAPTURE,capture)
            path=capture/'manifest.json';original=json.loads(path.read_text())
            for field,value in (('schema','other/v1'),('pool','0x'+'0'*40),
                                ('pool_fee_pips',500),('chain_id',True),('strict_eligible',True)):
                with self.subTest(field=field):
                    path.write_text(json.dumps({**original,field:value}))
                    with self.assertRaisesRegex(ValueError,'identity'):
                        verify_capture(capture)

    def test_wrong_or_missing_retained_chain_proof_is_rejected(self):
        from scripts.analyze_uni_cex_v3_experiment import verify_capture
        with tempfile.TemporaryDirectory() as folder:
            capture=Path(folder)/'capture';shutil.copytree(CAPTURE,capture)
            path=capture/'manifest.json';original=json.loads(path.read_text())
            index=next(i for i,m in enumerate(original['rpc_records'])
                       if any(r['method']=='eth_chainId' for r in m['request']))
            member=original['rpc_records'][index];raw_path=capture/member['path']
            raw=raw_path.read_bytes();response=json.loads(raw)
            chain_request=next(r for r in member['request'] if r['method']=='eth_chainId')
            for value in response:
                if value['id']==chain_request['id']: value['result']='0x89'
            mutated=json.dumps(response).encode();raw_path.write_bytes(mutated)
            manifest=json.loads(json.dumps(original));manifest['rpc_records'][index]['sha256']=hashlib.sha256(mutated).hexdigest()
            path.write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError,'chain'):
                verify_capture(capture)
            raw_path.write_bytes(raw);manifest=json.loads(json.dumps(original))
            for request in manifest['rpc_records'][index]['request']:
                if request['method']=='eth_chainId': request['method']='eth_blockNumber'
            path.write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError,'chain'):
                verify_capture(capture)
