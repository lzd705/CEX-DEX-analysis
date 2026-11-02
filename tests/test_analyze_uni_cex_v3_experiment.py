import json
from pathlib import Path
import tempfile
import unittest
from decimal import Decimal
from scripts import analyze_uni_cex_v3_experiment as analysis
from tests import test_uni_experiments as fixtures


class AnalyzeUniCexV3Tests(unittest.TestCase):
    def test_repeated_or_regressed_heads_cannot_be_independent_observations(self):
        records=[{'index':i,'status':'observed','block_number':n,'block_hash':h}
                 for i,(n,h) in enumerate(((100,'a'),(101,'b'),(100,'a'),(101,'b'),(100,'c'),(102,'d')))]
        filtered=analysis.mark_repeated_heads(records)
        self.assertEqual([r['index'] for r in filtered if r['status']=='observed'],[0,1,5])

    def test_raw_hash_tampering_and_parent_paths_are_rejected(self):
        import hashlib
        with tempfile.TemporaryDirectory() as name:
            root=Path(name); (root/'raw.json').write_bytes(b'original')
            member={'path':'raw.json','sha256':hashlib.sha256(b'original').hexdigest()}
            self.assertEqual(analysis.read_verified_member(root,member),b'original')
            (root/'raw.json').write_bytes(b'tampered')
            with self.assertRaises(ValueError): analysis.read_verified_member(root,member)
            with self.assertRaises(ValueError): analysis.read_verified_member(root,{**member,'path':'../raw.json'})

    def test_missing_delay_is_not_a_loss_or_success_and_cex_first_is_frozen(self):
        fixture=fixtures.UniExperimentsTests()
        book=fixture.book('binance','6.03','6.04')
        origin={'index':0,'status':'observed','block_number':10,'block_hash':'a','canonical_hash':'a',
            'block_age_seconds':10,'cex_age_seconds':1,'capture_seconds':2,'cex_request_seconds':0.5,
            'decision_mono':10,'decision_at':'start','book':book,
            'quotes':{'1000':{'dex_buy':{'usdt':'600'},'dex_sell':{'usdt':'604'}}}}
        later={**origin,'index':1,'block_number':11,'block_hash':'b','canonical_hash':'b','decision_mono':22,'decision_at':'end',
               'book':fixture.book('binance','12.03','12.04'),
               'quotes':{'1000':{'dex_buy':{'usdt':'650'},'dex_sell':{'usdt':'654'}}}}
        rows, delays, summary=analysis.analyze([origin,later], fixture.rules('binance'),{'1000':'100'})
        select=lambda x: x['direction']=='dex_buy' and x['fee_bps']=='0' and x['gas_usdt']=='1'
        d=next(r for r in delays if select(r) and r['sample']==0 and r['blocks_later']==1)
        self.assertEqual(d['cex_first_net'],'-48')
        self.assertEqual(Decimal(d['both_legs_net']),Decimal('552'))
        tail=next(r for r in delays if select(r) and r['sample']==1 and r['blocks_later']==1)
        self.assertIsNone(tail['cex_first_net'])
        self.assertEqual(tail['reason'],'missing_exact_block_or_tail')
        group=next(r for r in summary['delay_summary'] if select(r) and r['blocks_later']==1)
        self.assertEqual(group['origins'],2)
        self.assertEqual(group['paired'],1)

    def test_incomplete_capture_cannot_be_analyzed(self):
        with tempfile.TemporaryDirectory() as name:
            root=Path(name)
            (root/'manifest.json').write_text(json.dumps({'schema':'uni_cex_v3_research/v1'}))
            (root/'samples.jsonl').write_text('')
            with self.assertRaises(ValueError): analysis.verify_capture(root)


if __name__=='__main__': unittest.main()
