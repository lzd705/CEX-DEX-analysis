import unittest
from decimal import Decimal as D
from scripts import uni_cex_v3_experiment as study
from tests import test_uni_experiments as fixtures


class UniCexV3Tests(unittest.TestCase):
    def test_calldata_uses_exact_output_for_buy_and_exact_input_for_sell(self):
        buy = study.quote_calldata('dex_buy', 10**18)
        sell = study.quote_calldata('dex_sell', 10**18)
        self.assertEqual(buy[:10], '0xbd21704a')
        self.assertEqual(sell[:10], '0xc6a5026a')
        self.assertEqual(int(buy[138:202], 16), 10**18)
        self.assertEqual(int(buy[202:266], 16), 3000)
        self.assertEqual(int(buy[266:330], 16), 0)
        self.assertEqual(buy[10:74][-40:], study.USDT[2:])
        self.assertEqual(sell[10:74][-40:], study.UNI[2:])

    def test_decoder_rejects_partial_malformed_and_impossible_quote(self):
        value = '0x' + ''.join(f'{n:064x}' for n in (1000000, 2**96, 3, 80000))
        self.assertEqual(study.decode_quote(value), (1000000, 2**96, 3, 80000))
        for bad in (value[:-2], '0xzz', '0x' + '0'*256):
            with self.assertRaises(ValueError): study.decode_quote(bad)

    def test_cashflow_does_not_double_charge_pool_fee_and_gas_is_additive(self):
        fixture = fixtures.UniExperimentsTests()
        book = fixture.book('binance', '6.03', '6.04')
        rules = fixture.rules('binance')
        r = study.route_cashflow(book, rules, '100', 'dex_buy', '0', '600', '1')
        self.assertEqual(D(r['net']), D('2'))
        self.assertEqual(D(r['cex_base_quantity']), D('100'))
        r2 = study.route_cashflow(book, rules, '100', 'dex_buy', '10', '600', '1')
        self.assertEqual(D(r2['net']), D('1.397'))

    def test_cex_buy_grosses_up_to_fixed_net_quantity(self):
        fixture = fixtures.UniExperimentsTests()
        book = fixture.book('binance', '5.99', '6')
        r = study.route_cashflow(book, fixture.rules('binance'), '100', 'dex_sell', '10', '603', '1')
        self.assertEqual(D(r['cex_base_quantity']), D('100'))
        self.assertEqual(D(r['cex_cash']), D('600.66'))
        self.assertEqual(D(r['net']), D('1.34'))

    def test_rpc_ids_reordered_but_duplicate_or_error_cannot_pass(self):
        req = [{'id':1}, {'id':2}]
        good = [{'jsonrpc':'2.0','id':2,'result':'b'}, {'jsonrpc':'2.0','id':1,'result':'a'}]
        self.assertEqual(study.rpc_results(req, good), ['a','b'])
        with self.assertRaises(ValueError): study.rpc_results(req, [good[0], good[0]])
        values = study.rpc_results(req, [good[0], {'jsonrpc':'2.0','id':1,'error':{'code':-1}}])
        self.assertIsNone(values[0])

    def test_gate_and_exact_followup_do_not_skip_missing_or_bad_block(self):
        row = {'status':'observed', 'block_number':10, 'block_hash':'a', 'canonical_hash':'a',
               'block_age_seconds':10, 'cex_age_seconds':1, 'capture_seconds':2, 'cex_request_seconds':0.5}
        self.assertTrue(study.pair_gate(row)[0])
        bad = {**row, 'block_number':11, 'canonical_hash':'b'}
        self.assertFalse(study.pair_gate(bad)[0])
        records = [row, bad, {**row, 'block_number':13}]
        self.assertEqual(study.exact_followup(records, row, 1), bad)
        self.assertIsNone(study.exact_followup(records, row, 2))

    def test_delayed_cex_first_uses_initial_cashflow_not_later_cex_price(self):
        self.assertEqual(study.cex_first_net('dex_buy','603','650','1'), D('-48'))
        self.assertEqual(study.cex_first_net('dex_sell','600','650','1'), D('49'))

    def test_network_gas_is_a_scenario_not_the_quoter_estimate(self):
        row={'fee_history':{'oldestBlock':'0xa','baseFeePerGas':['0x1','0x64'],'reward':[['0xa']]},
             'block_number':10, 'eth_book':{'request_seconds':0.5,'received_mono':10,
                                          'bids':[['1999','1']],'asks':[['2001','1']]},'decision_mono':11}
        # (2*100 + 10) wei * assumed 200000 units * 2000 USDT/ETH.
        self.assertEqual(study.network_gas_scenario(row, 200000), D('0.000000084'))
        row['eth_book']['received_mono']=0
        self.assertIsNone(study.network_gas_scenario(row, 200000))

    def test_bad_pool_authority_is_rejected(self):
        values=['0x'+study.UNI[2:].zfill(64),'0x'+study.USDT[2:].zfill(64),'0x'+f'{3000:064x}',
                '0x'+study.FACTORY[2:].zfill(64),'0x'+f'{2**96:064x}'+'0'*384,'0x01',
                '0x'+study.POOL[2:].zfill(64),'0x'+study.FACTORY[2:].zfill(64),'0x12','0x06']
        study.validate_authority(values)
        values[2]='0x01'
        with self.assertRaises(ValueError): study.validate_authority(values)

    def test_followup_never_uses_an_earlier_visit_to_target_height(self):
        rows=[{'index':i,'block_number':n} for i,n in enumerate((100,101,100,101))]
        self.assertEqual(study.exact_followup(rows,rows[2],1)['index'],3)


if __name__ == '__main__': unittest.main()
