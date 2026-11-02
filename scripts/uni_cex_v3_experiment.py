#!/usr/bin/env python3
"""Read-only, non-publishing UNI Binance / Uniswap V3 research capture."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal as D
from functools import partial
import json
from pathlib import Path
import re
import ssl
import sys
import time
import urllib.request

if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.fetch_cex_depth import (fetch_source_book, request_json,
    _typed_market_rules_request, _binance_market_rules_source)
from scripts.route_quantity import CommonTarget, FeeSemantics, quote_cex_book_quantity
from scripts.uni_experiments import digest, utc, make_rules, make_target, capture_book, NOTIONALS

RPC_URL = 'https://ethereum-rpc.publicnode.com'
POOL = '0x3470447f3cecffac709d3e783a307790b0208d60'
UNI = '0x1f9840a85d5af5bf1d1762f925bdaddc4201f984'
USDT = '0xdac17f958d2ee523a2206206994597c13d831ec7'
FACTORY = '0x1f98431c8ad98523631ae4a59f267346ea31f984'
QUOTER = '0x61ffe014ba17989e743c5f6cb21bf9697530b21e'
FEES = ('0', '1', '2', '5', '10', '20')
GAS_USDT = ('0', '0.1', '0.5', '1', '2', '5', '10')


def word(n):
    if type(n) is not int or not 0 <= n < 2**256:
        raise ValueError('invalid ABI integer')
    return f'{n:064x}'


def quote_calldata(direction, raw_quantity):
    if direction not in ('dex_buy', 'dex_sell') or raw_quantity <= 0:
        raise ValueError('invalid quote direction or amount')
    buy = direction == 'dex_buy'
    return ('0xbd21704a' if buy else '0xc6a5026a') + ''.join((
        (USDT if buy else UNI)[2:].zfill(64), (UNI if buy else USDT)[2:].zfill(64),
        word(raw_quantity), word(3000), word(0)))


def decode_quote(value):
    if not isinstance(value, str) or not re.fullmatch(r'0x[0-9a-fA-F]{256}', value):
        raise ValueError('invalid QuoterV2 ABI result')
    result = tuple(int(value[i:i+64], 16) for i in range(2, 258, 64))
    amount, price, ticks, gas = result
    if not amount or not 0 < price < 2**160 or ticks >= 2**32 or not gas:
        raise ValueError('invalid QuoterV2 values')
    return result


def target_from_quantity(quantity, rules):
    raw = D(quantity) * 10**rules.base_unit_decimals
    lattice = rules.base_increment * 10**rules.base_unit_decimals
    if raw != raw.to_integral_value() or lattice != lattice.to_integral_value():
        raise ValueError('inexact target base units')
    return CommonTarget('UNI', rules.base_unit_decimals, int(raw), int(lattice))


def route_cashflow(book, rules, quantity, direction, fee_bps, dex_usdt, gas_usdt):
    if direction not in ('dex_buy', 'dex_sell'):
        raise ValueError('invalid direction')
    cex_side = 'sell' if direction == 'dex_buy' else 'buy'
    fee = FeeSemantics(rate_bps=D(fee_bps), fee_asset='USDT' if cex_side == 'sell' else 'UNI',
        charge_basis='received_quote' if cex_side == 'sell' else 'received_base',
        fee_increment=rules.quote_increment if cex_side == 'sell' else rules.base_increment,
        rounding_mode='ceiling', third_asset_quote_price=None,
        observed_at=rules.observed_at, valid_until=rules.valid_until,
        source_record_sha256=digest(('research-fee:' + str(fee_bps)).encode()),
        conversion_source_record_sha256=None)
    quoted = quote_cex_book_quantity(
        [(D(p), D(q)) for p,q in book['bids' if cex_side == 'sell' else 'asks']],
        target_from_quantity(quantity, rules), rules, fee, direction=cex_side,
        source_quote_asset='USDT', full_book_reported=False, state_id='research:binance')
    if not quoted.complete or dex_usdt is None:
        return {'net':None, 'reason':quoted.reason_code if not quoted.complete else 'dex_quote_unavailable'}
    dex, gas = D(dex_usdt), D(gas_usdt)
    if not dex.is_finite() or dex <= 0 or not gas.is_finite() or gas < 0:
        raise ValueError('invalid cashflow')
    cash = quoted.quote_received_quantity if cex_side == 'sell' else quoted.quote_debit_quantity
    pre_gas = cash-dex if cex_side == 'sell' else dex-cash
    base = quoted.base_debit_quantity if cex_side == 'sell' else quoted.net_base_received_quantity
    cost = dex if cex_side == 'sell' else cash
    return {'net':str(pre_gas-gas), 'pre_gas_net':str(pre_gas), 'gas_usdt':str(gas),
        'bps':str((pre_gas-gas)/cost*10000), 'cex_cash':str(cash),
        'cex_base_quantity':str(base), 'cex_fee':str(quoted.fee_debit_quantity),
        'cex_fee_asset':quoted.fee_debit_asset, 'cex_levels':quoted.levels_or_ticks_consumed,
        'dex_cash':str(dex), 'reason':None}


def pair_gate(row):
    if row.get('status') != 'observed': return False, row.get('reason', 'capture_failed')
    if row['block_hash'] != row['canonical_hash']: return False, 'reorganized_block'
    if row['cex_request_seconds'] > 2: return False, 'cex_request_over_2s'
    if not 0 <= row['cex_age_seconds'] <= 5: return False, 'cex_age_over_5s'
    if row['capture_seconds'] > 8: return False, 'capture_over_8s'
    if not -1 <= row['block_age_seconds'] <= 30: return False, 'block_age_outside_bounds'
    return True, None


def exact_followup(records, origin, offset):
    position=records.index(origin)
    return next((r for r in records[position+1:] if r.get('block_number') == origin['block_number']+offset), None)


def cex_first_net(direction, initial_cex_cash, future_dex_cash, gas_usdt):
    cex, dex, gas = D(initial_cex_cash), D(future_dex_cash), D(gas_usdt)
    if direction not in ('dex_buy','dex_sell'): raise ValueError('invalid direction')
    return (cex-dex if direction=='dex_buy' else dex-cex)-gas


def network_gas_scenario(row, assumed_units):
    """Observed fee inputs plus assumed full-tx units; never measured tx gas."""
    try:
        history, eth = row['fee_history'], row['eth_book']
        if int(history['oldestBlock'],16)!=row['block_number'] or len(history['baseFeePerGas'])!=2:
            return None
        if eth['request_seconds']>2 or not 0<=row['decision_mono']-eth['received_mono']<=5:
            return None
        cap=2*int(history['baseFeePerGas'][-1],16)+int(history['reward'][0][0],16)
        price=(D(eth['bids'][0][0])+D(eth['asks'][0][0]))/2
        if cap<=0 or price<=0 or assumed_units<=0: return None
        return D(cap)*assumed_units*price/D(10**18)
    except (KeyError,TypeError,ValueError,IndexError): return None


def rpc_results(requests, response):
    if not isinstance(response, list) or len(response) != len(requests):
        raise ValueError('incomplete RPC batch')
    by_id = {}
    for r in response:
        if not isinstance(r, dict) or r.get('jsonrpc') != '2.0' or type(r.get('id')) is not int or r['id'] in by_id:
            raise ValueError('invalid or duplicate RPC id')
        by_id[r['id']] = r
    if set(by_id) != {r['id'] for r in requests}: raise ValueError('RPC ids differ')
    return [None if 'error' in by_id[r['id']] else by_id[r['id']].get('result') for r in requests]


class PublicRpc:
    """Fixed public endpoint; allowlisted read methods; raw bytes retained."""
    def __init__(self, root):
        self.root, self.records = root, []
        self.context = ssl.create_default_context(cafile='/etc/ssl/cert.pem')

    def batch(self, calls):
        allowed = {'eth_chainId','eth_getBlockByNumber','eth_call','eth_feeHistory','eth_getCode'}
        if any(method not in allowed for method, params in calls): raise ValueError('non-read RPC forbidden')
        payload = [{'jsonrpc':'2.0','id':i+1,'method':method,'params':params}
                   for i,(method,params) in enumerate(calls)]
        start = time.monotonic()
        entry = {'request':payload, 'started_at':utc()}
        try:
            request = urllib.request.Request(RPC_URL, data=json.dumps(payload).encode(),
                headers={'Content-Type':'application/json','User-Agent':'UNI-readonly-research/1'})
            with urllib.request.urlopen(request, context=self.context, timeout=5) as response:
                raw = response.read(8*1024*1024+1)
            if len(raw)>8*1024*1024: raise ValueError('RPC response too large')
            filename = f'raw/rpc-{len(self.records):04d}.json'
            (self.root/filename).write_bytes(raw)
            entry.update(path=filename, sha256=digest(raw))
            return rpc_results(payload, json.loads(raw))
        except Exception as error:
            entry['error_type'] = type(error).__name__
            raise
        finally:
            entry.update(received_at=utc(), elapsed_seconds=time.monotonic()-start)
            self.records.append(entry)

    def one(self, method, params):
        value = self.batch([(method,params)])[0]
        if value is None: raise ValueError('RPC method failed')
        return value


def eth_call(to, data, block):
    return ('eth_call',[{'to':to,'data':data}, block])


def authority_calls(block):
    return [eth_call(POOL, selector, block) for selector in
            ('0x0dfe1681','0xd21220a7','0xddca3f43','0xc45a0155','0x3850c7bd','0x1a686502')] + [
        eth_call(FACTORY, '0x1698ee82'+UNI[2:].zfill(64)+USDT[2:].zfill(64)+word(3000), block),
        eth_call(QUOTER,'0xc45a0155',block), eth_call(UNI,'0x313ce567',block), eth_call(USDT,'0x313ce567',block)]


def validate_authority(values):
    if len(values)!=10 or any(v is None for v in values): raise ValueError('pool identity unavailable')
    expected = (UNI,USDT,3000,FACTORY)
    actual = (('0x'+values[0][-40:]).lower(), ('0x'+values[1][-40:]).lower(),
              int(values[2],16), ('0x'+values[3][-40:]).lower())
    if actual != expected or ('0x'+values[6][-40:]).lower()!=POOL or ('0x'+values[7][-40:]).lower()!=FACTORY:
        raise ValueError('pool authority mismatch')
    if (int(values[8],16),int(values[9],16))!=(18,6): raise ValueError('token decimals mismatch')
    if len(values[4]) != 2+7*64 or int(values[4][2:66],16)<=0 or int(values[5],16)<=0:
        raise ValueError('pool state unavailable')


def capture_rules(root, label):
    url = _typed_market_rules_request('binance','UNIUSDT')
    value, raw = request_json(url,timeout_seconds=5,max_retries=1)
    filename = 'raw/'+label+'-binance-rules.json'
    (root/filename).write_bytes(raw)
    observed=utc()
    return {'path':filename,'sha256':digest(raw),'observed_at':observed,'url':url,
        'projection':_binance_market_rules_source(value, market={'exchange':'binance','cex_symbol':'UNI/USDT'},
                                                source_instrument='UNIUSDT', observed_at=observed)}


def capture_eth(root,index):
    start=time.monotonic()
    book=fetch_source_book('binance','ETH/USDT',request=partial(request_json,timeout_seconds=4,max_retries=1))
    filename=f'raw/{index:04d}-eth.json'
    (root/filename).write_bytes(book['raw'])
    return {'path':filename,'sha256':digest(book['raw']),'received_at':utc(),
        'received_mono':time.monotonic(),'request_seconds':time.monotonic()-start,
        'bids':[[str(p),str(q)] for p,q in book['bids']], 'asks':[[str(p),str(q)] for p,q in book['asks']]}


def collect(root, polls=60, interval=10):
    root.mkdir(parents=True,exist_ok=False); (root/'raw').mkdir()
    rpc=PublicRpc(root)
    manifest={'schema':'uni_cex_v3_research/v1','started_at':utc(),'polls':polls,'interval_seconds':interval,
        'pool':POOL,'quoter':QUOTER,'chain_id':1,'pool_fee_pips':3000,'rpc_url':RPC_URL,
        'gas_status':'assumed_USDT_or_assumed_units_with_observed_network_parameters',
        'strict_eligible':False,'rules_start':capture_rules(root,'start')}
    rules=make_rules(manifest['rules_start']['projection'],manifest['rules_start']['sha256'])
    if int(rpc.one('eth_chainId',[]),16)!=1: raise ValueError('wrong chain')
    head=rpc.one('eth_getBlockByNumber',['latest',False])
    values=rpc.batch(authority_calls({'blockHash':head['hash'],'requireCanonical':True}))
    validate_authority(values)
    manifest['authority_start']=values
    fee_path=Path(__file__).resolve().parents[1]/'config/cex_public_fee_schedules_interview_2026-11-01.csv'
    fee_raw=fee_path.read_bytes()
    (root/'public-fees.csv').write_bytes(fee_raw)
    manifest['fee_evidence_sha256']=digest(fee_raw)
    manifest['targets']={}
    (root/'manifest.json').write_text(json.dumps(manifest,indent=2))
    seen_hashes=set(); highest_number=-1; start=time.monotonic(); observed=0
    with ThreadPoolExecutor(max_workers=2) as workers, (root/'samples.jsonl').open('x') as handle:
        for index in range(polls):
            wait=start+index*interval-time.monotonic()
            if wait>0: time.sleep(wait)
            began=time.monotonic(); record_start=len(rpc.records)
            row={'index':index,'status':'failed','started_at':utc()}
            try:
                head=rpc.one('eth_getBlockByNumber',['latest',False])
                row.update(block_number=int(head['number'],16),block_hash=head['hash'],block_timestamp=int(head['timestamp'],16))
                if head['hash'] in seen_hashes:
                    row.update(status='duplicate_head',reason='same_block_not_new_observation')
                elif row['block_number']<=highest_number:
                    row.update(status='regressed_head',reason='non_forward_chain_head')
                else:
                    seen_hashes.add(head['hash']); highest_number=row['block_number']
                    uni_future=workers.submit(capture_book,root,index,'binance')
                    eth_future=workers.submit(capture_eth,root,index)
                    book=uni_future.result(); row['book']=book
                    if book['status']!='observed': raise ValueError('Binance book unavailable')
                    try: row['eth_book']=eth_future.result()
                    except Exception as error: row['eth_error']=type(error).__name__
                    if not manifest['targets']:
                        manifest['targets']={n:str(make_target(book,book,{'binance':rules},n).quantity) for n in NOTIONALS}
                    block={'blockHash':head['hash'],'requireCanonical':True}
                    calls=[eth_call(QUOTER,quote_calldata(direction,int(D(quantity)*10**18)),block)
                           for quantity in manifest['targets'].values() for direction in ('dex_buy','dex_sell')]
                    results=rpc.batch(calls)
                    row['quotes']={}
                    for j,(n,q) in enumerate(manifest['targets'].items()):
                        row['quotes'][n]={}
                        for k,direction in enumerate(('dex_buy','dex_sell')):
                            try:
                                amount,price,ticks,gas=decode_quote(results[2*j+k])
                                row['quotes'][n][direction]={'usdt':str(D(amount)/10**6),
                                    'sqrt_price_after':price,'initialized_ticks_crossed':ticks,'quoter_gas_only':gas}
                            except (TypeError,ValueError): row['quotes'][n][direction]=None
                    row['fee_history']=rpc.one('eth_feeHistory',['0x1',head['number'],[50]])
                    canonical=rpc.one('eth_getBlockByNumber',[head['number'],False])
                    now=time.monotonic()
                    row.update(status='observed',canonical_hash=canonical['hash'],
                        cex_request_seconds=book['request_seconds'],cex_age_seconds=now-book['received_mono'],
                        block_age_seconds=datetime.now(timezone.utc).timestamp()-row['block_timestamp'],
                        capture_seconds=now-began,decision_mono=now,decision_at=utc())
                    observed+=1
            except Exception as error:
                row.update(reason='capture_failed',error_type=type(error).__name__,capture_seconds=time.monotonic()-began)
            row['rpc_records']=[record_start,len(rpc.records)]
            handle.write(json.dumps(row,separators=(',',':'))+'\n'); handle.flush()
            if index%3==0:
                print(json.dumps({'poll':index+1,'planned':polls,'observed':observed,'status':row['status'],
                                  'gate':pair_gate(row),'at':utc()}),flush=True)
    manifest['rules_end']=capture_rules(root,'end')
    final_head=rpc.one('eth_getBlockByNumber',['latest',False])
    final_authority=rpc.batch(authority_calls({'blockHash':final_head['hash'],'requireCanonical':True}))
    validate_authority(final_authority); manifest['authority_end']=final_authority
    manifest['rpc_records']=rpc.records
    manifest['completed_at']=utc(); manifest['samples_sha256']=digest((root/'samples.jsonl').read_bytes())
    (root/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    print(json.dumps({'completed':str(root),'observed':observed}),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--polls',type=int,default=60,choices=range(2,91))
    args=parser.parse_args()
    collect(args.output,args.polls)
