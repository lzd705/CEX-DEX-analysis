#!/usr/bin/env python3
"""Verify and replay the retained UNI CEX/V3 research capture, offline."""
import argparse
from collections import Counter, defaultdict
import csv
from datetime import datetime
from decimal import Decimal as D
import json
from pathlib import Path
from statistics import median
import sys

if __package__ in {None,''}:
    sys.path.insert(0,str(Path(__file__).resolve().parents[1]))

from scripts.uni_cex_v3_experiment import (POOL, QUOTER, FEES, GAS_USDT, quote_calldata,
    decode_quote, route_cashflow, pair_gate, exact_followup, rpc_results, authority_calls,
    validate_authority, cex_first_net, network_gas_scenario)
from scripts.uni_experiments import digest, make_rules, make_target
from scripts.fetch_cex_depth import parse_book, _binance_market_rules_source
from scripts.analyze_uni_experiments import write_csv


def stats(values):
    values=[float(v) for v in values]
    return {'n':len(values),'positive':sum(v>0 for v in values),
        'min':min(values) if values else None, 'median':median(values) if values else None,
        'max':max(values) if values else None}


def read_verified_member(root,member):
    path=Path(member['path'])
    if path.is_absolute() or '..' in path.parts: raise ValueError('unsafe member path')
    value=(root/path).read_bytes()
    if digest(value)!=member['sha256']: raise ValueError('raw member hash differs')
    return value


def mark_repeated_heads(records):
    seen=set(); highest=-1; result=[]
    for source in records:
        row=dict(source)
        if 'block_hash' in row:
            if row['status']=='observed' and (row['block_hash'] in seen or row['block_number']<=highest):
                row.update(status='repeated_or_regressed',reason='repeated_or_regressed_chain_head')
            seen.add(row['block_hash']); highest=max(highest,row['block_number'])
        result.append(row)
    return result


def verify_capture(root):
    manifest=json.loads((root/'manifest.json').read_text())
    identity={'schema':'uni_cex_v3_research/v1','chain_id':1,'pool':POOL,
              'quoter':QUOTER,'pool_fee_pips':3000,'strict_eligible':False}
    for field,expected in identity.items():
        actual=manifest.get(field)
        if type(actual) is not type(expected) or actual!=expected:
            raise ValueError('capture identity mismatch: '+field)
    samples=(root/'samples.jsonl').read_bytes()
    if not manifest.get('completed_at') or digest(samples)!=manifest.get('samples_sha256'):
        raise ValueError('capture incomplete or samples hash differs')
    rows=[json.loads(line) for line in samples.splitlines()]
    if [r['index'] for r in rows]!=list(range(manifest['polls'])): raise ValueError('capture slots differ')

    def raw(member): return read_verified_member(root,member)

    for role in ('rules_start','rules_end'):
        member=manifest[role]
        projected=_binance_market_rules_source(json.loads(raw(member)),
            market={'exchange':'binance','cex_symbol':'UNI/USDT'},source_instrument='UNIUSDT',
            observed_at=member['observed_at'])
        if projected!=member['projection']: raise ValueError('rules differ from source')
    semantic=lambda x:{k:v for k,v in x.items() if k not in ('observed_at','valid_until')}
    if semantic(manifest['rules_start']['projection'])!=semantic(manifest['rules_end']['projection']):
        raise ValueError('market rules changed')
    rules=make_rules(manifest['rules_start']['projection'],manifest['rules_start']['sha256'])
    fee_raw=(root/'public-fees.csv').read_bytes()
    if digest(fee_raw)!=manifest['fee_evidence_sha256']: raise ValueError('fee evidence hash differs')
    schedule=next(r for r in csv.DictReader(fee_raw.decode().splitlines()) if r['venue']=='binance' and r['instrument_pattern']=='UNI/USDT')
    date=lambda s:datetime.fromisoformat(s.replace('Z','+00:00'))
    if not date(schedule['checked_at'])<=date(manifest['started_at'])<=date(manifest['completed_at'])<=date(schedule['valid_until']):
        raise ValueError('fee evidence expired or does not cover experiment')
    if schedule['max_taker_fee_bps']!='10': raise ValueError('public reference fee differs')

    rpc=[]
    for member in manifest['rpc_records']:
        rpc.append(None if 'path' not in member else rpc_results(member['request'],json.loads(raw(member))))
    chain_proofs=[(request.get('params'),value)
                  for member,values in zip(manifest['rpc_records'],rpc) if values is not None
                  for request,value in zip(member['request'],values)
                  if request['method']=='eth_chainId']
    if not chain_proofs or any(params!=[] or value!='0x1' for params,value in chain_proofs):
        raise ValueError('Ethereum chain identity evidence missing or conflicting')
    for role in ('authority_start','authority_end'):
        authority=manifest[role]; validate_authority(authority)
        if not any(values==authority and member['request'][0]['params'][0].get('to')==POOL
                   for member,values in zip(manifest['rpc_records'],rpc)
                   if values is not None and len(values)==10 and member['request'][0]['method']=='eth_call'):
            raise ValueError('authority differs from retained RPC')
    anchor=None
    for row in rows:
        for key,symbol in (('book','UNIUSDT'),('eth_book','ETHUSDT')):
            book=row.get(key)
            if not book or book.get('status','observed')!='observed': continue
            parsed=parse_book('binance',json.loads(raw(book)),requested_instrument=symbol)
            if any([[str(p),str(q)] for p,q in parsed[side]]!=book[side] for side in ('bids','asks')):
                raise ValueError('normalized book differs from raw')
            if key=='book' and anchor is None: anchor=book
        if row['status']!='observed': continue
        start,end=row['rpc_records']; calls=[]
        head=canonical=history=None
        for member,values in zip(manifest['rpc_records'][start:end],rpc[start:end]):
            if values is None: continue
            for request,value in zip(member['request'],values):
                method,params=request['method'],request['params']
                if method=='eth_getBlockByNumber' and params==['latest',False]: head=value
                if method=='eth_getBlockByNumber' and params==[hex(row['block_number']),False]: canonical=value
                if method=='eth_feeHistory': history=value
                if method=='eth_call' and params[0]['to']==QUOTER: calls.append((params,value))
        if not head or head['hash']!=row['block_hash'] or int(head['number'],16)!=row['block_number'] or int(head['timestamp'],16)!=row['block_timestamp']:
            raise ValueError('head differs from retained RPC')
        if not canonical or canonical['hash']!=row['canonical_hash'] or history!=row['fee_history']:
            raise ValueError('canonical or network fee record differs')
        expected=[(n,direction,int(D(q)*10**18)) for n,q in manifest['targets'].items() for direction in ('dex_buy','dex_sell')]
        if len(calls)!=len(expected): raise ValueError('quote call inventory differs')
        for (params,value),(n,direction,q) in zip(calls,expected):
            if params!=[{'to':QUOTER,'data':quote_calldata(direction,q)},
                        {'blockHash':row['block_hash'],'requireCanonical':True}]:
                raise ValueError('quote request state/quantity differs')
            try:
                amount,price,ticks,gas=decode_quote(value)
                quote={'usdt':str(D(amount)/10**6),'sqrt_price_after':price,
                       'initialized_ticks_crossed':ticks,'quoter_gas_only':gas}
            except (ValueError,TypeError): quote=None
            if quote!=row['quotes'][n][direction]: raise ValueError('quote differs from raw RPC')
    if anchor is None: raise ValueError('no anchor book')
    if manifest['targets']!={n:str(make_target(anchor,anchor,{'binance':rules},n).quantity) for n in manifest['targets']}:
        raise ValueError('anchor quantities differ')
    return manifest,rows,rules


def analyze(records,rules,targets):
    records=mark_repeated_heads(records)
    rows,delays,network=[],[],[]
    rejected=Counter(); by_key={}
    for record in records:
        accepted,reason=pair_gate(record)
        if not accepted:
            rejected[reason]+=1; continue
        for n,q in targets.items():
            for direction in ('dex_buy','dex_sell'):
                quote=record['quotes'][n][direction]
                dex=None if quote is None else quote['usdt']
                for fee in FEES:
                    base=route_cashflow(record['book'],rules,q,direction,fee,dex,'0')
                    by_key[record['index'],n,direction,fee]=base
                    for gas in GAS_USDT:
                        result=dict(base)
                        if base['net'] is not None:
                            cost=D(base['dex_cash'] if direction=='dex_buy' else base['cex_cash'])
                            result.update(net=str(D(base['pre_gas_net'])-D(gas)),
                                          bps=str((D(base['pre_gas_net'])-D(gas))/cost*10000),gas_usdt=gas)
                        rows.append({'sample':record['index'],'at':record['decision_at'],
                            'block_number':record['block_number'],'notional':n,'quantity':q,
                            'direction':direction,'fee_bps':fee,'gas_usdt':gas,**result})
                    if fee=='10':
                        for units in (100000,200000,300000):
                            gas=network_gas_scenario(record,units)
                            network.append({'sample':record['index'],'notional':n,'direction':direction,
                                'assumed_gas_units':units,'fee_bps':fee,
                                'gas_usdt':None if gas is None else str(gas),
                                'net':None if gas is None or base['net'] is None else str(D(base['net'])-gas),
                                'status':'assumed_units_observed_fee_inputs' if gas is not None else 'network_inputs_unavailable'})
    for origin in records:
        if not pair_gate(origin)[0]: continue
        for offset in (1,2,3):
            future=exact_followup(records,origin,offset)
            reason='missing_exact_block_or_tail' if future is None else None
            if future is not None and not pair_gate(future)[0]: reason='future:'+pair_gate(future)[1]
            for n,q in targets.items():
                for direction in ('dex_buy','dex_sell'):
                    for fee in ('0','1','10'):
                        initial=by_key[origin['index'],n,direction,fee]
                        for gas in ('0','1','5'):
                            item={'sample':origin['index'],'notional':n,'quantity':q,'direction':direction,
                                'fee_bps':fee,'gas_usdt':gas,'blocks_later':offset,'reason':reason,
                                'initial_net':None if initial['net'] is None else str(D(initial['net'])-D(gas)),
                                'actual_delay_seconds':None if reason else future['decision_mono']-origin['decision_mono'],
                                'cex_first_net':None,'both_legs_net':None}
                            if not reason:
                                quote=future['quotes'][n][direction]
                                if initial['net'] is None: item['reason']='initial_unquotable'
                                elif quote is None: item['reason']='future_dex_quote_unavailable'
                                else:
                                    item['cex_first_net']=str(cex_first_net(direction,initial['cex_cash'],quote['usdt'],gas))
                                    simultaneous=route_cashflow(future['book'],rules,q,direction,fee,quote['usdt'],gas)
                                    item['both_legs_net']=simultaneous['net']
                            delays.append(item)
    groups=defaultdict(list)
    for row in rows: groups[row['notional'],row['direction'],row['fee_bps'],row['gas_usdt']].append(row)
    summaries=[]
    for (n,direction,fee,gas),members in groups.items():
        usable=[r for r in members if r['net'] is not None]
        summaries.append({'notional':n,'direction':direction,'fee_bps':fee,'gas_usdt':gas,
            'origins':len(members),'missing':len(members)-len(usable),
            'net_usdt':stats([r['net'] for r in usable]),'net_bps':stats([r['bps'] for r in usable])})
    groups=defaultdict(list)
    for row in delays: groups[row['notional'],row['direction'],row['fee_bps'],row['gas_usdt'],row['blocks_later']].append(row)
    delay_summary=[]
    for (n,direction,fee,gas,offset),members in groups.items():
        paired=[r for r in members if r['cex_first_net'] is not None]
        positive=[r for r in paired if r['initial_net'] is not None and D(r['initial_net'])>0]
        delay_summary.append({'notional':n,'direction':direction,'fee_bps':fee,'gas_usdt':gas,
            'blocks_later':offset,'origins':len(members),'paired':len(paired),
            'missing_reasons':dict(Counter(r['reason'] for r in members if r['reason'])),
            'initial_positive':sum(r['initial_net'] is not None and D(r['initial_net'])>0 for r in members),
            'positive_with_future':len(positive),'positive_at_future':sum(D(r['cex_first_net'])>0 for r in positive),
            'actual_delay_seconds':stats([r['actual_delay_seconds'] for r in paired]),
            'cex_first_net':stats([r['cex_first_net'] for r in paired]),
            'cex_first_change':stats([D(r['cex_first_net'])-D(r['initial_net']) for r in paired]),
            'both_legs_net':stats([r['both_legs_net'] for r in paired if r['both_legs_net'] is not None])})
    return rows,delays,{'rejected':dict(rejected),'fee_gas_summary':summaries,
        'delay_summary':delay_summary,'network_gas_scenarios':network}


def main():
    parser=argparse.ArgumentParser(description=__doc__); parser.add_argument('--capture',type=Path,required=True)
    args=parser.parse_args(); manifest,records,rules=verify_capture(args.capture)
    rows,delays,summary=analyze(records,rules,manifest['targets'])
    summary['capture']={k:manifest[k] for k in ('started_at','completed_at','samples_sha256','pool','pool_fee_pips','targets')}
    summary['polls']=len(records)
    summary['accepted_blocks']=sum(pair_gate(r)[0] for r in mark_repeated_heads(records))
    summary['observed_blocks']=sum(r['status']=='observed' for r in records)
    summary['block_age_seconds']=stats([r['block_age_seconds'] for r in records if r['status']=='observed'])
    summary['cex_age_seconds']=stats([r['cex_age_seconds'] for r in records if r['status']=='observed'])
    summary['gas_usdt_200k']=stats([str(v) for r in records if pair_gate(r)[0] and (v:=network_gas_scenario(r,200000)) is not None])
    out=args.capture/'analysis'; out.mkdir(exist_ok=True)
    write_csv(out/'fee_gas_timeseries.csv',rows); write_csv(out/'delay_pairs.csv',delays)
    write_csv(out/'network_gas_scenarios.csv',summary.pop('network_gas_scenarios'))
    (out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    print(json.dumps({'output':str(out),'fee_gas_rows':len(rows),'delay_rows':len(delays),'accepted_blocks':summary['accepted_blocks']}))


if __name__=='__main__': main()
