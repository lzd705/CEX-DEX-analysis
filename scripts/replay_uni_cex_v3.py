#!/usr/bin/env python3
"""Verify and reproduce the published UNI/V3 experiment entirely offline."""
import argparse
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sys

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import analyze_uni_cex_v3_experiment as analysis
from scripts.analyze_uni_experiments import write_csv
from scripts.run_research import validate_output_directory
from scripts.uni_cex_v3_experiment import network_gas_scenario, pair_gate

ROOT = Path(__file__).resolve().parents[1]
EXPERIMENT = ROOT/'experiments/uni_cex_v3_20261102'
RESULT_NAMES = ('fee_gas_timeseries.csv', 'delay_pairs.csv',
                'network_gas_scenarios.csv', 'summary.json')


def replay_capture(capture_dir, reference_dir, output_dir):
    capture = Path(capture_dir).resolve()
    reference = Path(reference_dir).resolve()
    output = validate_output_directory(output_dir, ROOT, (capture, reference))
    for name in RESULT_NAMES:
        if not (reference/name).is_file():
            raise ValueError('missing reference result: ' + name)
    try:
        manifest, records, rules = analysis.verify_capture(capture)
    except OSError as error:
        raise ValueError('capture verification failed: ' + str(error)) from error
    rows, delays, summary = analysis.analyze(records, rules, manifest['targets'])
    summary['capture'] = {k: manifest[k] for k in
                          ('started_at', 'completed_at', 'samples_sha256',
                           'pool', 'pool_fee_pips', 'targets')}
    summary['polls'] = len(records)
    accepted = [r for r in analysis.mark_repeated_heads(records) if pair_gate(r)[0]]
    summary['accepted_blocks'] = len(accepted)
    summary['observed_blocks'] = sum(r['status'] == 'observed' for r in records)
    summary['block_age_seconds'] = analysis.stats([r['block_age_seconds'] for r in records if r['status'] == 'observed'])
    summary['cex_age_seconds'] = analysis.stats([r['cex_age_seconds'] for r in records if r['status'] == 'observed'])
    summary['gas_usdt_200k'] = analysis.stats([
        str(value) for r in records if pair_gate(r)[0]
        and (value := network_gas_scenario(r, 200000)) is not None])
    network = summary.pop('network_gas_scenarios')
    output.mkdir(parents=True, exist_ok=False)
    write_csv(output/'fee_gas_timeseries.csv', rows)
    write_csv(output/'delay_pairs.csv', delays)
    write_csv(output/'network_gas_scenarios.csv', network)
    (output/'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    hashes = {}
    for name in RESULT_NAMES:
        content = (output/name).read_bytes()
        if content != (reference/name).read_bytes():
            raise ValueError('replay differs from reference: ' + name)
        hashes[name] = hashlib.sha256(content).hexdigest()
    return {'polls': len(records), 'accepted_blocks': len(accepted),
            'dex_quotes': sum(q is not None for r in accepted for directions in r['quotes'].values() for q in directions.values()),
            'fee_gas_rows': len(rows), 'delay_rows': len(delays),
            'positive_fee_gas_rows': sum(r['net'] is not None and Decimal(r['net']) > 0 for r in rows),
            'reference_byte_identical': True, 'result_sha256': hashes,
            'output_dir': str(output)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--capture', type=Path, default=EXPERIMENT/'capture')
    parser.add_argument('--reference', type=Path, default=EXPERIMENT/'results')
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        result = replay_capture(args.capture, args.reference, args.output_dir)
    except (ValueError, OSError) as error:
        parser.exit(1, str(error) + '\n')
    print(json.dumps(result, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
