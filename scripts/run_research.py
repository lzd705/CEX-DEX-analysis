#!/usr/bin/env python3
"""Run the repository's version-isolated research workflows."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
COMMANDS = ('v3-math', 'shib-replay', 'uni-v3-replay', 'historical-demo')


def validate_output_directory(output, project_root=ROOT, protected=()):
    """Resolve a new output location, keeping published inputs and sources intact."""
    candidate = Path(output).expanduser().resolve()
    root = Path(project_root).resolve()
    if candidate.exists():
        raise ValueError('output directory must be new: ' + str(candidate))
    local = root/'data/local'
    if candidate == root or root in candidate.parents:
        if local not in candidate.parents:
            raise ValueError('output overlaps source tree: ' + str(candidate))
    for source in protected:
        source = Path(source).resolve()
        if candidate == source or source in candidate.parents or candidate in source.parents:
            raise ValueError('output overlaps published input: ' + str(candidate))
    return candidate


def _child(args, cwd):
    environment = dict(os.environ)
    environment.pop('PYTHONPATH', None)
    environment.pop('PYTHONHOME', None)
    return subprocess.run([sys.executable, '-E', '-s', '-S', '-B', *args],
                          cwd=cwd, env=environment).returncode


def main(argv=None, project_root=ROOT):
    root = Path(project_root).resolve()
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('list')
    commands.add_parser('v3-math')
    for name in ('shib-replay', 'uni-v3-replay'):
        commands.add_parser(name).add_argument('--output-dir', type=Path)
    commands.add_parser('historical-demo').add_argument('--port', type=int, default=0)
    args = parser.parse_args(argv)
    if args.command == 'list':
        print(json.dumps({'commands': list(COMMANDS),
                          'sources': 'research/sources.json'}, indent=2))
        return 0
    if args.command == 'v3-math':
        return _child(['-m', 'unittest', 'discover', '-s', 'tests',
                       '-p', 'test_uniswap_v3_math.py', '-v'], root/'research/v3_exact')
    if args.command == 'historical-demo':
        return _child(['scripts/run_historical_opportunity_demo.py', '--port', str(args.port)], root)
    output = args.output_dir or root/'data/local'/('research-' + args.command + '-' + uuid.uuid4().hex[:12])
    output = validate_output_directory(output, root)
    if args.command == 'uni-v3-replay':
        return _child(['scripts/replay_uni_cex_v3.py', '--output-dir', str(output)], root)
    module = root/'research/shib_v2v2'
    source = module/'data/public/research/shib-v2v2/latest.json'
    expected = source.read_bytes()
    application_sha = json.loads(expected)['application_sha']
    output.mkdir(parents=True, exist_ok=False)
    target = output/'snapshot.json'
    code = _child(['scripts/build_shib_v2_research_snapshot.py',
                   '--registry', str(module/'config/shib_v2_research_pools.json'),
                   '--evidence', str(module/'data/public/research/shib-v2v2/evidence.json'),
                   '--application-sha', application_sha,
                   '--output', str(target)], module)
    if code:
        return code
    if target.read_bytes() != expected:
        raise ValueError('SHIB replay differs from retained snapshot')
    print(json.dumps({'byte_identical': True, 'output': str(target),
                      'source_application_sha': application_sha,
                      'sha256': hashlib.sha256(expected).hexdigest()}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
