"""Verify the byte/mode identity of imported, version-pinned research sources."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import subprocess


def _safe_path(root, text):
    path = PurePosixPath(text)
    if not text or path.is_absolute() or '..' in path.parts or '\\' in text:
        raise ValueError('unsafe source path: ' + text)
    target = root.joinpath(*path.parts)
    current = target
    while current != root:
        if current.is_symlink():
            raise ValueError('source symlink: ' + text)
        current = current.parent
    return target


def verify_sources(root, manifest):
    root = Path(root).resolve()
    seen = set()
    for row in manifest['files']:
        name = row['published_path']
        if name in seen:
            raise ValueError('duplicate source path: ' + name)
        seen.add(name)
        target = _safe_path(root, name)
        _safe_path(root, row['source_path'])
        if not target.is_file():
            raise ValueError('missing source file: ' + name)
        if hashlib.sha256(target.read_bytes()).hexdigest() != row['sha256']:
            raise ValueError('source hash mismatch: ' + name)
        if row['mode'] not in ('100644', '100755'):
            raise ValueError('unsupported source mode: ' + name)
        executable = bool(target.stat().st_mode & 0o111)
        if executable != (row['mode'] == '100755'):
            raise ValueError('source mode mismatch: ' + name)
        if not re.fullmatch('[0-9a-f]{40}', row['source_commit']):
            raise ValueError('invalid source commit: ' + name)
    additions=set()
    for name in manifest.get('additions', []):
        if name in seen or name in additions:
            raise ValueError('duplicate addition path: '+name)
        if not _safe_path(root,name).is_file():
            raise ValueError('missing addition file: '+name)
        additions.add(name)
    for row in manifest.get('gitlinks', []):
        _safe_path(root, row['published_path'])
        output = subprocess.check_output(['git', 'ls-files', '--stage', '--', row['published_path']], cwd=root, text=True)
        fields = output.strip().split()
        if len(fields) != 4 or fields[:2] != ['160000', row['oid']]:
            raise ValueError('Gitlink mismatch: ' + row['published_path'])
    return {'files_verified': len(seen), 'gitlinks_verified': len(manifest.get('gitlinks', [])),
            'additions_verified':len(additions)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    manifest = json.loads((args.root/'research/sources.json').read_text())
    print(json.dumps(verify_sources(args.root, manifest), sort_keys=True))


if __name__ == '__main__':
    main()
