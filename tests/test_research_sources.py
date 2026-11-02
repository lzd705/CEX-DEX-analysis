import hashlib
import importlib.util
import os
from pathlib import Path
import tempfile
import unittest


class ResearchSourcesTests(unittest.TestCase):
    def verify(self, root, manifest):
        source = Path(__file__).resolve().parents[1] / 'scripts/verify_research_sources.py'
        self.assertTrue(source.is_file(), 'source verifier is not implemented')
        spec = importlib.util.spec_from_file_location('source_verifier', source)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.verify_sources(root, manifest)

    def fixture(self, root, path='scripts/original.py', mode='100644'):
        target = root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b'print(42)\n')
        target.chmod(0o755 if mode == '100755' else 0o644)
        return {'files': [{'component': 'v3', 'source_commit': 'a'*40,
            'source_path': 'README.md' if path.endswith('.md') else path,
            'published_path': path, 'sha256': hashlib.sha256(b'print(42)\n').hexdigest(),
            'mode': mode}], 'gitlinks': [], 'additions': []}

    def test_valid_source_is_verified(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.assertEqual(self.verify(root, self.fixture(root))['files_verified'], 1)

    def test_hash_change_is_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); manifest = self.fixture(root)
            (root/'scripts/original.py').write_bytes(b'changed')
            with self.assertRaisesRegex(ValueError, 'hash'):
                self.verify(root, manifest)

    def test_unsafe_and_duplicate_paths_are_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); manifest = self.fixture(root)
            manifest['files'].append(dict(manifest['files'][0]))
            with self.assertRaisesRegex(ValueError, 'duplicate'):
                self.verify(root, manifest)
            manifest['files'] = [{**manifest['files'][0], 'published_path': '../escape'}]
            with self.assertRaisesRegex(ValueError, 'path'):
                self.verify(root, manifest)

    def test_executable_mode_is_preserved(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); manifest = self.fixture(root, mode='100755')
            self.assertEqual(self.verify(root, manifest)['files_verified'], 1)
            (root/'scripts/original.py').chmod(0o644)
            with self.assertRaisesRegex(ValueError, 'mode'):
                self.verify(root, manifest)

    def test_declared_readme_relocation_preserves_bytes(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            manifest = self.fixture(root, path='research/v3_exact/SOURCE_README.md')
            self.assertEqual(self.verify(root, manifest)['files_verified'], 1)

    def test_symlink_substitution_is_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); manifest = self.fixture(root)
            target = root/'scripts/original.py'
            target.rename(root/'saved.py'); target.symlink_to(root/'saved.py')
            with self.assertRaisesRegex(ValueError, 'symlink'):
                self.verify(root, manifest)

    def test_declared_additions_must_exist_and_be_unique(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);manifest=self.fixture(root)
            manifest['additions']=['missing.md']
            with self.assertRaisesRegex(ValueError,'addition'):
                self.verify(root,manifest)
            manifest['additions']=['scripts/original.py']
            with self.assertRaisesRegex(ValueError,'duplicate'):
                self.verify(root,manifest)
