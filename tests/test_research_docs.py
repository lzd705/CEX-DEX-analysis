import importlib.util
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class ResearchDocLinksTests(unittest.TestCase):
    def check(self, root):
        source=ROOT/'scripts/check_research_docs.py'
        self.assertTrue(source.is_file(), 'documentation link checker is not implemented')
        spec=importlib.util.spec_from_file_location('hub_docs',source)
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        return module.check_docs(root)

    def fixture(self, root):
        for name in ('engine.py','study.py','executor.sol','experiment.md'):
            (root/name).write_text('source')
        english='# Research\n\n[简体中文](README.zh-CN.md)\n[Website](http://43.156.102.166:8765)\n[Engine](engine.py)\n[Section](#run)\n\n## Run\n\n```bash\npython3 example.py\n```\n'
        chinese='# 研究\n\n[English](readme.md)\n[网站](http://43.156.102.166:8765)\n[引擎](engine.py)\n[运行](#运行)\n\n## 运行\n\n```bash\npython3 example.py\n```\n'
        (root/'readme.md').write_text(english);(root/'README.zh-CN.md').write_text(chinese)

    def test_resolves_bilingual_file_and_heading_links(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);self.fixture(root)
            self.assertEqual(self.check(root)['readmes'],2)

    def test_broken_source_link_is_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);self.fixture(root);(root/'engine.py').unlink()
            with self.assertRaisesRegex(ValueError,'link'):
                self.check(root)

    def test_broken_heading_is_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);self.fixture(root)
            path=root/'README.zh-CN.md';path.write_text(path.read_text().replace('(#运行)','(#不存在)'))
            with self.assertRaisesRegex(ValueError,'anchor'):
                self.check(root)

    def test_divergent_reproduction_commands_are_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);self.fixture(root)
            path=root/'README.zh-CN.md';path.write_text(path.read_text().replace('example.py','wrong.py'))
            with self.assertRaisesRegex(ValueError,'commands'):
                self.check(root)
