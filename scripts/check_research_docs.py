"""Check bilingual navigation and reproduction-command consistency."""
import json
from pathlib import Path
import re
from urllib.parse import unquote


def _headings(text):
    return {re.sub(r'[^\w -]', '', h.lower()).replace(' ', '-')
            for h in re.findall(r'^#{1,6} (.+)$', text, re.M)}


def check_docs(root):
    root=Path(root); commands=[]; checked=0
    for name, language_target in (('readme.md','README.zh-CN.md'),('README.zh-CN.md','readme.md')):
        file=root/name; text=file.read_text()
        links=re.findall(r'\]\(([^)\s]+)\)',text)
        if language_target not in links:
            raise ValueError('missing language link: '+name)
        if 'http://43.156.102.166:8765' not in links:
            raise ValueError('missing website link: '+name)
        for link in links:
            if '://' in link: continue
            path, _, anchor=unquote(link).partition('#')
            target=(file.parent/path) if path else file
            if not target.exists(): raise ValueError('missing link: '+link)
            if anchor and (not target.is_file() or anchor not in _headings(target.read_text())):
                raise ValueError('missing anchor: '+link)
            checked+=1
        commands.append(re.findall(r'```bash\n([\s\S]*?)```',text))
    if commands[0]!=commands[1]: raise ValueError('reproduction commands differ between languages')
    return {'readmes':2,'local_links_checked':checked,'matching_shell_blocks':len(commands[0])}


if __name__=='__main__':
    print(json.dumps(check_docs(Path(__file__).resolve().parents[1])))
