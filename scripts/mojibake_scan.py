from pathlib import Path
import re
root = Path('docs')
paths = list(root.rglob('*.*')) + list(Path('backend').rglob('*.*'))
paths = [p for p in paths if p.suffix.lower() in {'.html', '.js', '.py', '.css'}]
weird = {}
for path in paths:
    try:
        text = path.read_text(encoding='utf-8')
    except Exception as e:
        print(f'ERROR reading {path}: {e}')
        continue
    for m in re.finditer(r'[\u0080-\uFFFF]+', text):
        substr = m.group(0)
        weird.setdefault(substr, set()).add(str(path))
print('TOTAL_UNIQUE', len(weird))
for s, paths in sorted(weird.items(), key=lambda x:(len(x[0]), x[0])):
    print(repr(s), len(paths), list(paths)[:5])
