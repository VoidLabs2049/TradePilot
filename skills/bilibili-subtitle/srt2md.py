import re, glob, os, sys

d = sys.argv[1] if len(sys.argv) > 1 else os.path.dirname(__file__)
for srt in sorted(glob.glob(os.path.join(d, '*.srt'))):
    title = re.sub(r'^\d+-', '', os.path.basename(srt).replace('.ai-zh.srt', ''))
    lines = open(srt, encoding='utf-8').read().strip().split('\n')
    texts, seen = [], set()
    for line in lines:
        line = line.strip()
        if not line or re.match(r'^\d+$', line) or '-->' in line:
            continue
        line = re.sub(r'<[^>]*>', '', line)
        if line not in seen:
            seen.add(line)
            texts.append(line)
    md = os.path.join(d, os.path.basename(srt).replace('.ai-zh.srt', '.md'))
    with open(md, 'w', encoding='utf-8') as f:
        f.write(f'# {title}\n\n' + ' '.join(texts) + '\n')
    print(f'  {os.path.basename(md)}')
print('Done')
