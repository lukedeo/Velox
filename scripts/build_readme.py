#!/usr/bin/env python

if __name__ == '__main__':

    import sys
    import re
    import cupboard

    readme = re.sub(
        '(<!--begin_code-->)(\W*)(#!python)',
        '```python',
        cupboard.__doc__
    )

    readme = re.sub(
        '(<!--end_code-->)',
        '```',
        readme
    )

    code_block = False
    stitched = []
    for line in readme.split('\n'):
        line = line.replace('\t', 4 * ' ')
        if line.strip().replace(' ', '') == '---':
            continue
        if line.startswith(' '):
            line = line[4:]
        stitched.append(line)
    readme = '\n'.join(stitched)
    print readme
