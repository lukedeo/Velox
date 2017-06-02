#!/usr/bin/env python

if __name__ == '__main__':

    import sys
    import re
    import velox

    readme = re.sub(
        u'(<!--begin_code-->)(\W*)(#!python)',
        u'```python',
        velox.__doc__
    )

    readme = re.sub(
        u'(<!--end_code-->)',
        u'```',
        readme
    )

    code_block = False
    stitched = []
    for line in readme.split(u'\n'):
        line = line.replace(u'\t', 4 * u' ')
        if line.strip().replace(u' ', u'') == u'---':
            continue
        if line.startswith(' '):
            line = line[4:]
        stitched.append(line)
    readme = u'\n'.join(stitched)
    print readme
