#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" 
file: fix_links.py
description: fix idiotic links from pdoc
author: Luke de Oliveira (lukedeo@vaitech.io)
"""

import re
import argparse

import bs4
from strif import atomic_output_file

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Process pdoc HTML files')

    parser.add_argument('files', action="store", help='HTML files to read',
                        nargs='+')

    args = parser.parse_args()

    for input_page in args.files:
        print 'Processing file: {}'.format(input_page)
        with atomic_output_file(input_page) as overwritten_page:
            with open(input_page) as orig:
                text = re.sub(
                    r'/velox\.(\w+)[.]?([a-zA-Z._]*)\.ext',
                    r'./\1.m.html#velox.\1.\2',
                    orig.read()
                )

            soup = bs4.BeautifulSoup(text, 'html.parser')

            for a in soup.find_all('a', href=True):
                if a['href'].startswith('/') and 'velox' not in a['href']:
                    # del a['href']
                    a.replace_with_children()

            with open(overwritten_page, 'w') as new:
                new.write(str(soup))
