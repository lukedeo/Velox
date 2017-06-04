#!/usr/bin/env bash

export PYTHONIOENCODING=utf8
PYTHONPATH=./ scripts/build_readme.py > README.md && \
PYTHONPATH=./ scripts/build_docs.sh && \
PYTHONPATH=./ scripts/fix_links.py docs/*