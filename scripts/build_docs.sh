#!/usr/bin/env bash

pdoc \
		--html \
		--overwrite \
		--external-links \
		--template-dir=scripts/templates/ \
		--html-dir=docs/ \
		--html-no-source \
		velox &&
	cd docs && \
	mv velox/* . && \
	rm -fr velox/