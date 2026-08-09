SPHINXBUILD ?= sphinx-build
SOURCEDIR ?= source
BUILDDIR ?= _build
HTMLDIR ?= $(BUILDDIR)/html
SPHINXOPTS ?=

.PHONY: help html strict spelling linkcheck clean

help:
	@echo "html        Build the Sphinx site with warnings as errors"
	@echo "strict      Rebuild the complete Sphinx site"
	@echo "spelling    Check documentation spelling"
	@echo "linkcheck   Check external documentation links"
	@echo "clean       Remove Sphinx-generated documentation output"

html:
	$(SPHINXBUILD) -b html -a -E -n -W --keep-going \
		"$(SOURCEDIR)" "$(HTMLDIR)" $(SPHINXOPTS)

strict: html

spelling:
	$(SPHINXBUILD) -b spelling -a -E -W --keep-going \
		"$(SOURCEDIR)" "$(BUILDDIR)/spelling" $(SPHINXOPTS)

linkcheck:
	$(SPHINXBUILD) -b linkcheck -a -E --keep-going \
		"$(SOURCEDIR)" "$(BUILDDIR)/linkcheck" $(SPHINXOPTS)

clean:
	$(SPHINXBUILD) -M clean "$(SOURCEDIR)" "$(BUILDDIR)"
