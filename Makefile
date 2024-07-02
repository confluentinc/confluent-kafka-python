all:
	@echo "Targets:"
	@echo " clean"
	@echo " docs"


clean:
	pip cache purge
	rm -rf dist
	make -C docs clean

.PHONY: docs

docs:
	$(MAKE) -C docs html

style-check:
	@(tools/style-format.sh \
		$$(git ls-tree -r --name-only HEAD | egrep '\.(c|h|py)$$') )

style-check-changed:
	@(tools/style-format.sh \
		$$( (git diff --name-only ; git diff --name-only --staged) | egrep '\.(c|h|py)$$'))

style-fix:
	@(tools/style-format.sh --fix \
		$$(git ls-tree -r --name-only HEAD | egrep '\.(c|h|py)$$'))

style-fix-changed:
	@(tools/style-format.sh --fix \
		$$( (git diff --name-only ; git diff --name-only --staged) | egrep '\.(c|h|py)$$'))
