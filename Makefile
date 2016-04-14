all:
	@echo "Targets:"
	@echo " clean"
	@echo " docs"


clean:
	python setup.py clean
	make -C docs clean

.PHONY: docs

docs:
	$(MAKE) -C docs html
