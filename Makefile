.PHONY: all doc test clean

all: doc clean

doc:
	python setup.py build_sphinx
	@echo
	@echo Generated documentation: "file://"$$(readlink -f doc/build/html/index.html)
	@echo

test:
	-find coverage/ -mindepth 1 -delete
	python $$(which nosetests) $${TESTS}

clean:
	find . -name '*.py[co]' -delete

dist: test
	python setup.py sdist
