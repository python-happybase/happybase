.PHONY: all doc test clean

all: doc clean

doc:
	python setup.py build_sphinx
	@echo
	@echo Generated documentation: "file://"$$(readlink -f doc/build/html/index.html)
	@echo

doc3:
	python3 setup.py build_sphinx
	@echo
	@echo Generated documentation: "file://"$$(readlink -f doc/build/html/index.html)
	@echo

test:
	-find coverage/ -mindepth 1 -delete
	python $$(which nosetests) $${TESTS}

test3:
	-find coverage/ -mindepth 1 -delete
	python3 $$(which nosetests3) $${TESTS}

clean:
	find . -name '*.py[co]' -delete

dist: test
	python setup.py sdist

dist3: test3
	python3 setup.py sdist
