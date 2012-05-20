.PHONY: all doc test clean

all: doc clean

doc:
	python setup.py build_sphinx

test:
	-find coverage/ -mindepth 1 -delete
	python $$(which nosetests) $${TESTS}

clean:
	find . -name '*.py[co]' -delete
