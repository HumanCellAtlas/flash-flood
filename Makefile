.PHONY: test lint mypy tests clean build install
MODULES=flashflood tests

test: lint mypy tests

lint:
	flake8 $(MODULES) *.py

mypy:
	mypy --ignore-missing-imports $(MODULES)

tests:
	PYTHONWARNINGS=ignore:ResourceWarning coverage run --source=flashflood \
		-m unittest discover --start-directory tests --top-level-directory . --verbose

version: flashflood/version.py

flashflood/version.py: setup.py
	echo "__version__ = '$$(python setup.py --version)'" > $@

clean:
	-rm -rf build dist
	-rm -rf *.egg-info

build: version clean
	-rm -rf dist
	python setup.py bdist_wheel

install: build
	pip install --upgrade dist/*.whl

include common.mk
