# See: https://blog.thapaliya.com/posts/well-documented-makefiles/
help: ##- Show this help message.
	@awk 'BEGIN {FS = ":.*#{2}-"; printf "usage: make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z0-9_-]+:.*?#{2}-/ { printf "  \033[36m%-29s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
.PHONY: help

# ------------------------------------------------------------------------------

venv: ##- Create a new Python virtual environment in .venv/ (need to activate manually).
	@python3.6 -m venv .venv
.PHONY: devvenv

devinstall: ##- Install the project in editable mode with all test and dev dependencies (in the currently active environment).
	@pip install --upgrade pip setuptools wheel
	@grep git+git setup.cfg | awk '{ gsub (" ", "", $$0); print}' | xargs -r pip install --upgrade
	@pip install -e .[test,dev]
.PHONY: devinstall

# ------------------------------------------------------------------------------

test: test-pytest ##- Run all tests and report test coverage.
.PHONY: test

test-pytest: ##- Run all tests in the currently active environment.
	@pytest --cov --cov-report= --cov-context test --html tests-report.html --self-contained-html
	@coverage html --dir tests-coverage
	@coverage report
.PHONY: test-pytest

test-nox: ##- Run all tests against all supported Python versions (in separate environments).
	@coverage erase
	@nox
	@coverage html --dir tests-coverage
	@coverage report
.PHONY: test-nox

# ------------------------------------------------------------------------------

check: check-flake8 check-mypy check-vulture check-isort check-black ##- Run linters and perform static type-checking.
.PHONY: check

# Not using the following in `check`-rule because it always spams output, even
# in case of success (no quiet flag) and because most of the checking is already
# performed by flake8.
check-autoflake: ##- Check for unused imports and variables.
	@autoflake --check --remove-all-unused-imports --remove-duplicate-keys --remove-unused-variables --recursive .
.PHONY: check-autoflake

check-flake8: ##- Run linters.
	@flake8 src tests *.py
.PHONY: check-flake8

check-mypy: ##- Run static type-checking.
	@mypy src .
.PHONY: check-mypy

check-vulture: ##- Check for unsued code.
	@vulture src tests *.py
.PHONY: check-vulture

check-isort: ##- Check if imports are sorted correctly.
	@isort --check-only --recursive --quiet .
.PHONY: check-isort

check-black: ##- Check if code is formatted correctly.
	@black --check .
.PHONY: check-black

# ------------------------------------------------------------------------------

format: format-licenseheaders format-autoflake format-isort format-black ##- Auto format all code.
.PHONY: format

format-licenseheaders: ##- Prepend license headers to all code files.
	@licenseheaders --tmpl LICENSE.header --years 2019-2020 --owner "Lukas Schmelzeisen" --dir src
	@licenseheaders --tmpl LICENSE.header --years 2019-2020 --owner "Lukas Schmelzeisen" --dir tests
.PHONY: format-licenseheaders

format-autoflake: ##- Remove unused imports and variables.
	@autoflake --in-place --remove-all-unused-imports --remove-duplicate-keys --remove-unused-variables --recursive .
.PHONY: format-autoflake

format-isort: ##- Sort all imports.
	@isort --recursive --quiet .
.PHONY: format-isort

format-black: ##- Format all code.
	@black .
.PHONY: format-black

# ------------------------------------------------------------------------------

publish: publish-setuppy publish-twine-check ##- Build and check source and binary distributions.
.PHONY: publish

publish-setuppy: #-- Build source and binary distributions.
	@rm -rf build dist
	@python setup.py sdist bdist_wheel
.PHONY: publish-setuppy

publish-twine-check: ##- Check source and binary distributions for upload.
	@twine check dist/*
.PHONY: publish-twine-check

publish-twine-upload-testpypi: ##- Upload to TestPyPI.
	@twine upload --repository-url https://test.pypi.org/legacy/ dist/*
.PHONY: publish-twine-upload-testpypi

publish-twine-upload: ##- Upload to PyPI.
	@twine upload dist/*
.PHONY: publish-twine-upload

# ------------------------------------------------------------------------------

clean: ##- Remove all created cache/build files, test/coverage reports, and virtual environments.
	@rm -rf .coverage* .eggs .mypy_cache .pytest_cache .nox .venv build dist src/*/_version.py src/*.egg-info tests-coverage tests-report.html
	@find . -type d -name __pycache__ -exec rm -r {} +
.PHONY: clean

# ------------------------------------------------------------------------------

build-vulture-whitelistpy:  ##- Regenerate vulture whitelist (list of currently seemingly unused code that will not be reported).
	@vulture src tests *.py --make-whitelist > vulture-whitelist.py || true
.PHONY: build-vulture-whitelistpy
