export PIPENV_VERBOSITY=-1

# Dev utilities
dev:
	make deps
	@echo "Run 'source .venv/bin/activate' to activate the virtual environment"
	@echo "Then run './manage.py runserver' to start the development server"

deps:
	make venv
	make generate

generate:
	echo "Nothing to generate yet ..."

format:
	make ruff_format
	make black
	make isort
	make autoflake
	make mypy

venv:
	uv venv .venv
	uv pip install -e .

precommit:
	make format
	make generate
	make test
	make freeze

freeze:
	uv pip freeze | grep -vE '^zeroindex' > requirements.freeze.txt

# Hatchet workflow management

hatchet_status:
	./manage.py hatchet_status

hatchet_worker:
	./manage.py hatchet_worker --worker-name zeroindex-worker

# CI Pipeline & Tests

test:
	pytest

test_backend_coverage:
	pytest --cov=zeroindex/apps --cov-config=.coveragerc --cov-report html --cov-report term
	echo "View coverage report: file://${PWD}/htmlcov/index.html"

# Data management & Backups
dump_fixtures:
	bin/djmanage dumpdata --natural-primary --natural-foreign --format json --indent 2 users

# Codebase Linting & Cleanup

clean:
	find . -name '*.pyc' -delete

mypy:
	mypy

isort:
	isort zeroindex

flake8:
	flake8

autoflake:
	autoflake -r -i --expand-star-imports --remove-all-unused-imports --remove-duplicate-keys --remove-unused-variables --ignore-init-module-imports zeroindex/apps/

black:
	black zeroindex wsgi.py manage.py

ruff_check:
	ruff check .

ruff_format:
	ruff format .
