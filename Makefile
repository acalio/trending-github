.PHONY: notebook docs
.EXPORT_ALL_VARIABLES:

install: 
	@echo "Installing..."
	poetry install
	poetry run pre-commit install

activate:
	@echo "Activating virtual environment"
	poetry shell

initialize_git:
	git init 

setup: initialize_git install

test:
	pytest

docs_view:
	@echo View API documentation... 
	poetry run pdoc src

docs_save:
	@echo Save documentation to docs... 
	poetry run pdoc src -o docs

format:
	find . -type f -name "*.py" -exec echo {} \; -exec black {} \;
## Delete all compiled Python files
clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf .pytest_cache .flake8 outputs .mypy_cache
