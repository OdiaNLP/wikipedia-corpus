black -l 99 -t py38 scripts tests
mypy scripts
coverage run --source=scripts -m pytest tests --vv -s --pdb && coverage html -i
pylint scripts
