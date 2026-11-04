
# PC-A
uv venv --python 3.14.10 --no-python-downloads
(uv init)
uv add some
uv sync --locked
uv export --locked --no-emit-local --output-file requirements-offline.txt
mkdir offline_bundle\wheelhouse
uv run --no-sync --with pip python -m pip download --only-binary=:all: --dest offline_bundle\wheelhouse -r requirements-offline.txt

# PC-B
uv install standalone binary, python install standalone installer
copy uv.lock, pyproject.toml, requirements-offline.txt, .python-version
confirm `uv python list`
uv venv --python 3.14.10 --no-python-downloads
uv pip sync --offline --no-index --find-links \path\to\wheelhouse requirements-offline.txt
