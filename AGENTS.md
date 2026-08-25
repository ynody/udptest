# AGENTS.md

## Development Environment

- This project uses Volta for Node.js and npm.
- Prefer the Volta-managed binaries explicitly when running Node/npm commands:
  - `/Users/ynody/.volta/bin/node`
  - `/Users/ynody/.volta/bin/npm`
- Confirmed local versions on 2026-05-03:
  - Node.js: `v24.15.0`
  - npm: `11.12.1`
- Do not fall back to the bundled Codex Node.js runtime unless the Volta-managed runtime is unavailable.

## Python Environment

- This project uses `uv` for Python environment and dependency management.
- Prefer Python 3.13 for project work.
- Confirmed local versions on 2026-05-03:
  - uv: `/Users/ynody/.local/bin/uv` (`uv 0.11.8`)
  - Python: `3.13.13`
- Prefer the uv-managed Python 3.13 binary explicitly when needed:
  - `/Users/ynody/.local/bin/python3.13`
  - `/Users/ynody/.local/share/uv/python/cpython-3.13-macos-aarch64-none/bin/python3.13`
- The default `python` and `python3` commands currently resolve to pyenv-managed Python 3.12.7, so do not rely on them for this project unless intentionally checking the system state.

## Library and Framework Selection

- Before introducing new frameworks, runtime libraries, build tools, UI libraries, database libraries, or major developer tools, propose the selection and get user approval.
- The proposal should briefly include:
  - the library or framework name
  - why it fits this project
  - meaningful alternatives considered
  - any notable tradeoffs, risks, or future migration concerns
- Small standard-library usage and already-approved project dependencies do not require repeated approval.
- After approval, record important decisions in project documentation when they affect future development.
- Regardless of programming language, record approved third-party libraries and frameworks in `libraries.md` after user approval.
- Keep `libraries.md` updated when adding, replacing, or removing approved dependencies.
