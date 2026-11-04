# Python UDP Communication App

The Python implementation is in `udpcomapp.py`. It uses the uv-managed Python 3.14 environment and Textual 8.2.8; UDP, JSON, threads, and log writing use the standard library.

From the repository root:

```sh
uv sync --project python
uv run --project python --python 3.14 python python/udpcomapp.py examples/server-a.json
```

The settings JSON remains compatible with the C++ examples. `send <file-path>` adds a payload file to the FIFO send queue. Other commands are `status`, `help`, `resume`, and `quit`. A quoted path preserves leading or trailing spaces; spaces within an unquoted path are accepted as part of that path.

The command prompt keeps the last 100 non-empty commands in memory for the current run. Use Up/Down to browse history; moving down past the newest command restores the text that was in the prompt before browsing. History is not saved when the app exits.

For a `send` path, Textual shows the first matching path inline, which Right accepts. Tab completes a single match or opens a candidate list for multiple matches. In the list, Up/Down moves through candidates; Enter, Tab, or Right accepts the highlighted path, while Esc closes the list. Accepting a file only fills the prompt: press Enter again to queue it. Selecting a directory appends `/` and lists its contents. Quoted paths keep their opening quote while navigating directories and close the quote when a file is accepted. Candidate scans run in a worker thread and refresh as the path changes.

Relative packet and event log paths are resolved from the settings file directory. If omitted, separate timestamped log files are created beside the settings file. The packet log contains received datagrams; the event log contains send, warning, error, startup, and shutdown events without payload contents.

Run the standard-library test suite with:

```sh
uv run --project python --python 3.14 python -m unittest discover -s python/tests -v
```

Python 3.14.4 and Textual 8.2.8 have been exercised on macOS with loopback and Textual Pilot tests. Windows and Ubuntu terminal behavior have not yet been verified.
