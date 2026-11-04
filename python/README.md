# Python UDP Communication App

The Python implementation is in `udpcomapp.py`. It uses the uv-managed Python 3.14 environment and Textual 8.2.8; UDP, JSON, threads, and log writing use the standard library.

From the repository root:

```sh
uv sync --project python
uv run --project python --python 3.14 python python/udpcomapp.py examples/server-a.json
```

The settings JSON remains compatible with the C++ examples. The current TUI commands are `send <file-path>`, `sendrep <num> <file-path>`, `status`, `stop`, `help`, `resume`, and `quit`. `send` adds one file job to the FIFO send queue. `sendrep` adds one job that rereads the named file for each of 1–10,000 contiguous repetitions. The count uses ASCII decimal digits; leading zeroes are allowed when the resulting count is in range. A quoted path preserves leading or trailing spaces; spaces within an unquoted path are accepted as part of that path. Environment variables, tilde, and glob patterns are not expanded.

The command prompt keeps the last 100 non-empty commands in memory for the current run. Use Up/Down to browse history; moving down past the newest command restores the text that was in the prompt before browsing. History is not saved when the app exits.

For `send` and `sendrep` paths, Textual shows the first matching path inline, which Right accepts. Tab completes a single match or opens a candidate list for multiple matches. In the list, Up/Down moves through candidates; Enter, Tab, or Right accepts the highlighted path, while Esc closes the list. Accepting a file only fills the prompt: press Enter again to queue it. Selecting a directory appends `/` and lists its contents. Quoted paths keep their opening quote while navigating directories and close the quote when a file is accepted. Candidate scans run in a worker thread and refresh as the path changes.

`stop` requests cancellation of the active job at a line or repetition boundary, wakes an active `/Wait`, and discards jobs already waiting in the queue. It leaves the TUI and UDP receiver running. A `send` or `sendrep` entered afterward starts automatically. The active stopped job count and queue discard count are shown separately in status. `quit` and Ctrl+C still exit the whole program, stop the active job, discard pending work, and close the logs.

Relative packet and event log paths are resolved from the settings file directory. If omitted, separate timestamped log files are created beside the settings file. The packet log contains received datagrams; the event log contains send, warning, error, startup, shutdown, and stop events without payload contents.

The Python-only `sendrep` and `stop` TUI commands do not modify the C++ executables, their input-file syntax, or the shared settings JSON format. The C++ `udp_sender` continues to use its existing TCP IPC connection to the C++ `udp_server`.

Run the standard-library test suite with:

```sh
uv run --project python --python 3.14 python -m unittest discover -s python/tests -v
```

Python 3.14.4 and Textual 8.2.8 have been exercised on macOS with loopback and Textual Pilot tests. Windows and Ubuntu terminal behavior have not yet been verified.
