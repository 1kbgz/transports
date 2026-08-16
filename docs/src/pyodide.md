# How to run transports in a browser with Pyodide

This guide shows you how to build the Pyodide wheel and load it in a browser.

## Build the wheel

Build and test the Python 3.14 Pyodide wheel:

```bash
make test-pyodide
```

The wheel is written to `dist/pyodide/`.

## Open the example

Serve the repository root:

```bash
python -m http.server 8000
```

Open the example with the wheel URL in its query string, replacing `<wheel>` with the filename in
`dist/pyodide/`:

```text
http://127.0.0.1:8000/examples/pyodide.html?wheel=/dist/pyodide/<wheel>
```

The page loads Pyodide 314.0.4, installs the local wheel and its dependencies, then creates a Python
server and client mirror. Use **Toggle on server** to stream a server patch and **Rename from client**
to send a client proposal through the authoritative server.

## Connect to a live server

`Client.connect` and `Client.connect_sse` detect Pyodide (`sys.platform == "emscripten"`) and ride
the browser's native `WebSocket` / `EventSource` through the `js` FFI — the `websockets` and `httpx`
libraries need raw sockets, which the browser does not give Python. The API is unchanged:

```python
import transports

client = transports.Client(codec="msgpack")
await client.connect("wss://example.com/ws")  # the browser's WebSocket under Pyodide
```

Snapshots and patches update the mirror as they arrive; register `client.on_change` /
`client.on_reject` to react. Reconnect-with-resume (`Client.run`) is not wired to the browser path
yet.

## Run the browser test

Install the JavaScript test dependencies once, then run the focused Playwright test:

```bash
make develop-js
make test-pyodide-browser
```

The browser test uses the wheel already present in `dist/pyodide/`. Run `make test-pyodide` first
after changing Python or Rust code.

## Use the Jupyter widget

`transports.widget(server)` builds a turnkey [anywidget](https://anywidget.dev): display it and every
hosted model mirrors live in the notebook frontend, updating on each `transports.sync(server)`. The
frontend ships inside the wheel (`transports/extension/cdn/widget.js`) — mirroring and edits are pure
TypeScript, so it never fetches wasm. Custom frontends hook the bubbled `transports-change` /
`transports-reject` DOM events or use `el.transports = {client, edit}` —
`el.transports.edit(id, ["brightness"], 75)` sends a wasm-free server-authoritative proposal, and a
value the model rejects surfaces inline through the `reject` frame.

```python
import transports

session = transports.Session()
session.host(model)
server = transports.Server(session)
w = transports.widget(server)   # pip install anywidget
w                               # display; then mutate models + transports.sync(server)
```

## Run it all in JupyterLite

Both ends WebAssembly: the Pyodide kernel hosts the `Session`, the widget frontend mirrors it —
no server, no sockets.

```bash
make jupyterlite        # builds the site into dist/lite (wheel + demo notebook included)
make test-jupyterlite   # or: drive the site's REPL in Chromium end-to-end
```

Serve `dist/lite` from any static host and open `lab/index.html` → `transports-demo.ipynb`. The
transports wheel installs from the site's own wheel index (`%pip install transports`); `anywidget`
comes from PyPI.

## Deploy the example

Host the wheel on the same origin as the page and pass its URL through `?wheel=`. After a release
publishes a compatible Pyodide wheel to PyPI, omit the query parameter to install `transports`
directly from PyPI.
