import functools
import http.server
import os
import socketserver
import sys

# Serve this script's own directory; derived from __file__ (absolute when
# launched with an absolute path) so os.getcwd() is never called — the
# preview sandbox can leave the process cwd in a non-readable location.
DIRECTORY = os.path.dirname(os.path.abspath(__file__))
PORT = 4173

Handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=DIRECTORY)
print(f"Serving {DIRECTORY} on :{PORT}", file=sys.stderr)
socketserver.TCPServer.allow_reuse_address = True
with socketserver.TCPServer(("", PORT), Handler) as httpd:
    httpd.serve_forever()
