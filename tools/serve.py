import functools
import http.server
import os
import socketserver
import sys

# Serve the REPO ROOT (parent of tools/), pas le dossier tools/ lui-même —
# sinon index.html / design.html / design/tokens.json ne sont pas servis.
# Dérivé de __file__ pour ne jamais appeler os.getcwd() (cwd parfois non lisible
# dans le bac à sable de preview).
DIRECTORY = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PORT = 4173


class NoCacheHandler(http.server.SimpleHTTPRequestHandler):
    """Serveur de dev : désactive le cache navigateur pour que chaque rebuild
    (index.html, tokens.css…) soit vu au simple rechargement, sans hard-refresh."""

    def end_headers(self):
        self.send_header("Cache-Control", "no-store, must-revalidate")
        self.send_header("Expires", "0")
        super().end_headers()


Handler = functools.partial(NoCacheHandler, directory=DIRECTORY)
print(f"Serving {DIRECTORY} on :{PORT}", file=sys.stderr)
socketserver.TCPServer.allow_reuse_address = True
with socketserver.TCPServer(("", PORT), Handler) as httpd:
    httpd.serve_forever()
