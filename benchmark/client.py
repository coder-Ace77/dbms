"""
DocDB Python Client — communicates with the DocDB server via TCP sockets
using the length-prefixed JSON wire protocol.
"""

import socket
import struct
import json
import time


class DocDBClient:
    """Client for connecting to a DocDB server."""

    def __init__(self, host: str = "127.0.0.1", port: int = 6379):
        self.host = host
        self.port = port
        self.sock = None

    def connect(self):
        """Establish TCP connection to the server."""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.connect((self.host, self.port))

    def close(self):
        """Close the connection."""
        if self.sock:
            self.sock.close()
            self.sock = None

    def _send(self, request: dict) -> dict:
        """Send a request and receive a response."""
        payload = json.dumps(request).encode("utf-8")
        header = struct.pack("!I", len(payload))
        self.sock.sendall(header + payload)

        # Read 4-byte response header
        resp_header = self._recv_exact(4)
        resp_len = struct.unpack("!I", resp_header)[0]

        # Read response body
        resp_body = self._recv_exact(resp_len)
        return json.loads(resp_body.decode("utf-8"))

    def _recv_exact(self, n: int) -> bytes:
        """Read exactly n bytes from socket."""
        data = b""
        while len(data) < n:
            chunk = self.sock.recv(n - len(data))
            if not chunk:
                raise ConnectionError("Server closed connection")
            data += chunk
        return data

    # ---- High-level API ----

    def ping(self) -> str:
        resp = self._send({"cmd": "ping"})
        return resp.get("result", "error")

    def list_collections(self) -> list:
        resp = self._send({"cmd": "listCollections"})
        return resp.get("result", [])

    def create_collection(self, name: str) -> bool:
        resp = self._send({"cmd": "createCollection", "name": name})
        return resp.get("ok", False)

    def drop_collection(self, name: str) -> bool:
        resp = self._send({"cmd": "dropCollection", "name": name})
        return resp.get("ok", False)

    def insert(self, collection: str, document: dict) -> dict:
        resp = self._send({
            "cmd": "insert",
            "collection": collection,
            "document": document,
        })
        return resp

    def find(self, collection: str, filter_doc: dict = None) -> list:
        req = {"cmd": "find", "collection": collection}
        if filter_doc:
            req["filter"] = filter_doc
        resp = self._send(req)
        return resp.get("result", [])

    def count(self, collection: str) -> int:
        resp = self._send({"cmd": "count", "collection": collection})
        return resp.get("count", 0)

    def delete(self, collection: str, filter_doc: dict) -> int:
        resp = self._send({
            "cmd": "delete",
            "collection": collection,
            "filter": filter_doc,
        })
        return resp.get("deleted", 0)

    def update(self, collection: str, filter_doc: dict, update_doc: dict) -> int:
        resp = self._send({
            "cmd": "update",
            "collection": collection,
            "filter": filter_doc,
            "update": update_doc,
        })
        return resp.get("updated", 0)

    def create_index(self, collection: str, field: str) -> bool:
        resp = self._send({
            "cmd": "createIndex",
            "collection": collection,
            "field": field,
        })
        return resp.get("ok", False)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()
