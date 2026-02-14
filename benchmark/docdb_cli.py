#!/usr/bin/env python3
"""
DocDB Python CLI — MongoDB-style interactive shell
Connects to the DocDB TCP server via the wire protocol.
"""

import sys
import os
import readline  # enables arrow keys, history in input()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from client import DocDBClient

# ANSI colors
G = "\033[32m"    # green
C = "\033[36m"    # cyan
Y = "\033[33m"    # yellow
R = "\033[31m"    # red
M = "\033[35m"    # magenta
D = "\033[2m"     # dim
B = "\033[1m"     # bold
X = "\033[0m"     # reset


def format_value(v):
    """Format a JSON value with colors."""
    if isinstance(v, str):
        return f'{G}"{v}"{X}'
    elif isinstance(v, bool):
        return f'{M}{"true" if v else "false"}{X}'
    elif isinstance(v, (int, float)):
        return f'{Y}{v}{X}'
    elif isinstance(v, dict):
        return format_doc(v)
    elif isinstance(v, list):
        return "[" + ", ".join(format_value(x) for x in v) + "]"
    return f"{D}null{X}"


def format_doc(doc):
    """Pretty-print a document with ANSI colors."""
    parts = []
    for k, v in doc.items():
        parts.append(f'{C}"{k}"{X}: {format_value(v)}')
    return f"{D}{{ {X}" + f"{D}, {X}".join(parts) + f"{D} }}{X}"


def parse_json_arg(s):
    """Safely parse a JSON-like string from user input."""
    import json
    s = s.strip()
    if not s:
        return None
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        # Try fixing single quotes → double quotes
        try:
            return json.loads(s.replace("'", '"'))
        except json.JSONDecodeError:
            return None


def extract_between(s, open_ch, close_ch):
    """Extract content between matching braces/parens."""
    start = s.find(open_ch)
    if start == -1:
        return ""
    depth = 0
    for i in range(start, len(s)):
        if s[i] == open_ch:
            depth += 1
        elif s[i] == close_ch:
            depth -= 1
        if depth == 0:
            return s[start:i + 1]
    return ""


def main():
    host = "127.0.0.1"
    port = 6379

    # Parse optional --host and --port args
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == "--host" and i + 1 < len(args):
            host = args[i + 1]; i += 2
        elif args[i] == "--port" and i + 1 < len(args):
            port = int(args[i + 1]); i += 2
        else:
            i += 1

    # Connect
    db = DocDBClient(host, port)
    try:
        db.connect()
    except ConnectionRefusedError:
        print(f"{R}Error: Cannot connect to DocDB server at {host}:{port}{X}")
        print(f"{D}Start the server with: ./doc_db_engine --server {port}{X}")
        sys.exit(1)

    # Verify connection
    try:
        assert db.ping() == "pong"
    except Exception:
        print(f"{R}Error: Server not responding{X}")
        sys.exit(1)

    # Banner
    print(f"""{B}{G}
    ____             ____  ____
   / __ \\____  _____/ __ \\/ __ )
  / / / / __ \\/ ___/ / / / __  |
 / /_/ / /_/ / /__/ /_/ / /_/ /
/_____/\\____/\\___/_____/_____/
{X}{D}  Python Client v1.0 — connected to {host}:{port}
  Type 'help' for commands, 'exit' to quit.
{X}""")

    current_collection = ""

    while True:
        # Prompt
        try:
            if current_collection:
                prompt = f"{B}docdb{X}:{C}{current_collection}{X}> "
            else:
                prompt = f"{B}docdb{X}> "
            line = input(prompt).strip()
        except (EOFError, KeyboardInterrupt):
            print(f"\n{G}Goodbye!{X}")
            break

        if not line:
            continue

        # ---- Route commands ----
        try:
            if line in ("exit", "quit"):
                print(f"{G}Goodbye!{X}")
                break

            elif line == "help":
                print(f"""{B}
  DocDB Shell Commands
{X}
  {C}show collections{X}           — List all collections
  {C}use <name>{X}                 — Switch to (or create) a collection
  {C}db.insert({{...}}){X}           — Insert a JSON document
  {C}db.find(){X}                  — Find all documents
  {C}db.find({{...}}){X}             — Find with filter (equality match)
  {C}db.delete({{...}}){X}           — Delete matching documents
  {C}db.update({{filter}}, {{doc}}){X} — Update matching documents
  {C}db.createIndex("field"){X}    — Create B+ Tree index on a field
  {C}db.count(){X}                 — Count documents in collection
  {C}db.drop(){X}                  — Drop current collection
  {C}help{X}                       — Show this help
  {C}exit / quit{X}                — Exit the shell
""")

            elif line == "show collections":
                names = db.list_collections()
                if not names:
                    print(f"{D}  (no collections){X}")
                else:
                    for name in names:
                        print(f"  {C}{name}{X}")

            elif line.startswith("use "):
                name = line[4:].strip()
                if not name:
                    print(f"{R}Error: collection name required{X}")
                else:
                    # Auto-create if needed
                    existing = db.list_collections()
                    if name not in existing:
                        db.create_collection(name)
                    current_collection = name
                    print(f"{G}Switched to collection '{name}'{X}")

            elif line.startswith("db.insert("):
                if not current_collection:
                    print(f"{R}Error: no collection selected. Use 'use <name>' first.{X}")
                    continue
                inner = line[10:]
                if inner.endswith(")"):
                    inner = inner[:-1]
                doc = parse_json_arg(extract_between(inner, "{", "}"))
                if not doc:
                    print(f"{R}Error: invalid or empty document{X}")
                    continue
                resp = db.insert(current_collection, doc)
                if resp.get("ok"):
                    print(f"{G}Inserted 1 document {X}{D}(page={resp.get('page')}, slot={resp.get('slot')}){X}")
                else:
                    print(f"{R}Error: {resp.get('error', 'unknown')}{X}")

            elif line in ("db.find()", "db.find({})"):
                if not current_collection:
                    print(f"{R}Error: no collection selected.{X}")
                    continue
                docs = db.find(current_collection)
                for doc in docs:
                    print(format_doc(doc))
                print(f"{D}({len(docs)} documents){X}")

            elif line.startswith("db.find("):
                if not current_collection:
                    print(f"{R}Error: no collection selected.{X}")
                    continue
                inner = line[8:]
                if inner.endswith(")"):
                    inner = inner[:-1]
                filter_doc = parse_json_arg(extract_between(inner, "{", "}"))
                if filter_doc is None:
                    filter_doc = {}
                docs = db.find(current_collection, filter_doc if filter_doc else None)
                for doc in docs:
                    print(format_doc(doc))
                print(f"{D}({len(docs)} documents){X}")

            elif line.startswith("db.delete("):
                if not current_collection:
                    print(f"{R}Error: no collection selected.{X}")
                    continue
                inner = line[10:]
                if inner.endswith(")"):
                    inner = inner[:-1]
                filter_doc = parse_json_arg(extract_between(inner, "{", "}"))
                if not filter_doc:
                    print(f"{R}Error: filter required{X}")
                    continue
                deleted = db.delete(current_collection, filter_doc)
                print(f"{G}Deleted {deleted} document(s){X}")

            elif line.startswith("db.update("):
                if not current_collection:
                    print(f"{R}Error: no collection selected.{X}")
                    continue
                inner = line[10:]
                if inner.endswith(")"):
                    inner = inner[:-1]

                first = extract_between(inner, "{", "}")
                rest = inner[inner.find(first) + len(first):]
                second = extract_between(rest, "{", "}")

                filter_doc = parse_json_arg(first)
                update_doc = parse_json_arg(second)

                if not filter_doc or not update_doc:
                    print(f'{R}Usage: db.update({{"filter"}}, {{"update"}}){X}')
                    continue
                updated = db.update(current_collection, filter_doc, update_doc)
                print(f"{G}Updated {updated} document(s){X}")

            elif line.startswith("db.createIndex("):
                if not current_collection:
                    print(f"{R}Error: no collection selected.{X}")
                    continue
                inner = line[15:]
                if inner.endswith(")"):
                    inner = inner[:-1]
                field = inner.strip().strip('"').strip("'")
                if not field:
                    print(f"{R}Error: field name required{X}")
                    continue
                ok = db.create_index(current_collection, field)
                if ok:
                    print(f"{G}Index created on '{field}'{X}")
                else:
                    print(f"{R}Error creating index{X}")

            elif line == "db.count()":
                if not current_collection:
                    print(f"{R}Error: no collection selected.{X}")
                    continue
                count = db.count(current_collection)
                print(f"{Y}{count}{X}")

            elif line == "db.drop()":
                if not current_collection:
                    print(f"{R}Error: no collection selected.{X}")
                    continue
                db.drop_collection(current_collection)
                print(f"{G}Dropped collection '{current_collection}'{X}")
                current_collection = ""

            elif line == "ping":
                result = db.ping()
                print(f"{G}{result}{X}")

            else:
                print(f"{R}Unknown command: {X}{line}")
                print(f"{D}Type 'help' for available commands.{X}")

        except ConnectionError:
            print(f"{R}Error: Lost connection to server{X}")
            break
        except Exception as e:
            print(f"{R}Error: {e}{X}")

    db.close()


if __name__ == "__main__":
    main()
