# Architecture of application

Phase 1: The Storage Engine (The Foundation)

    Disk Manager: Raw 4KB file I/O.

    BSON Serializer: Convert JSON ↔ Bytes.

    Page Layout (Slotted Page): Manage free space inside one page.

        Constraint Check: Decide here if you will support records > 4KB (Overflow Pages).

    Buffer Pool Manager: Cache pages (LRU replacement policy).

Phase 2: The Access Methods (Data Organization)

    Free Space Map (FSM): (Crucial Sub-module)

        Why: When inserting, you can't scan every page to find space. Use a bitmap or linked list to track pages with free space.

    Heap File: Uses the FSM to find a page, then uses Buffer Pool to fetch it, then uses Slotted Page to insert.

    B+ Tree Index:

        Note: Since this is a document store, your B+ Tree keys might be strings. Ensure your B+ Tree handles variable-length keys.

Phase 3: The Execution Engine (The Logic)

    Catalog: Stores metadata (e.g., "Collection 'users' starts at Page 10").

    Query Executor: (NEW) Implement a simple "Volcano Iterator" model.

        SeqScan: Iterates the Heap.

        Filter: Checks BSON fields against the query.

        IndexScan: Iterates the B+ Tree.

Phase 4: Concurrency & Recovery (The Safety)

    Lock Manager: Two-Phase Locking (2PL).

    Transaction Manager: Assigns Transaction IDs (TID).

    WAL & Recovery: Write-Ahead Logging for crash consistency (ARIES).


Commands

```bash
    cmake -G "Unix Makefiles" ..
```

