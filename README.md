# Knowledge Texts Folder

## Easiest way: Upload UI (paperclip-style)

```bash
bun run ./upload-ui.ts
```

Then open:

```text
http://127.0.0.1:8787
```

Use the page to choose files, then click **Upload + Sync**.
Default per-file limit is **20 MB**.
Then use **Ask Your Knowledge** on the same page to chat with your notes.

## Terminal-only method

Drop your `.md`, `.markdown`, or `.txt` files in this folder (or subfolders), then run:

```bash
bun run ./sync-knowledge.ts
```

Both methods do this:
- Creates notes for new files
- Updates notes when file content changes
- Skips files that are unchanged
- Tracks source file paths and hashes for reliable sync

Suggested structure:

```text
texts/
  journals/
  essays/
  references/
```

Tip: Keep one topic per file for better retrieval quality.
