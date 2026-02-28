#!/usr/bin/env python3
"""
Merge OpenAI Batch embeddings results with upsert-ready metadata rows.

Input A (upsert-ready JSONL):
  {
    "id": "...",
    "namespace": "...",
    "text": "...",
    "metadata": {...}
  }

Input B (OpenAI Batch output JSONL):
  {
    "custom_id": "...",
    "response": {
      "status_code": 200,
      "body": {
        "data": [{"embedding": [...]}]
      }
    },
    "error": null
  }

Output (vector upsert JSONL):
  {
    "id": "...",
    "values": [...],
    "metadata": {...},
    "namespace": "..."
  }
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge OpenAI Batch embeddings output with upsert-ready rows."
    )
    parser.add_argument(
        "--upsert-ready",
        default="data/rag/journal314_embeddings_upsert_ready.jsonl",
        help="Path to upsert-ready JSONL rows.",
    )
    parser.add_argument(
        "--batch-results",
        required=True,
        help="Path to OpenAI Batch output JSONL rows.",
    )
    parser.add_argument(
        "--out",
        default="data/rag/journal314_vector_upserts.jsonl",
        help="Output path for merged vector upsert payloads.",
    )
    parser.add_argument(
        "--failures-out",
        default="data/rag/journal314_vector_upserts_failures.jsonl",
        help="Output path for unmatched/failed rows.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero if any failures occur.",
    )
    return parser.parse_args()


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_no, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{line_no} invalid JSON: {exc}") from exc
            if not isinstance(row, dict):
                raise ValueError(f"{path}:{line_no} expected JSON object.")
            row["_line_no"] = line_no
            rows.append(row)
    return rows


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            payload = dict(row)
            payload.pop("_line_no", None)
            handle.write(json.dumps(payload, ensure_ascii=False))
            handle.write("\n")


def coerce_embedding(raw: Any) -> list[float] | None:
    if not isinstance(raw, list) or len(raw) == 0:
        return None
    out: list[float] = []
    for item in raw:
        if isinstance(item, (int, float)):
            out.append(float(item))
            continue
        return None
    return out


def extract_embedding_from_batch_row(row: dict[str, Any]) -> tuple[list[float] | None, str | None]:
    # Standard OpenAI Batch output shape.
    error_obj = row.get("error")
    if error_obj:
        return None, f"batch_error={error_obj}"

    response = row.get("response")
    if isinstance(response, dict):
        status_code = response.get("status_code")
        if status_code and status_code != 200:
            return None, f"http_status={status_code}"
        body = response.get("body")
        if not isinstance(body, dict):
            return None, "missing_response_body"
        data = body.get("data")
        if not isinstance(data, list) or not data:
            return None, "missing_response_data"
        first = data[0]
        if not isinstance(first, dict):
            return None, "bad_response_data_item"
        vector = coerce_embedding(first.get("embedding"))
        if vector is None:
            return None, "missing_or_invalid_embedding"
        return vector, None

    # Fallback support for already flattened records.
    data = row.get("data")
    if isinstance(data, list) and data:
        first = data[0]
        if isinstance(first, dict):
            vector = coerce_embedding(first.get("embedding"))
            if vector is not None:
                return vector, None

    embedding = coerce_embedding(row.get("embedding"))
    if embedding is not None:
        return embedding, None

    return None, "unsupported_result_shape"


def main() -> int:
    args = parse_args()

    upsert_path = Path(args.upsert_ready).expanduser().resolve()
    batch_path = Path(args.batch_results).expanduser().resolve()
    out_path = Path(args.out).expanduser().resolve()
    failures_path = Path(args.failures_out).expanduser().resolve()

    if not upsert_path.exists():
        raise FileNotFoundError(f"Upsert-ready file not found: {upsert_path}")
    if not batch_path.exists():
        raise FileNotFoundError(f"Batch results file not found: {batch_path}")

    upsert_rows = load_jsonl(upsert_path)
    batch_rows = load_jsonl(batch_path)

    upsert_by_id: dict[str, dict[str, Any]] = {}
    ordered_ids: list[str] = []
    failures: list[dict[str, Any]] = []

    for row in upsert_rows:
        row_id = str(row.get("id", "")).strip()
        namespace = str(row.get("namespace", "")).strip()
        metadata = row.get("metadata")
        if not row_id:
            failures.append(
                {
                    "kind": "bad_upsert_row",
                    "reason": "missing_id",
                    "upsert_line": row.get("_line_no"),
                }
            )
            continue
        if row_id in upsert_by_id:
            failures.append(
                {
                    "kind": "bad_upsert_row",
                    "reason": "duplicate_upsert_id",
                    "id": row_id,
                    "upsert_line": row.get("_line_no"),
                }
            )
            continue
        if not namespace:
            failures.append(
                {
                    "kind": "bad_upsert_row",
                    "reason": "missing_namespace",
                    "id": row_id,
                    "upsert_line": row.get("_line_no"),
                }
            )
            continue
        if not isinstance(metadata, dict):
            failures.append(
                {
                    "kind": "bad_upsert_row",
                    "reason": "metadata_not_object",
                    "id": row_id,
                    "upsert_line": row.get("_line_no"),
                }
            )
            continue
        upsert_by_id[row_id] = row
        ordered_ids.append(row_id)

    embeddings_by_id: dict[str, list[float]] = {}
    seen_batch_ids: set[str] = set()

    for row in batch_rows:
        custom_id = str(row.get("custom_id", "")).strip()
        if not custom_id:
            failures.append(
                {
                    "kind": "bad_batch_row",
                    "reason": "missing_custom_id",
                    "batch_line": row.get("_line_no"),
                }
            )
            continue
        if custom_id in seen_batch_ids:
            failures.append(
                {
                    "kind": "duplicate_batch_result",
                    "id": custom_id,
                    "batch_line": row.get("_line_no"),
                }
            )
            continue
        seen_batch_ids.add(custom_id)

        vector, error_reason = extract_embedding_from_batch_row(row)
        if vector is None:
            failures.append(
                {
                    "kind": "embedding_missing",
                    "id": custom_id,
                    "reason": error_reason,
                    "batch_line": row.get("_line_no"),
                }
            )
            continue

        if custom_id not in upsert_by_id:
            failures.append(
                {
                    "kind": "batch_id_not_in_upsert",
                    "id": custom_id,
                    "batch_line": row.get("_line_no"),
                }
            )
            continue

        embeddings_by_id[custom_id] = vector

    upserts: list[dict[str, Any]] = []
    for row_id in ordered_ids:
        source_row = upsert_by_id[row_id]
        vector = embeddings_by_id.get(row_id)
        if vector is None:
            failures.append(
                {
                    "kind": "missing_batch_result",
                    "id": row_id,
                    "upsert_line": source_row.get("_line_no"),
                }
            )
            continue

        upserts.append(
            {
                "id": row_id,
                "values": vector,
                "metadata": source_row["metadata"],
                "namespace": source_row["namespace"],
            }
        )

    write_jsonl(out_path, upserts)
    write_jsonl(failures_path, failures)

    print(f"Upsert-ready rows read: {len(upsert_rows)}")
    print(f"Batch result rows read: {len(batch_rows)}")
    print(f"Vector upserts written: {len(upserts)}")
    print(f"Failures written: {len(failures)}")
    print(f"Wrote: {out_path}")
    print(f"Wrote: {failures_path}")

    if args.strict and failures:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
