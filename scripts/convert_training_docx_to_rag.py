#!/usr/bin/env python3
"""
Convert a .docx file containing embedded JSONL chat records into
RAG-ready chunk files with metadata.

Outputs:
  - journal314_clean_records.jsonl
  - journal314_rag_chunks.jsonl
  - journal314_embeddings_upsert_ready.jsonl
  - journal314_openai_embeddings_batch.jsonl
  - journal314_rag_manifest.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import zipfile
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET


WORD_DOC_NS = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
IDP_STEPS = ("Excavate", "Fracture", "Suspend", "Densify", "Attune")
SIGIL = "\u2e38"


@dataclass(frozen=True)
class ChatRecord:
    source_line: int
    system: str
    user: str
    assistant: str
    raw: dict[str, Any]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert embedded JSONL from a .docx training file into RAG chunks."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to source .docx file.",
    )
    parser.add_argument(
        "--out-dir",
        default="data/rag",
        help="Output directory for generated artifacts (default: data/rag).",
    )
    parser.add_argument(
        "--chunk-words",
        type=int,
        default=180,
        help="Chunk size in words (default: 180).",
    )
    parser.add_argument(
        "--overlap-words",
        type=int,
        default=40,
        help="Chunk overlap in words (default: 40).",
    )
    parser.add_argument(
        "--keep-duplicates",
        action="store_true",
        help="Keep duplicate records instead of removing exact duplicates.",
    )
    parser.add_argument(
        "--namespace",
        default="journal314.complete_training.v1",
        help="Namespace for vector DB upsert payloads.",
    )
    parser.add_argument(
        "--embed-model",
        default="text-embedding-3-large",
        help="Embedding model hint used in export metadata and OpenAI batch payload.",
    )
    parser.add_argument(
        "--skip-openai-batch",
        action="store_true",
        help="Skip emitting OpenAI Batch embeddings request JSONL.",
    )
    return parser.parse_args()


def extract_paragraph_lines(docx_path: Path) -> list[str]:
    with zipfile.ZipFile(docx_path) as archive:
        xml_content = archive.read("word/document.xml")

    root = ET.fromstring(xml_content)
    lines: list[str] = []
    for para in root.findall(".//w:p", WORD_DOC_NS):
        parts = [node.text for node in para.findall(".//w:t", WORD_DOC_NS) if node.text]
        if not parts:
            continue
        text = "".join(parts).strip()
        if text:
            lines.append(text)
    return lines


def extract_json_records(lines: list[str]) -> tuple[list[ChatRecord], list[str]]:
    records: list[ChatRecord] = []
    errors: list[str] = []

    for i, line in enumerate(lines, start=1):
        stripped = line.strip()
        if not (stripped.startswith("{") and stripped.endswith("}") and '"messages"' in stripped):
            continue

        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError as exc:
            errors.append(f"Line {i}: invalid JSON ({exc})")
            continue

        messages = payload.get("messages")
        if not isinstance(messages, list):
            errors.append(f"Line {i}: missing list field 'messages'")
            continue

        role_map: dict[str, str] = {}
        for item in messages:
            if not isinstance(item, dict):
                continue
            role = str(item.get("role", "")).strip()
            content = str(item.get("content", ""))
            if role in ("system", "user", "assistant"):
                role_map[role] = content

        if not all(role in role_map for role in ("system", "user", "assistant")):
            errors.append(f"Line {i}: expected roles system/user/assistant")
            continue

        records.append(
            ChatRecord(
                source_line=i,
                system=role_map["system"].strip(),
                user=role_map["user"].strip(),
                assistant=role_map["assistant"].strip(),
                raw=payload,
            )
        )

    return records, errors


def sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def canonical_record_hash(record: ChatRecord) -> str:
    blob = f"{record.system}\n\n{record.user}\n\n{record.assistant}"
    return sha1(blob)


def dedupe_records(records: list[ChatRecord]) -> tuple[list[ChatRecord], int]:
    seen: set[str] = set()
    unique: list[ChatRecord] = []
    removed = 0

    for record in records:
        digest = canonical_record_hash(record)
        if digest in seen:
            removed += 1
            continue
        seen.add(digest)
        unique.append(record)

    return unique, removed


def extract_user_metadata(user_text: str) -> dict[str, Any]:
    result: dict[str, Any] = {
        "prompt_type": "general",
        "figure": None,
        "quote": None,
        "excerpt_author": None,
    }

    stripped = user_text.strip()
    lower = stripped.lower()

    if lower.startswith("figure:"):
        result["prompt_type"] = "figure_quote"
        figure_match = re.search(r"^\s*Figure:\s*(.+?)\s*$", user_text, flags=re.IGNORECASE | re.MULTILINE)
        quote_match = re.search(r"^\s*Quote:\s*(.+)$", user_text, flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)
        if figure_match:
            result["figure"] = figure_match.group(1).strip()
        if quote_match:
            result["quote"] = quote_match.group(1).strip()
        return result

    if lower.startswith("analyze this journal314 excerpt by"):
        result["prompt_type"] = "journal314_excerpt"
        author_match = re.search(
            r"Analyze this Journal314 excerpt by\s+(.+?)\.\s+Use IDP",
            user_text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if author_match:
            result["excerpt_author"] = " ".join(author_match.group(1).split())

        # Capture trailing excerpt text after the first colon if present.
        colon_index = user_text.find(":")
        if colon_index != -1 and colon_index + 1 < len(user_text):
            excerpt = user_text[colon_index + 1 :].strip()
            if excerpt:
                result["quote"] = excerpt
        return result

    return result


def extract_assistant_metadata(assistant_text: str) -> dict[str, Any]:
    ren_raw = None
    ren_code = None
    ren_theme = None
    counter_left = None
    counter_right = None

    ren_match = re.search(r"REN Tag:\s*([^\r\n]+)", assistant_text, flags=re.IGNORECASE)
    if ren_match:
        ren_raw = ren_match.group(1).strip()
        code_match = re.search(r"\bREN\s*[IVXLCDM]+\b", ren_raw, flags=re.IGNORECASE)
        if code_match:
            ren_code = " ".join(code_match.group(0).upper().split())

        if "·" in ren_raw:
            ren_theme = ren_raw.split("·", 1)[1].strip() or None
        elif "-" in ren_raw:
            ren_theme = ren_raw.split("-", 1)[1].strip() or None

    counter_match = re.search(
        r"Counterposition\s*[—-]\s*Set\s+(.+?)\s+beside\s+(.+?);",
        assistant_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if counter_match:
        counter_left = " ".join(counter_match.group(1).split())
        counter_right = " ".join(counter_match.group(2).split())

    steps_present = [
        step
        for step in IDP_STEPS
        if re.search(rf"\b{re.escape(step)}\b", assistant_text, flags=re.IGNORECASE)
    ]

    return {
        "ren_tag": ren_raw,
        "ren_code": ren_code,
        "ren_theme": ren_theme,
        "counterposition_left": counter_left,
        "counterposition_right": counter_right,
        "idp_steps_present": steps_present,
        "has_sigil": SIGIL in assistant_text,
        "ends_with_sigil": assistant_text.rstrip().endswith(SIGIL),
    }


def build_retrieval_text(
    record_id: str,
    user_text: str,
    assistant_text: str,
    user_meta: dict[str, Any],
    assistant_meta: dict[str, Any],
) -> str:
    sections: list[str] = [f"Record: {record_id}"]

    prompt_type = user_meta.get("prompt_type")
    if prompt_type:
        sections.append(f"Prompt Type: {prompt_type}")

    if user_meta.get("figure"):
        sections.append(f"Figure: {user_meta['figure']}")
    if user_meta.get("excerpt_author"):
        sections.append(f"Excerpt Author: {user_meta['excerpt_author']}")
    if assistant_meta.get("ren_tag"):
        sections.append(f"REN Tag: {assistant_meta['ren_tag']}")
    if assistant_meta.get("counterposition_left") and assistant_meta.get("counterposition_right"):
        sections.append(
            f"Counterposition: {assistant_meta['counterposition_left']} vs {assistant_meta['counterposition_right']}"
        )

    if user_meta.get("quote"):
        sections.append(f"Quote:\n{user_meta['quote']}")
    else:
        sections.append(f"User Prompt:\n{user_text}")

    sections.append(f"Assistant Analysis:\n{assistant_text}")
    return "\n\n".join(section.strip() for section in sections if section and section.strip())


def chunk_text_by_words(text: str, chunk_words: int, overlap_words: int) -> list[tuple[int, int, str]]:
    words = re.findall(r"\S+", text)
    if not words:
        return []

    chunks: list[tuple[int, int, str]] = []
    start = 0
    total = len(words)
    overlap = min(max(overlap_words, 0), max(chunk_words - 1, 0))

    while start < total:
        end = min(start + chunk_words, total)
        chunk = " ".join(words[start:end])
        chunks.append((start + 1, end, chunk))
        if end >= total:
            break
        start = end - overlap
        if start < 0:
            start = 0
        if start >= end:
            start = end

    return chunks


def token_estimate(text: str) -> int:
    # Rough but stable estimate for retrieval pipelines.
    return max(1, round(len(text) / 4))


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False))
            handle.write("\n")


def compact_metadata(raw: dict[str, Any]) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for key, value in raw.items():
        if value is None:
            continue
        if isinstance(value, str):
            cleaned = value.strip()
            if not cleaned:
                continue
            compact[key] = cleaned
            continue
        if isinstance(value, (int, float, bool)):
            compact[key] = value
            continue
        if isinstance(value, list):
            clean_items = [str(item).strip() for item in value if str(item).strip()]
            if clean_items:
                compact[key] = clean_items
            continue
    return compact


def build_upsert_metadata(chunk_row: dict[str, Any], generated_at: str) -> dict[str, Any]:
    chunk_meta = dict(chunk_row["metadata"])
    steps = chunk_meta.get("idp_steps_present") or []
    if not isinstance(steps, list):
        steps = []

    typed = {
        "dataset": chunk_meta.get("dataset"),
        "record_id": chunk_meta.get("record_id"),
        "record_index": chunk_meta.get("record_index"),
        "chunk_index": chunk_meta.get("chunk_index"),
        "chunk_word_start": chunk_meta.get("chunk_word_start"),
        "chunk_word_end": chunk_meta.get("chunk_word_end"),
        "chunk_word_count": chunk_meta.get("chunk_word_count"),
        "source_line": chunk_meta.get("source_line"),
        "source_file": chunk_meta.get("source_file"),
        "source_format": chunk_meta.get("source_format"),
        "prompt_type": chunk_meta.get("prompt_type"),
        "figure": chunk_meta.get("figure"),
        "excerpt_author": chunk_meta.get("excerpt_author"),
        "ren_tag": chunk_meta.get("ren_tag"),
        "ren_code": chunk_meta.get("ren_code"),
        "ren_theme": chunk_meta.get("ren_theme"),
        "counterposition_left": chunk_meta.get("counterposition_left"),
        "counterposition_right": chunk_meta.get("counterposition_right"),
        "counterposition_pair": (
            f"{chunk_meta['counterposition_left']}|{chunk_meta['counterposition_right']}"
            if chunk_meta.get("counterposition_left") and chunk_meta.get("counterposition_right")
            else None
        ),
        "has_sigil": chunk_meta.get("has_sigil"),
        "ends_with_sigil": chunk_meta.get("ends_with_sigil"),
        "idp_steps_present": steps,
        "idp_steps_count": len(steps),
        "idp_has_excavate": "Excavate" in steps,
        "idp_has_fracture": "Fracture" in steps,
        "idp_has_suspend": "Suspend" in steps,
        "idp_has_densify": "Densify" in steps,
        "idp_has_attune": "Attune" in steps,
        "system_variant_id": chunk_meta.get("system_variant_id"),
        "system_prompt_hash": chunk_meta.get("system_prompt_hash"),
        "record_hash": chunk_meta.get("record_hash"),
        "generated_utc": generated_at,
    }
    return compact_metadata(typed)


def main() -> int:
    args = parse_args()
    input_path = Path(args.input).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()

    if args.chunk_words <= 0:
        raise ValueError("--chunk-words must be > 0")
    if args.overlap_words < 0:
        raise ValueError("--overlap-words must be >= 0")
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    if input_path.suffix.lower() != ".docx":
        raise ValueError("Input must be a .docx file")

    out_dir.mkdir(parents=True, exist_ok=True)

    lines = extract_paragraph_lines(input_path)
    records, parse_errors = extract_json_records(lines)
    original_count = len(records)
    duplicate_removed = 0
    if not args.keep_duplicates:
        records, duplicate_removed = dedupe_records(records)

    generated_at = datetime.now(timezone.utc).isoformat()

    system_counter = Counter(record.system for record in records)
    system_variant_ids = {
        system_text: f"sys-{sha1(system_text)[:10]}" for system_text in system_counter.keys()
    }

    clean_rows: list[dict[str, Any]] = []
    chunk_rows: list[dict[str, Any]] = []
    embeddings_rows: list[dict[str, Any]] = []
    openai_batch_rows: list[dict[str, Any]] = []
    prompt_types = Counter()

    for idx, record in enumerate(records, start=1):
        record_id = f"j314-rec-{idx:04d}"
        user_meta = extract_user_metadata(record.user)
        assistant_meta = extract_assistant_metadata(record.assistant)
        prompt_types[str(user_meta.get("prompt_type") or "general")] += 1

        retrieval_text = build_retrieval_text(
            record_id=record_id,
            user_text=record.user,
            assistant_text=record.assistant,
            user_meta=user_meta,
            assistant_meta=assistant_meta,
        )
        chunks = chunk_text_by_words(retrieval_text, args.chunk_words, args.overlap_words)

        clean_rows.append(
            {
                "record_id": record_id,
                "source_line": record.source_line,
                "system_prompt": record.system,
                "user_prompt": record.user,
                "assistant_response": record.assistant,
                "metadata": {
                    **user_meta,
                    **assistant_meta,
                    "system_variant_id": system_variant_ids[record.system],
                    "system_prompt_hash": sha1(record.system),
                    "record_hash": canonical_record_hash(record),
                    "user_chars": len(record.user),
                    "assistant_chars": len(record.assistant),
                    "quote_chars": len(user_meta.get("quote") or ""),
                    "generated_utc": generated_at,
                },
            }
        )

        for chunk_idx, (word_start, word_end, text) in enumerate(chunks, start=1):
            chunk_id = f"{record_id}-c{chunk_idx:02d}"
            chunk_row = {
                "id": chunk_id,
                "text": text,
                "tokens_est": token_estimate(text),
                "metadata": {
                    "dataset": "Journal314_CompleteTrainingData",
                    "record_id": record_id,
                    "record_index": idx,
                    "chunk_index": chunk_idx,
                    "chunk_word_start": word_start,
                    "chunk_word_end": word_end,
                    "chunk_word_count": (word_end - word_start + 1),
                    "source_file": str(input_path),
                    "source_format": "docx_with_embedded_jsonl",
                    "source_line": record.source_line,
                    "prompt_type": user_meta.get("prompt_type"),
                    "figure": user_meta.get("figure"),
                    "excerpt_author": user_meta.get("excerpt_author"),
                    "ren_tag": assistant_meta.get("ren_tag"),
                    "ren_code": assistant_meta.get("ren_code"),
                    "ren_theme": assistant_meta.get("ren_theme"),
                    "counterposition_left": assistant_meta.get("counterposition_left"),
                    "counterposition_right": assistant_meta.get("counterposition_right"),
                    "has_sigil": assistant_meta.get("has_sigil"),
                    "ends_with_sigil": assistant_meta.get("ends_with_sigil"),
                    "idp_steps_present": assistant_meta.get("idp_steps_present"),
                    "system_variant_id": system_variant_ids[record.system],
                    "system_prompt_hash": sha1(record.system),
                    "record_hash": canonical_record_hash(record),
                    "generated_utc": generated_at,
                },
            }
            chunk_rows.append(chunk_row)

            upsert_metadata = build_upsert_metadata(chunk_row=chunk_row, generated_at=generated_at)
            embeddings_rows.append(
                {
                    "id": chunk_id,
                    "namespace": args.namespace,
                    "text": text,
                    "text_sha1": sha1(text),
                    "tokens_est": chunk_row["tokens_est"],
                    "embedding_model_hint": args.embed_model,
                    "metadata": upsert_metadata,
                }
            )

            if not args.skip_openai_batch:
                openai_batch_rows.append(
                    {
                        "custom_id": chunk_id,
                        "method": "POST",
                        "url": "/v1/embeddings",
                        "body": {
                            "model": args.embed_model,
                            "input": text,
                        },
                    }
                )

    clean_path = out_dir / "journal314_clean_records.jsonl"
    chunks_path = out_dir / "journal314_rag_chunks.jsonl"
    embeddings_path = out_dir / "journal314_embeddings_upsert_ready.jsonl"
    openai_batch_path = out_dir / "journal314_openai_embeddings_batch.jsonl"
    manifest_path = out_dir / "journal314_rag_manifest.json"

    write_jsonl(clean_path, clean_rows)
    write_jsonl(chunks_path, chunk_rows)
    write_jsonl(embeddings_path, embeddings_rows)
    if not args.skip_openai_batch:
        write_jsonl(openai_batch_path, openai_batch_rows)

    manifest = {
        "generated_utc": generated_at,
        "input_docx": str(input_path),
        "output_dir": str(out_dir),
        "lines_extracted_from_docx": len(lines),
        "records_parsed": original_count,
        "records_retained": len(records),
        "duplicate_records_removed": duplicate_removed,
        "parse_error_count": len(parse_errors),
        "parse_errors": parse_errors[:50],
        "chunk_count": len(chunk_rows),
        "chunking": {
            "chunk_words": args.chunk_words,
            "overlap_words": args.overlap_words,
        },
        "embedding_export": {
            "namespace": args.namespace,
            "embedding_model_hint": args.embed_model,
            "embedding_records_count": len(embeddings_rows),
            "openai_batch_records_count": len(openai_batch_rows),
            "openai_batch_emitted": (not args.skip_openai_batch),
        },
        "prompt_type_counts": dict(prompt_types),
        "system_variants": [
            {
                "system_variant_id": system_variant_ids[system_text],
                "count": count,
                "system_prompt_preview": system_text[:220],
            }
            for system_text, count in system_counter.most_common()
        ],
        "outputs": {
            "clean_records_jsonl": str(clean_path),
            "rag_chunks_jsonl": str(chunks_path),
            "embeddings_upsert_ready_jsonl": str(embeddings_path),
            "openai_embeddings_batch_jsonl": (str(openai_batch_path) if not args.skip_openai_batch else None),
            "manifest_json": str(manifest_path),
        },
        "chunk_schema": {
            "id": "Unique chunk ID.",
            "text": "Chunk text intended for embedding.",
            "tokens_est": "Approximate token estimate.",
            "metadata": "Retrieval/filter metadata fields.",
        },
        "embeddings_upsert_schema": {
            "id": "Stable vector item ID.",
            "namespace": "Vector namespace for partitioning.",
            "text": "Raw text to embed.",
            "text_sha1": "Checksum for integrity and dedupe validation.",
            "tokens_est": "Approximate token estimate for budgeting.",
            "embedding_model_hint": "Recommended embedding model for this payload.",
            "metadata": "Flat, filter-safe metadata for vector DBs.",
        },
        "openai_batch_schema": {
            "custom_id": "Chunk ID to map outputs back to records.",
            "method": "HTTP method for OpenAI Batch request.",
            "url": "OpenAI endpoint path.",
            "body": "Embedding request payload per chunk.",
        },
    }

    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Input file: {input_path}")
    print(f"Docx text lines scanned: {len(lines)}")
    print(f"Records parsed: {original_count}")
    print(f"Records retained: {len(records)}")
    print(f"Duplicates removed: {duplicate_removed}")
    print(f"Parse errors: {len(parse_errors)}")
    print(f"Chunks written: {len(chunk_rows)}")
    print(f"Embeddings upsert rows: {len(embeddings_rows)}")
    if not args.skip_openai_batch:
        print(f"OpenAI batch request rows: {len(openai_batch_rows)}")
    print(f"Wrote: {clean_path}")
    print(f"Wrote: {chunks_path}")
    print(f"Wrote: {embeddings_path}")
    if not args.skip_openai_batch:
        print(f"Wrote: {openai_batch_path}")
    print(f"Wrote: {manifest_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
