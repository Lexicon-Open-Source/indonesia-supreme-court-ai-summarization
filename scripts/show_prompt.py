#!/usr/bin/env python3
"""
Script to output the full extraction prompt sent to the LLM.

Usage:
    uv run python scripts/show_prompt.py
    uv run python scripts/show_prompt.py --output prompt.md
"""

import argparse
import json
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.extraction import (
    EXTRACTION_PROMPT,
    EXTRACTION_SYSTEM_PROMPT,
    ExtractionResult,
)


def get_sample_extraction() -> dict:
    """Get an empty extraction result as sample."""
    return {}


def get_sample_chunk() -> str:
    """Get a sample document chunk."""
    return """[Sample document chunk would appear here]

P U T U S A N
Nomor: 123/Pid.Sus-TPK/2024/PN.Jkt.Pst

DEMI KEADILAN BERDASARKAN KETUHANAN YANG MAHA ESA

Pengadilan Tindak Pidana Korupsi pada Pengadilan Negeri Jakarta Pusat yang
memeriksa dan mengadili perkara tindak pidana korupsi...

TERDAKWA:
Nama lengkap    : JOHN DOE
Tempat lahir    : Jakarta
Umur/Tanggal lahir : 45 tahun / 15 Januari 1979
Jenis kelamin   : Laki-laki
Kebangsaan      : Indonesia
Tempat tinggal  : Jl. Contoh No. 123, Jakarta Selatan
Agama           : Islam
Pekerjaan       : Pegawai Negeri Sipil
Pendidikan      : S1

..."""


def main():
    parser = argparse.ArgumentParser(
        description="Output the full extraction prompt sent to the LLM"
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        help="Output file path (default: stdout)"
    )
    parser.add_argument(
        "--system-only",
        action="store_true",
        help="Only output the system prompt"
    )
    parser.add_argument(
        "--schema-only",
        action="store_true",
        help="Only output the JSON schema"
    )
    args = parser.parse_args()

    output_lines = []

    if args.schema_only:
        # Only output the JSON schema
        schema = ExtractionResult.model_json_schema()
        output_lines.append(json.dumps(schema, indent=2, ensure_ascii=False))
    elif args.system_only:
        # Only system prompt
        output_lines.append("# SYSTEM PROMPT")
        output_lines.append("=" * 80)
        output_lines.append(EXTRACTION_SYSTEM_PROMPT)
    else:
        # Full prompt
        output_lines.append("# EXTRACTION PROMPT")
        output_lines.append(f"Generated from ExtractionResult model with {len(ExtractionResult.model_fields)} top-level fields")
        output_lines.append("")

        output_lines.append("## SYSTEM PROMPT")
        output_lines.append("=" * 80)
        output_lines.append(EXTRACTION_SYSTEM_PROMPT)
        output_lines.append("")

        output_lines.append("## USER PROMPT (Template)")
        output_lines.append("=" * 80)
        sample_prompt = EXTRACTION_PROMPT.format(
            current_extraction=json.dumps(get_sample_extraction(), indent=2),
            chunk_number=1,
            total_chunks=1,
            chunk_content=get_sample_chunk(),
        )
        output_lines.append(sample_prompt)
        output_lines.append("")

        # Stats
        output_lines.append("## STATISTICS")
        output_lines.append("=" * 80)
        system_chars = len(EXTRACTION_SYSTEM_PROMPT)
        user_chars = len(sample_prompt)
        output_lines.append(f"System prompt length: {system_chars:,} characters (~{system_chars // 4:,} tokens)")
        output_lines.append(f"User prompt length: {user_chars:,} characters (~{user_chars // 4:,} tokens)")
        output_lines.append(f"Total: {system_chars + user_chars:,} characters (~{(system_chars + user_chars) // 4:,} tokens)")

    output = "\n".join(output_lines)

    if args.output:
        Path(args.output).write_text(output, encoding="utf-8")
        print(f"Prompt written to {args.output}")
    else:
        print(output)


if __name__ == "__main__":
    main()
