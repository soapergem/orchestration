"""Transcribe a recorded talk to Markdown with faster-whisper.

Usage:
    uv run python scripts/transcribe.py transcript.m4a transcript.md [--model small]

Groups segments into ~60-second blocks under `[mm:ss]` headings so the result is
navigable and diffable against SCRIPT.md. Passes the bake-off's proper nouns as an
initial prompt, which substantially improves recognition of tool names.
"""

import argparse
import logging
import time
from pathlib import Path

from faster_whisper import WhisperModel

logger = logging.getLogger(__name__)

# Whisper conditions on this, so listing the domain vocabulary up front stops
# "Kestra" becoming "Kestrel" and "Flyte" becoming "flight".
VOCAB = (
    "A talk comparing workflow orchestration engines: AWS Step Functions, Apache Airflow, "
    "Argo Workflows, Dagster, Temporal, Kestra, Prefect, Flyte, Luigi, Hatchet, "
    "Google Workflows, and Conductor. Topics include DAGs, directed acyclic graphs, "
    "idempotency, saga compensation, suspend and resume, retries with exponential backoff "
    "and jitter, Postgres, Kubernetes, Parquet, Terraform, Lambda, Cloud Run, and Neon."
)

BLOCK_SECONDS = 60


def fmt(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("audio", type=Path)
    ap.add_argument("out", type=Path)
    ap.add_argument("--model", default="small")
    ap.add_argument("--threads", type=int, default=16)
    ap.add_argument("--beam", type=int, default=1,
                    help="1 is ~6x faster than 5 on CPU with no visible quality loss here")
    args = ap.parse_args()

    logger.info("loading model %s (int8, %d threads)", args.model, args.threads)
    model = WhisperModel(
        args.model, device="cpu", compute_type="int8", cpu_threads=args.threads
    )

    started = time.monotonic()
    segments, info = model.transcribe(
        str(args.audio),
        language="en",
        beam_size=args.beam,
        vad_filter=True,
        vad_parameters={"min_silence_duration_ms": 700},
        initial_prompt=VOCAB,
        condition_on_previous_text=True,
    )
    total = info.duration
    logger.info("audio duration %s, detected language %s", fmt(total), info.language)

    blocks: list[tuple[float, list[str]]] = []
    block_start: float | None = None
    buf: list[str] = []

    for seg in segments:
        text = seg.text.strip()
        if not text:
            continue
        if block_start is None:
            block_start = seg.start
        buf.append(text)
        if seg.end - block_start >= BLOCK_SECONDS:
            blocks.append((block_start, buf))
            block_start, buf = None, []
            done = seg.end
            pct = 100.0 * done / total
            el = time.monotonic() - started
            rate = done / el if el else 0
            eta = (total - done) / rate if rate else 0
            logger.info(
                "%s / %s  (%.0f%%)  %.1fx realtime, eta %s",
                fmt(done), fmt(total), pct, rate, fmt(eta),
            )
    if buf and block_start is not None:
        blocks.append((block_start, buf))

    lines = [
        "# Orchest-Rated — presentation transcript",
        "",
        f"*Source: `{args.audio.name}` · {fmt(total)} ·"
        f" transcribed with faster-whisper `{args.model}` (int8, CPU)*",
        "",
        "> Machine transcript, lightly segmented. Timestamps are block starts.",
        "",
    ]
    for start, texts in blocks:
        lines.append(f"## [{fmt(start)}]")
        lines.append("")
        lines.append(" ".join(texts))
        lines.append("")

    args.out.write_text("\n".join(lines), encoding="utf-8")
    el = time.monotonic() - started
    words = sum(len(" ".join(t).split()) for _, t in blocks)
    logger.info(
        "wrote %s — %d blocks, ~%d words, in %s (%.1fx realtime)",
        args.out, len(blocks), words, fmt(el), total / el,
    )


if __name__ == "__main__":
    main()
