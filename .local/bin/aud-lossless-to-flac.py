#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "mutagen",
#   "rich",
# ]
# ///
"""
aud-lossless-to-flac — Batch convert lossless audio files to FLAC with full metadata
and cover-art preservation.

Supported input formats: WAV, AIFF/AIF, ALAC (.m4a), WavPack (.wv), Monkey's Audio (.ape)

Usage:
    aud-lossless-to-flac                              # converts current directory
    aud-lossless-to-flac ~/Music/Artist1 ~/Music/Artist2
    aud-lossless-to-flac --list                       # pick subdirs interactively
    aud-lossless-to-flac --list ~/Music               # pick from ~/Music subdirs
    aud-lossless-to-flac --dry-run ~/Music/
    aud-lossless-to-flac --keep-originals ~/Music/
    aud-lossless-to-flac --compression-level 5 ~/Music/
    aud-lossless-to-flac --workers 4 ~/Music/         # convert 4 files at a time
"""

import sys
import os
import argparse
import subprocess
import threading
import concurrent.futures
from pathlib import Path
from typing import List, Tuple

# ── shared library ────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path.home() / ".local" / "lib"))
import aud_convert_lib as lib
console = lib.console

# ── optional deps ─────────────────────────────────────────────────────────────
_missing = []
try:
    from rich.rule import Rule
    from rich.panel import Panel
    from rich.markup import escape
except ImportError:
    _missing.append("rich     →  pip install rich")

if _missing:
    print("Missing dependencies:\n  " + "\n  ".join(_missing))
    sys.exit(1)


DEFAULT_COMPRESSION = 8

# FLAC itself is excluded — re-encoding FLAC→FLAC at the same level is pointless.
SOURCE_SUFFIXES = {".wav", ".aiff", ".aif", ".m4a", ".wv", ".ape"}


# ─────────────────────────────────────────────────────────────────────────────
# File discovery
# ─────────────────────────────────────────────────────────────────────────────

def find_source_files(folders: List[Path]) -> List[Path]:
    found: List[Path] = []
    for folder in folders:
        if not folder.exists():
            console.print(f"  [red]✗[/red] Folder not found: [bold]{folder}[/bold]")
            continue
        if not folder.is_dir():
            console.print(f"  [red]✗[/red] Not a directory: [bold]{folder}[/bold]")
            continue
        files: List[Path] = []
        for suffix in SOURCE_SUFFIXES:
            lower = suffix.lstrip(".")
            upper = lower.upper()
            files.extend(folder.rglob(f"*.{lower}"))
            files.extend(folder.rglob(f"*.{upper}"))
        found.extend(sorted(set(files)))
    return found


# ─────────────────────────────────────────────────────────────────────────────
# Conversion
# ─────────────────────────────────────────────────────────────────────────────

def convert_to_flac(src: Path, dst: Path, compression: int) -> Tuple[bool, str]:
    """
    Encode *src* to FLAC using ffmpeg.

    - `-map 0` copies all streams, including any embedded cover-art picture
      stream (FLAC supports PICTURE blocks natively, so no mutagen workaround
      is needed).
    - `-map_metadata 0` preserves all tags; ffmpeg remaps ID3/APEv2/iTunes
      keys to Vorbis comments automatically.
    - `-c:a flac` + `-compression_level` control the FLAC encoder.
    - `-c:v copy` passes picture streams through without re-encoding.

    Returns (success, error_message).
    """
    cmd = [
        "ffmpeg",
        "-v", "error",
        "-i", str(src),
        "-map", "0",
        "-map_metadata", "0",
        "-c:a", "flac",
        "-compression_level", str(compression),
        "-c:v", "copy",
        "-y",
        str(dst),
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode != 0:
            # ffmpeg sometimes exits non-zero but still produces a valid file
            # (e.g. benign warnings promoted to errors). Accept it if the
            # output exists and is non-empty.
            if dst.exists() and dst.stat().st_size > 0:
                return True, ""
            err = (r.stderr or "unknown error").strip().splitlines()
            short = next(
                (ln for ln in reversed(err) if ln.strip()),
                err[-1] if err else "unknown",
            )
            return False, short
        return True, ""
    except Exception as e:
        return False, str(e)


def output_ok(dst: Path) -> bool:
    """Lightweight sanity check: file exists and is non-empty."""
    return dst.exists() and dst.stat().st_size > 0


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Batch convert lossless audio to FLAC."
    )
    parser.add_argument("folders", nargs="*", type=Path,
                        metavar="FOLDER",
                        help="Folders to process (default: current directory)")
    parser.add_argument("--list", action="store_true",
                        help="List subdirectories and interactively select which to process")
    parser.add_argument("--compression-level", type=int, default=DEFAULT_COMPRESSION,
                        metavar="N", dest="compression",
                        help=f"FLAC compression level 0–12 (default: {DEFAULT_COMPRESSION}; "
                             "higher = smaller file, slower encode)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would happen without converting anything")
    parser.add_argument("--keep-originals", action="store_true",
                        help="Do not delete source files after successful conversion")
    parser.add_argument("--skip-existing", action="store_true", default=True,
                        help="Skip file if a .flac sibling already exists (default: on)")
    parser.add_argument("--no-skip-existing", dest="skip_existing", action="store_false",
                        help="Re-convert even if .flac already exists")
    parser.add_argument("--workers", type=int, default=os.cpu_count() or 4,
                        metavar="N",
                        help="Number of files to convert in parallel "
                             f"(default: number of CPU cores, currently {os.cpu_count() or 4})")
    args = parser.parse_args()

    if args.compression < 0 or args.compression > 12:
        console.print("[red]✗[/red] --compression-level must be between 0 and 12.")
        sys.exit(1)

    if args.list:
        base = args.folders[0] if args.folders else Path.cwd()
        args.folders = lib.list_and_select(base)
        if not args.folders:
            sys.exit(0)
    elif not args.folders:
        args.folders = [Path.cwd()]

    interrupted_event = threading.Event()
    lock = threading.Lock()
    _active_flac: set[Path] = set()

    interrupted_event, _ = lib.make_interrupt_handler(_active_flac, lock)

    def interrupted() -> bool:
        return interrupted_event.is_set()

    console.print()
    flags = []
    if args.dry_run:        flags.append("[bold yellow]DRY RUN[/bold yellow]")
    if args.keep_originals: flags.append("keep originals")
    subtitle = "  ·  ".join(flags) if flags else "delete originals on success"
    console.print(Panel.fit(
        f"[bold cyan]Lossless → FLAC[/bold cyan]   "
        f"[dim]compression {args.compression}  ·  {subtitle}[/dim]",
        border_style="cyan",
        padding=(0, 2),
    ))
    console.print()

    if not lib.check_ffmpeg():
        console.print("[bold red]✗ ffmpeg not found.[/bold red] "
                      "Install it and make sure it is on your PATH.")
        sys.exit(1)

    with console.status("[bold]Scanning for lossless files…[/bold]", spinner="dots"):
        src_files = find_source_files(args.folders)

    if not src_files:
        console.print("[yellow]No lossless audio files found.[/yellow]")
        sys.exit(0)

    total_bytes = sum(f.stat().st_size for f in src_files)
    console.print(
        f"[green]●[/green] Found [bold]{len(src_files)}[/bold] file(s)  "
        f"[dim]({lib.fmt_size(total_bytes)} total)[/dim]"
    )
    console.print()

    if args.dry_run:
        console.print(Rule("[bold yellow]DRY RUN — nothing will be written[/bold yellow]"))
        console.print()
        for src in src_files:
            console.print(
                f"  [cyan]{src.name}[/cyan]  →  "
                f"[green]{src.with_suffix('.flac').name}[/green]"
                f"  [dim]compression {args.compression}[/dim]"
            )
        console.print()
        sys.exit(0)

    stats = dict(converted=0, skipped=0, failed=0, deleted=0, saved=0)
    failures: List[Tuple[Path, str]] = []

    console.print(Rule("[bold]Converting[/bold]"))
    if args.workers > 1:
        console.print(f"  [dim]Running {args.workers} workers in parallel[/dim]")
    console.print()

    with lib.make_progress() as prog:
        task = prog.add_task(
            "[bold cyan]Overall[/bold cyan]",
            total=len(src_files),
        )

        def process_one(src: Path) -> None:
            if interrupted():
                prog.advance(task)
                return

            dst  = src.with_suffix(".flac")
            name = escape(src.name)

            if args.workers == 1:
                label = name[:52] + "…" if len(name) > 53 else name
                prog.update(task, description=f"[dim]{label}[/dim]")

            if args.skip_existing and dst.exists():
                if output_ok(dst):
                    prog.print(
                        f"  [yellow]⏭[/yellow]  [dim]{name}[/dim]  "
                        f"[dim](flac exists)[/dim]"
                    )
                    with lock:
                        stats["skipped"] += 1
                    prog.advance(task)
                    return
                else:
                    prog.print(
                        f"  [yellow]⚠[/yellow]  [dim]{name}[/dim]  "
                        f"[yellow](empty/incomplete flac found — re-converting)[/yellow]"
                    )
                    dst.unlink()

            src_size = src.stat().st_size

            with lock:
                _active_flac.add(dst)
            ok, err = convert_to_flac(src, dst, args.compression)
            with lock:
                _active_flac.discard(dst)

            if interrupted() and not ok:
                prog.advance(task)
                return

            if not ok:
                prog.print(f"  [red]✗ FAIL[/red]  [dim]{name}[/dim]  [red]{err}[/red]")
                with lock:
                    stats["failed"] += 1
                    failures.append((src, f"ffmpeg: {err}"))
                prog.advance(task)
                return

            # Lightweight sanity check (no full verify needed for lossless→lossless).
            if not output_ok(dst):
                prog.print(
                    f"  [red]✗ EMPTY[/red]  [dim]{name}[/dim]  "
                    f"[red]Output file is missing or empty[/red]"
                )
                if dst.exists():
                    dst.unlink()
                with lock:
                    stats["failed"] += 1
                    failures.append((src, "Output file missing or empty"))
                prog.advance(task)
                return

            dst_size = dst.stat().st_size
            delta    = src_size - dst_size
            ratio    = dst_size / src_size * 100 if src_size else 0
            with lock:
                stats["saved"]     += delta
                stats["converted"] += 1

            size_str = (
                f"[dim]{lib.fmt_size(src_size)} → {lib.fmt_size(dst_size)} "
                f"({ratio:.0f}%)[/dim]"
            )

            if not args.keep_originals:
                src.unlink()
                with lock:
                    stats["deleted"] += 1
                prog.print(f"  [green]✓[/green]  {name}  {size_str}")
            else:
                prog.print(
                    f"  [green]✓[/green]  {name}  "
                    f"{size_str}  [dim](original kept)[/dim]"
                )

            prog.advance(task)

        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
            futs = {executor.submit(process_one, src): src for src in src_files}
            for fut in concurrent.futures.as_completed(futs):
                if interrupted():
                    for f in futs:
                        f.cancel()
                    prog.print("  [yellow]⚡ Interrupted by user.[/yellow]")
                    break
                try:
                    fut.result()
                except Exception as exc:
                    src = futs[fut]
                    prog.print(
                        f"  [red]✗[/red]  {escape(src.name)}  "
                        f"[red]Unexpected error: {exc}[/red]"
                    )
                    with lock:
                        stats["failed"] += 1

    lib.print_summary(stats, failures, args.keep_originals)
    if stats["failed"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
