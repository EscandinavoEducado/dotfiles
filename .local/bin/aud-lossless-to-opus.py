#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "mutagen",
#   "rich",
# ]
# ///
"""
aud-lossless-to-opus — Batch convert lossless audio files to Opus with full metadata preservation.

Supported input formats: FLAC, WAV, AIFF/AIF, ALAC (.m4a), WavPack (.wv), Monkey's Audio (.ape)

Usage:
    aud-lossless-to-opus                              # converts current directory
    aud-lossless-to-opus ~/Music/Artist1 ~/Music/Artist2
    aud-lossless-to-opus --list                       # pick subdirs interactively
    aud-lossless-to-opus --list ~/Music               # pick from ~/Music subdirs
    aud-lossless-to-opus --dry-run ~/Music/
    aud-lossless-to-opus --keep-originals ~/Music/
    aud-lossless-to-opus --bitrate 128 ~/Music/
    aud-lossless-to-opus --workers 4 ~/Music/         # convert 4 files at a time
"""

import sys
import os
import subprocess
import argparse
import signal
import base64
import shutil
import math
import threading
import concurrent.futures
from pathlib import Path
from typing import Optional, List, Tuple

_missing = []
try:
    from mutagen.oggopus import OggOpus
    from mutagen.flac import Picture, FLAC
    from mutagen.mp4 import MP4
    from mutagen.id3 import ID3NoHeaderError
    from mutagen import File as MutagenFile
except ImportError:
    _missing.append("mutagen  →  pip install mutagen")

try:
    from rich.console import Console
    from rich.progress import (
        Progress, SpinnerColumn, TextColumn,
        BarColumn, MofNCompleteColumn, TimeRemainingColumn,
    )
    from rich.table import Table
    from rich.panel import Panel
    from rich.rule import Rule
    from rich.columns import Columns
    from rich.text import Text
    from rich.markup import escape
except ImportError:
    _missing.append("rich     →  pip install rich")

if _missing:
    print("Missing dependencies:\n  " + "\n  ".join(_missing))
    sys.exit(1)


TARGET_BITRATE_KBPS = 180
DURATION_TOLS       = 1.0

LOSSLESS_SUFFIXES = {".flac", ".wav", ".aiff", ".aif", ".m4a", ".wv", ".ape"}

console = Console(highlight=False)


def fmt_size(n: int) -> str:
    if n < 0:
        return f"-{fmt_size(-n)}"
    if n < 1024:
        return f"{n} B"
    if n < 1024 ** 2:
        return f"{n/1024:.1f} KB"
    return f"{n/1024**2:.1f} MB"

def check_ffmpeg() -> bool:
    try:
        r = subprocess.run(["ffmpeg", "-version"], capture_output=True)
        return r.returncode == 0
    except FileNotFoundError:
        return False

def find_lossless_files(folders: List[Path]) -> List[Path]:
    found: List[Path] = []
    for folder in folders:
        if not folder.exists():
            console.print(f"  [red]✗[/red] Folder not found: [bold]{folder}[/bold]")
            continue
        if not folder.is_dir():
            console.print(f"  [red]✗[/red] Not a directory: [bold]{folder}[/bold]")
            continue
        files: List[Path] = []
        for suffix in LOSSLESS_SUFFIXES:
            lower = suffix.lstrip(".")
            upper = lower.upper()
            files.extend(folder.rglob(f"*.{lower}"))
            files.extend(folder.rglob(f"*.{upper}"))
        found.extend(sorted(set(files)))
    return found


def get_source_info(path: Path) -> dict:
    """Read duration, tag presence, and cover art flag from a lossless file."""
    info = {
        "duration":     0.0,
        "has_cover":    False,
        "has_tags":     False,
        "tag_snapshot": {},
    }
    try:
        audio = MutagenFile(path)
        if audio is None:
            return info

        if audio.info:
            info["duration"] = audio.info.length

        tags = audio.tags
        if tags:
            info["has_tags"] = True
            for key, val in tags.items():
                info["tag_snapshot"][key] = str(val)

        suffix = path.suffix.lower()
        if suffix == ".flac":
            flac = FLAC(path)
            info["has_cover"] = bool(flac.pictures)
        elif suffix == ".m4a":
            mp4 = MP4(path)
            info["has_cover"] = "covr" in (mp4.tags or {})
        else:
            if tags:
                keys_str = " ".join(str(k) for k in tags.keys())
                info["has_cover"] = (
                    any(str(k).startswith("APIC") for k in tags)           # ID3
                    or "Cover Art (Front)" in tags                         # APEv2
                    or "metadata_block_picture" in keys_str.lower()
                )
    except Exception as e:
        console.print(f"  [yellow]⚠[/yellow]  Could not read info for {path.name}: {e}")
    return info


def convert(src: Path, opus: Path, kbps: int) -> Tuple[bool, str]:
    """
    Run ffmpeg — audio only (-vn). Cover art is handled separately via mutagen
    so we avoid ffmpeg's unreliable OGG video-stream embedding.
    All Vorbis tags copied via -map_metadata 0.
    """
    cmd = [
        "ffmpeg",
        "-v", "error",
        "-i", str(src),
        "-vn",
        "-map_metadata", "0",
        "-c:a", "libopus",
        "-b:a", f"{kbps}k",
        "-vbr", "on",
        "-compression_level", "10",
        "-y",
        str(opus),
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode != 0:
            if opus.exists() and opus.stat().st_size > 0:
                return True, ""
            err = (r.stderr or "unknown error").strip().splitlines()
            short = next((l for l in reversed(err) if l.strip()), err[-1] if err else "unknown")
            return False, short
        return True, ""
    except Exception as e:
        return False, str(e)


def _picture_from_id3_apic(tags) -> Optional[Picture]:
    """Extract the first APIC frame from an ID3 tag set as a mutagen Picture."""
    for key in tags:
        if str(key).startswith("APIC"):
            apic = tags[key]
            pic = Picture()
            pic.data   = apic.data
            pic.type   = apic.type
            pic.mime   = apic.mime
            pic.desc   = apic.desc or ""
            pic.width = pic.height = pic.depth = pic.colors = 0
            return pic
    return None


def embed_cover_art(src: Path, opus: Path) -> bool:
    """
    Extract cover art from the source file and embed it into the Opus file as a
    base64-encoded METADATA_BLOCK_PICTURE Vorbis comment.

    Supports:
      - FLAC  → native Picture list
      - M4A   → MP4 'covr' atom
      - WAV / AIFF → embedded ID3 APIC frame
      - WavPack / APE → APEv2 'Cover Art (Front)' item
    """
    pic: Optional[Picture] = None
    suffix = src.suffix.lower()

    try:
        if suffix == ".flac":
            flac = FLAC(src)
            if flac.pictures:
                pic = flac.pictures[0]

        elif suffix == ".m4a":
            mp4 = MP4(src)
            covr = (mp4.tags or {}).get("covr")
            if covr:
                raw = bytes(covr[0])
                from mutagen.mp4 import MP4Cover
                mime = (
                    "image/png"
                    if covr[0].imageformat == MP4Cover.FORMAT_PNG
                    else "image/jpeg"
                )
                pic = Picture()
                pic.data   = raw
                pic.type   = 3        # front cover
                pic.mime   = mime
                pic.desc   = ""
                pic.width = pic.height = pic.depth = pic.colors = 0

        elif suffix in {".wav", ".aiff", ".aif"}:
            audio = MutagenFile(src)
            if audio and audio.tags:
                pic = _picture_from_id3_apic(audio.tags)

        elif suffix in {".wv", ".ape"}:
            audio = MutagenFile(src)
            tags = audio.tags if audio else None
            if tags and "Cover Art (Front)" in tags:
                raw = tags["Cover Art (Front)"].value
                null_pos = raw.find(b"\x00")
                img_data = raw[null_pos + 1:] if null_pos != -1 else raw
                mime = "image/jpeg" if img_data[:2] == b"\xff\xd8" else "image/png"
                pic = Picture()
                pic.data   = img_data
                pic.type   = 3
                pic.mime   = mime
                pic.desc   = ""
                pic.width = pic.height = pic.depth = pic.colors = 0

        if pic is None:
            return False

        encoded = base64.b64encode(pic.write()).decode("ascii")
        opus_audio = OggOpus(opus)
        opus_audio["metadata_block_picture"] = [encoded]
        opus_audio.save()
        return True

    except Exception:
        return False


def verify(src: Path, opus: Path, src_info: dict) -> Tuple[bool, List[str]]:
    """
    Sanity-check the converted Opus file. Returns (ok, list_of_issues).
    Checks: file exists & non-empty, duration, cover art, key tags.
    """
    issues: List[str] = []

    if not opus.exists():
        return False, ["Output file does not exist"]
    if opus.stat().st_size == 0:
        return False, ["Output file is empty (0 bytes)"]

    try:
        af = MutagenFile(opus)
        if af is None:
            return False, ["mutagen cannot open output file"]

        dur_diff = abs(af.info.length - src_info["duration"])
        allowed_diff = max(DURATION_TOLS, src_info["duration"] * 0.10)
        if dur_diff > allowed_diff:
            issues.append(
                f"Duration mismatch: source={src_info['duration']:.1f}s "
                f"opus={af.info.length:.1f}s  Δ={dur_diff:.1f}s"
            )

        tags = af.tags or {}

        if src_info["has_cover"]:
            has = (
                "metadata_block_picture" in tags
                or "METADATA_BLOCK_PICTURE" in tags
            )
            if not has:
                issues.append("Cover art missing in output")

        vorbis_keys = ["title", "artist", "album", "tracknumber", "date"]
        snap = src_info["tag_snapshot"]
        snap_lower = {k.lower(): v for k, v in snap.items()}

        for vk in vorbis_keys:
            src_val = snap_lower.get(vk, "").strip()
            if not src_val:
                continue
            dst_val = ""
            if vk in tags:
                v = tags[vk]
                dst_val = (str(v[0]) if isinstance(v, list) else str(v)).strip()
            if src_val.lower() != dst_val.lower():
                issues.append(
                    f"Tag mismatch [{vk}]: source={src_val!r} → opus={dst_val!r}"
                )

    except Exception as e:
        issues.append(f"Verification error: {e}")

    return len(issues) == 0, issues


def get_subdirs(base: Path) -> List[Path]:
    """Return sorted list of immediate subdirectories of *base*."""
    try:
        return sorted(
            (p for p in base.iterdir()
             if p.is_dir() and not p.name.startswith(".")),
            key=lambda p: p.name.lower(),
        )
    except PermissionError:
        console.print(f"[red]✗[/red] Permission denied: [bold]{base}[/bold]")
        return []


def parse_selection(raw: str, max_idx: int) -> List[int]:
    """
    Parse a selection string like "1 3 5-8 10" into a sorted list of
    0-based indices.  Returns an empty list if any token is invalid.
    """
    indices: set[int] = set()
    for token in raw.replace(",", " ").split():
        if "-" in token:
            parts = token.split("-", 1)
            try:
                lo, hi = int(parts[0]), int(parts[1])
            except ValueError:
                console.print(f"  [red]✗[/red] Invalid range: [bold]{token}[/bold]")
                return []
            if lo < 1 or hi > max_idx or lo > hi:
                console.print(
                    f"  [red]✗[/red] Range [bold]{token}[/bold] out of bounds "
                    f"(1–{max_idx})"
                )
                return []
            indices.update(range(lo - 1, hi))
        else:
            try:
                n = int(token)
            except ValueError:
                console.print(f"  [red]✗[/red] Not a number: [bold]{token}[/bold]")
                return []
            if n < 1 or n > max_idx:
                console.print(
                    f"  [red]✗[/red] Number [bold]{n}[/bold] out of bounds "
                    f"(1–{max_idx})"
                )
                return []
            indices.add(n - 1)
    return sorted(indices)


def print_dir_grid(subdirs: List[Path]) -> None:
    """Print numbered subdirectories in a compact multi-column grid."""
    if not subdirs:
        console.print("  [yellow]No subdirectories found.[/yellow]")
        return

    # Build labelled items: "  1  dirname"
    num_w   = len(str(len(subdirs)))          # width of the widest index
    items   = []
    for i, d in enumerate(subdirs, 1):
        label = Text()
        label.append(f"{i:>{num_w}}", style="dim cyan")
        label.append("  ")
        label.append(d.name, style="bold")
        items.append(label)

    console.print(Columns(items, padding=(0, 2), equal=True))


def list_and_select(base: Path) -> List[Path]:
    """
    Show a grid of subdirectories under *base*, prompt for selection,
    and return the chosen Path objects.
    """
    subdirs = get_subdirs(base)

    console.print()
    console.print(Panel.fit(
        f"[bold cyan]Select directories[/bold cyan]   "
        f"[dim]{base}[/dim]",
        border_style="cyan",
        padding=(0, 2),
    ))
    console.print()

    if not subdirs:
        console.print("  [yellow]No subdirectories found.[/yellow]")
        console.print()
        return []

    print_dir_grid(subdirs)
    console.print()
    console.print(
        "  [dim]Enter numbers, ranges, or both — e.g. [bold]1 3 5-8[/bold]  "
        "(space or comma separated)[/dim]"
    )
    console.print()

    while True:
        try:
            raw = input("  Selection: ").strip()
        except (EOFError, KeyboardInterrupt):
            console.print()
            console.print("  [yellow]Cancelled.[/yellow]")
            sys.exit(0)

        if not raw:
            console.print("  [yellow]Nothing selected. Exiting.[/yellow]")
            sys.exit(0)

        idxs = parse_selection(raw, len(subdirs))
        if idxs:
            chosen = [subdirs[i] for i in idxs]
            console.print()
            console.print(
                f"  [green]●[/green] Selected [bold]{len(chosen)}[/bold] "
                f"director{'y' if len(chosen) == 1 else 'ies'}:"
            )
            for d in chosen:
                console.print(f"    [cyan]•[/cyan] {d}")
            console.print()
            return chosen
        # parse_selection already printed the error; loop to re-prompt


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="aud-lossless-to-opus",
        description="Batch-convert lossless audio → Opus at ~180 kbps VBR. "
                    "Copies metadata & cover art, verifies before deleting originals.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
supported formats:
  FLAC (.flac), WAV (.wav), AIFF (.aiff/.aif), ALAC (.m4a), WavPack (.wv), Monkey's Audio (.ape)

examples:
  aud-lossless-to-opus                              # current directory
  aud-lossless-to-opus ~/Music/Artist1 ~/Music/Artist2
  aud-lossless-to-opus --list                       # pick subdirs interactively
  aud-lossless-to-opus --list ~/Music               # pick from ~/Music subdirs
  aud-lossless-to-opus --dry-run ~/Music/
  aud-lossless-to-opus --keep-originals --no-verify ~/Music/
  aud-lossless-to-opus --bitrate 128 ~/Music/
  aud-lossless-to-opus --workers 4 ~/Music/         # convert 4 files in parallel
        """,
    )
    parser.add_argument("folders", nargs="*", type=Path,
                        metavar="FOLDER",
                        help="Folders to search recursively (default: current directory)")
    parser.add_argument("--list", action="store_true",
                        help="List subdirectories and interactively select which to process")
    parser.add_argument("--bitrate", type=int, default=TARGET_BITRATE_KBPS,
                        metavar="KBPS",
                        help=f"Target VBR bitrate in kbps (default: {TARGET_BITRATE_KBPS})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would happen without converting anything")
    parser.add_argument("--keep-originals", action="store_true",
                        help="Do not delete source files after successful conversion")
    parser.add_argument("--no-verify", action="store_true",
                        help="Skip post-conversion verification (faster, riskier)")
    parser.add_argument("--skip-existing", action="store_true", default=True,
                        help="Skip file if a .opus sibling already exists (default: on)")
    parser.add_argument("--no-skip-existing", dest="skip_existing", action="store_false",
                        help="Re-convert even if .opus already exists")
    parser.add_argument("--workers", type=int, default=os.cpu_count() or 4,
                        metavar="N",
                        help="Number of files to convert in parallel "
                             f"(default: number of CPU cores, currently {os.cpu_count() or 4})")
    args = parser.parse_args()

    if args.list:
        # With --list, at most one folder makes sense as the parent to browse.
        base = args.folders[0] if args.folders else Path.cwd()
        args.folders = list_and_select(base)
        if not args.folders:
            sys.exit(0)
    elif not args.folders:
        args.folders = [Path.cwd()]

    interrupted = False
    lock = threading.Lock()
    _active_opus: set[Path] = set()

    def _sigint(sig, frame):
        nonlocal interrupted
        interrupted = True
        with lock:
            to_clean = set(_active_opus)
        for p in to_clean:
            if p.exists():
                try:
                    p.unlink()
                except OSError:
                    pass

    signal.signal(signal.SIGINT, _sigint)

    console.print()
    flags = []
    if args.dry_run:         flags.append("[bold yellow]DRY RUN[/bold yellow]")
    if args.keep_originals:  flags.append("keep originals")
    if args.no_verify:       flags.append("no verify")
    subtitle = "  ·  ".join(flags) if flags else "verify → delete originals"
    console.print(Panel.fit(
        f"[bold cyan]Lossless → Opus[/bold cyan]   "
        f"[dim]{args.bitrate} kbps VBR  ·  {subtitle}[/dim]",
        border_style="cyan",
        padding=(0, 2),
    ))
    console.print()

    if not check_ffmpeg():
        console.print("[bold red]✗ ffmpeg not found.[/bold red] "
                      "Install it and make sure it is on your PATH.")
        sys.exit(1)

    with console.status("[bold]Scanning for lossless files…[/bold]", spinner="dots"):
        src_files = find_lossless_files(args.folders)

    if not src_files:
        console.print("[yellow]No lossless audio files found.[/yellow]")
        sys.exit(0)

    total_bytes = sum(f.stat().st_size for f in src_files)
    console.print(
        f"[green]●[/green] Found [bold]{len(src_files)}[/bold] lossless file(s)  "
        f"[dim]({fmt_size(total_bytes)} total)[/dim]"
    )
    console.print()

    if args.dry_run:
        console.print(Rule("[bold yellow]DRY RUN — nothing will be written[/bold yellow]"))
        console.print()
        for src in src_files:
            info = get_source_info(src)
            cov  = " [dim]🖼[/dim]" if info["has_cover"] else ""
            console.print(
                f"  [cyan]{src.name}[/cyan]  →  "
                f"[green]{src.with_suffix('.opus').name}[/green]"
                f"  [dim]{args.bitrate} kbps VBR{cov}[/dim]"
            )
        console.print()
        sys.exit(0)

    stats = dict(converted=0, skipped=0, failed=0, deleted=0, saved=0)
    failures: List[Tuple[Path, str]] = []

    console.print(Rule("[bold]Converting[/bold]"))
    if args.workers > 1:
        console.print(f"  [dim]Running {args.workers} workers in parallel[/dim]")
    console.print()

    with Progress(
        SpinnerColumn(style="cyan"),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=28, style="cyan", complete_style="green"),
        MofNCompleteColumn(),
        TimeRemainingColumn(compact=True),
        console=console,
        transient=False,
    ) as prog:
        task = prog.add_task(
            "[bold cyan]Overall[/bold cyan]",
            total=len(src_files),
        )

        def process_one(src: Path) -> None:
            if interrupted:
                prog.advance(task)
                return

            opus = src.with_suffix(".opus")
            name = escape(src.name)

            if args.workers == 1:
                label = name[:52] + "…" if len(name) > 53 else name
                prog.update(task, description=f"[dim]{label}[/dim]")

            if args.skip_existing and opus.exists():
                src_info = get_source_info(src)
                valid, _ = verify(src, opus, src_info)
                if valid:
                    prog.print(f"  [yellow]⏭[/yellow]  [dim]{name}[/dim]  [dim](opus exists)[/dim]")
                    with lock:
                        stats["skipped"] += 1
                    prog.advance(task)
                    return
                else:
                    prog.print(
                        f"  [yellow]⚠[/yellow]  [dim]{name}[/dim]  "
                        f"[yellow](incomplete opus found — re-converting)[/yellow]"
                    )
                    opus.unlink()
            else:
                src_info = get_source_info(src)

            src_size = src.stat().st_size

            with lock:
                _active_opus.add(opus)
            ok, err = convert(src, opus, args.bitrate)
            with lock:
                _active_opus.discard(opus)

            if interrupted and not ok:
                prog.advance(task)
                return

            if not ok:
                prog.print(f"  [red]✗ FAIL[/red]  [dim]{name}[/dim]  [red]{err}[/red]")
                with lock:
                    stats["failed"] += 1
                    failures.append((src, f"ffmpeg: {err}"))
                prog.advance(task)
                return

            if src_info["has_cover"]:
                embed_cover_art(src, opus)

            if not args.no_verify:
                ok, issues = verify(src, opus, src_info)
                if not ok:
                    prog.print(
                        f"  [red]✗ VRFY[/red]  [dim]{name}[/dim]  "
                        f"[red]{'; '.join(issues)}[/red]"
                    )
                    if opus.exists():
                        opus.unlink()
                    with lock:
                        stats["failed"] += 1
                        failures.append((src, "; ".join(issues)))
                    prog.advance(task)
                    return

            opus_size = opus.stat().st_size
            delta     = src_size - opus_size
            ratio     = opus_size / src_size * 100 if src_size else 0
            with lock:
                stats["saved"] += delta
                stats["converted"] += 1

            cov_tag  = " [dim]🖼[/dim]" if src_info["has_cover"] else ""
            size_str = (
                f"[dim]{fmt_size(src_size)} → {fmt_size(opus_size)} "
                f"({ratio:.0f}%)[/dim]"
            )

            if not args.keep_originals:
                src.unlink()
                with lock:
                    stats["deleted"] += 1
                prog.print(
                    f"  [green]✓[/green]  {name}{cov_tag}  {size_str}"
                )
            else:
                prog.print(
                    f"  [green]✓[/green]  {name}{cov_tag}  "
                    f"{size_str}  [dim](original kept)[/dim]"
                )

            prog.advance(task)

        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
            futs = {executor.submit(process_one, src): src for src in src_files}
            for fut in concurrent.futures.as_completed(futs):
                if interrupted:
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

    console.print()
    console.print(Rule("[bold]Summary[/bold]"))
    console.print()

    t = Table(show_header=False, box=None, padding=(0, 2))
    t.add_column("k", style="dim")
    t.add_column("v", style="bold")

    t.add_row("Converted",   f"[green]{stats['converted']}[/green]")
    if stats["skipped"]:
        t.add_row("Skipped", f"[yellow]{stats['skipped']}[/yellow]")
    if stats["failed"]:
        t.add_row("Failed",  f"[red]{stats['failed']}[/red]")
    if not args.keep_originals and stats["deleted"]:
        t.add_row("Originals deleted", f"[green]{stats['deleted']}[/green]")
    if stats["saved"] > 0:
        t.add_row("Space freed", f"[cyan]{fmt_size(stats['saved'])}[/cyan]")
    elif stats["saved"] < 0:
        t.add_row("Space added", f"[yellow]+{fmt_size(-stats['saved'])}[/yellow]")

    console.print(t)

    if failures:
        console.print()
        console.print("[bold red]Failed files:[/bold red]")
        for path, reason in failures:
            console.print(f"  [red]•[/red] {path}")
            console.print(f"    [dim]{reason}[/dim]")

    console.print()
    if stats["failed"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
