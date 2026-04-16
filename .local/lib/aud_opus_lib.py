"""
aud_opus_lib — shared helpers for aud-*-to-opus conversion scripts.

Install at: ~/.local/lib/aud_opus_lib.py

Scripts load it by inserting ~/.local/lib into sys.path at startup:

    import sys, pathlib
    sys.path.insert(0, str(pathlib.Path.home() / ".local" / "lib"))
    import aud_opus_lib as lib
"""

from __future__ import annotations

import base64
import signal
import subprocess
import sys
import threading
from pathlib import Path
from typing import List, Optional, Tuple

# ── optional-dependency guard ─────────────────────────────────────────────────
_missing: list[str] = []
try:
    from mutagen.flac import Picture
    from mutagen.oggopus import OggOpus
    from mutagen import File as MutagenFile
except ImportError:
    _missing.append("mutagen  →  pip install mutagen")

try:
    from rich.columns import Columns
    from rich.console import Console
    from rich.markup import escape
    from rich.panel import Panel
    from rich.progress import (
        BarColumn,
        MofNCompleteColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TimeRemainingColumn,
    )
    from rich.rule import Rule
    from rich.table import Table
    from rich.text import Text
except ImportError:
    _missing.append("rich     →  pip install rich")

if _missing:
    print("aud_opus_lib: missing dependencies:\n  " + "\n  ".join(_missing))
    sys.exit(1)

# ── module-level console (importers may replace this) ─────────────────────────
console = Console(highlight=False)


# ─────────────────────────────────────────────────────────────────────────────
# Formatting
# ─────────────────────────────────────────────────────────────────────────────

def fmt_size(n: int) -> str:
    """Human-readable byte count (B / KB / MB)."""
    if n < 0:
        return f"-{fmt_size(-n)}"
    if n < 1024:
        return f"{n} B"
    if n < 1024 ** 2:
        return f"{n / 1024:.1f} KB"
    return f"{n / 1024 ** 2:.1f} MB"


# ─────────────────────────────────────────────────────────────────────────────
# ffmpeg helpers
# ─────────────────────────────────────────────────────────────────────────────

def check_ffmpeg() -> bool:
    """Return True if ffmpeg is on PATH and exits cleanly."""
    try:
        r = subprocess.run(["ffmpeg", "-version"], capture_output=True)
        return r.returncode == 0
    except FileNotFoundError:
        return False


def convert(src: Path, opus: Path, kbps: int) -> Tuple[bool, str]:
    """
    Encode *src* to Opus at *kbps* kbps VBR using ffmpeg.

    Audio-only (-vn); cover art is embedded separately via mutagen to avoid
    ffmpeg's unreliable OGG video-stream embedding.
    All Vorbis tags are copied via -map_metadata 0.

    Returns (success, error_message).
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
            short = next((ln for ln in reversed(err) if ln.strip()),
                         err[-1] if err else "unknown")
            return False, short
        return True, ""
    except Exception as e:
        return False, str(e)


# ─────────────────────────────────────────────────────────────────────────────
# Cover-art embedding
# ─────────────────────────────────────────────────────────────────────────────

def embed_picture_in_opus(pic: Picture, opus: Path) -> bool:
    """
    Write a mutagen Picture into an Opus file as a base64-encoded
    METADATA_BLOCK_PICTURE Vorbis comment.  Returns True on success.
    """
    try:
        encoded = base64.b64encode(pic.write()).decode("ascii")
        opus_audio = OggOpus(opus)
        opus_audio["metadata_block_picture"] = [encoded]
        opus_audio.save()
        return True
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Verification helpers
# ─────────────────────────────────────────────────────────────────────────────

def verify_opus_basics(
    opus: Path,
    src_info: dict,
    duration_tols: float,
) -> Tuple[bool, list[str], "MutagenFile | None"]:
    """
    Run the format-agnostic checks on a freshly converted Opus file:
      • file exists and is non-empty
      • duration is within tolerance
      • cover art is present if the source had it

    Returns (ok, issues, opus_mutagen_file).
    The mutagen file is returned so callers can run additional tag checks
    without reopening it.  It is None if the file could not be opened.
    """
    issues: list[str] = []

    if not opus.exists():
        return False, ["Output file does not exist"], None
    if opus.stat().st_size == 0:
        return False, ["Output file is empty (0 bytes)"], None

    try:
        af = MutagenFile(opus)
        if af is None:
            return False, ["mutagen cannot open output file"], None

        dur_diff = abs(af.info.length - src_info["duration"])
        allowed_diff = max(duration_tols, src_info["duration"] * 0.10)
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

    except Exception as e:
        return False, [f"Verification error: {e}"], None

    return len(issues) == 0, issues, af


# ─────────────────────────────────────────────────────────────────────────────
# Interactive directory picker
# ─────────────────────────────────────────────────────────────────────────────

def get_subdirs(base: Path) -> List[Path]:
    """Sorted list of non-hidden immediate subdirectories of *base*."""
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
    Space- or comma-separated; ranges supported (e.g. 5-8).
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

    num_w = len(str(len(subdirs)))
    items = []
    for i, d in enumerate(subdirs, 1):
        label = Text()
        label.append(f"{i:>{num_w}}", style="dim cyan")
        label.append("  ")
        label.append(d.name, style="bold")
        items.append(label)

    console.print(Columns(items, padding=(0, 2), equal=True))


def list_and_select(base: Path) -> List[Path]:
    """
    Show a grid of subdirectories under *base*, prompt for a selection,
    and return the chosen Path objects.
    """
    subdirs = get_subdirs(base)

    console.print()
    console.print(Panel.fit(
        f"[bold cyan]Select directories[/bold cyan]   [dim]{base}[/dim]",
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


# ─────────────────────────────────────────────────────────────────────────────
# Progress bar factory
# ─────────────────────────────────────────────────────────────────────────────

def make_progress() -> Progress:
    """Return the standard Rich Progress bar used by all conversion scripts."""
    return Progress(
        SpinnerColumn(style="cyan"),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=28, style="cyan", complete_style="green"),
        MofNCompleteColumn(),
        TimeRemainingColumn(compact=True),
        console=console,
        transient=False,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Interrupt handling
# ─────────────────────────────────────────────────────────────────────────────

def make_interrupt_handler(
    active_set: set,
    lock: threading.Lock,
) -> Tuple[threading.Event, object]:
    """
    Install a SIGINT handler that:
      • sets an Event (callers poll this to detect interruption)
      • deletes any in-progress .opus files tracked in *active_set*

    Returns (interrupted_event, handler_fn).
    """
    interrupted = threading.Event()

    def _handler(sig, frame):
        interrupted.set()
        with lock:
            to_clean = set(active_set)
        for p in to_clean:
            p = Path(p)
            if p.exists():
                try:
                    p.unlink()
                except OSError:
                    pass

    signal.signal(signal.SIGINT, _handler)
    return interrupted, _handler


# ─────────────────────────────────────────────────────────────────────────────
# Summary printing
# ─────────────────────────────────────────────────────────────────────────────

def print_summary(
    stats: dict,
    failures: List[Tuple[Path, str]],
    keep_originals: bool,
) -> None:
    """
    Print the end-of-run summary table and optional failures list.

    *stats* must have keys: converted, skipped, failed, deleted, saved.
    """
    console.print()
    console.print(Rule("[bold]Summary[/bold]"))
    console.print()

    t = Table(show_header=False, box=None, padding=(0, 2))
    t.add_column("k", style="dim")
    t.add_column("v", style="bold")

    t.add_row("Converted", f"[green]{stats['converted']}[/green]")
    if stats["skipped"]:
        t.add_row("Skipped", f"[yellow]{stats['skipped']}[/yellow]")
    if stats["failed"]:
        t.add_row("Failed", f"[red]{stats['failed']}[/red]")
    if not keep_originals and stats["deleted"]:
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
