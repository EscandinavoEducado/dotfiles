#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "mutagen",
#   "rich",
# ]
# ///
"""
mp3_to_opus — Batch convert MP3 files to Opus with full metadata preservation.

Usage:
    mp3_to_opus                              # converts current directory
    mp3_to_opus ~/Music/Artist1 ~/Music/Artist2
    mp3_to_opus --dry-run ~/Music/
    mp3_to_opus --keep-originals ~/Music/
"""

import sys
import os
import subprocess
import argparse
import signal
import base64
from pathlib import Path
from typing import Optional, List, Tuple

_missing = []
try:
    from mutagen.mp3 import MP3
    from mutagen.oggopus import OggOpus
    from mutagen.flac import Picture
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
except ImportError:
    _missing.append("rich     →  pip install rich")

if _missing:
    print("Missing dependencies:\n  " + "\n  ".join(_missing))
    sys.exit(1)


BITRATE_RATIO     = 0.7
MIN_BITRATE_KBPS  = 24
MAX_BITRATE_KBPS  = 320
DEFAULT_BITRATE   = 160
DURATION_TOLS     = 1.0

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

def find_mp3_files(folders: List[Path]) -> List[Path]:
    found: List[Path] = []
    for folder in folders:
        if not folder.exists():
            console.print(f"  [red]✗[/red] Folder not found: [bold]{folder}[/bold]")
            continue
        if not folder.is_dir():
            console.print(f"  [red]✗[/red] Not a directory: [bold]{folder}[/bold]")
            continue
        mp3s = sorted(folder.rglob("*.[mM][pP]3"))
        found.extend(mp3s)
    return found


def get_mp3_info(path: Path) -> dict:
    """Read bitrate, duration, tag presence, and cover art flag from an MP3."""
    info = {
        "bitrate_kbps": DEFAULT_BITRATE,
        "duration":     0.0,
        "has_cover":    False,
        "has_tags":     False,
        "tag_snapshot": {},
    }
    try:
        audio = MP3(path)
        info["duration"]     = audio.info.length
        info["bitrate_kbps"] = max(1, audio.info.bitrate // 1000)

        if audio.tags:
            info["has_tags"] = True
            for key, val in audio.tags.items():
                info["tag_snapshot"][key] = str(val)
            # APIC = Attached Picture (cover art in ID3)
            info["has_cover"] = any(k.startswith("APIC") for k in audio.tags)
    except Exception as e:
        console.print(f"  [yellow]⚠[/yellow]  Could not read MP3 info for {path.name}: {e}")
    return info

def target_bitrate(source_kbps: int) -> int:
    raw = round(source_kbps * BITRATE_RATIO)
    return max(MIN_BITRATE_KBPS, min(MAX_BITRATE_KBPS, raw))

def convert(mp3: Path, opus: Path, kbps: int) -> Tuple[bool, str]:
    """
    Run ffmpeg — audio only (-vn). Cover art is handled separately via mutagen
    so we avoid ffmpeg's unreliable OGG video-stream embedding.
    All Vorbis tags copied via -map_metadata 0.
    """
    cmd = [
        "ffmpeg",
        "-v", "error",
        "-i", str(mp3),
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
            err = (r.stderr or "unknown error").strip().splitlines()
            short = next((l for l in reversed(err) if l.strip()), err[-1] if err else "unknown")
            return False, short
        return True, ""
    except Exception as e:
        return False, str(e)

def embed_cover_art(mp3: Path, opus: Path) -> bool:
    """
    Extract APIC cover art from the MP3's ID3 tags and embed it into the Opus
    file as a base64-encoded METADATA_BLOCK_PICTURE Vorbis comment.
    """
    try:
        mp3_audio = MP3(mp3)
        if not mp3_audio.tags:
            return False

        apic = None
        for key in mp3_audio.tags:
            if key.startswith("APIC"):
                apic = mp3_audio.tags[key]
                break
        if apic is None:
            return False

        pic = Picture()
        pic.data = apic.data
        pic.type = apic.type        # 3 = front cover
        pic.mime = apic.mime
        pic.desc = apic.desc or ""
        pic.width = pic.height = pic.depth = pic.colors = 0

        encoded = base64.b64encode(pic.write()).decode("ascii")

        opus_audio = OggOpus(opus)
        opus_audio["metadata_block_picture"] = [encoded]
        opus_audio.save()
        return True
    except Exception:
        return False

def verify(mp3: Path, opus: Path, mp3_info: dict) -> Tuple[bool, List[str]]:
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

        dur_diff = abs(af.info.length - mp3_info["duration"])
        if dur_diff > DURATION_TOLS:
            issues.append(
                f"Duration mismatch: source={mp3_info['duration']:.1f}s "
                f"opus={af.info.length:.1f}s  Δ={dur_diff:.1f}s"
            )

        tags = af.tags or {}

        if mp3_info["has_cover"]:
            has = (
                "metadata_block_picture" in tags
                or "METADATA_BLOCK_PICTURE" in tags
            )
            if not has:
                issues.append("Cover art missing in output")

        id3_to_vorbis = {
            "TIT2": "title",
            "TPE1": "artist",
            "TALB": "album",
            "TRCK": "tracknumber",
            "TDRC": "date",
        }
        snap = mp3_info["tag_snapshot"]
        for id3_key, vorbis_key in id3_to_vorbis.items():
            if id3_key not in snap:
                continue
            src_val = snap[id3_key].strip()
            dst_val = ""
            if vorbis_key in tags:
                v = tags[vorbis_key]
                dst_val = (str(v[0]) if isinstance(v, list) else str(v)).strip()
            if src_val.lower() != dst_val.lower():
                issues.append(
                    f"Tag mismatch [{vorbis_key}]: "
                    f"source={src_val!r} → opus={dst_val!r}"
                )

    except Exception as e:
        issues.append(f"Verification error: {e}")

    return len(issues) == 0, issues


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="mp3_to_opus",
        description="Batch-convert MP3 → Opus. Copies metadata & cover art, "
                    "targets 70%% of source bitrate, verifies before deleting originals.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  mp3_to_opus                              # current directory
  mp3_to_opus ~/Music/Artist1 ~/Music/Artist2
  mp3_to_opus --dry-run ~/Music/
  mp3_to_opus --keep-originals --no-verify ~/Music/
        """,
    )
    parser.add_argument("folders", nargs="*", type=Path,
                        metavar="FOLDER",
                        help="Folders to search recursively (default: current directory)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would happen without converting anything")
    parser.add_argument("--keep-originals", action="store_true",
                        help="Do not delete MP3 files after successful conversion")
    parser.add_argument("--no-verify", action="store_true",
                        help="Skip post-conversion verification (faster, riskier)")
    parser.add_argument("--skip-existing", action="store_true", default=True,
                        help="Skip MP3 if a .opus sibling already exists (default: on)")
    parser.add_argument("--no-skip-existing", dest="skip_existing", action="store_false",
                        help="Re-convert even if .opus already exists")
    args = parser.parse_args()

    if not args.folders:
        args.folders = [Path.cwd()]

    interrupted = False
    def _sigint(sig, frame):
        nonlocal interrupted
        interrupted = True
    signal.signal(signal.SIGINT, _sigint)

    console.print()
    flags = []
    if args.dry_run:         flags.append("[bold yellow]DRY RUN[/bold yellow]")
    if args.keep_originals:  flags.append("keep originals")
    if args.no_verify:       flags.append("no verify")
    subtitle = "  ·  ".join(flags) if flags else "verify → delete originals"
    console.print(Panel.fit(
        f"[bold cyan]MP3 → Opus[/bold cyan]   [dim]{subtitle}[/dim]",
        border_style="cyan",
        padding=(0, 2),
    ))
    console.print()

    if not check_ffmpeg():
        console.print("[bold red]✗ ffmpeg not found.[/bold red] "
                      "Install it and make sure it is on your PATH.")
        sys.exit(1)

    with console.status("[bold]Scanning for MP3 files…[/bold]", spinner="dots"):
        mp3_files = find_mp3_files(args.folders)

    if not mp3_files:
        console.print("[yellow]No MP3 files found.[/yellow]")
        sys.exit(0)

    total_bytes = sum(f.stat().st_size for f in mp3_files)
    console.print(
        f"[green]●[/green] Found [bold]{len(mp3_files)}[/bold] MP3 file(s)  "
        f"[dim]({fmt_size(total_bytes)} total)[/dim]"
    )
    console.print()

    if args.dry_run:
        console.print(Rule("[bold yellow]DRY RUN — nothing will be written[/bold yellow]"))
        console.print()
        for mp3 in mp3_files:
            info = get_mp3_info(mp3)
            tb   = target_bitrate(info["bitrate_kbps"])
            cov  = " [dim]🖼[/dim]" if info["has_cover"] else ""
            console.print(
                f"  [cyan]{mp3.name}[/cyan]  →  "
                f"[green]{mp3.with_suffix('.opus').name}[/green]"
                f"  [dim]{info['bitrate_kbps']} → {tb} kbps{cov}[/dim]"
            )
        console.print()
        sys.exit(0)

    stats = dict(converted=0, skipped=0, failed=0, deleted=0, saved=0)
    failures: List[Tuple[Path, str]] = []

    console.print(Rule("[bold]Converting[/bold]"))
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
            total=len(mp3_files),
        )

        for mp3 in mp3_files:
            if interrupted:
                prog.print("  [yellow]⚡ Interrupted by user.[/yellow]")
                break

            opus = mp3.with_suffix(".opus")
            name = mp3.name

            label = name[:52] + "…" if len(name) > 53 else name
            prog.update(task, description=f"[dim]{label}[/dim]")

            if args.skip_existing and opus.exists():
                prog.print(f"  [yellow]⏭[/yellow]  [dim]{name}[/dim]  [dim](opus exists)[/dim]")
                stats["skipped"] += 1
                prog.advance(task)
                continue

            mp3_info = get_mp3_info(mp3)
            kbps     = target_bitrate(mp3_info["bitrate_kbps"])
            mp3_size = mp3.stat().st_size

            ok, err = convert(mp3, opus, kbps)
            if not ok:
                prog.print(f"  [red]✗ FAIL[/red]  [dim]{name}[/dim]  [red]{err}[/red]")
                stats["failed"] += 1
                failures.append((mp3, f"ffmpeg: {err}"))
                prog.advance(task)
                continue

            if mp3_info["has_cover"]:
                embed_cover_art(mp3, opus)

            if not args.no_verify:
                ok, issues = verify(mp3, opus, mp3_info)
                if not ok:
                    prog.print(
                        f"  [red]✗ VRFY[/red]  [dim]{name}[/dim]  "
                        f"[red]{'; '.join(issues)}[/red]"
                    )
                    if opus.exists():
                        opus.unlink()
                    stats["failed"] += 1
                    failures.append((mp3, "; ".join(issues)))
                    prog.advance(task)
                    continue

            opus_size = opus.stat().st_size
            delta     = mp3_size - opus_size
            ratio     = opus_size / mp3_size * 100 if mp3_size else 0
            stats["saved"] += delta
            stats["converted"] += 1

            cov_tag = " [dim]🖼[/dim]" if mp3_info["has_cover"] else ""
            size_str = (
                f"[dim]{fmt_size(mp3_size)} → {fmt_size(opus_size)} "
                f"({ratio:.0f}%)[/dim]"
            )
            br_str = f"[dim]{mp3_info['bitrate_kbps']} → {kbps} kbps[/dim]"

            if not args.keep_originals:
                mp3.unlink()
                stats["deleted"] += 1
                prog.print(
                    f"  [green]✓[/green]  {name}{cov_tag}  "
                    f"{br_str}  {size_str}"
                )
            else:
                prog.print(
                    f"  [green]✓[/green]  {name}{cov_tag}  "
                    f"{br_str}  {size_str}  [dim](original kept)[/dim]"
                )

            prog.advance(task)

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
