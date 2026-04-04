#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "mutagen",
#   "rich",
# ]
# ///
"""
lossless_to_opus — Batch convert lossless audio files to Opus with full metadata preservation.

Supported input formats: FLAC, WAV, AIFF/AIF, ALAC (.m4a), WavPack (.wv), Monkey's Audio (.ape)

Usage:
    lossless_to_opus                              # converts current directory
    lossless_to_opus ~/Music/Artist1 ~/Music/Artist2
    lossless_to_opus --dry-run ~/Music/
    lossless_to_opus --keep-originals ~/Music/
    lossless_to_opus --bitrate 128 ~/Music/
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
        if dur_diff > DURATION_TOLS:
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


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="lossless_to_opus",
        description="Batch-convert lossless audio → Opus at ~180 kbps VBR. "
                    "Copies metadata & cover art, verifies before deleting originals.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
supported formats:
  FLAC (.flac), WAV (.wav), AIFF (.aiff/.aif), ALAC (.m4a), WavPack (.wv), Monkey's Audio (.ape)

examples:
  lossless_to_opus                              # current directory
  lossless_to_opus ~/Music/Artist1 ~/Music/Artist2
  lossless_to_opus --dry-run ~/Music/
  lossless_to_opus --keep-originals --no-verify ~/Music/
  lossless_to_opus --bitrate 128 ~/Music/
        """,
    )
    parser.add_argument("folders", nargs="*", type=Path,
                        metavar="FOLDER",
                        help="Folders to search recursively (default: current directory)")
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

        for src in src_files:
            if interrupted:
                prog.print("  [yellow]⚡ Interrupted by user.[/yellow]")
                break

            opus = src.with_suffix(".opus")
            name = src.name

            label = name[:52] + "…" if len(name) > 53 else name
            prog.update(task, description=f"[dim]{label}[/dim]")

            if args.skip_existing and opus.exists():
                prog.print(f"  [yellow]⏭[/yellow]  [dim]{name}[/dim]  [dim](opus exists)[/dim]")
                stats["skipped"] += 1
                prog.advance(task)
                continue

            src_info = get_source_info(src)
            src_size = src.stat().st_size

            ok, err = convert(src, opus, args.bitrate)
            if not ok:
                prog.print(f"  [red]✗ FAIL[/red]  [dim]{name}[/dim]  [red]{err}[/red]")
                stats["failed"] += 1
                failures.append((src, f"ffmpeg: {err}"))
                prog.advance(task)
                continue

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
                    stats["failed"] += 1
                    failures.append((src, "; ".join(issues)))
                    prog.advance(task)
                    continue

            opus_size = opus.stat().st_size
            delta     = src_size - opus_size
            ratio     = opus_size / src_size * 100 if src_size else 0
            stats["saved"] += delta
            stats["converted"] += 1

            cov_tag  = " [dim]🖼[/dim]" if src_info["has_cover"] else ""
            size_str = (
                f"[dim]{fmt_size(src_size)} → {fmt_size(opus_size)} "
                f"({ratio:.0f}%)[/dim]"
            )

            if not args.keep_originals:
                src.unlink()
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
