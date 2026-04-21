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
import argparse
import threading
import concurrent.futures
from pathlib import Path
from typing import Optional, List, Tuple

# ── shared library ────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path.home() / ".local" / "lib"))
import aud_convert_lib as lib
console = lib.console

# ── mutagen (format-specific pieces not in the lib) ───────────────────────────
_missing = []
try:
    from mutagen.oggopus import OggOpus
    from mutagen.flac import Picture, FLAC
    from mutagen.mp4 import MP4
    from mutagen import File as MutagenFile
except ImportError:
    _missing.append("mutagen  →  pip install mutagen")

try:
    from rich.rule import Rule
    from rich.panel import Panel
    from rich.markup import escape
except ImportError:
    _missing.append("rich     →  pip install rich")

if _missing:
    print("Missing dependencies:\n  " + "\n  ".join(_missing))
    sys.exit(1)


TARGET_BITRATE_KBPS = 180
DURATION_TOLS       = 1.0

LOSSLESS_SUFFIXES = {".flac", ".wav", ".aiff", ".aif", ".m4a", ".wv", ".ape"}


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
                info["tag_snapshot"][key] = (
                    str(val[0]) if isinstance(val, list) and val else str(val)
                )

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


convert = lib.convert  # ffmpeg invocation is identical for all source formats


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
    Extract cover art from *src* and embed it into *opus*.

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
                from mutagen.mp4 import MP4Cover
                mime = (
                    "image/png"
                    if covr[0].imageformat == MP4Cover.FORMAT_PNG
                    else "image/jpeg"
                )
                pic = Picture()
                pic.data  = bytes(covr[0])
                pic.type  = 3
                pic.mime  = mime
                pic.desc  = ""
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
                pic.data  = img_data
                pic.type  = 3
                pic.mime  = mime
                pic.desc  = ""
                pic.width = pic.height = pic.depth = pic.colors = 0

        if pic is None:
            return False

        return lib.embed_picture_in_opus(pic, opus)

    except Exception:
        return False


def verify(src: Path, opus: Path, src_info: dict) -> Tuple[bool, List[str]]:
    """
    Sanity-check the converted Opus file. Returns (ok, list_of_issues).
    Checks: file exists & non-empty, duration, cover art, key tags.
    """
    ok, issues, af = lib.verify_opus_basics(opus, src_info, DURATION_TOLS)
    if af is None:
        return ok, issues

    # Lossless sources carry native Vorbis keys already, so compare directly.
    tags = af.tags or {}
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

    return len(issues) == 0, issues


get_subdirs      = lib.get_subdirs
parse_selection  = lib.parse_selection
print_dir_grid   = lib.print_dir_grid
list_and_select  = lib.list_and_select


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

    interrupted_event = threading.Event()
    lock = threading.Lock()
    _active_opus: set[Path] = set()

    interrupted_event, _ = lib.make_interrupt_handler(_active_opus, lock)

    def interrupted() -> bool:
        return interrupted_event.is_set()

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

    if not lib.check_ffmpeg():
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
        f"[dim]({lib.fmt_size(total_bytes)} total)[/dim]"
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

    with lib.make_progress() as prog:
        task = prog.add_task(
            "[bold cyan]Overall[/bold cyan]",
            total=len(src_files),
        )

        def process_one(src: Path) -> None:
            if interrupted():
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
                f"[dim]{lib.fmt_size(src_size)} → {lib.fmt_size(opus_size)} "
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
