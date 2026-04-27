#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "mutagen",
#   "rich",
# ]
# ///
"""
aud-mp3-to-opus — Batch convert MP3, Ogg Vorbis, and AAC files to Opus with full metadata preservation.

Usage:
    aud-mp3-to-opus                              # converts current directory
    aud-mp3-to-opus ~/Music/Artist1 ~/Music/Artist2
    aud-mp3-to-opus --dry-run ~/Music/
    aud-mp3-to-opus --keep-originals ~/Music/
    aud-mp3-to-opus --list                       # pick subdirs of current directory
    aud-mp3-to-opus --list ~/Music/              # pick subdirs of ~/Music/
    aud-mp3-to-opus --workers 4 ~/Music/         # convert 4 files at a time
"""

import sys
import os
import subprocess
import argparse
import threading
import concurrent.futures
from pathlib import Path
from typing import Optional, List, Tuple

# ── shared library ────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path.home() / ".local" / "lib"))
import aud_convert_lib as lib
console = lib.console

# ── mutagen (MP3-specific pieces not in the lib) ──────────────────────────────
_missing = []
try:
    from mutagen.mp3 import MP3
    from mutagen.oggvorbis import OggVorbis
    from mutagen.mp4 import MP4
    from mutagen.flac import Picture
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


BITRATE_RATIO     = 0.7
MIN_BITRATE_KBPS  = 24
MAX_BITRATE_KBPS  = 320
DEFAULT_BITRATE   = 160
DURATION_TOLS     = 2.0

convert = lib.convert  # ffmpeg invocation is identical for all source formats

SOURCE_GLOB_PATTERNS = [
    "*.[mM][pP]3",
    "*.[oO][gG][gG]",
    "*.[mM][4Aa][aA]",   # .m4a and .aac
]

def find_source_files(folders: List[Path]) -> List[Path]:
    found: List[Path] = []
    for folder in folders:
        if not folder.exists():
            console.print(f"  [red]✗[/red] Folder not found: [bold]{folder}[/bold]")
            continue
        if not folder.is_dir():
            console.print(f"  [red]✗[/red] Not a directory: [bold]{folder}[/bold]")
            continue
        matches: List[Path] = []
        for pattern in SOURCE_GLOB_PATTERNS:
            matches.extend(folder.rglob(pattern))
        found.extend(sorted(set(matches)))
    return found


def get_true_duration(path: Path) -> float:
    """Use ffprobe to get the actual stream duration, ignoring bad MP3 headers."""
    try:
        cmd = [
            "ffprobe",
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            str(path)
        ]
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode == 0 and r.stdout.strip():
            return float(r.stdout.strip())
    except Exception:
        pass
    return 0.0

def get_source_info(path: Path) -> dict:
    """Read bitrate, duration, tag presence, and cover art flag from an MP3, Ogg Vorbis, or AAC file."""
    info = {
        "bitrate_kbps": DEFAULT_BITRATE,
        "duration":     0.0,
        "has_cover":    False,
        "has_tags":     False,
        "tag_snapshot": {},
    }
    suffix = path.suffix.lower()
    try:
        if suffix == ".mp3":
            audio = MP3(path)
            true_dur = get_true_duration(path)
            info["duration"] = true_dur if true_dur > 0 else audio.info.length
            info["bitrate_kbps"] = max(1, audio.info.bitrate // 1000)
            if audio.tags:
                info["has_tags"] = True
                for key, val in audio.tags.items():
                    info["tag_snapshot"][key] = str(val)
                info["has_cover"] = any(k.startswith("APIC") for k in audio.tags)

        elif suffix == ".ogg":
            audio = OggVorbis(path)
            info["duration"] = audio.info.length
            info["bitrate_kbps"] = max(1, (audio.info.bitrate or DEFAULT_BITRATE * 1000) // 1000)
            if audio.tags:
                info["has_tags"] = True
                for key, val in audio.tags.items():
                    info["tag_snapshot"][key] = str(val[0]) if isinstance(val, list) else str(val)
                info["has_cover"] = "metadata_block_picture" in (audio.tags or {})

        elif suffix in (".m4a", ".aac"):
            audio = MP4(path)
            info["duration"] = audio.info.length
            info["bitrate_kbps"] = max(1, (audio.info.bitrate or DEFAULT_BITRATE * 1000) // 1000)
            if audio.tags:
                info["has_tags"] = True
                for key, val in audio.tags.items():
                    if key == "covr":
                        continue
                    # MP4 tag values are always lists; unwrap the first element.
                    item = val[0] if isinstance(val, list) and val else val
                    # trkn / disk are (number, total) tuples — keep just the number.
                    if isinstance(item, tuple):
                        item = item[0]
                    info["tag_snapshot"][key] = str(item)
                info["has_cover"] = "covr" in audio.tags

    except Exception as e:
        console.print(f"  [yellow]⚠[/yellow]  Could not read info for {path.name}: {e}")
    return info

def target_bitrate(source_kbps: int) -> int:
    raw = round(source_kbps * BITRATE_RATIO)
    return max(MIN_BITRATE_KBPS, min(MAX_BITRATE_KBPS, raw))

def embed_cover_art(src: Path, opus: Path) -> bool:
    """
    Extract cover art from the source file and embed it into the Opus file
    as a base64-encoded METADATA_BLOCK_PICTURE Vorbis comment.
    Supports MP3 (ID3 APIC), Ogg Vorbis (METADATA_BLOCK_PICTURE), and AAC/M4A (covr).
    """
    suffix = src.suffix.lower()
    try:
        pic = Picture()

        if suffix == ".mp3":
            src_audio = MP3(src)
            if not src_audio.tags:
                return False
            apic = None
            for key in src_audio.tags:
                if key.startswith("APIC"):
                    apic = src_audio.tags[key]
                    break
            if apic is None:
                return False
            pic.data  = apic.data
            pic.type  = apic.type
            pic.mime  = apic.mime
            pic.desc  = apic.desc or ""
            pic.width = pic.height = pic.depth = pic.colors = 0

        elif suffix == ".ogg":
            import base64 as _base64
            src_audio = OggVorbis(src)
            raw = (src_audio.tags or {}).get("metadata_block_picture")
            if not raw:
                return False
            pic = Picture(_base64.b64decode(raw[0]))

        elif suffix in (".m4a", ".aac"):
            src_audio = MP4(src)
            covr = (src_audio.tags or {}).get("covr")
            if not covr:
                return False
            atom = covr[0]
            from mutagen.mp4 import MP4Cover
            pic.data   = bytes(atom)
            pic.type   = 3  # front cover
            pic.mime   = "image/jpeg" if atom.imageformat == MP4Cover.FORMAT_JPEG else "image/png"
            pic.desc   = ""
            pic.width  = pic.height = pic.depth = pic.colors = 0

        else:
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

    suffix = src.suffix.lower()
    tags = af.tags or {}
    snap = src_info["tag_snapshot"]

    if suffix == ".mp3":
        # MP3 sources use ID3 keys; map them to Vorbis equivalents for comparison.
        key_map = {
            "TIT2": "title",
            "TPE1": "artist",
            "TALB": "album",
            "TRCK": "tracknumber",
            "TDRC": "date",
        }
    elif suffix == ".ogg":
        # Vorbis tags are already Vorbis-comment keys; compare directly.
        key_map = {k: k for k in ("title", "artist", "album", "tracknumber", "date")}
    elif suffix in (".m4a", ".aac"):
        # MP4/iTunes atom names → Vorbis comment equivalents.
        key_map = {
            "\xa9nam": "title",
            "\xa9ART": "artist",
            "\xa9alb": "album",
            "trkn":    "tracknumber",
            "\xa9day": "date",
        }
    else:
        key_map = {}

    for src_key, vorbis_key in key_map.items():
        if src_key not in snap:
            continue
        src_val = str(snap[src_key]).strip()
        dst_val = ""
        if vorbis_key in tags:
            v = tags[vorbis_key]
            dst_val = str(v[0]).strip() if isinstance(v, list) else str(v).strip()
        if src_val.lower() != dst_val.lower():
            issues.append(
                f"Tag mismatch [{vorbis_key}]: "
                f"source={src_val!r} → opus={dst_val!r}"
            )

    return len(issues) == 0, issues


get_subdirs      = lib.get_subdirs
parse_selection  = lib.parse_selection
print_dir_grid   = lib.print_dir_grid
list_and_select  = lib.list_and_select


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="aud-mp3-to-opus",
        description="Batch-convert MP3, Ogg Vorbis, and AAC → Opus. Copies metadata & cover art, "
                    "targets 70%% of source bitrate, verifies before deleting originals.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  aud-mp3-to-opus                              # current directory
  aud-mp3-to-opus ~/Music/Artist1 ~/Music/Artist2
  aud-mp3-to-opus --dry-run ~/Music/
  aud-mp3-to-opus --keep-originals --no-verify ~/Music/
  aud-mp3-to-opus --list                       # browse & pick subdirs of current dir
  aud-mp3-to-opus --list ~/Music/              # browse & pick subdirs of ~/Music/
  aud-mp3-to-opus --workers 4 ~/Music/         # convert 4 files in parallel
        """,
    )
    parser.add_argument("folders", nargs="*", type=Path,
                        metavar="FOLDER",
                        help="Folders to search recursively (default: current directory)")
    parser.add_argument("--list", "-l", action="store_true",
                        help="Show a numbered grid of subdirectories and interactively "
                             "select which ones to convert (supports ranges, e.g. 1-10)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would happen without converting anything")
    parser.add_argument("--keep-originals", action="store_true",
                        help="Do not delete source files after successful conversion")
    parser.add_argument("--no-verify", action="store_true",
                        help="Skip post-conversion verification (faster, riskier)")
    parser.add_argument("--skip-existing", action="store_true", default=True,
                        help="Skip MP3 if a .opus sibling already exists (default: on)")
    parser.add_argument("--no-skip-existing", dest="skip_existing", action="store_false",
                        help="Re-convert even if .opus already exists")
    parser.add_argument("--workers", type=int, default=os.cpu_count() or 4,
                        metavar="N",
                        help="Number of files to convert in parallel "
                             f"(default: number of CPU cores, currently {os.cpu_count() or 4})")
    args = parser.parse_args()

    # ── --list mode ───────────────────────────────────────────────────────────
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
        f"[bold cyan]MP3 / Vorbis / AAC → Opus[/bold cyan]   [dim]{subtitle}[/dim]",
        border_style="cyan",
        padding=(0, 2),
    ))
    console.print()

    if not lib.check_ffmpeg():
        console.print("[bold red]✗ ffmpeg not found.[/bold red] "
                      "Install it and make sure it is on your PATH.")
        sys.exit(1)

    with console.status("[bold]Scanning for MP3 / Vorbis / AAC files…[/bold]", spinner="dots"):
        mp3_files = find_source_files(args.folders)

    if not mp3_files:
        console.print("[yellow]No MP3, Ogg Vorbis, or AAC files found.[/yellow]")
        sys.exit(0)

    total_bytes = sum(f.stat().st_size for f in mp3_files)
    console.print(
        f"[green]●[/green] Found [bold]{len(mp3_files)}[/bold] source file(s)  "
        f"[dim]({lib.fmt_size(total_bytes)} total)[/dim]"
    )
    console.print()

    if args.dry_run:
        console.print(Rule("[bold yellow]DRY RUN — nothing will be written[/bold yellow]"))
        console.print()
        for mp3 in mp3_files:
            info = get_source_info(mp3)
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
    if args.workers > 1:
        console.print(f"  [dim]Running {args.workers} workers in parallel[/dim]")
    console.print()

    with lib.make_progress() as prog:
        task = prog.add_task(
            "[bold cyan]Overall[/bold cyan]",
            total=len(mp3_files),
        )

        def process_one(mp3: Path) -> None:
            if interrupted():
                prog.advance(task)
                return

            opus = mp3.with_suffix(".opus")
            name = escape(mp3.name)

            if args.workers == 1:
                label = name[:52] + "…" if len(name) > 53 else name
                prog.update(task, description=f"[dim]{label}[/dim]")

            if args.skip_existing and opus.exists():
                mp3_info = get_source_info(mp3)
                valid, _ = verify(mp3, opus, mp3_info)
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
                mp3_info = get_source_info(mp3)

            kbps     = target_bitrate(mp3_info["bitrate_kbps"])
            mp3_size = mp3.stat().st_size

            with lock:
                _active_opus.add(opus)
            ok, err = convert(mp3, opus, kbps)
            with lock:
                _active_opus.discard(opus)

            if interrupted() and not ok:
                prog.advance(task)
                return

            if not ok:
                prog.print(f"  [red]✗ FAIL[/red]  [dim]{name}[/dim]  [red]{err}[/red]")
                with lock:
                    stats["failed"] += 1
                    failures.append((mp3, f"ffmpeg: {err}"))
                prog.advance(task)
                return

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
                    with lock:
                        stats["failed"] += 1
                        failures.append((mp3, "; ".join(issues)))
                    prog.advance(task)
                    return

            opus_size = opus.stat().st_size
            delta     = mp3_size - opus_size
            ratio     = opus_size / mp3_size * 100 if mp3_size else 0
            with lock:
                stats["saved"] += delta
                stats["converted"] += 1

            cov_tag = " [dim]🖼[/dim]" if mp3_info["has_cover"] else ""
            size_str = (
                f"[dim]{lib.fmt_size(mp3_size)} → {lib.fmt_size(opus_size)} "
                f"({ratio:.0f}%)[/dim]"
            )
            br_str = f"[dim]{mp3_info['bitrate_kbps']} → {kbps} kbps[/dim]"

            if not args.keep_originals:
                mp3.unlink()
                with lock:
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

        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
            futs = {executor.submit(process_one, mp3): mp3 for mp3 in mp3_files}
            for fut in concurrent.futures.as_completed(futs):
                if interrupted():
                    for f in futs:
                        f.cancel()
                    prog.print("  [yellow]⚡ Interrupted by user.[/yellow]")
                    break
                try:
                    fut.result()
                except Exception as exc:
                    mp3 = futs[fut]
                    prog.print(
                        f"  [red]✗[/red]  {escape(mp3.name)}  "
                        f"[red]Unexpected error: {exc}[/red]"
                    )
                    with lock:
                        stats["failed"] += 1

    lib.print_summary(stats, failures, args.keep_originals)
    if stats["failed"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
