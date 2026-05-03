#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "mutagen",
#   "rich",
# ]
# ///
"""
aud-sort-into-albums — Group loose audio files into album subfolders using metadata tags.

Reads audio files that sit directly in a directory (not already in a subfolder),
extracts their album tag, and moves them into a subfolder named after the album.
Files with no album tag are treated as singles and each gets its own folder.

Supported formats: FLAC, MP3, Opus, OGG, M4A, AAC, WAV, AIFF, WavPack, Monkey's Audio

Usage:
    aud-sort-into-albums                                     # process current directory
    aud-sort-into-albums ~/Music/Unsorted                    # process a specific directory
    aud-sort-into-albums ~/Music/A ~/Music/B                 # process multiple directories
    aud-sort-into-albums --dry-run                           # preview without moving
    aud-sort-into-albums --include-artist                    # prefix folders with artist name
    aud-sort-into-albums --include-year                      # prefix folders with year
    aud-sort-into-albums --singles-folder Singles            # group all singles into one folder
    aud-sort-into-albums --on-conflict rename                # rename instead of skipping dupes
    aud-sort-into-albums --min-files 2                       # only move if album has 2+ files
    aud-sort-into-albums --recursive                         # also process files in subdirs
    aud-sort-into-albums --extensions mp3,flac               # only process these extensions
"""

import sys
import os
import re
import shutil
import argparse
from pathlib import Path
from typing import Optional

try:
    from mutagen import File as MutagenFile
except ImportError:
    print("Missing dependency: mutagen  →  pip install mutagen")
    sys.exit(1)

try:
    from rich.console import Console
    from rich.rule import Rule
    from rich.panel import Panel
    from rich.markup import escape
    from rich.table import Table
except ImportError:
    print("Missing dependency: rich  →  pip install rich")
    sys.exit(1)

console = Console()

# ── constants ─────────────────────────────────────────────────────────────────

DEFAULT_EXTENSIONS = {
    ".flac", ".mp3", ".opus", ".ogg", ".m4a", ".aac",
    ".wav", ".aiff", ".aif", ".wv", ".ape",
}

CONFLICT_MODES = ("skip", "overwrite", "rename")

# Characters not allowed in folder names on common filesystems
_UNSAFE_RE = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


# ── helpers ───────────────────────────────────────────────────────────────────

def sanitize(name: str) -> str:
    """Strip filesystem-unsafe characters and trim whitespace/dots."""
    name = _UNSAFE_RE.sub("_", name).strip().strip(".")
    return name or "_unknown"


def get_tag(audio, *keys: str) -> Optional[str]:
    """
    Try each key in order against a mutagen tag dict and return the first
    non-empty string value found, or None.
    Works for Vorbis comments, ID3, MP4, and APEv2 tags.
    """
    tags = audio.tags if audio else None
    if not tags:
        return None
    for key in keys:
        for variant in (key, key.upper(), key.lower()):
            val = tags.get(variant)
            if val is None:
                continue
            if isinstance(val, list):
                val = val[0] if val else None
            if val is None:
                continue
            text = str(val).strip()
            if text:
                return text
    return None


def read_tags(path: Path) -> dict:
    """Return a dict with normalised tag values for a single audio file."""
    result = {"album": None, "artist": None, "albumartist": None,
              "year": None, "title": None}
    try:
        audio = MutagenFile(path, easy=True)
        if audio is None:
            return result
        result["album"]       = get_tag(audio, "album")
        result["artist"]      = get_tag(audio, "artist")
        result["albumartist"] = get_tag(audio, "albumartist", "album artist", "album_artist")
        result["year"]        = get_tag(audio, "date", "year")
        result["title"]       = get_tag(audio, "title")
    except Exception as e:
        console.print(f"  [yellow]⚠[/yellow]  Could not read tags for [dim]{path.name}[/dim]: {e}")
    return result


def year_from_tag(raw: Optional[str]) -> Optional[str]:
    """Extract a 4-digit year from a date string like '2003-06-12' or '2003'."""
    if not raw:
        return None
    m = re.search(r"\b(\d{4})\b", raw)
    return m.group(1) if m else None


def build_folder_name(tags: dict, *, include_artist: bool, include_year: bool,
                       singles_folder: Optional[str]) -> str:
    """Derive the destination subfolder name from a file's tags."""
    album = tags["album"]

    if not album:
        # Single — use dedicated shared folder, or fall back to track title / _single
        if singles_folder:
            return sanitize(singles_folder)
        title = tags["title"] or None
        return sanitize(title) if title else "_singles"

    parts = []

    if include_artist:
        artist = tags["albumartist"] or tags["artist"]
        if artist:
            parts.append(sanitize(artist))

    if include_year:
        year = year_from_tag(tags["year"])
        if year:
            parts.append(year)

    parts.append(sanitize(album))
    return " - ".join(parts)


def unique_dest(dest: Path) -> Path:
    """
    Return a path that does not exist by appending (2), (3), … to the stem.
    E.g.  song.flac → song (2).flac → song (3).flac …
    """
    if not dest.exists():
        return dest
    stem, suffix, parent = dest.stem, dest.suffix, dest.parent
    counter = 2
    while True:
        candidate = parent / f"{stem} ({counter}){suffix}"
        if not candidate.exists():
            return candidate
        counter += 1


def find_loose_files(directory: Path, extensions: set[str],
                     recursive: bool) -> list[Path]:
    """
    Return audio files that are direct children of *directory*
    (or any descendant when recursive=True) and sit directly in their
    containing folder (i.e. not already inside a subfolder of *directory*).
    When recursive=True we collect files from ALL subdirectories too.
    """
    files: list[Path] = []
    if recursive:
        for f in sorted(directory.rglob("*")):
            if f.is_file() and f.suffix.lower() in extensions:
                files.append(f)
    else:
        for f in sorted(directory.iterdir()):
            if f.is_file() and f.suffix.lower() in extensions:
                files.append(f)
    return files


# ── core logic ────────────────────────────────────────────────────────────────

def process_directory(
    directory: Path,
    *,
    dry_run: bool,
    recursive: bool,
    include_artist: bool,
    include_year: bool,
    singles_folder: Optional[str],
    on_conflict: str,
    min_files: int,
    extensions: set[str],
    stats: dict,
    failures: list,
) -> None:
    console.print()
    console.print(Rule(f"[bold cyan]{escape(str(directory))}[/bold cyan]"))
    console.print()

    loose = find_loose_files(directory, extensions, recursive)
    if not loose:
        console.print("  [yellow]No loose audio files found.[/yellow]")
        return

    console.print(
        f"  [green]●[/green] Found [bold]{len(loose)}[/bold] loose audio file(s) — "
        f"reading tags…"
    )
    console.print()

    # Build groups: folder_name → list[Path]
    groups: dict[str, list[Path]] = {}
    for path in loose:
        tags = read_tags(path)
        folder = build_folder_name(
            tags,
            include_artist=include_artist,
            include_year=include_year,
            singles_folder=singles_folder,
        )
        groups.setdefault(folder, []).append(path)

    # Filter by --min-files
    skipped_groups: list[tuple[str, list[Path]]] = []
    eligible_groups: dict[str, list[Path]] = {}
    for name, files in groups.items():
        if len(files) < min_files:
            skipped_groups.append((name, files))
        else:
            eligible_groups[name] = files

    if skipped_groups:
        console.print(
            f"  [dim]Skipping [bold]{sum(len(f) for _, f in skipped_groups)}[/bold] "
            f"file(s) in {len(skipped_groups)} group(s) below --min-files={min_files}[/dim]"
        )
        console.print()

    if not eligible_groups:
        console.print("  [yellow]Nothing to move after applying --min-files filter.[/yellow]")
        return

    # Process each group
    for folder_name in sorted(eligible_groups):
        files = eligible_groups[folder_name]
        dest_dir = directory / folder_name

        console.print(
            f"  [bold]{escape(folder_name)}[/bold]  "
            f"[dim]({len(files)} file{'s' if len(files) != 1 else ''})[/dim]"
        )

        if not dry_run:
            try:
                dest_dir.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                console.print(f"    [red]✗ Could not create folder: {e}[/red]")
                for f in files:
                    failures.append((f, f"mkdir failed: {e}"))
                    stats["failed"] += 1
                continue

        for src in sorted(files):
            dest = dest_dir / src.name
            name_esc = escape(src.name)

            if dest.exists():
                if on_conflict == "skip":
                    console.print(f"    [yellow]⏭[/yellow]  [dim]{name_esc}[/dim]  [dim](conflict — skipped)[/dim]")
                    stats["skipped"] += 1
                    continue
                elif on_conflict == "overwrite":
                    action_label = "[dim](overwrite)[/dim]"
                elif on_conflict == "rename":
                    dest = unique_dest(dest)
                    action_label = f"[dim](renamed → {escape(dest.name)})[/dim]"
                else:
                    action_label = ""
            else:
                action_label = ""

            if dry_run:
                console.print(
                    f"    [cyan]→[/cyan]  [dim]{name_esc}[/dim]  "
                    f"[dim]would move to [bold]{escape(folder_name)}/{escape(dest.name)}[/bold][/dim]"
                    + (f"  {action_label}" if action_label else "")
                )
                stats["moved"] += 1
            else:
                try:
                    shutil.move(str(src), str(dest))
                    console.print(
                        f"    [green]✓[/green]  [dim]{name_esc}[/dim]"
                        + (f"  {action_label}" if action_label else "")
                    )
                    stats["moved"] += 1
                except Exception as e:
                    console.print(f"    [red]✗[/red]  [dim]{name_esc}[/dim]  [red]{e}[/red]")
                    failures.append((src, str(e)))
                    stats["failed"] += 1

        console.print()


def print_summary(stats: dict, failures: list, dry_run: bool) -> None:
    console.print(Rule("[bold]Summary[/bold]"))
    console.print()

    verb = "Would move" if dry_run else "Moved"
    lines = [
        f"  {verb}:    [bold green]{stats['moved']}[/bold green] file(s)",
        f"  Skipped:  [bold yellow]{stats['skipped']}[/bold yellow] file(s)",
        f"  Failed:   [bold red]{stats['failed']}[/bold red] file(s)",
    ]
    for line in lines:
        console.print(line)

    if failures:
        console.print()
        console.print("  [bold red]Failures:[/bold red]")
        for path, reason in failures:
            console.print(f"    [red]✗[/red]  [dim]{escape(str(path))}[/dim]  [red]{reason}[/red]")

    console.print()


# ── entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="aud-sort-into-albums",
        description="Group loose audio files into album subfolders using metadata tags.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "dirs", nargs="*", type=Path, metavar="DIR",
        help="Directories to process (default: current directory)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would happen without moving any files",
    )
    parser.add_argument(
        "--recursive", action="store_true",
        help="Also process audio files found inside subdirectories",
    )
    parser.add_argument(
        "--include-artist", action="store_true",
        help="Prefix folder names with the album artist (or artist) tag",
    )
    parser.add_argument(
        "--include-year", action="store_true",
        help="Prefix folder names with the year tag",
    )
    parser.add_argument(
        "--singles-folder", metavar="NAME",
        help="Move all singles (no album tag) into one shared folder instead of one each",
    )
    parser.add_argument(
        "--on-conflict", choices=CONFLICT_MODES, default="skip", metavar="MODE",
        help=(
            "What to do when the destination file already exists: "
            "skip (default) — leave the file where it is; "
            "overwrite — replace the destination; "
            "rename — append (2), (3)… to the incoming filename"
        ),
    )
    parser.add_argument(
        "--min-files", type=int, default=1, metavar="N",
        help="Only create an album folder if it contains at least N files (default: 1)",
    )
    parser.add_argument(
        "--extensions", metavar="EXT[,EXT…]",
        help=(
            "Comma-separated list of file extensions to consider "
            f"(default: {', '.join(sorted(DEFAULT_EXTENSIONS))})"
        ),
    )
    args = parser.parse_args()

    # Resolve directories
    dirs: list[Path] = args.dirs if args.dirs else [Path.cwd()]

    # Resolve extensions
    if args.extensions:
        raw_exts = [e.strip().lstrip(".") for e in args.extensions.split(",") if e.strip()]
        extensions = {"." + e.lower() for e in raw_exts if e}
        if not extensions:
            console.print("[red]✗ --extensions produced an empty set. Aborting.[/red]")
            sys.exit(1)
    else:
        extensions = DEFAULT_EXTENSIONS

    # Header panel
    console.print()
    flags = []
    if args.dry_run:       flags.append("[bold yellow]DRY RUN[/bold yellow]")
    if args.recursive:     flags.append("recursive")
    if args.include_artist: flags.append("include artist")
    if args.include_year:  flags.append("include year")
    flags.append(f"on-conflict: {args.on_conflict}")
    if args.min_files > 1: flags.append(f"min-files: {args.min_files}")
    subtitle = "  ·  ".join(flags)
    console.print(Panel.fit(
        f"[bold cyan]Sort Audio → Albums[/bold cyan]   [dim]{subtitle}[/dim]",
        border_style="cyan",
        padding=(0, 2),
    ))

    stats = {"moved": 0, "skipped": 0, "failed": 0}
    failures: list[tuple[Path, str]] = []

    for directory in dirs:
        if not directory.exists():
            console.print(f"\n  [red]✗[/red] Directory not found: [bold]{directory}[/bold]")
            continue
        if not directory.is_dir():
            console.print(f"\n  [red]✗[/red] Not a directory: [bold]{directory}[/bold]")
            continue
        process_directory(
            directory,
            dry_run=args.dry_run,
            recursive=args.recursive,
            include_artist=args.include_artist,
            include_year=args.include_year,
            singles_folder=args.singles_folder,
            on_conflict=args.on_conflict,
            min_files=args.min_files,
            extensions=extensions,
            stats=stats,
            failures=failures,
        )

    print_summary(stats, failures, args.dry_run)

    if stats["failed"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
