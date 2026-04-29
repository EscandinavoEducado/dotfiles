#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "mutagen",
#   "Pillow",
# ]
# ///
"""
aud-organize-library — Scan, rename, and organize audio library folders.

Usage:
    aud-organize-library                        # scan current directory
    aud-organize-library ~/Music/
    aud-organize-library --list                 # pick subdirs interactively
    aud-organize-library --list ~/Music/        # pick from ~/Music subdirs
    aud-organize-library -p ~/Music/            # interactive web preview
    aud-organize-library -c ~/Music/            # check only, no changes
    aud-organize-library -y ~/Music/            # auto-confirm everything
    aud-organize-library --cover-size ~/Music/  # resize embedded covers > 700px
"""

import os
import re
import sys
import io
import base64
import hashlib
import shutil
import tempfile
import webbrowser
import html
import threading
import json
import argparse
from http.server import BaseHTTPRequestHandler, HTTPServer
from collections import Counter
from typing import Optional, Dict, List, Tuple

try:
    from mutagen import File as MutagenFile, MutagenError
except ImportError:
    print("Error: The 'mutagen' library is required.")
    print("Please install it by running: pip install mutagen")
    sys.exit(1)

try:
    from PIL import Image
except ImportError:
    print("Error: The 'Pillow' library is required.")
    print("Please install it by running: pip install Pillow")
    sys.exit(1)

SUPPORTED_EXTENSIONS       = ('.mp3', '.flac', '.m4a', '.ogg', '.opus', '.wav')
SUPPORTED_IMAGE_EXTENSIONS = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tif', '.tiff')
STANDARD_AUDIO_FORMATS     = ('.opus', '.flac')
COVER_MAX_SIZE             = 700
PATH_LENGTH_LIMIT_BYTES    = 230
FILE_NAME_RESERVE_BYTES    = 20
ARTWORK_DIRNAME            = 'artwork'

if os.name == 'nt':
    os.system('')


class Color:
    HEADER = '\033[95m'
    BLUE   = '\033[94m'
    CYAN   = '\033[96m'
    GREEN  = '\033[92m'
    YELLOW = '\033[93m'
    RED    = '\033[91m'
    ENDC   = '\033[0m'
    BOLD   = '\033[1m'

    @classmethod
    def disable(cls):
        cls.HEADER = cls.BLUE = cls.CYAN = cls.GREEN = cls.YELLOW = cls.RED = cls.ENDC = cls.BOLD = ''

if not sys.stdout.isatty():
    Color.disable()


def get_subdirs(base):
    """Return sorted list of immediate (non-hidden) subdirectories of *base*."""
    try:
        return sorted(
            (p for p in os.scandir(base)
             if p.is_dir() and not p.name.startswith(".")),
            key=lambda e: e.name.lower(),
        )
    except PermissionError:
        print(f"{Color.RED}✗ Permission denied: {base}{Color.ENDC}")
        return []


def parse_selection(raw, max_idx):
    """Parse "1 3 5-8 10" into a sorted list of 0-based indices."""
    indices = set()
    for token in raw.replace(",", " ").split():
        if "-" in token:
            parts = token.split("-", 1)
            try:
                lo, hi = int(parts[0]), int(parts[1])
            except ValueError:
                print(f"{Color.RED}  ✗ Invalid range: {token}{Color.ENDC}")
                return []
            if lo < 1 or hi > max_idx or lo > hi:
                print(f"{Color.RED}  ✗ Range {token} out of bounds (1–{max_idx}){Color.ENDC}")
                return []
            indices.update(range(lo - 1, hi))
        else:
            try:
                n = int(token)
            except ValueError:
                print(f"{Color.RED}  ✗ Not a number: {token}{Color.ENDC}")
                return []
            if n < 1 or n > max_idx:
                print(f"{Color.RED}  ✗ Number {n} out of bounds (1–{max_idx}){Color.ENDC}")
                return []
            indices.add(n - 1)
    return sorted(indices)


def print_dir_grid(entries):
    """Print numbered subdirectories in a compact two-column grid."""
    if not entries:
        print(f"  {Color.YELLOW}No subdirectories found.{Color.ENDC}")
        return
    num_w  = len(str(len(entries)))
    col_w  = max(len(e.name) for e in entries) + num_w + 4
    n_cols = max(1, min(2, shutil.get_terminal_size(fallback=(80, 24)).columns // col_w))
    n_rows = -(-len(entries) // n_cols)
    for row in range(n_rows):
        line = ""
        for col in range(n_cols):
            idx = row + col * n_rows
            if idx >= len(entries):
                break
            pad = col_w - (num_w + 2 + len(entries[idx].name))
            line += f"{Color.CYAN}{idx + 1:>{num_w}}{Color.ENDC}  {Color.BOLD}{entries[idx].name}{Color.ENDC}" + " " * pad
        print("  " + line)


def list_and_select(base):
    """Show a numbered grid of subdirs under *base*, prompt for selection,
    return chosen directory paths as strings."""
    entries = get_subdirs(base)
    print()
    print(f"{Color.CYAN}{Color.BOLD}Select directories{Color.ENDC}  {Color.CYAN}{base}{Color.ENDC}")
    print()
    if not entries:
        print(f"  {Color.YELLOW}No subdirectories found.{Color.ENDC}")
        print()
        return []
    print_dir_grid(entries)
    print()
    print(f"  {Color.YELLOW}Enter numbers, ranges, or both — e.g. 1 3 5-8  (space or comma separated){Color.ENDC}")
    print()
    while True:
        try:
            raw = input("  Selection: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            print(f"  {Color.YELLOW}Cancelled.{Color.ENDC}")
            sys.exit(0)
        if not raw:
            print(f"  {Color.YELLOW}Nothing selected. Exiting.{Color.ENDC}")
            sys.exit(0)
        idxs = parse_selection(raw, len(entries))
        if idxs:
            chosen = [entries[i].path for i in idxs]
            print()
            label = "directory" if len(chosen) == 1 else "directories"
            print(f"  {Color.GREEN}●{Color.ENDC} Selected {Color.BOLD}{len(chosen)}{Color.ENDC} {label}:")
            for d in chosen:
                print(f"    {Color.CYAN}•{Color.ENDC} {d}")
            print()
            return chosen


def sanitize_filename(name: str, is_path_component: bool = False) -> str:
    if is_path_component:
        name = name.replace('/', ' ').replace('\\', ' ')
    else:
        name = name.replace('/', ' - ').replace('\\', ' - ')
    name = re.sub(r'[<>:"|?*]', '', name)
    name = re.sub(r'\s+', ' ', name)
    return name.strip(' .')


def truncate_to_budget(text: str, budget_bytes: int) -> str:
    """Truncate *text* so its UTF-8 encoding fits within *budget_bytes*.

    Strategy:
      1. If the text already fits, return it unchanged.
      2. Find the longest UTF-8-safe prefix within the budget.
      3. Try to back off to the last word boundary within that prefix.
         Only do so if it would leave at least half the budget's worth of
         content — otherwise a single very long word would vanish entirely.
      4. If we're left with a single word that still overflows (shouldn't
         happen after step 2, but guards against edge cases), strip
         characters one by one until it fits.
      5. No ellipsis is appended — the caller treats truncation as a silent
         size constraint, not a user-visible signal.
    """
    encoded = text.encode('utf-8')
    if len(encoded) <= budget_bytes:
        return text
    if budget_bytes <= 0:
        return ''

    # Step 2: walk back from the cut point to a valid UTF-8 character boundary.
    cut = budget_bytes
    while cut > 0 and (encoded[cut] & 0xC0) == 0x80:
        cut -= 1
    prefix = encoded[:cut].decode('utf-8').strip()

    # Step 3: back off to last word boundary if it keeps at least half the budget.
    space_idx = prefix.rfind(' ')
    if space_idx >= 0 and len(prefix[:space_idx].encode('utf-8')) >= budget_bytes // 2:
        prefix = prefix[:space_idx].strip()

    # Step 4: single-word fallback — character-strip until it fits.
    while len(prefix.encode('utf-8')) > budget_bytes:
        prefix = prefix[:-1]

    return prefix


def get_audio_metadata(file_path: str) -> Tuple[Optional[Dict[str, str]], Optional[str]]:
    try:
        audio = MutagenFile(file_path, easy=True)
        if not audio:
            return None, None

        tags = {k: audio.get(k, [None])[0] for k in ['album', 'artist', 'albumartist', 'title', 'date', 'tracknumber', 'discnumber']}

        year = None
        invalid_year = None
        if tags['date']:
            date_str = str(tags['date']).strip()
            if re.fullmatch(r'\d{4}', date_str):
                year = date_str
            else:
                invalid_year = date_str

        return {
            'album':            str(tags['album'])       if tags['album']       else None,
            'artist':           str(tags['artist'])      if tags['artist']      else None,
            'albumartist':      str(tags['albumartist']) if tags['albumartist'] else None,
            'title':            str(tags['title'])       if tags['title']       else None,
            'track':            str(tags['tracknumber']) if tags['tracknumber'] else None,
            'disc':             str(tags['discnumber'])  if tags['discnumber']  else None,
            'year':             str(year)                if year                else None,
            'invalid_year_tag': invalid_year,
            'filename':         os.path.basename(file_path),
        }, None
    except MutagenError as e:
        return None, f"Could not read metadata from {os.path.basename(file_path)}: {e}"


def get_cover_art_info(file_path: str) -> Tuple[int, Optional[str], int, int]:
    """Returns (count, md5_hash, width, height). Width/height are 0 if unreadable."""
    def _dims(data: bytes) -> Tuple[int, int]:
        try:
            img = Image.open(io.BytesIO(data))
            return img.size  # (width, height)
        except Exception:
            return 0, 0

    try:
        audio = MutagenFile(file_path)
        if not audio:
            return 0, None, 0, 0

        # OggOpus / Vorbis: cover art is a base64-encoded FLAC Picture block
        tags = audio.tags or {}
        mbp = tags.get('metadata_block_picture') or tags.get('METADATA_BLOCK_PICTURE')
        if mbp:
            from mutagen.flac import Picture
            try:
                pic = Picture(base64.b64decode(mbp[0]))
                w, h = _dims(pic.data)
                return len(mbp), hashlib.md5(pic.data).hexdigest(), w, h
            except Exception:
                pass

        # FLAC native picture list
        if hasattr(audio, 'pictures') and audio.pictures:
            pictures = audio.pictures
            pic_data = pictures[0].data
            w, h = _dims(pic_data)
            return len(pictures), hashlib.md5(pic_data).hexdigest(), w, h

        # ID3 (MP3): APIC frame
        if hasattr(audio, 'tags') and audio.tags and 'APIC:' in audio.tags:
            pictures = audio.tags.getall('APIC:')
            w, h = _dims(pictures[0].data)
            return len(pictures), hashlib.md5(pictures[0].data).hexdigest(), w, h

        # MP4/M4A: covr atom
        if 'covr' in (audio.tags or {}):
            pictures = audio['covr']
            raw = bytes(pictures[0])
            w, h = _dims(raw)
            return len(pictures), hashlib.md5(raw).hexdigest(), w, h

        return 0, None, 0, 0
    except Exception:
        return 0, None, 0, 0


def _resize_image_bytes(data: bytes, mime: str, max_size: int) -> Tuple[bytes, str, int, int]:
    """
    Resize *data* so its longest side is at most *max_size* px.
    Returns (new_bytes, mime, new_width, new_height).
    Raises if the image cannot be decoded or is already within limits.
    """
    img = Image.open(io.BytesIO(data))
    w, h = img.size
    if max(w, h) <= max_size:
        raise ValueError("already within limits")
    scale  = max_size / max(w, h)
    new_w  = max(1, int(w * scale))
    new_h  = max(1, int(h * scale))
    img    = img.resize((new_w, new_h), Image.BILINEAR)
    buf    = io.BytesIO()
    fmt    = 'JPEG' if mime in ('image/jpeg', 'image/jpg') else 'PNG'
    out_mime = 'image/jpeg' if fmt == 'JPEG' else 'image/png'
    if fmt == 'JPEG':
        img.convert('RGB').save(buf, format='JPEG', quality=90)
    else:
        img.save(buf, format='PNG')
    return buf.getvalue(), out_mime, new_w, new_h


def _write_cover_to_file(file_path: str, pic_data: bytes, mime: str, width: int, height: int) -> bool:
    """Write cover bytes into the audio file's embedded tag, creating the tag if absent."""
    try:
        from mutagen.flac   import FLAC, Picture
        from mutagen.mp3    import MP3
        from mutagen.id3    import APIC, ID3
        from mutagen.mp4    import MP4, MP4Cover
        from mutagen.oggopus   import OggOpus
        from mutagen.oggvorbis import OggVorbis

        audio = MutagenFile(file_path)
        if audio is None:
            return False

        def _make_picture() -> Picture:
            pic        = Picture()
            pic.type   = 3          # Front Cover
            pic.mime   = mime
            pic.width  = width
            pic.height = height
            pic.depth  = 24
            pic.data   = pic_data
            return pic

        # ── FLAC ─────────────────────────────────────────────────────────────
        if isinstance(audio, FLAC):
            audio.clear_pictures()
            audio.add_picture(_make_picture())
            audio.save()
            return True

        # ── OggOpus / OggVorbis ──────────────────────────────────────────────
        if isinstance(audio, (OggOpus, OggVorbis)):
            if audio.tags is None:
                audio.add_tags()
            audio.tags['metadata_block_picture'] = [
                base64.b64encode(_make_picture().write()).decode('ascii')
            ]
            audio.save()
            return True

        # ── MP3 / ID3 ────────────────────────────────────────────────────────
        if isinstance(audio, MP3):
            if audio.tags is None:
                audio.add_tags()
            audio.tags.delall('APIC')
            audio.tags.add(APIC(encoding=3, mime=mime, type=3, desc='', data=pic_data))
            audio.save()
            return True

        # ── MP4 / M4A ────────────────────────────────────────────────────────
        if isinstance(audio, MP4):
            if audio.tags is None:
                audio.add_tags()
            fmt = MP4Cover.FORMAT_JPEG if mime in ('image/jpeg', 'image/jpg') else MP4Cover.FORMAT_PNG
            audio['covr'] = [MP4Cover(pic_data, imageformat=fmt)]
            audio.save()
            return True

        return False

    except Exception as e:
        print(f"  {Color.RED}Error writing cover to {os.path.basename(file_path)}: {e}{Color.ENDC}")
        return False


def embed_file_cover_into_audio(image_path: str, audio_paths: List[str]) -> Tuple[int, int]:
    """
    Read *image_path* from disk, resize to at most COVER_MAX_SIZE px on the
    longest side, and embed the result into each file in *audio_paths*.
    Returns (succeeded, failed) counts.
    """
    if not os.path.exists(image_path):
        print(f"  {Color.RED}Image not found: {image_path}{Color.ENDC}")
        return 0, len(audio_paths)

    try:
        with open(image_path, 'rb') as fh:
            raw = fh.read()
    except OSError as e:
        print(f"  {Color.RED}Cannot read image {os.path.basename(image_path)}: {e}{Color.ENDC}")
        return 0, len(audio_paths)

    ext  = os.path.splitext(image_path)[1].lower()
    mime = 'image/jpeg' if ext in ('.jpg', '.jpeg') else 'image/png' if ext == '.png' else 'image/jpeg'

    try:
        new_data, new_mime, new_w, new_h = _resize_image_bytes(raw, mime, COVER_MAX_SIZE)
    except ValueError:
        # Already within limits — use as-is
        new_data, new_mime = raw, mime
        try:
            img = Image.open(io.BytesIO(raw))
            new_w, new_h = img.size
        except Exception:
            new_w = new_h = 0
    except Exception as e:
        print(f"  {Color.RED}Error resizing {os.path.basename(image_path)}: {e}{Color.ENDC}")
        return 0, len(audio_paths)

    ok = fail = 0
    for apath in audio_paths:
        if not os.path.exists(apath):
            print(f"  {Color.RED}Audio file not found: {apath}{Color.ENDC}")
            fail += 1
            continue
        if _write_cover_to_file(apath, new_data, new_mime, new_w, new_h):
            ok += 1
        else:
            print(f"  {Color.RED}Unsupported format: {os.path.basename(apath)}{Color.ENDC}")
            fail += 1
    return ok, fail


def _read_raw_cover(file_path: str):
    """
    Return (raw_bytes, mime) for the first embedded cover in *file_path*,
    or (None, None) if not found.
    """
    try:
        from mutagen.flac import Picture
        audio = MutagenFile(file_path)
        if not audio:
            return None, None
        tags = audio.tags or {}

        # OggOpus / Vorbis
        for key in ('metadata_block_picture', 'METADATA_BLOCK_PICTURE'):
            if key in tags:
                pic = Picture(base64.b64decode(tags[key][0]))
                return pic.data, pic.mime

        # FLAC native
        if hasattr(audio, 'pictures') and audio.pictures:
            pic = audio.pictures[0]
            return pic.data, pic.mime

        # ID3 (MP3)
        for key in tags.keys():
            if key.startswith('APIC:'):
                return tags[key].data, tags[key].mime

        # MP4/M4A
        if 'covr' in tags:
            pic = tags['covr'][0]
            mime = 'image/jpeg' if bytes(pic).startswith(b'\xff\xd8') else 'image/png'
            return bytes(pic), mime

    except Exception:
        pass
    return None, None


def flatten_container_folder(dirpath: str, dirnames: List[str], general_warnings: List[str]) -> bool:
    print(f"  {Color.BLUE}-> Flattening '{os.path.basename(dirpath)}'...{Color.ENDC}")
    try:
        for subfolder_name in dirnames:
            subfolder_path = os.path.join(dirpath, subfolder_name)
            for filename in os.listdir(subfolder_path):
                source = os.path.join(subfolder_path, filename)
                dest   = os.path.join(dirpath, filename)
                if os.path.exists(dest):
                    name, ext = os.path.splitext(filename)
                    counter = 1
                    while os.path.exists(dest):
                        dest = os.path.join(dirpath, f"{name} ({counter}){ext}")
                        counter += 1
                os.rename(source, dest)
            os.rmdir(subfolder_path)
        return True
    except OSError as e:
        general_warnings.append(f"{Color.RED}[Error] Failed to flatten '{os.path.basename(dirpath)}': {e}{Color.ENDC}")
        return False


def _is_image_only_dir(dirpath: str) -> bool:
    """Return True if *dirpath* is non-empty and contains only image files (no audio)."""
    try:
        entries = [e for e in os.listdir(dirpath) if not e.startswith('.')]
    except OSError:
        return False
    if not entries:
        return False
    return all(e.lower().endswith(SUPPORTED_IMAGE_EXTENSIONS) for e in entries)


def _verify_image(fpath: str) -> bool:
    """Return True if Pillow can open and verify the image at *fpath*."""
    try:
        with Image.open(fpath) as img:
            img.verify()
        return True
    except Exception:
        return False


def collect_album_images(dirpath: str, filenames: List[str]) -> Tuple[List[Dict], List[str]]:
    """
    Collect all images belonging to an album folder:
      - images at the root (from *filenames*)
      - images inside any subdir whose entire content is images
        (considered an artwork dir regardless of its name)

    Returns:
        image_infos  — list of dicts: filename, subdir (None=root or str),
                       size_bytes, readable, ext
        artwork_dirs — subdir names identified as image-only (all will be
                       collapsed into ARTWORK_DIRNAME during planning)
    """
    image_infos : List[Dict] = []
    artwork_dirs: List[str]  = []

    for f in sorted(filenames):
        if not f.lower().endswith(SUPPORTED_IMAGE_EXTENSIONS):
            continue
        fpath = os.path.join(dirpath, f)
        try:
            size = os.path.getsize(fpath)
        except OSError:
            size = 0
        image_infos.append({
            'filename':   f,
            'subdir':     None,
            'size_bytes': size,
            'readable':   _verify_image(fpath),
            'ext':        os.path.splitext(f)[1].lower(),
        })

    try:
        subdirs = sorted(e.name for e in os.scandir(dirpath)
                         if e.is_dir() and not e.name.startswith('.'))
    except OSError:
        subdirs = []

    for sub in subdirs:
        subpath = os.path.join(dirpath, sub)
        if not _is_image_only_dir(subpath):
            continue
        artwork_dirs.append(sub)
        try:
            sub_files = sorted(os.listdir(subpath))
        except OSError:
            sub_files = []
        for f in sub_files:
            if not f.lower().endswith(SUPPORTED_IMAGE_EXTENSIONS):
                continue
            fpath = os.path.join(subpath, f)
            try:
                size = os.path.getsize(fpath)
            except OSError:
                size = 0
            image_infos.append({
                'filename':   f,
                'subdir':     sub,
                'size_bytes': size,
                'readable':   _verify_image(fpath),
                'ext':        os.path.splitext(f)[1].lower(),
            })

    return image_infos, artwork_dirs


def analyze_album_folder(dirpath: str, filenames: List[str]) -> Optional[Dict]:
    audio_files = [f for f in filenames if f.lower().endswith(SUPPORTED_EXTENSIONS)]
    if not audio_files:
        return None

    print(f"{Color.BOLD}Analyzing: {os.path.basename(dirpath)}{Color.ENDC}")
    files_metadata, album_warnings = [], []

    for filename in audio_files:
        path = os.path.join(dirpath, filename)
        _, ext = os.path.splitext(filename)
        if ext.lower() not in STANDARD_AUDIO_FORMATS:
            album_warnings.append(f"[Format: {ext.strip('.')}]")

        md, warn = get_audio_metadata(path)
        if warn:
            album_warnings.append(warn)
        if md:
            count, hash_val, cov_w, cov_h = get_cover_art_info(path)
            md.update({'cover_art_count': count, 'cover_art_hash': hash_val,
                       'cover_art_w': cov_w, 'cover_art_h': cov_h})
            files_metadata.append(md)

    if not files_metadata:
        return None

    album_tags        = [md['album'] for md in files_metadata if md.get('album')]
    most_common_album = Counter(album_tags).most_common(1)[0][0] if album_tags else os.path.basename(dirpath)

    years = [md.get('year') for md in files_metadata if md.get('album') == most_common_album and md.get('year')]
    year  = Counter(years).most_common(1)[0][0] if years else None

    image_infos, artwork_dirs = collect_album_images(dirpath, filenames)

    print(f"  {Color.GREEN}-> Found album: '{most_common_album}'{f' ({year})' if year else ''}{Color.ENDC}")

    return {
        'path':           dirpath,
        'album':          most_common_album,
        'year':           year,
        'files_metadata': files_metadata,
        'image_infos':    image_infos,
        'artwork_dirs':   artwork_dirs,
        'has_images':     bool(image_infos),
        'album_warnings': sorted(list(set(album_warnings))),
    }


def check_warnings(info: Dict) -> List[str]:
    md_list  = info['files_metadata']
    warnings = set(info.get('album_warnings', []))

    image_infos = info.get('image_infos', [])
    if not image_infos:
        warnings.add("[No Image]")
    elif len(image_infos) > 1:
        has_folder = any(
            os.path.splitext(img['filename'])[0].lower() == 'folder' and img['subdir'] is None
            for img in image_infos
        )
        if not has_folder:
            warnings.add("[Main Image Not Selected]")
    if any(not img['readable'] for img in image_infos):
        warnings.add("[Corrupt Image]")
    if any(m.get('track') == '0' or m.get('disc') == '0' for m in md_list):
        warnings.add("[Zero Metadata]")
    if any(m.get('invalid_year_tag') for m in md_list):
        warnings.add("[Invalid Year]")

    tracks_by_disc = {}
    for m in md_list:
        if m.get('track'):
            d = m.get('disc') or '1'
            try:
                t = int(str(m['track']).split('/')[0])
                tracks_by_disc.setdefault(d, []).append(t)
            except:
                pass

    for d, tracks in tracks_by_disc.items():
        tracks = sorted(list(set(tracks)))
        if tracks[0] != 1:
            warnings.add("[Track Numbering Start]")
        if len(tracks) != (tracks[-1] - tracks[0] + 1):
            warnings.add("[Track Gap]")

    hashes = {m.get('cover_art_hash') for m in md_list if m.get('cover_art_hash')}
    if any(m.get('cover_art_count', 0) > 1 for m in md_list):
        warnings.add("[Multiple Covers]")
    if len(hashes) > 1:
        warnings.add("[Inconsistent Covers]")
    if len([m for m in md_list if m.get('cover_art_hash')]) < len(md_list):
        warnings.add("[Missing Cover]")

    if any(not m.get('title') for m in md_list):
        warnings.add("[Missing Title]")
    if any(not m.get('artist') for m in md_list):
        warnings.add("[Missing Artist]")
    if len({m.get('album') for m in md_list if m.get('album')}) > 1:
        warnings.add("[Inconsistent Album]")

    discs    = [m.get('disc') for m in md_list]
    has_d    = any(discs)
    has_no_d = any(d is None for d in discs)
    if has_d and has_no_d:
        warnings.add("[Inconsistent Disc #]")
    elif has_d and len(set(discs)) == 1:
        warnings.add("[Redundant Disc #]")

    if any(count > 1 for count in Counter([(m.get('track'), m.get('disc')) for m in md_list if m.get('track')]).values()):
        warnings.add("[Duplicate Track]")

    oversized = [(m.get('cover_art_w', 0), m.get('cover_art_h', 0))
                 for m in md_list
                 if max(m.get('cover_art_w', 0), m.get('cover_art_h', 0)) > COVER_MAX_SIZE]
    if oversized:
        w, h = max(oversized, key=lambda s: max(s[0], s[1]))
        warnings.add(f"[Large Cover {w}×{h}]")

    return sorted(list(warnings))


def plan_renames(info: Dict, final_folder_path: str, ignore_disc: bool) -> List[Tuple[str, str]]:
    plan     = []
    md_list  = info['files_metadata']
    has_disc = any(m.get('disc') for m in md_list)

    if has_disc and any(not m.get('disc') for m in md_list):
        return []

    # Byte cost of the folder path prefix including the trailing separator.
    folder_prefix_bytes = len((final_folder_path + os.sep).encode('utf-8'))

    proposed = set()
    for m in md_list:
        if not m.get('track') or not m.get('title'):
            continue

        track    = str(m['track']).split('/')[0].zfill(2)
        disc     = str(m.get('disc')) if (has_disc and not ignore_disc) else ''
        ext      = os.path.splitext(m['filename'])[1]
        s_title  = sanitize_filename(m['title'], True)
        s_artist = sanitize_filename(m['artist'], True) if m.get('artist') else ''

        def full_path_bytes(filename: str) -> int:
            return folder_prefix_bytes + len(filename.encode('utf-8'))

        if has_disc and not ignore_disc:
            base = f"{disc}-{track} {s_artist} - {s_title}" if s_artist else f"{disc}-{track} - {s_title}"
        else:
            base = f"{track} {s_artist} - {s_title}" if s_artist else f"{track} - {s_title}"

        new_name = f"{base}{ext}"

        if full_path_bytes(new_name) > PATH_LENGTH_LIMIT_BYTES:
            # Step 1: drop the artist
            base     = f"{disc}-{track} - {s_title}" if (has_disc and not ignore_disc) else f"{track} - {s_title}"
            new_name = f"{base}{ext}"

        if full_path_bytes(new_name) > PATH_LENGTH_LIMIT_BYTES:
            # Step 2: truncate the title to fit within the budget
            prefix       = f"{disc}-{track} - " if (has_disc and not ignore_disc) else f"{track} - "
            title_budget = PATH_LENGTH_LIMIT_BYTES - folder_prefix_bytes - len((prefix + ext).encode('utf-8'))
            s_title      = truncate_to_budget(s_title, max(1, title_budget))
            base         = f"{prefix}{s_title}"
            new_name     = f"{base}{ext}"

        final_base = base
        c = 1
        while new_name.lower() in proposed:
            new_name = f"{final_base} ({c}){ext}"
            c += 1
        proposed.add(new_name.lower())

        if m['filename'] != new_name:
            plan.append((os.path.join(info['path'], m['filename']), os.path.join(info['path'], new_name)))

    return plan


def plan_image_moves(info: Dict, album_dirpath: str) -> Tuple[List[Tuple[str,str]], Optional[str]]:
    """
    Build a list of (src, dst) rename/move operations for all images in *info*,
    and return the chosen artwork subdir path (or None if no extras exist).

    Rules:
      - 0 images  → nothing to do
      - 1 image   → rename to folder.<ext> at root (existing behaviour)
      - 2+ images → the image whose filename was chosen as main (stored in
                    info['chosen_main_image']) becomes folder.<ext> at root;
                    all others go into ARTWORK_DIRNAME/ with names
                    image-01.<ext>, image-02.<ext>, … (alphabetical order,
                    always sorts after "folder").
                    Any pre-existing image-only subdirs (artwork_dirs) that are
                    not already named ARTWORK_DIRNAME are renamed to
                    ARTWORK_DIRNAME (handled separately in plan step).

    Returns (moves, artwork_subdir_path | None).
    """
    image_infos  = info.get('image_infos', [])
    artwork_dirs = info.get('artwork_dirs', [])
    dirpath      = info['path']

    if not image_infos:
        return [], None

    if len(image_infos) == 1:
        img       = image_infos[0]
        src_path  = os.path.join(dirpath,
                                 img['filename'] if img['subdir'] is None
                                 else os.path.join(img['subdir'], img['filename']))
        new_name  = f"folder{img['ext']}"
        dst_path  = os.path.join(dirpath, new_name)
        moves     = []
        if os.path.normcase(src_path) != os.path.normcase(dst_path):
            moves.append((src_path, dst_path))
        return moves, None

    # 2+ images
    chosen_key = info.get('chosen_main_image')  # 'subdir/filename' or 'filename'
    main_move  = []   # at most one entry; applied AFTER extras to avoid clobbering
    art_path   = os.path.join(dirpath, ARTWORK_DIRNAME)

    # Separate main from extras
    extras = []
    for img in image_infos:
        key = img['filename'] if img['subdir'] is None else f"{img['subdir']}/{img['filename']}"
        src = os.path.join(dirpath,
                           img['filename'] if img['subdir'] is None
                           else os.path.join(img['subdir'], img['filename']))
        if key == chosen_key:
            dst = os.path.join(dirpath, f"folder{img['ext']}")
            if os.path.normcase(src) != os.path.normcase(dst):
                main_move.append((src, dst))
        else:
            extras.append((img, src))

    # Sort extras alphabetically by original filename for deterministic numbering
    extras.sort(key=lambda x: x[0]['filename'].lower())

    # Assign numbered names: image-01, image-02, … (always > "folder" alphabetically)
    extra_moves = []
    for idx, (img, src) in enumerate(extras, start=1):
        new_name = f"image-{idx:02d}{img['ext']}"
        dst = os.path.join(art_path, new_name)
        # Guard against case collision with its own destination
        if os.path.normcase(src) != os.path.normcase(dst):
            extra_moves.append((src, dst))

    # Extras are moved out first so the root slot is free before the new main arrives.
    moves = extra_moves + main_move

    return moves, art_path if extras else None


def _safe_move(src: str, dst: str) -> None:
    """os.rename with a case-only guard and parent-dir creation."""
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    if os.path.normcase(src) == os.path.normcase(dst):
        tmp = src + "__tmp_img_rename__"
        os.rename(src, tmp)
        os.rename(tmp, dst)
    else:
        os.rename(src, dst)


def run_scan_and_plan(root_folders: List[str], options: Dict):
    check_only     = options.get('check_only', False)
    force_yes      = options.get('force_yes', False)
    force_no       = options.get('force_no', False)
    folder_only    = options.get('folder_only', False)
    interactive    = options.get('interactive', True)
    chosen_images  = options.get('_chosen_images', {})  # {album_path: chosen_key}

    general_warnings  = []
    warnings_by_album = {}
    folder_info       = []

    print(f"{Color.HEADER}{Color.BOLD}--- Phase 1: Analyzing Folders ---{Color.ENDC}")

    for root_folder in root_folders:
        for dirpath, dirnames, filenames in os.walk(root_folder):
            if dirpath == root_folder:
                continue

            # Prune image-only subdirs from the walk so they are never mistaken
            # for container folders and are never walked into as album dirs.
            # collect_album_images() handles them directly from the parent.
            dirnames[:] = [d for d in dirnames
                           if not _is_image_only_dir(os.path.join(dirpath, d))]

            is_container = False
            if dirnames:
                for sub in dirnames:
                    try:
                        if any(f.lower().endswith(SUPPORTED_EXTENSIONS) for f in os.listdir(os.path.join(dirpath, sub))):
                            is_container = True
                            break
                    except:
                        continue

            if is_container:
                if not interactive:
                    choice = 'y' if force_yes else 'n'
                else:
                    choice = 'n' if check_only or force_no else 'y' if force_yes else ''
                    if not choice:
                        try:
                            choice = input(f"\n{Color.YELLOW}Container '{os.path.basename(dirpath)}' found. Flatten? (y/n): {Color.ENDC}").lower()
                        except:
                            choice = 'n'

                if choice == 'y':
                    if flatten_container_folder(dirpath, dirnames, general_warnings):
                        dirnames[:], filenames = [], os.listdir(dirpath)
                        info = analyze_album_folder(dirpath, filenames)
                        if info:
                            folder_info.append(info)
                else:
                    general_warnings.append(f"[Container Skipped] {os.path.basename(dirpath)}")
                    dirnames[:] = []
                continue

            info = analyze_album_folder(dirpath, filenames)
            if info:
                folder_info.append(info)

    print(f"\n{Color.HEADER}{Color.BOLD}--- Phase 2: Planning ---{Color.ENDC}")

    # Duplicates are only meaningful within the same parent directory.
    # Two artists each having "Greatest Hits" is not a conflict.
    counts = Counter(
        (os.path.dirname(i['path']), i['album'].lower())
        for i in folder_info if i['album']
    )
    dupes = {(parent, name) for (parent, name), c in counts.items() if c > 1}

    for info in folder_info:
        info['base_name'] = sanitize_filename(info['album'])
        parent = os.path.dirname(info['path'])
        if info['album'] and (parent, info['album'].lower()) in dupes and info['year']:
            info['base_name'] += f" ({info['year']})"

    base_counts = Counter(
        (os.path.dirname(i['path']), i['base_name'])
        for i in folder_info
    )
    needs_num = {(parent, name) for (parent, name), c in base_counts.items() if c > 1}
    counters  = Counter()

    folder_rename_plan, file_rename_plan, tag_plan, cover_resize_plan = [], [], [], []
    image_rename_plan, artwork_dir_rename_plan, embed_cover_plan = [], [], []
    preview_data   = []
    proposed_paths = set()
    folder_info.sort(key=lambda x: x['path'])

    for info in folder_info:
        base        = info['base_name']
        year_suffix = f" ({info['year']})" if info['year'] and base.endswith(f" ({info['year']})") else ""
        pure_base   = base[:-len(year_suffix)] if year_suffix else base

        parent = os.path.dirname(info['path'])
        budget = PATH_LENGTH_LIMIT_BYTES - len(parent.encode('utf-8')) - 1 - FILE_NAME_RESERVE_BYTES - len(year_suffix.encode('utf-8')) - 6
        final_base = truncate_to_budget(pure_base, max(1, budget)) + year_suffix

        counter_key = (parent, final_base)
        if counter_key in needs_num or counters[counter_key] > 0:
            final_name = f"{final_base} ({counters[counter_key] + 1})"
            counters[counter_key] += 1
        else:
            final_name = final_base

        final_path   = os.path.join(parent, final_name)

        # Apply any user image choice from the web preview
        if info['path'] in chosen_images:
            info['chosen_main_image'] = chosen_images[info['path']]

        w            = check_warnings(info)
        has_critical = "[Track Gap]" in w or "[Duplicate Track]" in w

        planned_files  = []
        file_map       = {}
        redundant_disc = "[Redundant Disc #]" in w

        if redundant_disc:
            for m in info['files_metadata']:
                tag_plan.append(os.path.join(info['path'], m['filename']))

        if not folder_only and not has_critical:
            p_files = plan_renames(info, final_path, ignore_disc=redundant_disc)
            if p_files:
                file_rename_plan.extend(p_files)
                planned_files = p_files
                for o, n in p_files:
                    file_map[os.path.basename(o)] = os.path.basename(n)

            image_infos = info.get('image_infos', [])

            if len(image_infos) == 1:
                # Single image: rename to folder.<ext> at root (existing behaviour)
                img_moves, _ = plan_image_moves(info, final_path)
                for o, n in img_moves:
                    file_rename_plan.append((o, n))
                    planned_files.append((o, n))
                    file_map[os.path.basename(o)] = os.path.basename(n)
                # Embed the cover into audio files that are missing it
                if '[Missing Cover]' in w:
                    img = image_infos[0]
                    final_img_path = os.path.join(final_path, f"folder{img['ext']}")
                    audio_final_paths = [
                        os.path.join(final_path, file_map.get(m['filename'], m['filename']))
                        for m in info['files_metadata']
                    ]
                    embed_cover_plan.append((final_img_path, audio_final_paths))

            elif len(image_infos) > 1:
                # Multiple images: only act if a main was chosen (preview mode)
                chosen = info.get('chosen_main_image')

                # CLI fallback: if no explicit selection but folder.* already exists
                # at the album root, treat it as the main image so we can still embed.
                if not chosen:
                    folder_img = next(
                        (img for img in image_infos
                         if os.path.splitext(img['filename'])[0].lower() == 'folder'
                         and img['subdir'] is None),
                        None
                    )
                    if folder_img:
                        chosen = folder_img['filename']
                        info['chosen_main_image'] = chosen
                if chosen:
                    img_moves, _art_path = plan_image_moves(info, final_path)
                    for o, n in img_moves:
                        image_rename_plan.append((o, n))
                        planned_files.append((o, n))
                        src_rel = os.path.relpath(o, info['path'])
                        dst_rel = os.path.relpath(n, info['path'])
                        file_map[src_rel] = dst_rel

                    # Rename pre-existing non-standard artwork dirs to ARTWORK_DIRNAME
                    for adir in info.get('artwork_dirs', []):
                        if adir != ARTWORK_DIRNAME:
                            adir_src = os.path.join(info['path'], adir)
                            adir_dst = os.path.join(info['path'], ARTWORK_DIRNAME)
                            if not os.path.exists(adir_dst) or os.path.normcase(adir_src) == os.path.normcase(adir_dst):
                                artwork_dir_rename_plan.append((adir_src, adir_dst))

                    # If any audio files are missing embedded cover art, plan to embed
                    # the chosen main image (at its post-move destination) into them.
                    if '[Missing Cover]' in w:
                        chosen_img_info = next(
                            (img for img in image_infos
                             if (img['filename'] if img['subdir'] is None
                                 else f"{img['subdir']}/{img['filename']}") == chosen),
                            None
                        )
                        if chosen_img_info:
                            final_img_path = os.path.join(final_path, f"folder{chosen_img_info['ext']}")
                            audio_final_paths = [
                                os.path.join(final_path, file_map.get(m['filename'], m['filename']))
                                for m in info['files_metadata']
                            ]
                            embed_cover_plan.append((final_img_path, audio_final_paths))
                # else: no main chosen — warn already added in check_warnings, do nothing

        if info['path'] != final_path:
            # On case-insensitive filesystems (macOS, Windows) os.path.exists() returns
            # True for case-only renames. Check normcase to avoid false conflict reports.
            is_case_only_rename = (
                os.path.exists(final_path)
                and os.path.normcase(info['path']) == os.path.normcase(final_path)
            )
            if os.path.exists(final_path) and not is_case_only_rename and final_path not in proposed_paths:
                general_warnings.append(f"{Color.RED}[Conflict] Target '{final_name}' exists.{Color.ENDC}")
                w.append("[Conflict: Target Exists]")
            else:
                folder_rename_plan.append((info['path'], final_path))
                proposed_paths.add(final_path)

        if w:
            warnings_by_album[final_name] = w

        # Collect files with oversized covers: store (path, hash) so execute
        # can group by hash and resize each unique image only once.
        for m in info['files_metadata']:
            if max(m.get('cover_art_w', 0), m.get('cover_art_h', 0)) > COVER_MAX_SIZE:
                final_filename = file_map.get(m['filename'], m['filename'])
                fpath = os.path.join(final_path, final_filename)
                cover_resize_plan.append((fpath, m.get('cover_art_hash', '')))

        p_files_list = []
        for m in info['files_metadata']:
            orig = m['filename']
            p_files_list.append({'original': orig, 'new': file_map.get(orig, orig)})
        # Also surface images in the file list for preview
        for img in info.get('image_infos', []):
            orig_rel = img['filename'] if img['subdir'] is None else f"{img['subdir']}/{img['filename']}"
            p_files_list.append({'original': orig_rel, 'new': file_map.get(orig_rel, orig_rel), 'is_image': True})
        p_files_list.sort(key=lambda x: x['new'])

        has_changes = (info['path'] != final_path) or bool(planned_files) or redundant_disc

        preview_data.append({
            'original_name':  os.path.basename(info['path']),
            'new_name':       final_name,
            'warnings':       w,
            'files':          p_files_list,
            'has_changes':    has_changes,
            'image_infos':    info.get('image_infos', []),
            'artwork_dirs':   info.get('artwork_dirs', []),
            'album_path':     info['path'],
        })

    stats = {'changed_albums': sum(1 for p in preview_data if p['has_changes'])}
    return preview_data, file_rename_plan, folder_rename_plan, tag_plan, cover_resize_plan, image_rename_plan, artwork_dir_rename_plan, embed_cover_plan, stats, general_warnings, warnings_by_album


def execute_changes(file_plan, folder_plan, tag_plan, cover_resize_plan=None,
                    resize_covers=False, image_rename_plan=None, artwork_dir_rename_plan=None,
                    embed_cover_plan=None, embed_cover=False):
    print(f"\n{Color.HEADER}{Color.BOLD}--- Phase 3: Executing ---{Color.ENDC}")

    if artwork_dir_rename_plan:
        print(f"\n{Color.BOLD}Step 0a: Renaming artwork subdirectories...{Color.ENDC}")
        dir_renames = {}  # old_dir_path -> new_dir_path
        for o, n in artwork_dir_rename_plan:
            try:
                _safe_move(o, n)
                print(f"  Dir: {os.path.basename(o)} -> {os.path.basename(n)}")
                dir_renames[o] = n
            except Exception as e:
                print(f"  {Color.RED}Error renaming artwork dir: {e}{Color.ENDC}")

        # Patch image source paths that now live under a renamed directory
        if dir_renames and image_rename_plan:
            updated = []
            for o, n in image_rename_plan:
                for old_dir, new_dir in dir_renames.items():
                    if o.startswith(old_dir + os.sep):
                        o = new_dir + o[len(old_dir):]
                        break
                updated.append((o, n))
            image_rename_plan = updated

    if image_rename_plan:
        print(f"\n{Color.BOLD}Step 0b: Organising images...{Color.ENDC}")
        # Resolve swap conflicts: if a move's destination is also a pending source,
        # that file would be clobbered before it gets a chance to move.
        # Pre-move it to a temp name and update the plan so the rest of the chain
        # still works (handles arbitrary swap chains, not just pairs).
        plan = list(image_rename_plan)
        src_index = {os.path.normcase(o): i for i, (o, _) in enumerate(plan)}
        for i, (o, n) in enumerate(plan):
            nc_n = os.path.normcase(n)
            if nc_n in src_index and os.path.exists(n):
                _ext = os.path.splitext(n)[1]
                _dir = os.path.dirname(os.path.abspath(n))
                tmp_fd, tmp = tempfile.mkstemp(suffix=_ext, dir=_dir)
                os.close(tmp_fd)
                try:
                    os.rename(n, tmp)
                    j = src_index.pop(nc_n)
                    plan[j] = (tmp, plan[j][1])
                    src_index[os.path.normcase(tmp)] = j
                except Exception as e:
                    print(f"  {Color.RED}Error pre-moving {os.path.basename(n)}: {e}{Color.ENDC}")
        for o, n in plan:
            try:
                _safe_move(o, n)
                print(f"  Image: {os.path.relpath(o)} -> {os.path.relpath(n)}")
            except Exception as e:
                print(f"  {Color.RED}Error moving image {os.path.basename(o)}: {e}{Color.ENDC}")

    if tag_plan:
        print(f"\n{Color.BOLD}Step 1: Removing redundant disc tags...{Color.ENDC}")
        for path in tag_plan:
            try:
                audio = MutagenFile(path, easy=True)
                if audio and 'discnumber' in audio:
                    del audio['discnumber']
                    audio.save()
            except Exception as e:
                print(f"  {Color.RED}Error cleaning tags for {os.path.basename(path)}: {e}{Color.ENDC}")

    if file_plan:
        print(f"\n{Color.BOLD}Step 2: Renaming files...{Color.ENDC}")
        for o, n in file_plan:
            try:
                os.rename(o, n)
                print(f"  File: {os.path.basename(o)} -> {os.path.basename(n)}")
            except Exception as e:
                print(f"  {Color.RED}Error: {e}{Color.ENDC}")

    if folder_plan:
        print(f"\n{Color.BOLD}Step 3: Renaming folders...{Color.ENDC}")
        folder_plan.sort(key=lambda x: len(x[0]), reverse=True)
        for o, n in folder_plan:
            try:
                # Case-only renames silently fail on case-insensitive filesystems
                # (macOS, Windows). Route through a temp name to force the update.
                if os.path.normcase(o) == os.path.normcase(n):
                    tmp = o + "__tmp_case_rename__"
                    os.rename(o, tmp)
                    os.rename(tmp, n)
                else:
                    os.rename(o, n)
                print(f"  Folder: {os.path.basename(o)} -> {os.path.basename(n)}")
            except Exception as e:
                print(f"  {Color.RED}Error: {e}{Color.ENDC}")

    if embed_cover and embed_cover_plan:
        print(f"\n{Color.BOLD}Step 4: Embedding cover art into audio files...{Color.ENDC}")
        for image_path, audio_paths in embed_cover_plan:
            ok, fail = embed_file_cover_into_audio(image_path, audio_paths)
            status = f"{Color.GREEN}✓{Color.ENDC} {os.path.basename(image_path)} → {ok} file(s) embedded"
            if fail:
                status += f"  {Color.RED}({fail} failed){Color.ENDC}"
            print(f"  {status}")

    if resize_covers and cover_resize_plan:
        print(f"\n{Color.BOLD}Step 5: Resizing oversized cover art (max {COVER_MAX_SIZE}px)...{Color.ENDC}")

        # Group files by cover hash so each unique image is resized exactly once.
        by_hash: Dict[str, List[str]] = {}
        for path, cover_hash in cover_resize_plan:
            if os.path.exists(path):
                by_hash.setdefault(cover_hash or path, []).append(path)

        written = 0
        for cover_hash, paths in by_hash.items():
            # Read and resize the cover image from the first file in the group.
            raw, mime = _read_raw_cover(paths[0])
            if raw is None:
                continue
            try:
                new_data, new_mime, new_w, new_h = _resize_image_bytes(raw, mime or 'image/jpeg', COVER_MAX_SIZE)
            except ValueError:
                continue  # already within limits (shouldn't happen, but be safe)
            except Exception as e:
                print(f"  {Color.RED}Error resizing image: {e}{Color.ENDC}")
                continue

            # Write the pre-resized bytes to every file in the group.
            for path in paths:
                if _write_cover_to_file(path, new_data, new_mime, new_w, new_h):
                    print(f"  {Color.GREEN}✓{Color.ENDC}  {os.path.basename(path)}")
                    written += 1

        unique = len(by_hash)
        print(f"  Resized {unique} unique cover image{'s' if unique != 1 else ''}, "
              f"written to {written} file{'s' if written != 1 else ''}.")

    print(f"{Color.GREEN}Done.{Color.ENDC}")


class AudioPreviewServer(BaseHTTPRequestHandler):
    data = {}

    def do_GET(self):
        if self.path == '/':
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(self.generate_html().encode('utf-8'))

    def do_POST(self):
        if self.path == '/apply':
            content_length = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(content_length)) if content_length else {}
            resize_covers   = body.get('resize_covers', False)
            embed_cover     = body.get('embed_cover', False)
            chosen_images   = body.get('chosen_images', {})  # {album_path: "subdir/filename" or "filename"}

            # Inject user image choices back into preview data so re-planning picks them up
            for album in self.data['preview']:
                key = album.get('album_path', '')
                if key in chosen_images:
                    album['chosen_main_image'] = chosen_images[key]

            # Re-run planning with choices embedded so image plans reflect selections
            opts = self.data['options'].copy()
            opts['interactive'] = False
            opts['_chosen_images'] = chosen_images
            p_data, f_plan, d_plan, t_plan, cov_plan, img_plan, adir_plan, emb_plan, stats, _, _ = run_scan_and_plan(
                self.data['roots'], opts)

            self.send_response(200)
            self.end_headers()
            print(f"\n{Color.CYAN}[Web] Applying changes...{Color.ENDC}")
            execute_changes(
                f_plan, d_plan, t_plan,
                cover_resize_plan=cov_plan,
                resize_covers=resize_covers,
                image_rename_plan=img_plan,
                artwork_dir_rename_plan=adir_plan,
                embed_cover_plan=emb_plan,
                embed_cover=embed_cover,
            )
            threading.Thread(target=self.server.shutdown).start()

        elif self.path == '/shutdown':
            self.send_response(200)
            self.end_headers()
            print(f"\n{Color.YELLOW}[Web] Cancelled.{Color.ENDC}")
            threading.Thread(target=self.server.shutdown).start()

        elif self.path == '/recheck':
            print(f"\n{Color.BLUE}[Web] Re-checking files...{Color.ENDC}")
            opts = self.data['options'].copy()
            opts['interactive'] = False
            p_data, f_plan, d_plan, t_plan, cov_plan, img_plan, adir_plan, emb_plan, stats, _, _ = run_scan_and_plan(self.data['roots'], opts)
            self.data.update({'preview': p_data, 'file_plan': f_plan, 'folder_plan': d_plan,
                              'tag_plan': t_plan, 'cover_resize_plan': cov_plan,
                              'image_rename_plan': img_plan, 'artwork_dir_rename_plan': adir_plan,
                              'embed_cover_plan': emb_plan, 'stats': stats})
            self.send_response(200)
            self.end_headers()
            print(f"{Color.GREEN}[Web] Re-check complete. Updating view.{Color.ENDC}")

    def generate_html(self):
        d    = self.data
        rows = ""
        for i, album in enumerate(d['preview']):
            name_html   = html.escape(album['new_name'])
            status_icon = "✓"
            card_class  = "card clean"

            if album['original_name'] != album['new_name']:
                name_html = f"<span class='old'>{html.escape(album['original_name'])}</span><span class='arrow'>➜</span><span class='new'>{name_html}</span>"

            if album['has_changes']:
                card_class  = "card changed"
                status_icon = "✎"

            warns = "".join(f"<span class='badge {'err' if 'Error' in w else 'warn'}'>{html.escape(w)}</span>" for w in album['warnings'])

            files_html = ""
            for f in album['files']:
                icon     = "🖼" if f.get('is_image') else "🎵"
                f_txt    = html.escape(f['new'])
                li_class = ""
                if f['original'] != f['new']:
                    f_txt    = f"<span class='old-file'>{html.escape(f['original'])}</span> <span class='new-file'>{f_txt}</span>"
                    li_class = "f-changed"
                files_html += f"<li class='{li_class}'><span class='f-icon'>{icon}</span> {f_txt}</li>"

            # Image selector — shown whenever there are 2+ images
            image_infos  = album.get('image_infos', [])
            album_path   = album.get('album_path', '')
            img_selector = ""
            if len(image_infos) >= 2:
                # Determine which image is currently the folder.* (pre-selected)
                current_folder = next(
                    (img for img in image_infos
                     if img['subdir'] is None and
                        os.path.splitext(img['filename'])[0].lower() == 'folder'),
                    None
                )
                safe_path = html.escape(album_path, quote=True)
                options_html = ""
                for img in sorted(image_infos, key=lambda x: (x['subdir'] or '', x['filename'].lower())):
                    key      = img['filename'] if img['subdir'] is None else f"{img['subdir']}/{img['filename']}"
                    label    = key
                    extra    = ""
                    if not img['readable']:
                        extra += " <span class='img-corrupt'>[Corrupt]</span>"
                    is_cur   = current_folder and img is current_folder
                    checked  = "checked" if is_cur else ""
                    cur_tag  = " <span class='img-current'>[current folder.*]</span>" if is_cur else ""
                    options_html += f"""
                    <label class='img-option{"  img-option-current" if is_cur else ""}'>
                        <input type='radio' name='img_{safe_path}' value='{html.escape(key, quote=True)}' {checked}
                               onchange='imgPick("{safe_path}", this.value, {i})'>
                        <span class='img-label'>{html.escape(label)}{cur_tag}{extra}</span>
                    </label>"""
                img_selector = f"""
                <div class='img-selector' id='imgsel-{i}'>
                    <div class='img-selector-title'>🖼 Pick main image <span class='img-hint'>(others → artwork/)</span></div>
                    <div class='img-options'>{options_html}
                    <label class='img-option'>
                        <input type='radio' name='img_{safe_path}' value=''
                               onchange='imgPick("{safe_path}", "", {i})'>
                        <span class='img-label img-none'>— Leave images as-is —</span>
                    </label>
                    </div>
                </div>"""

            rows += f"""
            <div class="{card_class}" id="card-{i}">
                <div class="card-header" onclick="toggle({i})">
                    <div class="card-title">
                        <span class="status-icon">{status_icon}</span>
                        <div class="name-container">{name_html}</div>
                    </div>
                    <div class="meta-container">
                        {warns}
                        <span class="chevron" id="chev-{i}">›</span>
                    </div>
                </div>
                <div id="ul-{i}" class="file-list">
                    {img_selector}
                    <ul>{files_html}</ul>
                </div>
            </div>"""

        return f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Audio Organizer</title>
            <style>
                :root {{
                    --bg: #121212; --card-bg: #1e1e1e; --text: #e0e0e0; --subtext: #a0a0a0;
                    --accent: #4caf50; --danger: #ef5350; --warn: #ffa726; --border: #333;
                    --hover: #2c2c2c; --header-bg: rgba(18, 18, 18, 0.85);
                }}
                body {{ background: var(--bg); color: var(--text); font-family: 'Inter', system-ui, -apple-system, sans-serif; margin: 0; padding: 20px; padding-top: 90px; }}
                .bar {{
                    position: fixed; top: 0; left: 0; right: 0;
                    background: var(--header-bg); backdrop-filter: blur(12px); -webkit-backdrop-filter: blur(12px);
                    padding: 15px 30px; border-bottom: 1px solid var(--border);
                    display: flex; justify-content: space-between; align-items: center; z-index: 100; box-shadow: 0 4px 20px rgba(0,0,0,0.3);
                }}
                .brand {{ font-weight: 700; font-size: 1.1em; display: flex; align-items: center; gap: 10px; }}
                .stats {{ font-size: 0.9em; color: var(--subtext); margin-left: 15px; border-left: 1px solid var(--border); padding-left: 15px; }}
                .controls {{ display: flex; gap: 12px; align-items: center; }}
                button {{
                    border: none; padding: 8px 16px; border-radius: 6px; font-weight: 600; cursor: pointer;
                    font-size: 0.9em; transition: all 0.2s ease; display: flex; align-items: center; gap: 6px;
                }}
                button:hover {{ transform: translateY(-1px); filter: brightness(1.1); }}
                button:active {{ transform: translateY(0); }}
                .btn-go {{ background: var(--accent); color: #000; box-shadow: 0 2px 8px rgba(76, 175, 80, 0.3); }}
                .btn-re {{ background: #2d2d2d; color: var(--text); border: 1px solid var(--border); }}
                .btn-no {{ background: transparent; color: var(--danger); border: 1px solid transparent; }}
                .btn-no:hover {{ background: rgba(239, 83, 80, 0.1); border-color: var(--danger); }}
                .switch-label {{ display: flex; align-items: center; gap: 8px; cursor: pointer; font-size: 0.9em; color: var(--subtext); margin-right: 10px; }}
                input[type="checkbox"] {{ accent-color: var(--accent); width: 16px; height: 16px; }}
                .grid {{ max-width: 1200px; margin: 0 auto; display: flex; flex-direction: column; gap: 12px; }}
                .card {{ background: var(--card-bg); border: 1px solid var(--border); border-radius: 8px; overflow: hidden; transition: border-color 0.2s; }}
                .card:hover {{ border-color: #444; }}
                .card.changed {{ border-left: 4px solid var(--accent); }}
                .card.clean {{ opacity: 0.8; }}
                .card-header {{ padding: 15px 20px; display: flex; justify-content: space-between; align-items: center; cursor: pointer; background: var(--card-bg); user-select: none; }}
                .card-header:hover {{ background: var(--hover); }}
                .card-title {{ display: flex; align-items: center; gap: 12px; font-weight: 500; font-size: 1.05em; }}
                .status-icon {{ color: var(--subtext); width: 20px; text-align: center; font-weight: bold; }}
                .card.changed .status-icon {{ color: var(--accent); }}
                .old {{ text-decoration: line-through; color: var(--subtext); font-size: 0.9em; }}
                .arrow {{ color: var(--subtext); margin: 0 8px; font-size: 0.8em; }}
                .new {{ color: var(--accent); font-weight: 600; }}
                .meta-container {{ display: flex; align-items: center; gap: 10px; }}
                .badge {{ font-size: 0.75em; padding: 4px 8px; border-radius: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; }}
                .badge.warn {{ background: rgba(255, 167, 38, 0.15); color: var(--warn); border: 1px solid rgba(255, 167, 38, 0.3); }}
                .badge.err {{ background: rgba(239, 83, 80, 0.15); color: var(--danger); border: 1px solid rgba(239, 83, 80, 0.3); }}
                .chevron {{ color: var(--subtext); font-size: 1.5em; transition: transform 0.3s; line-height: 0.5; margin-left: 10px; }}
                .chevron.rotate {{ transform: rotate(90deg); }}
                .file-list {{ display: none; background: #161616; border-top: 1px solid var(--border); padding: 10px 0; }}
                .file-list ul {{ list-style: none; padding: 0; margin: 0; }}
                .file-list li {{ padding: 6px 20px 6px 54px; font-size: 0.9em; font-family: 'Roboto Mono', monospace; color: var(--subtext); display: flex; align-items: center; gap: 8px; }}
                .f-icon {{ opacity: 0.5; }}
                .f-changed {{ color: var(--text) !important; background: rgba(76, 175, 80, 0.05); }}
                .f-changed .f-icon {{ color: var(--accent); opacity: 1; }}
                .old-file {{ text-decoration: line-through; opacity: 0.6; font-size: 0.9em; margin-right: 6px; }}
                .new-file {{ color: var(--accent); }}
                #status {{ font-size: 0.9em; color: var(--accent); font-weight: 600; animation: pulse 1.5s infinite; display: none; margin-right: 15px; }}
                @keyframes pulse {{ 0% {{ opacity: 0.6; }} 50% {{ opacity: 1; }} 100% {{ opacity: 0.6; }} }}
                .img-selector {{ padding: 10px 20px 6px 20px; border-bottom: 1px solid var(--border); }}
                .img-selector-title {{ font-size: 0.82em; font-weight: 600; color: var(--subtext); text-transform: uppercase; letter-spacing: 0.6px; margin-bottom: 6px; }}
                .img-hint {{ font-weight: 400; text-transform: none; letter-spacing: 0; color: #666; }}
                .img-options {{ display: flex; flex-wrap: wrap; gap: 6px; padding-bottom: 4px; }}
                .img-option {{ display: flex; align-items: center; gap: 6px; padding: 5px 10px; border-radius: 6px; border: 1px solid var(--border); cursor: pointer; font-size: 0.85em; font-family: 'Roboto Mono', monospace; color: var(--subtext); transition: border-color 0.15s, background 0.15s; }}
                .img-option:hover {{ border-color: var(--accent); background: rgba(76,175,80,0.05); color: var(--text); }}
                .img-option-current {{ border-color: #555; color: var(--text); }}
                .img-option input[type=radio] {{ accent-color: var(--accent); margin: 0; }}
                .img-current {{ color: var(--accent); font-size: 0.85em; }}
                .img-corrupt {{ color: var(--danger); font-size: 0.85em; }}
                .img-none {{ color: #666; font-style: italic; }}
            </style>
            <script>
                function toggle(id) {{
                    var el   = document.getElementById('ul-'+id);
                    var chev = document.getElementById('chev-'+id);
                    if (el.style.display === 'block') {{
                        el.style.display = 'none';
                        chev.classList.remove('rotate');
                    }} else {{
                        el.style.display = 'block';
                        chev.classList.add('rotate');
                    }}
                }}
                var chosenImages = {{}};
                function imgPick(albumPath, value, cardIdx) {{
                    if (value === '') {{
                        delete chosenImages[albumPath];
                    }} else {{
                        chosenImages[albumPath] = value;
                    }}
                    var card = document.getElementById('card-' + cardIdx);
                    if (card) {{
                        card.querySelectorAll('.badge').forEach(function(b) {{
                            if (b.textContent === '[Main Image Not Selected]') {{
                                b.style.display = value ? 'none' : '';
                            }}
                        }});
                    }}
                }}
                function post(url) {{
                    var statusEl = document.getElementById('status');
                    if (url === '/recheck') {{
                        statusEl.style.display = "inline";
                        statusEl.innerText = "Scanning...";
                    }}
                    var opts = {{method:'POST'}};
                    if (url === '/apply') {{
                        var resizeCovers = document.getElementById('resize-covers').checked;
                        var embedCover   = document.getElementById('embed-cover').checked;
                        opts.headers = {{'Content-Type': 'application/json'}};
                        opts.body    = JSON.stringify({{resize_covers: resizeCovers, embed_cover: embedCover, chosen_images: chosenImages}});
                    }}
                    fetch(url, opts).then(() => {{
                        if (url === '/recheck') location.reload();
                        else if (url === '/apply') {{
                            document.body.innerHTML = "<div style='display:flex;justify-content:center;align-items:center;height:100vh;flex-direction:column;color:#4caf50'><h1>All Done!</h1><p style='color:#aaa'>You can close this tab now.</p></div>";
                        }}
                        else window.close();
                    }});
                }}
                function filter() {{
                    var chk   = document.getElementById('chk').checked;
                    var items = document.getElementsByClassName('clean');
                    for (var i = 0; i < items.length; i++) items[i].style.display = chk ? 'none' : 'block';
                }}
            </script>
        </head>
        <body>
            <div class="bar">
                <div class="brand">
                    <span>🎵 Audio Organizer</span>
                    <span class="stats">
                        {len(d['preview'])} Albums • <span style="color: var(--accent)">{d['stats']['changed_albums']} Changes</span>
                    </span>
                </div>
                <div class="controls">
                    <span id="status"></span>
                    <label class="switch-label"><input type="checkbox" id="chk" onclick="filter()"> Changes Only</label>
                    <label class="switch-label"><input type="checkbox" id="resize-covers" {'checked' if d['options'].get('cover_size') else ''}> Resize Covers (&gt;{COVER_MAX_SIZE}px)</label>
                    <label class="switch-label"><input type="checkbox" id="embed-cover" checked> Embed cover <span style="font-size:0.85em;opacity:0.7">[Missing Cover]</span></label>
                    <button class="btn-re" onclick="post('/recheck')">↻ Re-check</button>
                    <button class="btn-no" onclick="post('/shutdown')">Cancel</button>
                    <button class="btn-go" onclick="post('/apply')">PROCEED →</button>
                </div>
            </div>
            <div class="grid">
                {rows}
            </div>
        </body>
        </html>
        """

    def log_message(self, format, *args):
        return


def organize_music_folders(roots: List[str], **kwargs):
    for root in roots:
        if not os.path.exists(root):
            return print(f"Folder not found: {root}")

    p_data, f_plan, d_plan, t_plan, cov_plan, img_plan, adir_plan, emb_plan, stats, warns, alb_warns = run_scan_and_plan(roots, kwargs)

    if kwargs.get('preview_mode'):
        print(f"\n{Color.CYAN}Starting Web Preview...{Color.ENDC}")
        AudioPreviewServer.data = {
            'preview': p_data, 'file_plan': f_plan, 'folder_plan': d_plan,
            'tag_plan': t_plan, 'cover_resize_plan': cov_plan,
            'image_rename_plan': img_plan, 'artwork_dir_rename_plan': adir_plan,
            'embed_cover_plan': emb_plan,
            'stats': stats, 'roots': roots, 'options': kwargs,
        }
        try:
            server = HTTPServer(('localhost', 8000), AudioPreviewServer)
            print(f"{Color.GREEN}Open: http://localhost:8000{Color.ENDC}")
            webbrowser.open("http://localhost:8000")
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        except OSError:
            print("Port 8000 in use.")
        return

    if not f_plan and not d_plan and not img_plan and not adir_plan and not warns and not alb_warns:
        print(f"\n{Color.GREEN}No changes needed.{Color.ENDC}")
    elif kwargs.get('check_only'):
        print(f"\n{Color.YELLOW}Check-only mode.{Color.ENDC}")
    else:
        execute_changes(f_plan, d_plan, t_plan,
                        cover_resize_plan=cov_plan,
                        resize_covers=kwargs.get('cover_size', False),
                        image_rename_plan=img_plan,
                        artwork_dir_rename_plan=adir_plan,
                        embed_cover_plan=emb_plan,
                        embed_cover=True)

    if warns or alb_warns:
        print(f"\n{Color.HEADER}--- Warnings ---{Color.ENDC}")
        for w in sorted(warns):
            print(w)
        for a, ws in sorted(alb_warns.items()):
            print(f"{Color.YELLOW}{', '.join(ws)}{Color.ENDC} in '{a}'")


def main():
    parser = argparse.ArgumentParser(
        prog="aud-organize-library",
        description="Scan, rename, and organize audio library folders.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  aud-organize-library                          # current directory
  aud-organize-library ~/Music/
  aud-organize-library --list                   # pick subdirs interactively
  aud-organize-library --list ~/Music/          # pick from ~/Music subdirs
  aud-organize-library -p ~/Music/              # interactive web preview
  aud-organize-library -c ~/Music/              # check only, no changes
  aud-organize-library -y ~/Music/              # auto-confirm everything
  aud-organize-library --folder-only ~/Music/   # skip file renaming
  aud-organize-library --cover-size ~/Music/    # resize embedded covers > 700px
        """,
    )
    parser.add_argument("folder",        nargs="?", default=None,        help="Folder to scan (or base for --list; default: current directory)")
    parser.add_argument("--list",             action="store_true", help="List subdirectories and interactively select which to process")
    parser.add_argument("-p", "--preview",     action="store_true", help="Open interactive web preview")
    parser.add_argument("-c", "--check",       action="store_true", help="Check only, make no changes")
    parser.add_argument("-y", "--force-yes",   action="store_true", help="Auto-confirm all prompts")
    parser.add_argument("-n", "--force-no",    action="store_true", help="Auto-decline all prompts")
    parser.add_argument("--folder-only",       action="store_true", help="Rename folders only, skip file renaming")
    parser.add_argument("--cover-size",        action="store_true", help=f"Resize embedded covers larger than {COVER_MAX_SIZE}px on longest side")
    args = parser.parse_args()

    if args.list:
        base = args.folder if args.folder else os.getcwd()
        if not os.path.isdir(base):
            print(f"{Color.RED}Error: '{base}' is not a directory.{Color.ENDC}")
            sys.exit(1)
        chosen = list_and_select(base)
        if not chosen:
            sys.exit(0)
        roots = chosen
    else:
        roots = [args.folder if args.folder else os.getcwd()]

    organize_music_folders(
        roots,
        check_only    = args.check,
        force_yes     = args.force_yes,
        force_no      = args.force_no,
        preview_mode  = args.preview,
        folder_only   = args.folder_only,
        cover_size    = args.cover_size,
        interactive   = True,
    )


if __name__ == "__main__":
    main()
