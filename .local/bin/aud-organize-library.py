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
    aud-organize-library                        # default: interactive web preview (covers resized >700px, desc stripped)
    aud-organize-library ~/Music/               # preview specific directory
    aud-organize-library --list ~/Music/        # pick subdirectories interactively
    aud-organize-library --no-preview           # run in terminal CLI mode without web browser
    aud-organize-library -c ~/Music/            # check only in terminal, no changes
    aud-organize-library -y ~/Music/            # auto-confirm everything in terminal mode
    aud-organize-library --no-cover-size        # do not resize embedded covers > 700px
    aud-organize-library --no-strip-desc        # preserve cover art description tags
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
import urllib.request
import urllib.parse
import urllib.error
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
MIN_FILENAME_BYTES         = 50   # guaranteed floor for the filename (track + title + ext)
MIN_FOLDERNAME_BYTES       = 30   # guaranteed floor for the album folder name
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


def embed_file_cover_into_audio(image_path: str, audio_paths: List[str], max_size: Optional[int] = COVER_MAX_SIZE) -> Tuple[int, int]:
    """
    Read *image_path* from disk, resize to at most max_size px on the
    longest side (if max_size given), and embed the result into each file in *audio_paths*.
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

    if max_size:
        try:
            new_data, new_mime, new_w, new_h = _resize_image_bytes(raw, mime, max_size)
        except ValueError:
            new_data, new_mime = raw, mime
            try:
                img = Image.open(io.BytesIO(raw))
                new_w, new_h = img.size
            except Exception:
                new_w = new_h = 0
        except Exception as e:
            print(f"  {Color.RED}Error resizing {os.path.basename(image_path)}: {e}{Color.ENDC}")
            return 0, len(audio_paths)
    else:
        new_data, new_mime = raw, mime
        try:
            img = Image.open(io.BytesIO(raw))
            new_w, new_h = img.size
        except Exception:
            new_w = new_h = 0

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


_thumb_cache: Dict[str, bytes] = {}


def _get_cached_thumbnail(img_bytes: bytes, max_dim: int) -> bytes:
    """Generate or retrieve a cached JPEG thumbnail with max dimension max_dim."""
    h = hashlib.md5(img_bytes).hexdigest()
    key = f"{h}:{max_dim}"
    if key in _thumb_cache:
        return _thumb_cache[key]
    try:
        img = Image.open(io.BytesIO(img_bytes))
        img.thumbnail((max_dim, max_dim), Image.BILINEAR)
        buf = io.BytesIO()
        img.convert('RGB').save(buf, format='JPEG', quality=85)
        res = buf.getvalue()
        _thumb_cache[key] = res
        return res
    except Exception:
        return img_bytes


def _is_safe_path(requested_path: str, roots: List[str]) -> bool:
    """Verify that requested_path is within one of the allowed roots."""
    try:
        norm = os.path.normcase(os.path.abspath(requested_path))
        for r in roots:
            r_norm = os.path.normcase(os.path.abspath(r))
            if norm == r_norm or norm.startswith(r_norm + os.sep):
                return True
        return False
    except Exception:
        return False


def clean_cover_art_descriptions(file_path: str) -> bool:
    """
    Remove cover art / picture description metadata from an audio file.
    Returns True if any description was modified and saved.
    """
    try:
        audio = MutagenFile(file_path)
        if not audio:
            return False
        modified = False

        if hasattr(audio, 'tags') and audio.tags is not None:
            if hasattr(audio.tags, 'getall'):
                for apic in audio.tags.getall('APIC'):
                    if getattr(apic, 'desc', '') != '':
                        apic.desc = ''
                        modified = True
            for key in list(audio.tags.keys()):
                if key.lower() in ('coverartdescription', 'cover_art_description', 'picture_description'):
                    del audio.tags[key]
                    modified = True
            if 'metadata_block_picture' in audio.tags:
                from mutagen.flac import Picture
                new_blocks = []
                for block in audio.tags['metadata_block_picture']:
                    try:
                        pic = Picture(base64.b64decode(block))
                        if getattr(pic, 'desc', '') != '':
                            pic.desc = ''
                            new_blocks.append(base64.b64encode(pic.write()).decode('ascii'))
                            modified = True
                        else:
                            new_blocks.append(block)
                    except Exception:
                        new_blocks.append(block)
                if modified:
                    audio.tags['metadata_block_picture'] = new_blocks

        if hasattr(audio, 'pictures'):
            for pic in audio.pictures:
                if getattr(pic, 'desc', '') != '':
                    pic.desc = ''
                    modified = True

        if modified:
            audio.save()
            return True
    except Exception:
        pass
    return False


def extract_cover_from_audio(album_path: str, track_rel_path: Optional[str] = None) -> Tuple[bool, str, Optional[Dict]]:
    """
    Extract embedded cover art from an audio file in *album_path* to folder.<ext>
    at original resolution (no resizing for disk files).
    Returns (success, message, image_info_dict).
    """
    if not os.path.exists(album_path):
        return False, f"Album directory not found: {album_path}", None

    source_path = None
    if track_rel_path:
        cand = os.path.join(album_path, track_rel_path)
        if os.path.exists(cand):
            source_path = cand

    if not source_path:
        # Check all audio files in the album folder
        try:
            for f in sorted(os.listdir(album_path)):
                if f.lower().endswith(SUPPORTED_EXTENSIONS):
                    cand = os.path.join(album_path, f)
                    raw, _ = _read_raw_cover(cand)
                    if raw:
                        source_path = cand
                        break
        except OSError as e:
            return False, f"Cannot read album folder: {e}", None

    if not source_path:
        return False, "No embedded cover art found in any audio file in this album.", None

    raw_data, _ = _read_raw_cover(source_path)
    if not raw_data:
        return False, f"Failed to extract cover art from {os.path.basename(source_path)}.", None

    try:
        img = Image.open(io.BytesIO(raw_data))
        fmt = (img.format or 'JPEG').upper()
        ext = '.png' if fmt == 'PNG' else '.jpg'
        w, h = img.size
    except Exception as e:
        return False, f"Failed to decode extracted image: {e}", None

    target_path = os.path.join(album_path, f"folder{ext}")
    try:
        with open(target_path, 'wb') as fh:
            fh.write(raw_data)
    except OSError as e:
        return False, f"Failed to write image file: {e}", None

    img_info = {
        'filename':   f"folder{ext}",
        'subdir':     None,
        'size_bytes': len(raw_data),
        'readable':   True,
        'ext':        ext,
        'width':      w,
        'height':     h,
    }
    return True, f"Extracted cover to folder{ext} ({w}×{h})", img_info


def fetch_cover_from_url(album_path: str, url: str) -> Tuple[bool, str, Optional[Dict]]:
    """
    Download cover image from Bandcamp, RYM, or direct image link.
    Rewrites Bandcamp links to _0.jpg for original resolution.
    Saves directly to folder.<ext> in *album_path* without resizing.
    Returns (success, message, image_info_dict).
    """
    if not os.path.exists(album_path):
        return False, f"Album directory not found: {album_path}", None

    url = url.strip()
    if not url.startswith(('http://', 'https://')):
        return False, "Invalid URL: Must begin with http:// or https://", None

    # Bandcamp URL transformation: rewrite _<num>.jpg to _0.jpg for maximum original resolution
    # e.g., https://f4.bcbits.com/img/a2377694191_10.jpg -> ..._0.jpg
    if 'bcbits.com/img/' in url:
        url = re.sub(r'(_\d+)(\.[a-zA-Z0-9]+)?(\?.*)?$', r'_0\2', url)
        if not re.search(r'_\d+', url):
            url = re.sub(r'(_\d+)', '_0', url)

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0',
        'Accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
    }
    if 'sonemic.net' in url or 'rateyourmusic.com' in url or 'snmc.io' in url:
        headers['Referer'] = 'https://rateyourmusic.com/'

    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=20) as resp:
            content_type = resp.headers.get('Content-Type', '')
            data = resp.read()
    except urllib.error.HTTPError as e:
        if e.code == 403 and ('rateyourmusic.com' in url or 'sonemic.net' in url):
            return False, "RYM returned 403 Forbidden. Please copy the direct image link (e.g. cdn.sonemic.net/i/...).", None
        return False, f"HTTP Error {e.code}: {e.reason}", None
    except Exception as e:
        return False, f"Failed to download image: {e}", None

    # If the response is HTML, inspect for og:image or link rel="image_src"
    if 'text/html' in content_type:
        try:
            html_text = data.decode('utf-8', errors='ignore')
            m = re.search(r'<meta\s+property=["\']og:image["\']\s+content=["\']([^"\']+)["\']', html_text, re.IGNORECASE)
            if not m:
                m = re.search(r'<link\s+rel=["\']image_src["\']\s+href=["\']([^"\']+)["\']', html_text, re.IGNORECASE)
            if m:
                found_url = html.unescape(m.group(1))
                return fetch_cover_from_url(album_path, found_url)
            else:
                return False, "Provided link is an HTML webpage with no detectable album cover image.", None
        except Exception as e:
            return False, f"Could not parse HTML page: {e}", None

    # Verify image with Pillow
    try:
        img = Image.open(io.BytesIO(data))
        fmt = (img.format or 'JPEG').upper()
        ext = '.png' if fmt == 'PNG' else '.jpg'
        w, h = img.size
    except Exception as e:
        return False, f"Downloaded data is not a valid image: {e}", None

    target_path = os.path.join(album_path, f"folder{ext}")
    try:
        with open(target_path, 'wb') as fh:
            fh.write(data)
    except OSError as e:
        return False, f"Failed to save image file: {e}", None

    img_info = {
        'filename':   f"folder{ext}",
        'subdir':     None,
        'size_bytes': len(data),
        'readable':   True,
        'ext':        ext,
        'width':      w,
        'height':     h,
    }
    return True, f"Saved folder{ext} ({w}×{h})", img_info


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


def _get_image_details(fpath: str) -> Tuple[bool, int, int]:
    """Return (readable, width, height) for the image file at *fpath*."""
    try:
        with Image.open(fpath) as img:
            return True, img.width, img.height
    except Exception:
        return False, 0, 0


def _verify_image(fpath: str) -> bool:
    """Return True if Pillow can open and verify the image at *fpath*."""
    return _get_image_details(fpath)[0]


def collect_album_images(dirpath: str, filenames: List[str]) -> Tuple[List[Dict], List[str]]:
    """
    Collect all images belonging to an album folder:
      - images at the root (from *filenames*)
      - images inside any subdir whose entire content is images
        (considered an artwork dir regardless of its name)

    Returns:
        image_infos  — list of dicts: filename, subdir (None=root or str),
                       size_bytes, readable, ext, width, height
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
        readable, w, h = _get_image_details(fpath)
        image_infos.append({
            'filename':   f,
            'subdir':     None,
            'size_bytes': size,
            'readable':   readable,
            'ext':        os.path.splitext(f)[1].lower(),
            'width':      w,
            'height':     h,
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
            readable, w, h = _get_image_details(fpath)
            image_infos.append({
                'filename':   f,
                'subdir':     sub,
                'size_bytes': size,
                'readable':   readable,
                'ext':        os.path.splitext(f)[1].lower(),
                'width':      w,
                'height':     h,
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

    chosen_main = None
    if len(image_infos) == 1:
        img = image_infos[0]
        chosen_main = img['filename'] if img['subdir'] is None else f"{img['subdir']}/{img['filename']}"

    print(f"  {Color.GREEN}-> Found album: '{most_common_album}'{f' ({year})' if year else ''}{Color.ENDC}")

    return {
        'path':           dirpath,
        'album':          most_common_album,
        'year':           year,
        'files_metadata': files_metadata,
        'image_infos':    image_infos,
        'artwork_dirs':   artwork_dirs,
        'has_images':     bool(image_infos),
        'chosen_main_image': chosen_main,
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
        has_chosen = bool(info.get('chosen_main_image'))
        if not has_folder and not has_chosen:
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
    replace_all_covers   = options.get('replace_cover', False)
    replace_cover_albums = set(options.get('_replace_cover_albums', []))

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
        # available = bytes left after the parent path and its trailing separator.
        # We split this pool between the folder name and the filename, guaranteeing
        # a floor for each side so neither can starve the other.
        #   available = folder_name + sep + filename
        parent_prefix_bytes = len((parent + os.sep).encode('utf-8'))
        available           = PATH_LENGTH_LIMIT_BYTES - parent_prefix_bytes
        year_suffix_bytes   = len(year_suffix.encode('utf-8'))
        # folder gets whatever is left after reserving the filename floor and the
        # separator between folder and file; year_suffix bytes are also reserved
        # because they're appended after truncation.
        folder_budget = max(MIN_FOLDERNAME_BYTES, available - MIN_FILENAME_BYTES - 1 - year_suffix_bytes)
        final_base = truncate_to_budget(pure_base, max(1, folder_budget)) + year_suffix

        counter_key = (parent, final_base)
        if counter_key in needs_num or counters[counter_key] > 0:
            final_name = f"{final_base} ({counters[counter_key] + 1})"
            counters[counter_key] += 1
        else:
            final_name = final_base

        final_path   = os.path.join(parent, final_name)

        # Apply any user image choice from the web preview or auto-select single image
        if info['path'] in chosen_images and chosen_images[info['path']]:
            info['chosen_main_image'] = chosen_images[info['path']]
        elif len(info.get('image_infos', [])) == 1:
            img = info['image_infos'][0]
            info['chosen_main_image'] = img['filename'] if img['subdir'] is None else f"{img['subdir']}/{img['filename']}"
        elif info['path'] in chosen_images:
            info['chosen_main_image'] = None

        w            = check_warnings(info)
        has_critical = "[Track Gap]" in w or "[Duplicate Track]" in w

        planned_files  = []
        file_map       = {}
        redundant_disc = "[Redundant Disc #]" in w
        album_will_embed = False
        replace_this_album = replace_all_covers or (info['path'] in replace_cover_albums)

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
                # Embed the cover into audio files that are missing it or if replacing
                if '[Missing Cover]' in w or replace_this_album:
                    img = image_infos[0]
                    final_img_path = os.path.join(final_path, f"folder{img['ext']}")
                    audio_final_paths = [
                        os.path.join(final_path, file_map.get(m['filename'], m['filename']))
                        for m in info['files_metadata']
                    ]
                    embed_cover_plan.append((final_img_path, audio_final_paths))
                    album_will_embed = True

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

                    # If any audio files are missing embedded cover art or replacement requested, plan to embed
                    # the chosen main image (at its post-move destination) into them.
                    if '[Missing Cover]' in w or replace_this_album:
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
                            album_will_embed = True
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
            if (m.get('cover_art_w', 0) > COVER_MAX_SIZE or
                    m.get('cover_art_h', 0) > COVER_MAX_SIZE):
                old_f = m['filename']
                new_f = file_map.get(old_f, old_f)
                cover_resize_plan.append((os.path.join(final_path, new_f), m.get('cover_art_hash')))

        # Collect majority cover hash for track cover consistency check
        cover_hashes = [m.get('cover_art_hash') for m in info['files_metadata'] if m.get('cover_art_hash')]
        majority_cover_hash = Counter(cover_hashes).most_common(1)[0][0] if cover_hashes else None

        p_files_list = []
        for m in info['files_metadata']:
            orig = m['filename']
            c_hash = m.get('cover_art_hash')
            is_inconsistent = bool(c_hash and majority_cover_hash and c_hash != majority_cover_hash)
            p_files_list.append({
                'original':        orig,
                'new':             file_map.get(orig, orig),
                'track':           m.get('track'),
                'disc':            m.get('disc'),
                'title':           m.get('title'),
                'artist':          m.get('artist'),
                'cover_count':     m.get('cover_art_count', 0),
                'cover_hash':      c_hash,
                'cover_w':         m.get('cover_art_w', 0),
                'cover_h':         m.get('cover_art_h', 0),
                'is_inconsistent': is_inconsistent,
            })
        # Also surface images in the file list for preview
        for img in info.get('image_infos', []):
            orig_rel = img['filename'] if img['subdir'] is None else f"{img['subdir']}/{img['filename']}"
            p_files_list.append({
                'original':   orig_rel,
                'new':        file_map.get(orig_rel, orig_rel),
                'is_image':   True,
                'size_bytes': img.get('size_bytes', 0),
                'width':      img.get('width', 0),
                'height':     img.get('height', 0),
            })
        p_files_list.sort(key=lambda x: (x.get('is_image', False), x['new']))

        has_changes = (info['path'] != final_path) or bool(planned_files) or redundant_disc or album_will_embed

        preview_data.append({
            'original_name':       os.path.basename(info['path']),
            'new_name':            final_name,
            'warnings':            w,
            'files':               p_files_list,
            'has_changes':         has_changes,
            'image_infos':         info.get('image_infos', []),
            'artwork_dirs':        info.get('artwork_dirs', []),
            'album_path':          info['path'],
            'chosen_main_image':   info.get('chosen_main_image'),
            'majority_cover_hash': majority_cover_hash,
            'has_embedded_cover':  len(cover_hashes) > 0,
            'total_audio_files':   len(info['files_metadata']),
            'will_embed_cover':    album_will_embed,
            'will_replace_embedded': bool(album_will_embed and replace_this_album),
        })

    stats = {
        'total_albums': len(preview_data),
        'changed_albums': sum(1 for p in preview_data if p['has_changes']),
        'warning_albums': sum(1 for p in preview_data if p['warnings']),
        'missing_image_albums': sum(1 for p in preview_data if '[No Image]' in p['warnings'] or '[Main Image Not Selected]' in p['warnings']),
        'inconsistent_cover_albums': sum(1 for p in preview_data if '[Inconsistent Covers]' in p['warnings']),
    }
    return preview_data, file_rename_plan, folder_rename_plan, tag_plan, cover_resize_plan, image_rename_plan, artwork_dir_rename_plan, embed_cover_plan, stats, general_warnings, warnings_by_album


def execute_changes(file_plan, folder_plan, tag_plan, cover_resize_plan=None,
                    resize_covers=False, image_rename_plan=None, artwork_dir_rename_plan=None,
                    embed_cover_plan=None, embed_cover=False, strip_descriptions=False,
                    all_audio_files=None):
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
            max_s = COVER_MAX_SIZE if resize_covers else None
            ok, fail = embed_file_cover_into_audio(image_path, audio_paths, max_size=max_s)
            status = f"{Color.GREEN}[OK]{Color.ENDC} {os.path.basename(image_path)} -> {ok} file(s) embedded"
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
                    print(f"  {Color.GREEN}[OK]{Color.ENDC}  {os.path.basename(path)}")
                    written += 1

        unique = len(by_hash)
        print(f"  Resized {unique} unique cover image{'s' if unique != 1 else ''}, "
              f"written to {written} file{'s' if written != 1 else ''}.")

    if strip_descriptions:
        print(f"\n{Color.BOLD}Step 6: Stripping description metadata from embedded covers...{Color.ENDC}")
        targets = set()
        if all_audio_files:
            targets.update(all_audio_files)
        if file_plan:
            targets.update(dst for _, dst in file_plan if dst.lower().endswith(SUPPORTED_EXTENSIONS))
        stripped_count = 0
        for fpath in targets:
            if os.path.exists(fpath) and clean_cover_art_descriptions(fpath):
                stripped_count += 1
        print(f"  Cleaned description tags in {stripped_count} audio file{'s' if stripped_count != 1 else ''}.")

    print(f"{Color.GREEN}Done.{Color.ENDC}")


class AudioPreviewServer(BaseHTTPRequestHandler):
    data = {}

    def _send_json(self, status: int, payload: Dict):
        body = json.dumps(payload).encode('utf-8')
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path_only = parsed.path

        if path_only == '/':
            html_content = self.generate_html().encode('utf-8')
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(html_content)))
            self.end_headers()
            self.wfile.write(html_content)
            return

        if path_only == '/api/image':
            params = urllib.parse.parse_qs(parsed.query)
            target = params.get('path', [None])[0]
            thumb = params.get('thumb', ['0'])[0] == '1'

            if not target or not _is_safe_path(target, self.data.get('roots', [])) or not os.path.isfile(target):
                self.send_error(404, "Not Found")
                return

            try:
                with open(target, 'rb') as fh:
                    raw_bytes = fh.read()
                if thumb:
                    out_bytes = _get_cached_thumbnail(raw_bytes, 260)
                    mime = 'image/jpeg'
                else:
                    out_bytes = raw_bytes
                    ext = os.path.splitext(target)[1].lower()
                    mime = 'image/png' if ext == '.png' else 'image/jpeg'

                self.send_response(200)
                self.send_header("Content-Type", mime)
                self.send_header("Cache-Control", "public, max-age=3600")
                self.send_header("Content-Length", str(len(out_bytes)))
                self.end_headers()
                self.wfile.write(out_bytes)
            except Exception as e:
                self.send_error(500, f"Error reading image: {e}")
            return

        if path_only == '/api/track_cover':
            params = urllib.parse.parse_qs(parsed.query)
            target = params.get('path', [None])[0]
            thumb = params.get('thumb', ['0'])[0] == '1'

            if not target or not _is_safe_path(target, self.data.get('roots', [])) or not os.path.isfile(target):
                self.send_error(404, "Not Found")
                return

            raw_bytes, raw_mime = _read_raw_cover(target)
            if not raw_bytes:
                self.send_error(404, "No embedded cover found")
                return

            try:
                if thumb:
                    out_bytes = _get_cached_thumbnail(raw_bytes, 180)
                    mime = 'image/jpeg'
                else:
                    out_bytes = raw_bytes
                    mime = raw_mime or 'image/jpeg'

                self.send_response(200)
                self.send_header("Content-Type", mime)
                self.send_header("Cache-Control", "public, max-age=3600")
                self.send_header("Content-Length", str(len(out_bytes)))
                self.end_headers()
                self.wfile.write(out_bytes)
            except Exception as e:
                self.send_error(500, f"Error reading track cover: {e}")
            return

        self.send_error(404, "Not Found")

    def do_POST(self):
        content_length = int(self.headers.get('Content-Length', 0))
        body = json.loads(self.rfile.read(content_length)) if content_length else {}
        parsed = urllib.parse.urlparse(self.path)
        path_only = parsed.path

        if path_only in ('/recheck', '/api/recheck'):
            print(f"\n{Color.BLUE}[Web] Re-checking files...{Color.ENDC}")
            chosen_images = body.get('chosen_images', {})
            self.data['chosen_images'] = chosen_images
            replace_cover = body.get('replace_cover', False)
            replace_cover_albums = body.get('replace_cover_albums', [])

            opts = self.data['options'].copy()
            opts['interactive'] = False
            opts['_chosen_images'] = chosen_images
            opts['replace_cover'] = replace_cover
            opts['_replace_cover_albums'] = replace_cover_albums

            p_data, f_plan, d_plan, t_plan, cov_plan, img_plan, adir_plan, emb_plan, stats, _, _ = run_scan_and_plan(
                self.data['roots'], opts
            )
            self.data.update({
                'preview': p_data, 'file_plan': f_plan, 'folder_plan': d_plan,
                'tag_plan': t_plan, 'cover_resize_plan': cov_plan,
                'image_rename_plan': img_plan, 'artwork_dir_rename_plan': adir_plan,
                'embed_cover_plan': emb_plan, 'stats': stats,
            })
            print(f"{Color.GREEN}[Web] Re-check complete. {len(p_data)} album(s) scanned.{Color.ENDC}")
            self._send_json(200, {'status': 'ok', 'preview': p_data, 'stats': stats})
            return

        if path_only == '/api/extract_cover':
            album_path = body.get('album_path', '')
            track_rel = body.get('track_rel')

            if not _is_safe_path(album_path, self.data.get('roots', [])):
                self._send_json(403, {'status': 'error', 'message': 'Forbidden: Album path is outside scanned roots'})
                return

            ok, msg, img_info = extract_cover_from_audio(album_path, track_rel)
            if not ok:
                self._send_json(400, {'status': 'error', 'message': msg})
                return

            if 'chosen_images' in body and isinstance(body['chosen_images'], dict):
                self.data['chosen_images'] = dict(body['chosen_images'])

            chosen = self.data.setdefault('chosen_images', {})
            chosen[album_path] = img_info['filename']

            opts = self.data['options'].copy()
            opts['interactive'] = False
            opts['_chosen_images'] = chosen

            p_data, f_plan, d_plan, t_plan, cov_plan, img_plan, adir_plan, emb_plan, stats, _, _ = run_scan_and_plan(
                self.data['roots'], opts
            )
            self.data.update({
                'preview': p_data, 'file_plan': f_plan, 'folder_plan': d_plan,
                'tag_plan': t_plan, 'cover_resize_plan': cov_plan,
                'image_rename_plan': img_plan, 'artwork_dir_rename_plan': adir_plan,
                'embed_cover_plan': emb_plan, 'stats': stats,
            })
            updated_album = next((a for a in p_data if a['album_path'] == album_path), None)
            print(f"  {Color.GREEN}[Web] {msg} for '{os.path.basename(album_path)}'{Color.ENDC}")
            self._send_json(200, {
                'status': 'ok',
                'message': msg,
                'image_info': img_info,
                'album': updated_album,
                'stats': stats,
            })
            return

        if path_only == '/api/fetch_cover':
            album_path = body.get('album_path', '')
            url = body.get('url', '')

            if not _is_safe_path(album_path, self.data.get('roots', [])):
                self._send_json(403, {'status': 'error', 'message': 'Forbidden: Album path is outside scanned roots'})
                return

            ok, msg, img_info = fetch_cover_from_url(album_path, url)
            if not ok:
                self._send_json(400, {'status': 'error', 'message': msg})
                return

            if 'chosen_images' in body and isinstance(body['chosen_images'], dict):
                self.data['chosen_images'] = dict(body['chosen_images'])

            chosen = self.data.setdefault('chosen_images', {})
            chosen[album_path] = img_info['filename']

            opts = self.data['options'].copy()
            opts['interactive'] = False
            opts['_chosen_images'] = chosen

            p_data, f_plan, d_plan, t_plan, cov_plan, img_plan, adir_plan, emb_plan, stats, _, _ = run_scan_and_plan(
                self.data['roots'], opts
            )
            self.data.update({
                'preview': p_data, 'file_plan': f_plan, 'folder_plan': d_plan,
                'tag_plan': t_plan, 'cover_resize_plan': cov_plan,
                'image_rename_plan': img_plan, 'artwork_dir_rename_plan': adir_plan,
                'embed_cover_plan': emb_plan, 'stats': stats,
            })
            updated_album = next((a for a in p_data if a['album_path'] == album_path), None)
            print(f"  {Color.GREEN}[Web] {msg} for '{os.path.basename(album_path)}'{Color.ENDC}")
            self._send_json(200, {
                'status': 'ok',
                'message': msg,
                'image_info': img_info,
                'album': updated_album,
                'stats': stats,
            })
            return

        if path_only == '/api/delete_image':
            album_path = body.get('album_path', '')
            image_key = body.get('image_key', '').strip()

            if not album_path or not image_key:
                self._send_json(400, {'status': 'error', 'message': 'Missing album_path or image_key'})
                return

            if not _is_safe_path(album_path, self.data.get('roots', [])):
                self._send_json(403, {'status': 'error', 'message': 'Forbidden: Album path is outside scanned roots'})
                return

            target_path = os.path.normpath(os.path.join(album_path, image_key))
            album_abs = os.path.abspath(album_path)
            target_abs = os.path.abspath(target_path)

            try:
                common = os.path.commonpath([album_abs, target_abs])
            except ValueError:
                common = ''

            if common != album_abs or not _is_safe_path(target_path, self.data.get('roots', [])):
                self._send_json(403, {'status': 'error', 'message': 'Forbidden: Image path is outside album directory'})
                return

            _, ext = os.path.splitext(target_path)
            if ext.lower() not in SUPPORTED_IMAGE_EXTENSIONS:
                self._send_json(400, {'status': 'error', 'message': f'Cannot delete non-image file: {ext}'})
                return

            if not os.path.isfile(target_path):
                self._send_json(404, {'status': 'error', 'message': f'File not found: {os.path.basename(target_path)}'})
                return

            try:
                os.remove(target_path)
            except OSError as exc:
                self._send_json(500, {'status': 'error', 'message': f'Failed to delete file: {exc}'})
                return

            parent = os.path.dirname(target_path)
            if parent != album_abs:
                try:
                    if not os.listdir(parent):
                        os.rmdir(parent)
                except OSError:
                    pass

            if 'chosen_images' in body and isinstance(body['chosen_images'], dict):
                self.data['chosen_images'] = dict(body['chosen_images'])

            chosen = self.data.setdefault('chosen_images', {})
            if chosen.get(album_path) == image_key:
                chosen.pop(album_path, None)

            opts = self.data['options'].copy()
            opts['interactive'] = False
            opts['_chosen_images'] = chosen

            p_data, f_plan, d_plan, t_plan, cov_plan, img_plan, adir_plan, emb_plan, stats, _, _ = run_scan_and_plan(
                self.data['roots'], opts
            )
            self.data.update({
                'preview': p_data, 'file_plan': f_plan, 'folder_plan': d_plan,
                'tag_plan': t_plan, 'cover_resize_plan': cov_plan,
                'image_rename_plan': img_plan, 'artwork_dir_rename_plan': adir_plan,
                'embed_cover_plan': emb_plan, 'stats': stats,
            })
            updated_album = next((a for a in p_data if a['album_path'] == album_path), None)
            msg = f"Deleted {os.path.basename(target_path)}"
            print(f"  {Color.GREEN}[Web] {msg} from '{os.path.basename(album_path)}'{Color.ENDC}")
            self._send_json(200, {
                'status': 'ok',
                'message': msg,
                'album': updated_album,
                'stats': stats,
            })
            return

        if path_only == '/api/embed_cover':
            album_path = body.get('album_path', '')
            image_key = body.get('image_key', '').strip()
            resize = body.get('resize', True)

            if not _is_safe_path(album_path, self.data.get('roots', [])):
                self._send_json(403, {'status': 'error', 'message': 'Forbidden: Album path is outside scanned roots'})
                return

            if not os.path.isdir(album_path):
                self._send_json(404, {'status': 'error', 'message': 'Album directory not found'})
                return

            # Determine image file on disk
            img_path = None
            if image_key:
                candidate = os.path.normpath(os.path.join(album_path, image_key))
                if os.path.isfile(candidate) and _is_safe_path(candidate, self.data.get('roots', [])):
                    img_path = candidate

            if not img_path:
                chosen = self.data.get('chosen_images', {}).get(album_path)
                if chosen:
                    candidate = os.path.normpath(os.path.join(album_path, chosen))
                    if os.path.isfile(candidate) and _is_safe_path(candidate, self.data.get('roots', [])):
                        img_path = candidate

            if not img_path:
                # Find folder.* or any image file in album
                candidates = []
                for f in sorted(os.listdir(album_path)):
                    cand = os.path.join(album_path, f)
                    if os.path.isfile(cand) and os.path.splitext(f)[1].lower() in SUPPORTED_IMAGE_EXTENSIONS:
                        candidates.append(cand)
                folder_cand = next((c for c in candidates if os.path.splitext(os.path.basename(c))[0].lower() == 'folder'), None)
                img_path = folder_cand if folder_cand else (candidates[0] if candidates else None)

            if not img_path or not os.path.isfile(img_path):
                self._send_json(400, {'status': 'error', 'message': 'No valid image file found to embed'})
                return

            # Find all audio files in the album
            audio_paths = []
            for root_d, _, files in os.walk(album_path):
                for f in sorted(files):
                    ext = os.path.splitext(f)[1].lower()
                    if ext in STANDARD_AUDIO_FORMATS:
                        audio_paths.append(os.path.join(root_d, f))

            if not audio_paths:
                self._send_json(400, {'status': 'error', 'message': 'No audio files found in album'})
                return

            max_s = COVER_MAX_SIZE if resize else None
            ok, fail = embed_file_cover_into_audio(img_path, audio_paths, max_size=max_s)

            if 'chosen_images' in body and isinstance(body['chosen_images'], dict):
                self.data['chosen_images'] = dict(body['chosen_images'])

            opts = self.data['options'].copy()
            opts['interactive'] = False
            opts['_chosen_images'] = self.data.get('chosen_images', {})
            if 'replace_cover' in body:
                opts['replace_cover'] = body['replace_cover']
            if 'replace_cover_albums' in body:
                opts['_replace_cover_albums'] = body['replace_cover_albums']

            p_data, f_plan, d_plan, t_plan, cov_plan, img_plan, adir_plan, emb_plan, stats, _, _ = run_scan_and_plan(
                self.data['roots'], opts
            )
            self.data.update({
                'preview': p_data, 'file_plan': f_plan, 'folder_plan': d_plan,
                'tag_plan': t_plan, 'cover_resize_plan': cov_plan,
                'image_rename_plan': img_plan, 'artwork_dir_rename_plan': adir_plan,
                'embed_cover_plan': emb_plan, 'stats': stats,
            })
            updated_album = next((a for a in p_data if a['album_path'] == album_path), None)
            msg = f"Embedded cover into {ok} track(s)" + (f" ({fail} failed)" if fail else "")
            print(f"  {Color.GREEN}[Web] {msg} for '{os.path.basename(album_path)}'{Color.ENDC}")
            self._send_json(200, {
                'status': 'ok',
                'message': msg,
                'album': updated_album,
                'stats': stats,
            })
            return

        if path_only == '/apply':
            resize_covers = body.get('resize_covers', False)
            embed_cover = body.get('embed_cover', False)
            replace_cover = body.get('replace_cover', False)
            replace_cover_albums = body.get('replace_cover_albums', [])
            strip_descriptions = body.get('strip_descriptions', False)
            chosen_images = body.get('chosen_images', {})

            opts = self.data['options'].copy()
            opts['interactive'] = False
            opts['_chosen_images'] = chosen_images
            opts['replace_cover'] = replace_cover
            opts['_replace_cover_albums'] = replace_cover_albums

            p_data, f_plan, d_plan, t_plan, cov_plan, img_plan, adir_plan, emb_plan, stats, _, _ = run_scan_and_plan(
                self.data['roots'], opts
            )

            self._send_json(200, {'status': 'ok'})
            print(f"\n{Color.CYAN}[Web] Applying changes...{Color.ENDC}")
            execute_changes(
                f_plan, d_plan, t_plan,
                cover_resize_plan=cov_plan,
                resize_covers=resize_covers,
                image_rename_plan=img_plan,
                artwork_dir_rename_plan=adir_plan,
                embed_cover_plan=emb_plan,
                embed_cover=embed_cover or replace_cover or bool(replace_cover_albums),
                strip_descriptions=strip_descriptions,
            )
            threading.Thread(target=self.server.shutdown).start()
            return

        if path_only == '/shutdown':
            self._send_json(200, {'status': 'ok'})
            print(f"\n{Color.YELLOW}[Web] Cancelled.{Color.ENDC}")
            threading.Thread(target=self.server.shutdown).start()
            return

        self.send_error(404, "Not Found")

    def generate_html(self):
        d = self.data
        preview_json = json.dumps(d.get('preview', []))
        stats_json = json.dumps(d.get('stats', {}))
        options_json = json.dumps(d.get('options', {}))
        cover_max_size = COVER_MAX_SIZE

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>aud-organize-library</title>
    <style>
        :root {{
            --bg: #0d1117;
            --surface: #161b22;
            --surface-hover: #1c2128;
            --border: #30363d;
            --border-subtle: #21262d;
            --text: #c9d1d9;
            --text-muted: #8b949e;
            --accent: #58a6ff;
            --success: #3fb950;
            --warning: #d29922;
            --danger: #f85149;
            --header-bg: rgba(13, 17, 23, 0.94);
        }}
        * {{ box-sizing: border-box; }}
        body {{
            background: var(--bg);
            color: var(--text);
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
            margin: 0;
            padding: 20px 24px 80px 24px;
            padding-top: 100px;
        }}
        .top-bar {{
            position: fixed;
            top: 0; left: 0; right: 0;
            background: var(--header-bg);
            backdrop-filter: blur(14px);
            -webkit-backdrop-filter: blur(14px);
            border-bottom: 1px solid var(--border);
            padding: 12px 24px;
            z-index: 100;
        }}
        .bar-row-1 {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 16px;
            flex-wrap: wrap;
        }}
        .brand-area {{
            display: flex;
            align-items: center;
            gap: 12px;
        }}
        .brand-title {{
            font-size: 1.05rem;
            font-weight: 700;
            letter-spacing: -0.02em;
            color: var(--text);
            font-family: monospace;
        }}
        .stats-strip {{
            display: flex;
            align-items: center;
            gap: 8px;
            flex-wrap: wrap;
        }}
        .stat-chip {{
            background: var(--surface);
            border: 1px solid var(--border);
            padding: 2px 10px;
            border-radius: 16px;
            font-size: 0.78rem;
            color: var(--text-muted);
            display: flex;
            align-items: center;
            gap: 6px;
        }}
        .stat-chip b {{ color: var(--text); }}
        .stat-chip.stat-changed b {{ color: var(--success); }}
        .stat-chip.stat-warn b {{ color: var(--warning); }}
        .stat-chip.stat-err b {{ color: var(--danger); }}

        .action-controls {{
            display: flex;
            align-items: center;
            gap: 10px;
        }}
        button {{
            border: 1px solid var(--border);
            background: var(--surface);
            color: var(--text);
            padding: 6px 14px;
            border-radius: 6px;
            font-weight: 600;
            font-size: 0.84rem;
            cursor: pointer;
            transition: background 0.15s, border-color 0.15s, color 0.15s;
            display: inline-flex;
            align-items: center;
            gap: 6px;
        }}
        button:hover {{ background: var(--surface-hover); border-color: var(--text-muted); }}
        button:active {{ transform: scale(0.98); }}
        .btn-go {{
            background: var(--success);
            color: #0d1117;
            border-color: var(--success);
            font-weight: 700;
        }}
        .btn-go:hover {{
            background: #2ea043;
            border-color: #2ea043;
            color: #0d1117;
        }}
        .btn-blue {{
            background: var(--accent);
            color: #0d1117;
            border-color: var(--accent);
            font-weight: 600;
        }}
        .btn-blue:hover {{
            background: #79b8ff;
            border-color: #79b8ff;
            color: #0d1117;
        }}
        .btn-cancel {{
            background: transparent;
            color: var(--danger);
            border-color: transparent;
        }}
        .btn-cancel:hover {{
            background: rgba(248, 81, 73, 0.1);
            border-color: var(--danger);
        }}
        .btn-subtle {{
            background: var(--surface);
            color: var(--text-muted);
            border: 1px solid var(--border);
            font-size: 0.78rem;
            padding: 4px 8px;
        }}
        .btn-subtle:hover {{ color: var(--text); border-color: var(--text-muted); }}
        .btn-xs {{
            padding: 2px 8px;
            font-size: 0.74rem;
            border-radius: 4px;
        }}

        .bar-row-2 {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 16px;
            margin-top: 10px;
            padding-top: 8px;
            border-top: 1px solid var(--border-subtle);
            flex-wrap: wrap;
        }}
        .filter-tabs {{
            display: flex;
            align-items: center;
            gap: 6px;
        }}
        .tab-btn {{
            background: transparent;
            border: 1px solid transparent;
            color: var(--text-muted);
            padding: 4px 10px;
            border-radius: 6px;
            font-size: 0.8rem;
            cursor: pointer;
        }}
        .tab-btn:hover {{ color: var(--text); background: var(--surface); }}
        .tab-btn.active {{
            background: var(--surface);
            border-color: var(--border);
            color: var(--text);
            font-weight: 600;
        }}
        .search-box {{
            flex: 1;
            max-width: 320px;
            position: relative;
        }}
        input[type="text"] {{
            background: #090d12;
            border: 1px solid var(--border);
            color: var(--text);
            padding: 5px 10px;
            border-radius: 6px;
            font-size: 0.82rem;
            outline: none;
            width: 100%;
        }}
        input[type="text"]:focus {{
            border-color: var(--accent);
            box-shadow: 0 0 0 2px rgba(88, 166, 255, 0.2);
        }}
        .options-group {{
            display: flex;
            align-items: center;
            gap: 14px;
            font-size: 0.8rem;
            color: var(--text-muted);
        }}
        .options-group label {{
            display: inline-flex;
            align-items: center;
            gap: 6px;
            cursor: pointer;
            user-select: none;
        }}
        input[type="checkbox"] {{
            accent-color: var(--accent);
            cursor: pointer;
            width: 15px;
            height: 15px;
        }}

        /* Grid & Cards */
        .cards-container {{
            max-width: 1400px;
            margin: 0 auto;
            display: flex;
            flex-direction: column;
            gap: 12px;
        }}
        .card {{
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 8px;
            overflow: hidden;
            transition: border-color 0.15s, box-shadow 0.15s;
        }}
        .card.changed {{
            border-left: 4px solid var(--success);
        }}
        .card.has-err {{
            border-left: 4px solid var(--danger);
        }}
        .card.has-warn {{
            border-left: 4px solid var(--warning);
        }}
        .card.clean {{
            opacity: 0.9;
        }}

        .card-header {{
            padding: 12px 18px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            cursor: pointer;
            background: var(--surface);
            user-select: none;
            gap: 16px;
        }}
        .card-header:hover {{
            background: var(--surface-hover);
        }}
        .card-title-area {{
            display: flex;
            align-items: center;
            gap: 12px;
            font-size: 0.96rem;
            font-weight: 500;
            flex: 1;
            min-width: 0;
        }}
        .status-badge {{
            font-size: 0.72rem;
            font-weight: 700;
            padding: 2px 7px;
            border-radius: 4px;
            font-family: monospace;
            letter-spacing: 0.04em;
        }}
        .status-badge.badge-changed {{
            background: rgba(63, 185, 80, 0.15);
            color: var(--success);
            border: 1px solid rgba(63, 185, 80, 0.3);
        }}
        .status-badge.badge-clean {{
            background: rgba(139, 148, 158, 0.1);
            color: var(--text-muted);
            border: 1px solid var(--border);
        }}
        .name-diff {{
            display: flex;
            align-items: center;
            gap: 8px;
            flex-wrap: wrap;
            word-break: break-word;
        }}
        .old-name {{
            text-decoration: line-through;
            color: var(--text-muted);
            font-size: 0.88rem;
        }}
        .rename-arrow {{
            color: var(--text-muted);
            font-size: 0.8rem;
            font-family: monospace;
        }}
        .new-name {{
            color: var(--text);
            font-weight: 600;
        }}
        .card-meta-area {{
            display: flex;
            align-items: center;
            gap: 8px;
            flex-wrap: wrap;
        }}
        .warn-badge {{
            font-size: 0.72rem;
            font-weight: 600;
            padding: 2px 6px;
            border-radius: 4px;
            font-family: monospace;
            letter-spacing: 0.03em;
        }}
        .warn-badge.badge-warn {{
            background: rgba(210, 153, 34, 0.15);
            color: var(--warning);
            border: 1px solid rgba(210, 153, 34, 0.3);
        }}
        .warn-badge.badge-err {{
            background: rgba(248, 81, 73, 0.15);
            color: var(--danger);
            border: 1px solid rgba(248, 81, 73, 0.3);
        }}
        .warn-badge.badge-info {{
            background: rgba(88, 166, 255, 0.15);
            color: var(--accent);
            border: 1px solid rgba(88, 166, 255, 0.3);
        }}
        .chevron-icon {{
            font-size: 0.8rem;
            color: var(--text-muted);
            font-family: monospace;
            padding-left: 6px;
        }}

        .card-body {{
            display: none;
            background: #090d12;
            border-top: 1px solid var(--border);
            padding: 16px 20px;
        }}

        /* Artwork Section */
        .section-box {{
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 6px;
            padding: 12px 16px;
            margin-bottom: 14px;
        }}
        .section-box:last-child {{ margin-bottom: 0; }}
        .section-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            font-size: 0.8rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: var(--text-muted);
            margin-bottom: 10px;
        }}
        .img-cards-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
            gap: 12px;
            margin-bottom: 12px;
        }}
        .img-item-card {{
            background: #090d12;
            border: 1px solid var(--border);
            border-radius: 6px;
            padding: 8px;
            display: flex;
            flex-direction: column;
            gap: 8px;
            position: relative;
            transition: border-color 0.15s;
        }}
        .img-item-card.is-chosen {{
            border-color: var(--success);
            box-shadow: 0 0 0 1px var(--success);
        }}
        .img-thumb-wrap {{
            width: 100%;
            height: 160px;
            background: #000;
            border-radius: 4px;
            overflow: hidden;
            display: flex;
            justify-content: center;
            align-items: center;
            cursor: pointer;
            position: relative;
        }}
        .img-thumb-wrap img {{
            max-width: 100%;
            max-height: 100%;
            object-fit: contain;
        }}
        .img-thumb-wrap:hover::after {{
            content: "View";
            position: absolute;
            background: rgba(0,0,0,0.65);
            color: #fff;
            padding: 4px 10px;
            border-radius: 4px;
            font-size: 0.75rem;
            font-weight: 600;
        }}
        .img-meta-row {{
            font-size: 0.76rem;
            font-family: monospace;
            color: var(--text-muted);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .img-title-text {{
            font-weight: 600;
            color: var(--text);
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }}
        .img-radio-label {{
            display: flex;
            align-items: center;
            gap: 6px;
            font-size: 0.8rem;
            cursor: pointer;
            user-select: none;
            margin-top: 4px;
        }}
        .img-radio-label input[type="radio"] {{
            accent-color: var(--success);
            margin: 0;
        }}
        .img-card-bottom {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 8px;
            margin-top: auto;
            padding-top: 4px;
        }}
        .img-card-bottom .img-radio-label {{
            margin-top: 0;
            flex: 1;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }}
        .btn-del {{
            color: var(--danger);
            border-color: rgba(248, 81, 73, 0.3);
            background: transparent;
            cursor: pointer;
        }}
        .btn-del:hover {{
            color: #ff7b72;
            background: rgba(248, 81, 73, 0.15);
            border-color: var(--danger);
        }}
        .leave-asis-option {{
            display: inline-flex;
            align-items: center;
            gap: 6px;
            font-size: 0.8rem;
            color: var(--text-muted);
            cursor: pointer;
            margin-top: 4px;
        }}
        .replace-embed-row {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 12px;
            background: rgba(56, 139, 253, 0.08);
            border: 1px solid rgba(56, 139, 253, 0.25);
            border-radius: 4px;
            padding: 8px 12px;
            margin-top: 10px;
            font-size: 0.82rem;
            flex-wrap: wrap;
        }}
        .replace-embed-label {{
            display: inline-flex;
            align-items: center;
            gap: 8px;
            cursor: pointer;
            user-select: none;
            color: var(--text);
            font-weight: 500;
        }}
        .replace-embed-label input[type="checkbox"] {{
            accent-color: var(--primary);
            margin: 0;
            cursor: pointer;
        }}

        .art-actions-bar {{
            display: flex;
            align-items: center;
            gap: 10px;
            flex-wrap: wrap;
            margin-top: 10px;
            padding-top: 10px;
            border-top: 1px solid var(--border-subtle);
        }}
        .url-fetch-group {{
            display: flex;
            align-items: center;
            gap: 6px;
            flex: 1;
            min-width: 280px;
        }}

        /* Embedded Track Cover Inspector */
        .collapsible-trigger {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            width: 100%;
            background: none;
            border: none;
            color: var(--text);
            font-size: 0.84rem;
            font-weight: 600;
            padding: 8px 0;
            cursor: pointer;
            text-align: left;
        }}
        .collapsible-trigger:hover {{
            color: var(--accent);
            background: none;
        }}
        .tracks-table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.8rem;
            margin-top: 8px;
        }}
        .tracks-table th {{
            text-align: left;
            padding: 6px 10px;
            background: #090d12;
            border-bottom: 1px solid var(--border);
            color: var(--text-muted);
            font-weight: 600;
            text-transform: uppercase;
            font-size: 0.72rem;
            letter-spacing: 0.04em;
        }}
        .tracks-table td {{
            padding: 6px 10px;
            border-bottom: 1px solid var(--border-subtle);
            vertical-align: middle;
            font-family: monospace;
        }}
        .tracks-table tr:hover td {{
            background: var(--surface-hover);
        }}
        .track-thumb-cell {{
            width: 50px;
            text-align: center;
        }}
        .track-thumb-cell img {{
            width: 36px;
            height: 36px;
            object-fit: cover;
            border-radius: 3px;
            border: 1px solid var(--border);
            cursor: pointer;
            display: block;
        }}
        .track-no-cover {{
            font-size: 0.72rem;
            color: var(--danger);
        }}

        /* Diff Styles */
        .diff-old {{
            text-decoration: line-through;
            color: var(--text-muted);
            opacity: 0.75;
        }}
        .diff-new {{
            color: var(--success);
            font-weight: 600;
        }}

        /* Toast Notifications */
        .toast-container {{
            position: fixed;
            bottom: 24px;
            right: 24px;
            display: flex;
            flex-direction: column;
            gap: 8px;
            z-index: 1000;
            pointer-events: none;
        }}
        .toast {{
            background: var(--surface);
            border: 1px solid var(--border);
            color: var(--text);
            padding: 10px 16px;
            border-radius: 6px;
            font-size: 0.85rem;
            box-shadow: 0 4px 16px rgba(0,0,0,0.4);
            pointer-events: auto;
            display: flex;
            align-items: center;
            gap: 8px;
            animation: slideIn 0.2s ease;
        }}
        .toast.success {{ border-left: 4px solid var(--success); }}
        .toast.error {{ border-left: 4px solid var(--danger); }}
        .toast.info {{ border-left: 4px solid var(--accent); }}
        @keyframes slideIn {{
            from {{ transform: translateX(20px); opacity: 0; }}
            to {{ transform: translateX(0); opacity: 1; }}
        }}

        /* Lightbox */
        .lightbox-overlay {{
            position: fixed;
            top: 0; left: 0; right: 0; bottom: 0;
            background: rgba(0, 0, 0, 0.85);
            display: none;
            justify-content: center;
            align-items: center;
            z-index: 2000;
            padding: 40px;
        }}
        .lightbox-modal {{
            max-width: 90vw;
            max-height: 90vh;
            display: flex;
            flex-direction: column;
            align-items: center;
            gap: 12px;
        }}
        .lightbox-modal img {{
            max-width: 85vw;
            max-height: 80vh;
            object-fit: contain;
            border: 1px solid var(--border);
            border-radius: 6px;
            background: #000;
        }}
        .lightbox-info {{
            color: var(--text-muted);
            font-size: 0.85rem;
            font-family: monospace;
        }}
    </style>
</head>
<body>
    <div class="top-bar">
        <div class="bar-row-1">
            <div class="brand-area">
                <span class="brand-title">aud-organize-library</span>
                <div class="stats-strip">
                    <span class="stat-chip">Total: <b id="statTotal">0</b></span>
                    <span class="stat-chip stat-changed">Changes: <b id="statChanges">0</b></span>
                    <span class="stat-chip stat-warn">Warnings: <b id="statWarnings">0</b></span>
                    <span class="stat-chip stat-warn">Missing Image: <b id="statMissing">0</b></span>
                    <span class="stat-chip stat-warn">Inconsistent Art: <b id="statInconsistent">0</b></span>
                </div>
            </div>
            <div class="action-controls">
                <button class="btn-subtle" onclick="expandAll(true)">Expand All</button>
                <button class="btn-subtle" onclick="expandAll(false)">Collapse All</button>
                <button onclick="triggerRecheck()">Re-check</button>
                <button class="btn-cancel" onclick="triggerCancel()">Cancel</button>
                <button class="btn-go" onclick="triggerApply()">PROCEED</button>
            </div>
        </div>
        <div class="bar-row-2">
            <div class="filter-tabs">
                <button class="tab-btn active" data-filter="all" onclick="setFilter('all')">All</button>
                <button class="tab-btn" data-filter="changes" onclick="setFilter('changes')">Changes Only</button>
                <button class="tab-btn" data-filter="warnings" onclick="setFilter('warnings')">Warnings</button>
                <button class="tab-btn" data-filter="images" onclick="setFilter('images')">Image Issues</button>
                <button class="tab-btn" data-filter="clean" onclick="setFilter('clean')">Clean</button>
            </div>
            <div class="search-box">
                <input type="text" id="searchInput" placeholder="Filter by album, artist, path, warning..." oninput="onSearchChange(this.value)">
            </div>
            <div class="options-group">
                <label title="Resize embedded covers larger than {cover_max_size}px (only for tags)">
                    <input type="checkbox" id="optResizeCovers" {'checked' if d.get('options', {}).get('cover_size', True) else ''}>
                    Resize Tags (&gt;{cover_max_size}px)
                </label>
                <label title="Embed selected main image into tracks missing embedded art">
                    <input type="checkbox" id="optEmbedCover" checked>
                    Embed Cover
                </label>
                <label title="Replace existing embedded cover art in tracks with album cover file">
                    <input type="checkbox" id="optReplaceCover" {'checked' if d.get('options', {}).get('replace_cover', False) else ''} onchange="toggleGlobalReplaceCover(this.checked)">
                    Replace Embedded Art
                </label>
                <label title="Strip description metadata from embedded covers">
                    <input type="checkbox" id="optStripDesc" {'checked' if d.get('options', {}).get('strip_desc', True) else ''}>
                    Strip Tag Descriptions
                </label>
            </div>
        </div>
    </div>

    <div class="cards-container" id="cardsContainer">
        <!-- Rendered by JavaScript -->
    </div>

    <div id="toastContainer" class="toast-container"></div>

    <div id="lightboxOverlay" class="lightbox-overlay" onclick="closeLightbox(event)">
        <div class="lightbox-modal">
            <img id="lightboxImg" src="" alt="Cover Fullview">
            <div class="lightbox-info" id="lightboxInfo"></div>
            <button class="btn-subtle" onclick="closeLightbox()">Close</button>
        </div>
    </div>

    <script>
        const INITIAL_DATA = {preview_json};
        const INITIAL_STATS = {stats_json};
        const INITIAL_OPTIONS = {options_json};

        let previewData = INITIAL_DATA;
        let statsData = INITIAL_STATS;
        let chosenImages = {{}};
        let replaceCoverGlobal = {'true' if d.get('options', {}).get('replace_cover', False) else 'false'};
        let replaceCoverAlbums = new Set();
        let activeFilter = 'all';
        let searchQuery = '';
        let openCards = new Set();
        let openTrackInspectors = new Set();
        let openFileRenames = new Set();

        // Pre-populate chosenImages and replaceCoverAlbums from scan data
        previewData.forEach(album => {{
            if (album.chosen_main_image) {{
                chosenImages[album.album_path] = album.chosen_main_image;
            }} else if (album.image_infos && album.image_infos.length === 1) {{
                const img = album.image_infos[0];
                const key = img.subdir ? `${{img.subdir}}/${{img.filename}}` : img.filename;
                chosenImages[album.album_path] = key;
                album.chosen_main_image = key;
            }}
            if (album.will_replace_embedded) {{
                replaceCoverAlbums.add(album.album_path);
            }}
        }});

        function showToast(msg, type = 'info') {{
            const container = document.getElementById('toastContainer');
            const toast = document.createElement('div');
            toast.className = 'toast ' + type;
            toast.textContent = msg;
            container.appendChild(toast);
            setTimeout(() => {{
                toast.style.opacity = '0';
                toast.style.transition = 'opacity 0.3s ease';
                setTimeout(() => toast.remove(), 300);
            }}, 3500);
        }}

        function recalcChangedStat() {{
            const count = previewData.filter(a => a.has_changes || replaceCoverGlobal || replaceCoverAlbums.has(a.album_path)).length;
            const el = document.getElementById('statChanges');
            if (el) el.textContent = count;
        }}

        function updateStats(stats) {{
            if (!stats) return;
            statsData = stats;
            document.getElementById('statTotal').textContent = stats.total_albums || previewData.length;
            recalcChangedStat();
            document.getElementById('statWarnings').textContent = stats.warning_albums || 0;
            document.getElementById('statMissing').textContent = stats.missing_image_albums || 0;
            document.getElementById('statInconsistent').textContent = stats.inconsistent_cover_albums || 0;
        }}

        function toggleGlobalReplaceCover(checked) {{
            replaceCoverGlobal = checked;
            renderCards();
            recalcChangedStat();
        }}

        function toggleAlbumReplaceCover(albumIdx, checked) {{
            const album = previewData[albumIdx];
            if (!album) return;
            if (checked) {{
                replaceCoverAlbums.add(album.album_path);
            }} else {{
                replaceCoverAlbums.delete(album.album_path);
            }}
            renderSingleCard(albumIdx);
            recalcChangedStat();
        }}

        function embedCoverToTracks(albumIdx) {{
            const album = previewData[albumIdx];
            if (!album) return;
            const resizeCovers = document.getElementById('optResizeCovers').checked;
            showToast('Embedding cover art into tracks...', 'info');
            fetch('/api/embed_cover', {{
                method: 'POST',
                headers: {{ 'Content-Type': 'application/json' }},
                body: JSON.stringify({{
                    album_path: album.album_path,
                    image_key: chosenImages[album.album_path] || album.chosen_main_image,
                    resize: resizeCovers,
                    chosen_images: chosenImages,
                    replace_cover: replaceCoverGlobal,
                    replace_cover_albums: Array.from(replaceCoverAlbums)
                }})
            }})
            .then(res => res.json())
            .then(data => {{
                if (data.status === 'ok') {{
                    showToast(data.message, 'success');
                    if (data.album) {{
                        previewData[albumIdx] = data.album;
                        if (data.stats) updateStats(data.stats);
                        renderSingleCard(albumIdx);
                    }}
                }} else {{
                    showToast(data.message || 'Failed to embed cover', 'error');
                }}
            }})
            .catch(err => {{
                showToast('Embedding error: ' + err, 'error');
            }});
        }}

        function setFilter(filterName) {{
            activeFilter = filterName;
            document.querySelectorAll('.tab-btn').forEach(btn => {{
                btn.classList.toggle('active', btn.dataset.filter === filterName);
            }});
            renderCards();
        }}

        function onSearchChange(val) {{
            searchQuery = val.trim().toLowerCase();
            renderCards();
        }}

        function expandAll(expand) {{
            if (expand) {{
                previewData.forEach((_, idx) => openCards.add(idx));
            }} else {{
                openCards.clear();
            }}
            renderCards();
        }}

        function toggleCard(idx) {{
            if (openCards.has(idx)) {{
                openCards.delete(idx);
            }} else {{
                openCards.add(idx);
            }}
            const body = document.getElementById('cardBody_' + idx);
            const chev = document.getElementById('cardChev_' + idx);
            if (body && chev) {{
                const isOpen = openCards.has(idx);
                body.style.display = isOpen ? 'block' : 'none';
                chev.textContent = isOpen ? '[-]' : '[+]';
            }}
        }}

        function toggleTrackInspector(idx, event) {{
            if (event) event.stopPropagation();
            if (openTrackInspectors.has(idx)) {{
                openTrackInspectors.delete(idx);
            }} else {{
                openTrackInspectors.add(idx);
            }}
            const el = document.getElementById('tracksBox_' + idx);
            if (el) {{
                el.style.display = openTrackInspectors.has(idx) ? 'block' : 'none';
            }}
        }}

        function toggleFileRenames(idx, event) {{
            if (event) event.stopPropagation();
            if (openFileRenames.has(idx)) {{
                openFileRenames.delete(idx);
            }} else {{
                openFileRenames.add(idx);
            }}
            const el = document.getElementById('renamesBox_' + idx);
            if (el) {{
                el.style.display = openFileRenames.has(idx) ? 'block' : 'none';
            }}
        }}

        function pickImage(albumIdx, value) {{
            const album = previewData[albumIdx];
            if (!album) return;

            if (value === '') {{
                delete chosenImages[album.album_path];
                album.chosen_main_image = null;
            }} else {{
                chosenImages[album.album_path] = value;
                album.chosen_main_image = value;
            }}

            // Remove [Main Image Not Selected] warning dynamically if selected
            if (value) {{
                album.warnings = album.warnings.filter(w => w !== '[Main Image Not Selected]');
            }}

            renderSingleCard(albumIdx);
        }}

        function extractCover(albumIdx, trackRel = null) {{
            const album = previewData[albumIdx];
            if (!album) return;

            showToast('Extracting cover to folder image...', 'info');
            fetch('/api/extract_cover', {{
                method: 'POST',
                headers: {{ 'Content-Type': 'application/json' }},
                body: JSON.stringify({{
                    album_path: album.album_path,
                    track_rel: trackRel,
                    chosen_images: chosenImages
                }})
            }})
            .then(res => res.json())
            .then(data => {{
                if (data.status === 'ok') {{
                    showToast(data.message, 'success');
                    if (data.album) {{
                        previewData[albumIdx] = data.album;
                        chosenImages[album.album_path] = data.image_info.filename;
                        if (data.stats) updateStats(data.stats);
                        renderSingleCard(albumIdx);
                    }}
                }} else {{
                    showToast(data.message || 'Failed to extract cover', 'error');
                }}
            }})
            .catch(err => {{
                showToast('Extraction error: ' + err, 'error');
            }});
        }}

        function fetchCover(albumIdx) {{
            const album = previewData[albumIdx];
            if (!album) return;
            const input = document.getElementById('urlInput_' + albumIdx);
            if (!input) return;
            const url = input.value.trim();
            if (!url) {{
                showToast('Please enter an image link.', 'error');
                return;
            }}

            showToast('Downloading cover image...', 'info');
            fetch('/api/fetch_cover', {{
                method: 'POST',
                headers: {{ 'Content-Type': 'application/json' }},
                body: JSON.stringify({{
                    album_path: album.album_path,
                    url: url,
                    chosen_images: chosenImages
                }})
            }})
            .then(res => res.json())
            .then(data => {{
                if (data.status === 'ok') {{
                    showToast(data.message, 'success');
                    input.value = '';
                    if (data.album) {{
                        previewData[albumIdx] = data.album;
                        chosenImages[album.album_path] = data.image_info.filename;
                        if (data.stats) updateStats(data.stats);
                        renderSingleCard(albumIdx);
                    }}
                }} else {{
                    showToast(data.message || 'Failed to download cover', 'error');
                }}
            }})
            .catch(err => {{
                showToast('Download error: ' + err, 'error');
            }});
        }}

        function deleteImage(albumIdx, imageKey, event) {{
            if (event) event.stopPropagation();
            const album = previewData[albumIdx];
            if (!album) return;

            showToast('Deleting image ' + imageKey + '...', 'info');
            fetch('/api/delete_image', {{
                method: 'POST',
                headers: {{ 'Content-Type': 'application/json' }},
                body: JSON.stringify({{
                    album_path: album.album_path,
                    image_key: imageKey,
                    chosen_images: chosenImages
                }})
            }})
            .then(res => res.json())
            .then(data => {{
                if (data.status === 'ok') {{
                    showToast(data.message, 'success');
                    if (chosenImages[album.album_path] === imageKey) {{
                        delete chosenImages[album.album_path];
                    }}
                    if (data.album) {{
                        previewData[albumIdx] = data.album;
                        if (data.stats) updateStats(data.stats);
                        renderSingleCard(albumIdx);
                    }}
                }} else {{
                    showToast(data.message || 'Failed to delete image', 'error');
                }}
            }})
            .catch(err => {{
                showToast('Delete error: ' + err, 'error');
            }});
        }}

        function triggerRecheck() {{
            showToast('Re-scanning audio folders...', 'info');
            fetch('/api/recheck', {{
                method: 'POST',
                headers: {{ 'Content-Type': 'application/json' }},
                body: JSON.stringify({{
                    chosen_images: chosenImages,
                    replace_cover: replaceCoverGlobal,
                    replace_cover_albums: Array.from(replaceCoverAlbums)
                }})
            }})
            .then(res => res.json())
            .then(data => {{
                if (data.status === 'ok') {{
                    previewData = data.preview;
                    if (data.stats) updateStats(data.stats);
                    showToast('Re-check complete: ' + previewData.length + ' albums.', 'success');
                    renderCards();
                }} else {{
                    showToast('Re-check failed: ' + (data.message || 'Unknown error'), 'error');
                }}
            }})
            .catch(err => {{
                showToast('Re-check error: ' + err, 'error');
            }});
        }}

        function triggerCancel() {{
            fetch('/shutdown', {{ method: 'POST' }})
            .then(() => {{
                document.body.innerHTML = "<div style='display:flex;justify-content:center;align-items:center;height:80vh;flex-direction:column;color:#8b949e;font-family:monospace'><h2>Cancelled</h2><p>Server stopped. You can close this window.</p></div>";
            }});
        }}

        function triggerApply() {{
            const resizeCovers = document.getElementById('optResizeCovers').checked;
            const embedCover = document.getElementById('optEmbedCover').checked;
            const stripDesc = document.getElementById('optStripDesc').checked;

            showToast('Applying changes to library...', 'info');
            fetch('/apply', {{
                method: 'POST',
                headers: {{ 'Content-Type': 'application/json' }},
                body: JSON.stringify({{
                    resize_covers: resizeCovers,
                    embed_cover: embedCover,
                    replace_cover: replaceCoverGlobal,
                    replace_cover_albums: Array.from(replaceCoverAlbums),
                    strip_descriptions: stripDesc,
                    chosen_images: chosenImages
                }})
            }})
            .then(res => res.json())
            .then(data => {{
                document.body.innerHTML = "<div style='display:flex;justify-content:center;align-items:center;height:80vh;flex-direction:column;color:#3fb950;font-family:monospace'><h2>Changes Applied Successfully</h2><p style='color:#8b949e'>Server shut down cleanly. You can close this tab.</p></div>";
            }})
            .catch(err => {{
                showToast('Error applying changes: ' + err, 'error');
            }});
        }}

        function openLightbox(src, infoText = '') {{
            const overlay = document.getElementById('lightboxOverlay');
            const img = document.getElementById('lightboxImg');
            const info = document.getElementById('lightboxInfo');
            img.src = src;
            info.textContent = infoText;
            overlay.style.display = 'flex';
        }}

        function closeLightbox(event) {{
            if (!event || event.target.id === 'lightboxOverlay' || event.target.tagName === 'BUTTON') {{
                document.getElementById('lightboxOverlay').style.display = 'none';
            }}
        }}

        document.addEventListener('keydown', (e) => {{
            if (e.key === 'Escape') {{
                closeLightbox();
            }}
        }});

        function formatBytes(bytes) {{
            if (!bytes || bytes <= 0) return '0 B';
            const units = ['B', 'KB', 'MB', 'GB'];
            let idx = 0;
            let val = bytes;
            while (val >= 1024 && idx < units.length - 1) {{
                val /= 1024;
                idx++;
            }}
            return val.toFixed(1) + ' ' + units[idx];
        }}

        function isCardMatching(album) {{
            const willReplace = replaceCoverGlobal || replaceCoverAlbums.has(album.album_path) || Boolean(album.will_replace_embedded);
            const isChanged = album.has_changes || willReplace;

            // Filter Tabs
            if (activeFilter === 'changes' && !isChanged) return false;
            if (activeFilter === 'warnings' && (!album.warnings || album.warnings.length === 0)) return false;
            if (activeFilter === 'images') {{
                const hasImgWarn = album.warnings.some(w =>
                    w.includes('[No Image]') ||
                    w.includes('[Main Image Not Selected]') ||
                    w.includes('[Inconsistent Covers]') ||
                    w.includes('[Missing Cover]') ||
                    w.includes('[Corrupt Image]')
                );
                if (!hasImgWarn) return false;
            }}
            if (activeFilter === 'clean' && (isChanged || (album.warnings && album.warnings.length > 0))) return false;

            // Search query
            if (searchQuery) {{
                const matchName = (album.original_name + ' ' + album.new_name + ' ' + album.album_path).toLowerCase();
                const matchWarn = album.warnings.join(' ').toLowerCase();
                const matchFiles = album.files.map(f => f.original + ' ' + f.new).join(' ').toLowerCase();
                if (!matchName.includes(searchQuery) && !matchWarn.includes(searchQuery) && !matchFiles.includes(searchQuery)) {{
                    return false;
                }}
            }}
            return true;
        }}

        function buildCardHtml(album, idx) {{
            const willReplace = replaceCoverGlobal || replaceCoverAlbums.has(album.album_path) || Boolean(album.will_replace_embedded);
            const willEmbed = willReplace || Boolean(album.will_embed_cover);
            const isChanged = album.has_changes || willReplace;
            const hasErr = album.warnings.some(w => w.includes('Error') || w.includes('Conflict') || w.includes('Corrupt') || w.includes('Gap') || w.includes('Duplicate'));
            const hasWarn = !hasErr && album.warnings.length > 0;
            const cardClass = isChanged ? 'card changed' : (hasErr ? 'card has-err' : (hasWarn ? 'card has-warn' : 'card clean'));

            // Header rename diff
            let nameHtml = '';
            if (album.original_name !== album.new_name) {{
                nameHtml = `<span class="old-name">${{escapeHtml(album.original_name)}}</span> <span class="rename-arrow">-&gt;</span> <span class="new-name">${{escapeHtml(album.new_name)}}</span>`;
            }} else {{
                nameHtml = `<span class="new-name">${{escapeHtml(album.new_name)}}</span>`;
            }}

            // Warning chips
            const warnChips = album.warnings.map(w => {{
                let cls = 'badge-warn';
                if (w.includes('Error') || w.includes('Conflict') || w.includes('Corrupt') || w.includes('Gap') || w.includes('Duplicate')) {{
                    cls = 'badge-err';
                }} else if (w.includes('Format')) {{
                    cls = 'badge-info';
                }}
                return `<span class="warn-badge ${{cls}}">${{escapeHtml(w)}}</span>`;
            }}).join('');

            const replaceBadge = willReplace ? `<span class="warn-badge badge-info" style="border-color: rgba(56, 139, 253, 0.4); color: #58a6ff; background: rgba(56, 139, 253, 0.15)">[Replace Art]</span>` : '';

            const isOpen = openCards.has(idx);

            // Artwork Section
            const imageInfos = album.image_infos || [];
            let chosenKey = chosenImages[album.album_path] || album.chosen_main_image;
            if (!chosenKey) {{
                if (imageInfos.length === 1) {{
                    const img = imageInfos[0];
                    chosenKey = img.subdir ? `${{img.subdir}}/${{img.filename}}` : img.filename;
                    chosenImages[album.album_path] = chosenKey;
                    album.chosen_main_image = chosenKey;
                }} else {{
                    // fallback to folder.* if present at root
                    const fImg = imageInfos.find(img => img.subdir === null && img.filename.toLowerCase().startsWith('folder.'));
                    if (fImg) chosenKey = fImg.filename;
                }}
            }}

            let imgCardsHtml = '';
            imageInfos.forEach(img => {{
                const key = img.subdir ? `${{img.subdir}}/${{img.filename}}` : img.filename;
                const isSelected = key === chosenKey;
                const encodedPath = encodeURIComponent(album.album_path + '/' + (img.subdir ? img.subdir + '/' : '') + img.filename);
                const thumbUrl = `/api/image?path=${{encodedPath}}&thumb=1`;
                const fullUrl = `/api/image?path=${{encodedPath}}`;
                const dimsText = (img.width && img.height) ? `${{img.width}}×${{img.height}}` : '';
                const sizeText = formatBytes(img.size_bytes);

                imgCardsHtml += `
                <div class="img-item-card ${{isSelected ? 'is-chosen' : ''}}">
                    <div class="img-thumb-wrap" onclick="openLightbox('${{fullUrl}}', '${{escapeHtml(key)}} (${{dimsText}} - ${{sizeText}})')">
                        <img src="${{thumbUrl}}" alt="${{escapeHtml(key)}}" loading="lazy" />
                    </div>
                    <div class="img-meta-row">
                        <span class="img-title-text" title="${{escapeHtml(key)}}">${{escapeHtml(key)}}</span>
                        <span>${{dimsText}}</span>
                    </div>
                    <div class="img-meta-row">
                        <span>${{sizeText}}</span>
                        <span style="color: ${{img.readable ? 'var(--text-muted)' : 'var(--danger)'}}">${{img.readable ? img.ext : '[Corrupt]'}}</span>
                    </div>
                    <div class="img-card-bottom">
                        <label class="img-radio-label">
                            <input type="radio" name="img_choice_${{idx}}" value="${{escapeHtml(key)}}" ${{isSelected ? 'checked' : ''}} onchange="pickImage(${{idx}}, this.value)">
                            <span>Main Image ${{isSelected ? '<b style="color:var(--success)">[Selected]</b>' : ''}}</span>
                        </label>
                        <button class="btn-subtle btn-xs btn-del" data-key="${{escapeHtml(key)}}" onclick="deleteImage(${{idx}}, this.getAttribute('data-key'), event)" title="Delete image file">Delete</button>
                    </div>
                </div>`;
            }});

            const replaceRowHtml = imageInfos.length > 0 ? `
            <div class="replace-embed-row">
                <label class="replace-embed-label" title="Replace existing embedded cover art in all tracks of this album with the selected image">
                    <input type="checkbox" onchange="toggleAlbumReplaceCover(${{idx}}, this.checked)" ${{willReplace ? 'checked' : ''}}>
                    <span>Replace embedded cover art in tracks with this image</span>
                </label>
                <button class="btn-subtle btn-xs" onclick="embedCoverToTracks(${{idx}})" title="Immediately write this cover into all audio files in this album">Embed to All Tracks Now</button>
            </div>` : '';

            const audioTracks = (album.files || []).filter(f => !f.is_image);
            const canExtract = album.has_embedded_cover || audioTracks.some(t => t.cover_count > 0);

            // Track Inspector Table
            let trackRowsHtml = '';
            audioTracks.forEach(t => {{
                const trackEncoded = encodeURIComponent(album.album_path + '/' + t.original);
                const trackThumbUrl = `/api/track_cover?path=${{trackEncoded}}&thumb=1`;
                const trackFullUrl = `/api/track_cover?path=${{trackEncoded}}`;
                const hasCover = t.cover_count > 0;
                const dims = (t.cover_w && t.cover_h) ? `${{t.cover_w}}×${{t.cover_h}}` : '-';
                const hashSnippet = t.cover_hash ? t.cover_hash.substring(0, 8) : '-';

                let statusBadge = '<span class="warn-badge badge-err">No Cover</span>';
                if (hasCover) {{
                    if (t.is_inconsistent) {{
                        statusBadge = '<span class="warn-badge badge-warn">Inconsistent</span>';
                    }} else {{
                        statusBadge = '<span class="warn-badge" style="background:rgba(63,185,80,0.15);color:var(--success);border:1px solid rgba(63,185,80,0.3)">Match</span>';
                    }}
                }}

                trackRowsHtml += `
                <tr>
                    <td style="width:50px">${{escapeHtml(t.track || '-')}}</td>
                    <td>${{escapeHtml(t.title || t.original)}}</td>
                    <td class="track-thumb-cell">
                        ${{hasCover ? `<img src="${{trackThumbUrl}}" onclick="openLightbox('${{trackFullUrl}}', 'Track: ${{escapeHtml(t.original)}} (${{dims}})')" alt="Cover" />` : '<span class="track-no-cover">-</span>'}}
                    </td>
                    <td>${{dims}}</td>
                    <td>${{hashSnippet}}</td>
                    <td>${{statusBadge}}</td>
                    <td>
                        ${{hasCover ? `<button class="btn-subtle btn-xs" onclick="extractCover(${{idx}}, '${{escapeHtml(t.original)}}')">Use as Album Cover</button>` : ''}}
                    </td>
                </tr>`;
            }});

            // File Renames Table
            let renameRowsHtml = '';
            (album.files || []).forEach(f => {{
                const isImg = Boolean(f.is_image);
                const isChangedFile = f.original !== f.new;
                renameRowsHtml += `
                <tr>
                    <td style="width:60px;color:var(--text-muted)">${{isImg ? '[IMG]' : '[AUDIO]'}}</td>
                    <td><span class="${{isChangedFile ? 'diff-old' : ''}}">${{escapeHtml(f.original)}}</span></td>
                    <td><span class="${{isChangedFile ? 'diff-new' : ''}}">${{escapeHtml(f.new)}}</span></td>
                </tr>`;
            }});

            const isTrackInspOpen = openTrackInspectors.has(idx);
            const isFileRenamesOpen = openFileRenames.has(idx);

            return `
            <div class="${{cardClass}}" id="card_${{idx}}">
                <div class="card-header" onclick="toggleCard(${{idx}})">
                    <div class="card-title-area">
                        <span class="status-badge ${{isChanged ? 'badge-changed' : 'badge-clean'}}">${{isChanged ? '[CHANGED]' : '[CLEAN]'}}</span>
                        <div class="name-diff">${{nameHtml}}</div>
                    </div>
                    <div class="card-meta-area">
                        ${{replaceBadge}}
                        ${{warnChips}}
                        <span class="chevron-icon" id="cardChev_${{idx}}">${{isOpen ? '[-]' : '[+]'}}</span>
                    </div>
                </div>
                <div class="card-body" id="cardBody_${{idx}}" style="display: ${{isOpen ? 'block' : 'none'}}">
                    <div class="section-box">
                        <div class="section-header">
                            <span>Folder Artwork (${{imageInfos.length}} files)</span>
                        </div>
                        <div class="img-cards-grid">
                            ${{imgCardsHtml || '<div style="grid-column: 1/-1; color: var(--text-muted); font-size: 0.85rem; padding: 10px 0;">No image files found on disk in this album folder.</div>'}}
                        </div>
                        ${{imageInfos.length > 1 ? `
                        <label class="leave-asis-option">
                            <input type="radio" name="img_choice_${{idx}}" value="" ${{!chosenKey ? 'checked' : ''}} onchange="pickImage(${{idx}}, '')">
                            <span>Leave images as-is (do not designate main cover)</span>
                        </label>` : ''}}

                        ${{replaceRowHtml}}

                        <div class="art-actions-bar">
                            ${{canExtract ? `
                            <button class="btn-subtle" onclick="extractCover(${{idx}})">
                                Extract Cover to folder.jpg
                            </button>` : ''}}
                            <div class="url-fetch-group">
                                <input type="text" id="urlInput_${{idx}}" placeholder="Paste Bandcamp, RYM, or image URL..." onkeydown="if(event.key==='Enter') fetchCover(${{idx}})">
                                <button class="btn-blue btn-xs" style="padding: 5px 12px;" onclick="fetchCover(${{idx}})">Download &amp; Set</button>
                            </div>
                        </div>
                    </div>

                    <div class="section-box">
                        <button class="collapsible-trigger" onclick="toggleTrackInspector(${{idx}}, event)">
                            <span>Track Covers (${{audioTracks.length}} tracks)</span>
                            <span style="font-size:0.75rem; color:var(--text-muted)">${{isTrackInspOpen ? '[-]' : '[+]'}}</span>
                        </button>
                        <div id="tracksBox_${{idx}}" style="display: ${{isTrackInspOpen ? 'block' : 'none'}}">
                            <table class="tracks-table">
                                <thead>
                                    <tr>
                                        <th>Track</th>
                                        <th>Title</th>
                                        <th>Art</th>
                                        <th>Dimensions</th>
                                        <th>Hash</th>
                                        <th>Status</th>
                                        <th>Action</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    ${{trackRowsHtml}}
                                </tbody>
                            </table>
                        </div>
                    </div>

                    <div class="section-box">
                        <button class="collapsible-trigger" onclick="toggleFileRenames(${{idx}}, event)">
                            <span>File Renames (${{album.files.length}} files)</span>
                            <span style="font-size:0.75rem; color:var(--text-muted)">${{isFileRenamesOpen ? '[-]' : '[+]'}}</span>
                        </button>
                        <div id="renamesBox_${{idx}}" style="display: ${{isFileRenamesOpen ? 'block' : 'none'}}">
                            <table class="tracks-table">
                                <thead>
                                    <tr>
                                        <th>Type</th>
                                        <th>Original Filename</th>
                                        <th>Proposed Filename</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    ${{renameRowsHtml}}
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>`;
        }}

        function renderSingleCard(idx) {{
            const cardEl = document.getElementById('card_' + idx);
            if (!cardEl) return;
            const album = previewData[idx];
            if (!album) return;
            cardEl.outerHTML = buildCardHtml(album, idx);
        }}

        function renderCards() {{
            const container = document.getElementById('cardsContainer');
            let html = '';
            let visibleCount = 0;
            previewData.forEach((album, idx) => {{
                if (isCardMatching(album)) {{
                    html += buildCardHtml(album, idx);
                    visibleCount++;
                }}
            }});

            if (visibleCount === 0) {{
                html = '<div style="text-align:center; padding: 60px 0; color: var(--text-muted); font-size: 0.95rem;">No albums match the current filter or search criteria.</div>';
            }}
            container.innerHTML = html;
        }}

        function escapeHtml(str) {{
            if (!str) return '';
            return String(str)
                .replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;')
                .replace(/"/g, '&quot;')
                .replace(/'/g, '&#39;');
        }}

        // Initial setup
        updateStats(INITIAL_STATS);
        // By default, expand albums that have warnings or changes
        previewData.forEach((album, idx) => {{
            if (album.has_changes || (album.warnings && album.warnings.length > 0)) {{
                openCards.add(idx);
            }}
        }});
        renderCards();
    </script>
</body>
</html>"""

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

    if not f_plan and not d_plan and not img_plan and not adir_plan and not emb_plan and not cov_plan and not warns and not alb_warns:
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
                        embed_cover=True,
                        strip_descriptions=kwargs.get('strip_desc', False))

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
  aud-organize-library                          # current directory (interactive web preview)
  aud-organize-library ~/Music/                 # preview specific directory
  aud-organize-library --list                   # pick subdirs interactively
  aud-organize-library --list ~/Music/          # pick from ~/Music subdirs
  aud-organize-library --no-preview             # run in terminal CLI mode without web preview
  aud-organize-library -c ~/Music/              # check only in terminal, no changes
  aud-organize-library -y ~/Music/              # auto-confirm everything in terminal mode
  aud-organize-library --folder-only ~/Music/   # skip file renaming
  aud-organize-library --cover-size ~/Music/    # resize embedded covers larger than 1000px
  aud-organize-library --replace-cover ~/Music/ # replace existing embedded cover art in tracks
  aud-organize-library --no-strip-desc ~/Music/ # preserve cover art description tags
        """,
    )
    parser.add_argument("folder",        nargs="?", default=None,        help="Folder to scan (or base for --list; default: current directory)")
    parser.add_argument("--list",             action="store_true", help="List subdirectories and interactively select which to process")
    parser.add_argument("-p", "--preview",    dest="preview",    action="store_true",  default=True, help="Open interactive web preview (default: enabled)")
    parser.add_argument("--no-preview",       dest="preview",    action="store_false",               help="Run in terminal CLI mode without opening web preview")
    parser.add_argument("-c", "--check",       action="store_true", help="Check only, make no changes")
    parser.add_argument("-y", "--force-yes",   action="store_true", help="Auto-confirm all prompts")
    parser.add_argument("-n", "--force-no",    action="store_true", help="Auto-decline all prompts")
    parser.add_argument("--folder-only",       action="store_true", help="Rename folders only, skip file renaming")
    parser.add_argument("--cover-size",       dest="cover_size", action="store_true",  default=True, help=f"Resize embedded covers larger than {COVER_MAX_SIZE}px (default: enabled)")
    parser.add_argument("--no-cover-size",    dest="cover_size", action="store_false",               help="Do not resize oversized embedded covers")
    parser.add_argument("--replace-cover",    dest="replace_cover", action="store_true",  default=False, help="Replace existing embedded cover art in tracks with the album cover image file")
    parser.add_argument("--no-replace-cover", dest="replace_cover", action="store_false",               help="Do not replace existing embedded cover art")
    parser.add_argument("--strip-desc",       dest="strip_desc", action="store_true",  default=True, help="Remove description metadata from embedded cover art (default: enabled)")
    parser.add_argument("--no-strip-desc",    dest="strip_desc", action="store_false",               help="Preserve description metadata on embedded cover art")
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

    cli_check = args.check and ('-p' not in sys.argv and '--preview' not in sys.argv)
    preview_mode = False if cli_check else args.preview

    organize_music_folders(
        roots,
        check_only    = args.check,
        force_yes     = args.force_yes,
        force_no      = args.force_no,
        preview_mode  = preview_mode,
        folder_only   = args.folder_only,
        cover_size    = args.cover_size,
        replace_cover = args.replace_cover,
        strip_desc    = args.strip_desc,
        interactive   = True,
    )


if __name__ == "__main__":
    main()
