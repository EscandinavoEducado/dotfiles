#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "mutagen",
#   "langdetect",
#   "titlecase",
# ]
# ///
"""
aud-fix-metadata — Batch capitalize, format, and trim audio file metadata.

Usage:
    aud-fix-metadata                        # comprehensive scan of current dir (Title, Album, Artist; all languages; web preview)
    aud-fix-metadata ~/Music/Artist           # scan specific directory
    aud-fix-metadata --list ~/Music/          # pick subdirectories interactively
    aud-fix-metadata --dry-run                # preview proposed changes in terminal without modifying files
    aud-fix-metadata --no-preview             # run in terminal mode without launching web browser
    aud-fix-metadata --undo                   # revert the most recent batch modification
    aud-fix-metadata --clear-cache            # purge saved drafts and undo logs
    aud-fix-metadata --no-save-cache          # do not save session draft cache
"""

import os
import sys
import json
import re
import html
import shutil
import socket
import hashlib
import threading
import webbrowser
import argparse
import unicodedata
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from collections import defaultdict

try:
    from mutagen import File as MutagenFile
    from langdetect import detect, DetectorFactory
    from titlecase import titlecase
except ImportError:
    print("Error: Missing dependencies.")
    print("Please run: pip install mutagen langdetect titlecase")
    sys.exit(1)

SUPPORTED_EXTENSIONS = ('.mp3', '.flac', '.m4a', '.ogg', '.opus', '.wav')
DetectorFactory.seed = 0

CACHE_DIR = os.path.expanduser("~/.cache/aud-fix-metadata")

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
    DIM    = '\033[2m'


def get_subdirs(base):
    """Return sorted list of immediate (non-hidden) subdirectories of base."""
    try:
        return sorted(
            (p for p in os.scandir(base)
             if p.is_dir() and not p.name.startswith(".")),
            key=lambda e: e.name.lower(),
        )
    except PermissionError:
        print(f"{Color.RED}Error: Permission denied: {base}{Color.ENDC}")
        return []


def parse_selection(raw, max_idx):
    """Parse selection string like '1 3 5-8' into 0-based indices."""
    indices = set()
    for token in raw.replace(",", " ").split():
        if "-" in token:
            parts = token.split("-", 1)
            try:
                lo, hi = int(parts[0]), int(parts[1])
            except ValueError:
                print(f"{Color.RED}  Error: Invalid range: {token}{Color.ENDC}")
                return []
            if lo < 1 or hi > max_idx or lo > hi:
                print(f"{Color.RED}  Error: Range {token} out of bounds (1–{max_idx}){Color.ENDC}")
                return []
            indices.update(range(lo - 1, hi))
        else:
            try:
                n = int(token)
            except ValueError:
                print(f"{Color.RED}  Error: Not a number: {token}{Color.ENDC}")
                return []
            if n < 1 or n > max_idx:
                print(f"{Color.RED}  Error: Number {n} out of bounds (1–{max_idx}){Color.ENDC}")
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
            label = f"{Color.CYAN}{idx + 1:>{num_w}}{Color.ENDC}  {Color.BOLD}{entries[idx].name}{Color.ENDC}"
            pad = col_w - (num_w + 2 + len(entries[idx].name))
            line += label + " " * pad
        print("  " + line)


def list_and_select(base):
    """Show numbered grid of subdirectories and prompt for selection."""
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
            print(f"  {Color.GREEN}Selected {Color.BOLD}{len(chosen)}{Color.ENDC} {label}:")
            for d in chosen:
                print(f"    {Color.CYAN}-{Color.ENDC} {d}")
            print()
            return chosen


def capitalize_roman_numerals(text):
    ROMAN_EXCEPTIONS = {'mix', 'dim', 'mid', 'did', 'lid'}
    candidate_pattern = r"\b(?<!')[mdclxvi]+(?!')\b"
    validator = re.compile(r"^M{0,3}(CM|CD|D?C{0,3})?(XC|XL|L?X{0,3})?(IX|IV|V?I{0,3})?$", re.VERBOSE | re.IGNORECASE)

    def replacer(match):
        word = match.group(0)
        if word.lower() in ROMAN_EXCEPTIONS:
            return word
        if validator.match(word):
            return word.upper()
        return word

    return re.sub(candidate_pattern, replacer, text, flags=re.IGNORECASE)


def titlecase_callback(word, all_caps=False, **kwargs):
    w_lower = word.lower().rstrip('.')
    if w_lower in {'dj', 'mc', 'ep', 'lp', 'vip', 'ost', 'bgm', 'b2b'}:
        return w_lower.upper()
    if w_lower in {'feat', 'ft'}:
        return 'feat.'

    # Handle elision prefixes like l' and d' (e.g. L'incertitude, l'amour, d'accord, D'or)
    # Prevents titlecase's APOS_SECOND rule from converting L'incertitude -> l'Incertitude -> L'Incertitude
    m = re.match(r"^([(\[{'\"“‘]*)((?i:[ld])['’])([^\W\d_].*?)([)\]}'\"”’]*)$", word)
    if m:
        lead, prefix, rest, trail = m.groups()
        if all_caps:
            return lead + prefix[0].upper() + prefix[1] + rest.lower() + trail
        return lead + prefix + rest + trail

    return None


def detect_whitespace_issues(text):
    warnings = []
    if text.startswith(' ') or text.startswith('\t'):
        warnings.append("Leading whitespace")
    if text.endswith(' ') or text.endswith('\t'):
        warnings.append("Trailing whitespace")
    if re.search(r'[ \t]{2,}', text):
        warnings.append("Multiple consecutive spaces")
    if '\n' in text or '\r' in text:
        warnings.append("Line break in tag")
    return warnings


def to_sentence_case(text):
    if not text:
        return text
    cleaned = re.sub(r'[ \t]+', ' ', text.strip())
    lower = cleaned.lower()
    res = lower[:1].upper() + lower[1:]
    # Capitalize after sentence-ending punctuation followed by space
    res = re.sub(r'([.!?:]\s+)([a-z])', lambda m: m.group(1) + m.group(2).upper(), res)
    # Capitalize after open brackets/parentheses
    res = re.sub(r'([(\[{]\s*)([a-z])', lambda m: m.group(1) + m.group(2).upper(), res)
    # Capitalize after slashes or dashes
    res = re.sub(r'([/\\–—]\s*)([a-z])', lambda m: m.group(1) + m.group(2).upper(), res)
    # Normalize feat./ft.
    res = re.sub(r'\b(?i:feat|ft)\b\.?', 'feat.', res)
    res = re.sub(r'\.{2,}', '.', res)
    # Roman numerals
    res = capitalize_roman_numerals(res)
    return res


def has_cover_art_description(audio):
    """Check if audio has any non-empty cover art description."""
    if hasattr(audio, 'tags') and audio.tags is not None:
        if hasattr(audio.tags, 'getall'):
            for apic in audio.tags.getall('APIC'):
                if getattr(apic, 'desc', '') != '':
                    return True
        for key in audio.tags.keys():
            if key.lower() in ('coverartdescription', 'cover_art_description', 'picture_description'):
                if audio.tags[key]:
                    return True
        if 'metadata_block_picture' in audio.tags:
            import base64
            from mutagen.flac import Picture
            for block in audio.tags['metadata_block_picture']:
                try:
                    pic = Picture(base64.b64decode(block))
                    if getattr(pic, 'desc', '') != '':
                        return True
                except Exception:
                    pass
    if hasattr(audio, 'pictures'):
        for pic in audio.pictures:
            if getattr(pic, 'desc', '') != '':
                return True
    return False


def clean_cover_art_descriptions(audio):
    """
    Ensure cover art / picture description metadata is removed (set to empty string),
    across ID3, FLAC, Vorbis (Ogg/Opus), and other container formats.
    Returns True if any description was stripped.
    """
    modified = False

    # 1. ID3 (MP3, WAV, AIFF) - APIC frames
    if hasattr(audio, 'tags') and audio.tags is not None:
        if hasattr(audio.tags, 'getall'):
            for apic in audio.tags.getall('APIC'):
                if getattr(apic, 'desc', '') != '':
                    apic.desc = ''
                    modified = True

    # 2. FLAC - audio.pictures list of mutagen.flac.Picture
    if hasattr(audio, 'pictures'):
        for pic in audio.pictures:
            if getattr(pic, 'desc', '') != '':
                pic.desc = ''
                modified = True

    # 3. Vorbis Comments (FLAC vorbis, OGG Vorbis, Opus)
    if hasattr(audio, 'tags') and audio.tags is not None:
        for key in list(audio.tags.keys()):
            if key.lower() in ('coverartdescription', 'cover_art_description', 'picture_description'):
                del audio.tags[key]
                modified = True

        if 'metadata_block_picture' in audio.tags:
            import base64
            from mutagen.flac import Picture
            new_blocks = []
            for block in audio.tags['metadata_block_picture']:
                try:
                    data = base64.b64decode(block)
                    pic = Picture(data)
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

    return modified


def smart_format_text(text, ignore_language_filter=True, case_mode='title'):
    if not text:
        return text, False, []

    text = unicodedata.normalize('NFC', text)
    warnings = detect_whitespace_issues(text)
    cleaned_text = text.strip()

    # Normalize double quotes & curly quotes
    cleaned_text = re.sub(r'[‘’]', "'", cleaned_text)
    cleaned_text = re.sub(r'[“”]', '"', cleaned_text)
    # Collapse multiple spaces inside tag
    cleaned_text = re.sub(r'[ \t]+', ' ', cleaned_text)

    if cleaned_text.lower() in ('untitled', '[untitled]'):
        return '[untitled]', True, warnings

    if case_mode == 'sentence':
        formatted = to_sentence_case(cleaned_text)
        return formatted, True, warnings

    try:
        is_english = True
        if not ignore_language_filter and len(cleaned_text.split()) > 2:
            try:
                lang = detect(cleaned_text)
                if lang != 'en':
                    is_english = False
            except Exception:
                pass

        should_process = is_english or ignore_language_filter

        if should_process:
            formatted = titlecase(cleaned_text, callback=titlecase_callback)

            if formatted:
                formatted = re.sub(r'^([^\w]*)([a-z])', lambda m: m.group(1) + m.group(2).upper(), formatted)

            # Capitalize letter after slash, backslash, dashes, or colons
            def fix_separator_capitalization(m):
                return m.group(1) + m.group(2) + m.group(3).upper()

            formatted = re.sub(r'([/\\–—:])(\s*)([a-z])', fix_separator_capitalization, formatted)

            # Capitalize inside brackets/parentheses: (live at wembley) -> (Live at Wembley)
            def fix_bracket_capitalization(m):
                return m.group(1) + m.group(2).upper()

            formatted = re.sub(r'([(\[{]\s*)([a-z])', fix_bracket_capitalization, formatted)

            # Ensure the last word is capitalized (English title case rule: first and last words always capitalized)
            formatted = re.sub(r'\b([a-z])([a-zA-Z]*)([^\w]*)$', lambda m: m.group(1).upper() + m.group(2) + m.group(3), formatted)

            # Normalize feature artist: feat. or ft.
            formatted = re.sub(r'\b(?i:feat|ft)\b\.?', 'feat.', formatted)
            formatted = re.sub(r'\.{2,}', '.', formatted)

            # Roman numerals
            formatted = capitalize_roman_numerals(formatted)
            return formatted, is_english, warnings

        return cleaned_text, is_english, warnings
    except Exception:
        return cleaned_text, True, warnings


class TitleProcessor:
    def __init__(self, root_folders, check_title=True, check_album=True, check_artist=True, ignore_lang=True, case_mode='title'):
        if isinstance(root_folders, (str, bytes)):
            root_folders = [root_folders]
        self.root_folders = [os.path.abspath(f) for f in root_folders]
        self.check_title  = check_title
        self.check_album  = check_album
        self.check_artist = check_artist
        self.ignore_lang  = ignore_lang
        self.case_mode    = case_mode

        self.groups = {
            'Title':  {},
            'Album':  {},
            'Artist': {},
        }
        self.files_with_cover_desc = set()
        self.proposals = []

    def scan(self):
        self.groups = {
            'Title':  {},
            'Album':  {},
            'Artist': {},
        }
        self.files_with_cover_desc = set()

        for folder in self.root_folders:
            print(f"{Color.BLUE}Scanning '{folder}'...{Color.ENDC}")
            for dirpath, _, filenames in os.walk(folder):
                for f in filenames:
                    if f.lower().endswith(SUPPORTED_EXTENSIONS):
                        path = os.path.join(dirpath, f)
                        try:
                            audio = MutagenFile(path, easy=True)
                            if not audio:
                                continue

                            def get_tag(tag_name):
                                val = audio.get(tag_name, [None])[0]
                                if val:
                                    return unicodedata.normalize('NFC', val)
                                return None

                            def add_to_group(tag_type, raw_val):
                                if not raw_val:
                                    return
                                nfc_val = unicodedata.normalize('NFC', raw_val)
                                norm_key = nfc_val.strip().casefold()
                                if norm_key not in self.groups[tag_type]:
                                    self.groups[tag_type][norm_key] = {
                                        'canonical': nfc_val,
                                        'files': [path],
                                        'raw_variants': {raw_val}
                                    }
                                else:
                                    self.groups[tag_type][norm_key]['files'].append(path)
                                    self.groups[tag_type][norm_key]['raw_variants'].add(raw_val)
                                    current_can = self.groups[tag_type][norm_key]['canonical']
                                    if current_can != current_can.strip() and nfc_val == nfc_val.strip():
                                        self.groups[tag_type][norm_key]['canonical'] = nfc_val
                                    elif current_can.islower() and not nfc_val.islower():
                                        self.groups[tag_type][norm_key]['canonical'] = nfc_val

                            if self.check_title:
                                add_to_group('Title', get_tag('title'))

                            if self.check_album:
                                add_to_group('Album', get_tag('album'))

                            if self.check_artist:
                                add_to_group('Artist', get_tag('artist'))

                            # Check for cover art description
                            try:
                                full_audio = MutagenFile(path)
                                if full_audio and has_cover_art_description(full_audio):
                                    self.files_with_cover_desc.add(path)
                            except Exception:
                                pass

                        except Exception as e:
                            print(f"{Color.RED}Error reading {f}: {e}{Color.ENDC}")

    def generate_proposals(self, previous_state=None, case_mode=None):
        if case_mode:
            self.case_mode = case_mode
        print(f"{Color.CYAN}Analyzing metadata ({self.case_mode} case) and checking for whitespace/formatting...{Color.ENDC}")
        self.proposals = []

        prev_map = {}
        if previous_state:
            for item in previous_state:
                prev_map[item['id']] = item

        for tag_type, group_dict in self.groups.items():
            for norm_key, group_info in group_dict.items():
                original = group_info['canonical']
                file_paths = group_info['files']
                raw_variants = group_info.get('raw_variants', set())
                new_text, is_english, warnings = smart_format_text(original, self.ignore_lang, case_mode=self.case_mode)

                if len(raw_variants) > 1 and "Inconsistent tags in group" not in warnings:
                    warnings.append("Inconsistent tags in group")
                if any(f in self.files_with_cover_desc for f in file_paths) and "Cover description" not in warnings:
                    warnings.append("Cover description")

                item_id = hashlib.sha256(f"{tag_type}:{norm_key}".encode('utf-8')).hexdigest()[:16]

                prev = prev_map.get(item_id)
                if prev and prev.get('is_manually_edited'):
                    current_text = prev.get('current', prev.get('new', new_text))
                    is_edited = (current_text != original and current_text != new_text)
                    is_apply = prev.get('apply', (current_text != original) or bool(warnings))
                else:
                    current_text = new_text
                    is_edited = False
                    is_apply = (new_text != original) or bool(warnings)

                self.proposals.append({
                    'id': item_id,
                    'type': tag_type,
                    'original': original,
                    'proposed': new_text,
                    'current': current_text,
                    'files': file_paths,
                    'count': len(file_paths),
                    'is_english': is_english,
                    'is_manually_edited': is_edited,
                    'apply': is_apply,
                    'warnings': warnings,
                })

        self.proposals.sort(key=lambda x: (x['type'], x['original']))
        return len(self.proposals)

    def apply_changes(self, change_list, no_save_cache=False):
        applied_count = 0
        error_count = 0
        undo_entries = []
        touched_files = set()

        print(f"\n{Color.HEADER}--- Applying Changes ---{Color.ENDC}")

        for item in change_list:
            if not item.get('apply'):
                continue

            original = item['original']
            new_text = item.get('current', item.get('new', '')).strip()
            tag_type = item.get('type')
            paths = item.get('files', [])

            if not new_text:
                continue

            group_file_modified = False
            for path in paths:
                try:
                    audio = MutagenFile(path, easy=True)
                    if not audio:
                        continue
                    current_val = audio.get(tag_type.lower(), [None])[0]
                    # Update if file's tag differs from new_text in content, casing, or Unicode normalization
                    if current_val != new_text:
                        audio[tag_type.lower()] = new_text
                        audio.save()
                        touched_files.add(path)
                        group_file_modified = True

                        undo_entries.append({
                            'path': path,
                            'tag': tag_type.lower(),
                            'old': current_val if current_val is not None else original,
                            'new': new_text,
                        })
                except Exception as e:
                    error_count += 1
                    print(f"{Color.RED}Failed to update {os.path.basename(path)}: {e}{Color.ENDC}")

            if group_file_modified:
                applied_count += 1
                print(f"[{Color.GREEN}OK{Color.ENDC}] {tag_type}: '{original}' -> '{Color.BOLD}{new_text}{Color.ENDC}' ({len(paths)} files)")

        # Always clean cover art descriptions across all touched files AND any scanned file with cover desc
        all_cover_clean_files = touched_files.union(self.files_with_cover_desc)
        cover_cleaned_count = 0
        for path in all_cover_clean_files:
            try:
                full_audio = MutagenFile(path)
                if full_audio and clean_cover_art_descriptions(full_audio):
                    full_audio.save()
                    cover_cleaned_count += 1
            except Exception as e:
                print(f"{Color.YELLOW}Warning: Could not clean cover art description for {os.path.basename(path)}: {e}{Color.ENDC}")

        if cover_cleaned_count > 0:
            print(f"{Color.CYAN}Cleared cover art description metadata in {cover_cleaned_count} file(s).{Color.ENDC}")

        if undo_entries and not no_save_cache:
            os.makedirs(CACHE_DIR, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            undo_file = os.path.join(CACHE_DIR, f"undo_{ts}.json")
            try:
                with open(undo_file, 'w', encoding='utf-8') as f:
                    json.dump({'timestamp': ts, 'entries': undo_entries}, f, indent=2)
                print(f"{Color.CYAN}Undo snapshot saved to {undo_file}{Color.ENDC}")
            except Exception as e:
                print(f"{Color.YELLOW}Warning: Could not write undo snapshot: {e}{Color.ENDC}")

        effective_applied = applied_count if applied_count > 0 else (1 if cover_cleaned_count > 0 else 0)
        print(f"\n{Color.GREEN}Done. Updated {applied_count} groups ({len(undo_entries)} files changed, {cover_cleaned_count} cover descriptions cleared). Errors: {error_count}{Color.ENDC}")
        return effective_applied, error_count


def execute_undo():
    """Reverts the most recent batch modification using undo snapshots."""
    if not os.path.exists(CACHE_DIR):
        print(f"{Color.YELLOW}No cache directory found at {CACHE_DIR}. Nothing to undo.{Color.ENDC}")
        return

    undo_files = sorted(
        [os.path.join(CACHE_DIR, f) for f in os.listdir(CACHE_DIR) if f.startswith("undo_") and f.endswith(".json")],
        reverse=True
    )

    if not undo_files:
        print(f"{Color.YELLOW}No undo snapshots found in {CACHE_DIR}.{Color.ENDC}")
        return

    target_file = undo_files[0]
    print(f"{Color.CYAN}Reading undo snapshot: {os.path.basename(target_file)}...{Color.ENDC}")

    try:
        with open(target_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"{Color.RED}Failed to read undo snapshot: {e}{Color.ENDC}")
        return

    entries = data.get('entries', [])
    reverted_count = 0
    error_count = 0

    for entry in entries:
        path = entry['path']
        tag = entry['tag']
        old_val = entry['old']
        if not os.path.exists(path):
            print(f"{Color.YELLOW}Skipping missing file: {path}{Color.ENDC}")
            continue
        try:
            audio = MutagenFile(path, easy=True)
            if not audio:
                continue
            audio[tag] = old_val
            audio.save()
            reverted_count += 1
        except Exception as e:
            error_count += 1
            print(f"{Color.RED}Failed to revert {os.path.basename(path)}: {e}{Color.ENDC}")

    reverted_name = target_file.replace("undo_", "reverted_")
    try:
        os.rename(target_file, reverted_name)
    except Exception:
        pass

    print(f"{Color.GREEN}Undo complete! Restored {reverted_count} files ({error_count} errors).{Color.ENDC}")


def clear_cache():
    """Removes cache directory containing drafts and undo logs."""
    if os.path.exists(CACHE_DIR):
        try:
            shutil.rmtree(CACHE_DIR)
            print(f"{Color.GREEN}Cleared cache directory: {CACHE_DIR}{Color.ENDC}")
        except Exception as e:
            print(f"{Color.RED}Error clearing cache: {e}{Color.ENDC}")
    else:
        print(f"{Color.YELLOW}Cache directory is already clean.{Color.ENDC}")


def find_available_port(start_port=8000, max_port=8050):
    for port in range(start_port, max_port + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(('127.0.0.1', port))
                return port
            except OSError:
                continue
    return start_port


HTML_SPA = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>aud-fix-metadata Preview</title>
    <style>
        :root {
            --bg: #0d1117;
            --surface: #161b22;
            --surface-hover: #21262d;
            --surface-active: #30363d;
            --border: #30363d;
            --text: #e6edf3;
            --text-muted: #8b949e;
            --primary: #58a6ff;
            --primary-hover: #79c0ff;
            --success: #3fb950;
            --success-hover: #56d364;
            --danger: #f85149;
            --warning: #d29922;
            --tag-title: #bc8cff;
            --tag-album: #39c5cf;
            --tag-artist: #f0883e;
            --diff-ins-bg: rgba(63, 185, 80, 0.25);
            --diff-del-bg: rgba(248, 81, 73, 0.25);
        }

        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            background: var(--bg);
            color: var(--text);
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
            font-size: 14px;
            line-height: 1.5;
            padding-bottom: 80px;
        }

        /* Top Header */
        header {
            position: sticky;
            top: 0;
            background: rgba(13, 17, 23, 0.95);
            backdrop-filter: blur(8px);
            border-bottom: 1px solid var(--border);
            z-index: 100;
            padding: 12px 24px;
        }
        .header-top {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 16px;
        }
        .app-title {
            display: flex;
            align-items: center;
            gap: 10px;
        }
        .app-title h1 {
            font-size: 1.25rem;
            font-weight: 600;
            color: var(--text);
            letter-spacing: -0.02em;
        }
        .path-badge {
            background: var(--surface);
            border: 1px solid var(--border);
            color: var(--text-muted);
            padding: 3px 8px;
            border-radius: 6px;
            font-size: 0.8rem;
            font-family: monospace;
            max-width: 320px;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .header-actions {
            display: flex;
            align-items: center;
            gap: 10px;
        }

        /* Buttons */
        button, .btn {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            background: var(--surface);
            border: 1px solid var(--border);
            color: var(--text);
            padding: 6px 14px;
            border-radius: 6px;
            font-size: 0.85rem;
            font-weight: 500;
            cursor: pointer;
            transition: all 0.15s ease;
            user-select: none;
            text-decoration: none;
        }
        button:hover, .btn:hover { background: var(--surface-hover); border-color: #8b949e; }
        button:active, .btn:active { background: var(--surface-active); }

        button.btn-primary {
            background: #238636;
            border-color: rgba(240, 246, 252, 0.1);
            color: #fff;
            font-weight: 600;
        }
        button.btn-primary:hover { background: #2ea043; border-color: rgba(240, 246, 252, 0.2); }

        button.btn-blue {
            background: #1f6feb;
            border-color: rgba(240, 246, 252, 0.1);
            color: #fff;
        }
        button.btn-blue:hover { background: #388bfd; }

        button.btn-danger {
            color: var(--danger);
            border-color: rgba(248, 81, 73, 0.4);
        }
        button.btn-danger:hover { background: rgba(248, 81, 73, 0.15); border-color: var(--danger); }

        button.btn-active {
            background: var(--primary);
            color: #000;
            font-weight: 600;
            border-color: var(--primary);
        }

        /* Stats Strip */
        .stats-bar {
            display: flex;
            align-items: center;
            gap: 8px;
            margin-top: 10px;
            flex-wrap: wrap;
        }
        .stat-chip {
            background: var(--surface);
            border: 1px solid var(--border);
            padding: 3px 10px;
            border-radius: 20px;
            font-size: 0.8rem;
            color: var(--text-muted);
            display: flex;
            align-items: center;
            gap: 5px;
        }
        .stat-chip b { color: var(--text); }
        .stat-chip.stat-changed b { color: var(--success); }
        .stat-chip.stat-warn b { color: var(--warning); }
        .stat-chip.stat-edit b { color: var(--primary); }

        /* Find & Replace Panel */
        #findReplacePanel {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 8px;
            margin: 16px 24px 0 24px;
            padding: 16px;
            display: none;
            animation: slideDown 0.2s ease;
        }
        #findReplacePanel.open { display: block; }
        @keyframes slideDown {
            from { opacity: 0; transform: translateY(-8px); }
            to { opacity: 1; transform: translateY(0); }
        }
        .fr-grid {
            display: grid;
            grid-template-columns: 140px 1.5fr 1.5fr auto;
            gap: 12px;
            align-items: center;
        }
        .fr-options {
            display: flex;
            align-items: center;
            gap: 16px;
            margin-top: 10px;
            font-size: 0.85rem;
            color: var(--text-muted);
        }
        .fr-options label {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            cursor: pointer;
            user-select: none;
        }
        .fr-stats {
            margin-left: auto;
            font-size: 0.85rem;
            color: var(--primary);
            font-weight: 500;
        }

        /* Inputs */
        input[type="text"], select {
            background: #090d12;
            border: 1px solid var(--border);
            color: var(--text);
            padding: 6px 10px;
            border-radius: 6px;
            font-size: 0.85rem;
            outline: none;
            width: 100%;
        }
        input[type="text"]:focus, select:focus {
            border-color: var(--primary);
            box-shadow: 0 0 0 2px rgba(88, 166, 255, 0.2);
        }

        /* Toolbar */
        .toolbar {
            padding: 16px 24px 8px 24px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 16px;
            flex-wrap: wrap;
        }
        .filter-group {
            display: flex;
            align-items: center;
            gap: 6px;
            flex-wrap: wrap;
        }
        .tab-btn {
            background: transparent;
            border: 1px solid transparent;
            color: var(--text-muted);
            padding: 4px 10px;
            border-radius: 6px;
            font-size: 0.82rem;
            cursor: pointer;
        }
        .tab-btn:hover { color: var(--text); background: var(--surface); }
        .tab-btn.active {
            color: var(--text);
            background: var(--surface);
            border-color: var(--border);
            font-weight: 600;
        }

        .search-box {
            position: relative;
            min-width: 240px;
        }
        .search-box input {
            padding-left: 10px;
        }

        /* Batch Bar */
        .batch-bar {
            padding: 0 24px 12px 24px;
            display: flex;
            align-items: center;
            gap: 12px;
            font-size: 0.85rem;
        }
        .batch-bar select { width: auto; }

        /* Main Table Container */
        .table-container {
            padding: 0 24px;
        }
        .table-header {
            display: grid;
            grid-template-columns: 50px 90px 1.5fr 1.5fr 1.6fr 90px;
            gap: 10px;
            padding: 8px 12px;
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 8px 8px 0 0;
            color: var(--text-muted);
            font-weight: 600;
            font-size: 0.8rem;
            text-transform: uppercase;
            letter-spacing: 0.04em;
        }

        .row-item {
            background: var(--bg);
            border-left: 1px solid var(--border);
            border-right: 1px solid var(--border);
            border-bottom: 1px solid var(--border);
            transition: background 0.1s ease;
        }
        .row-item:last-child {
            border-radius: 0 0 8px 8px;
        }
        .row-item:hover {
            background: rgba(22, 27, 34, 0.6);
        }
        .row-item.is-modified {
            border-left: 3px solid var(--success);
        }
        .row-item.is-edited {
            border-left: 3px solid var(--primary);
        }

        .row-main {
            display: grid;
            grid-template-columns: 50px 90px 1.5fr 1.5fr 1.6fr 90px;
            gap: 10px;
            padding: 10px 12px;
            align-items: center;
        }

        .field-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 4px;
            min-height: 18px;
        }
        .field-case-select {
            background: #161b22;
            border: 1px solid var(--border);
            color: var(--text-muted);
            font-size: 0.72rem;
            padding: 1px 6px;
            border-radius: 4px;
            cursor: pointer;
            width: auto;
            margin-left: auto;
        }
        .field-case-select:hover {
            color: var(--text);
            border-color: #8b949e;
        }

        /* Custom Checkbox */
        input[type="checkbox"] {
            accent-color: var(--primary);
            cursor: pointer;
            width: 16px;
            height: 16px;
        }

        /* Badges */
        .tag-badge {
            display: inline-block;
            font-size: 0.72rem;
            font-weight: 700;
            text-transform: uppercase;
            padding: 2px 8px;
            border-radius: 4px;
            text-align: center;
            width: 65px;
        }
        .tag-Title  { color: var(--tag-title); background: rgba(188, 140, 255, 0.15); border: 1px solid rgba(188, 140, 255, 0.3); }
        .tag-Album  { color: var(--tag-album); background: rgba(57, 197, 207, 0.15); border: 1px solid rgba(57, 197, 207, 0.3); }
        .tag-Artist { color: var(--tag-artist); background: rgba(240, 136, 62, 0.15); border: 1px solid rgba(240, 136, 62, 0.3); }

        .warn-badge {
            display: inline-block;
            background: rgba(210, 153, 34, 0.15);
            color: var(--warning);
            border: 1px solid rgba(210, 153, 34, 0.3);
            border-radius: 4px;
            font-size: 0.7rem;
            padding: 1px 5px;
            margin-top: 4px;
            font-weight: 500;
        }
        .edit-badge {
            display: inline-block;
            background: rgba(88, 166, 255, 0.15);
            color: var(--primary);
            border: 1px solid rgba(88, 166, 255, 0.3);
            border-radius: 4px;
            font-size: 0.7rem;
            padding: 1px 5px;
            margin-left: 6px;
        }

        /* Diff Rendering */
        .diff-container {
            font-family: monospace;
            font-size: 0.85rem;
            line-height: 1.4;
            word-break: break-word;
            padding: 4px 6px;
            border-radius: 4px;
            background: rgba(255,255,255,0.02);
        }
        ins.diff-ins {
            background: var(--diff-ins-bg);
            color: #7ee787;
            text-decoration: none;
            padding: 0 1px;
            border-radius: 2px;
        }
        del.diff-del {
            background: var(--diff-del-bg);
            color: #ffa198;
            text-decoration: line-through;
            padding: 0 1px;
            border-radius: 2px;
        }
        .ws-glyph {
            opacity: 0.6;
            color: #f0883e;
            font-weight: bold;
        }

        /* Editable Input */
        textarea.edit-input {
            width: 100%;
            background: #090d12;
            border: 1px solid var(--border);
            color: var(--text);
            padding: 6px 8px;
            border-radius: 6px;
            font-size: 0.88rem;
            font-family: inherit;
            resize: none;
            min-height: 32px;
            line-height: 1.4;
            overflow: hidden;
            outline: none;
        }
        textarea.edit-input:focus {
            border-color: var(--primary);
            box-shadow: 0 0 0 2px rgba(88, 166, 255, 0.2);
        }
        .is-edited textarea.edit-input {
            border-color: rgba(88, 166, 255, 0.6);
            background: rgba(88, 166, 255, 0.05);
        }

        /* Count Button */
        .count-btn {
            background: var(--surface);
            border: 1px solid var(--border);
            color: var(--text-muted);
            padding: 4px 8px;
            border-radius: 12px;
            font-size: 0.75rem;
            cursor: pointer;
            text-align: center;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 4px;
        }
        .count-btn:hover {
            background: var(--surface-hover);
            color: var(--text);
        }

        /* Expandable Files Accordion */
        .file-list {
            display: none;
            padding: 10px 16px 12px 50px;
            background: #090d12;
            border-top: 1px dashed var(--border);
            font-size: 0.8rem;
        }
        .file-list.open { display: block; }
        .file-item {
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 4px 0;
            color: var(--text-muted);
            font-family: monospace;
            border-bottom: 1px solid rgba(255,255,255,0.03);
        }
        .file-item:last-child { border-bottom: none; }
        .file-item span { word-break: break-all; }
        .detach-btn {
            font-size: 0.72rem;
            padding: 2px 6px;
            margin-left: 12px;
            white-space: nowrap;
        }

        /* Toast notifications */
        #toast {
            position: fixed;
            bottom: 24px;
            right: 24px;
            background: #1f6feb;
            color: #fff;
            padding: 10px 18px;
            border-radius: 8px;
            font-size: 0.85rem;
            box-shadow: 0 4px 16px rgba(0,0,0,0.5);
            opacity: 0;
            transform: translateY(20px);
            transition: all 0.2s ease;
            pointer-events: none;
            z-index: 1000;
        }
        #toast.show {
            opacity: 1;
            transform: translateY(0);
        }
        #toast.toast-success { background: #238636; }
        #toast.toast-error { background: #da3633; }

        /* Empty state */
        .empty-state {
            text-align: center;
            padding: 60px 20px;
            color: var(--text-muted);
        }
        .empty-state h3 { color: var(--text); margin-bottom: 8px; }
    </style>
</head>
<body>

    <header>
        <div class="header-top">
            <div class="app-title">
                <h1>aud-fix-metadata</h1>
                <span class="path-badge" id="rootFoldersBadge" title="Scanned Directory">Scanning...</span>
            </div>
            <div class="header-actions">
                <button id="toggleFrBtn" onclick="toggleFindReplace()">
                    Find & Replace
                </button>
                <select id="headerCaseMode" onchange="changeGlobalCaseMode(this.value)" style="width: auto; padding: 5px 8px; font-size: 0.82rem; background: var(--surface); color: var(--text); border: 1px solid var(--border); border-radius: 6px;" title="Default capitalization format for proposals">
                    <option value="title" selected>Title Case</option>
                    <option value="sentence">Sentence case</option>
                </select>
                <button onclick="recheckProposals(false)" title="Re-evaluate rules without losing manual edits">
                    Recheck
                </button>
                <button onclick="rescanDisk()" title="Rescan files from disk">
                    Rescan
                </button>
                <button class="btn-danger" onclick="cancelAndShutdown()">Cancel</button>
                <button class="btn-primary" onclick="applyChanges()">
                    Apply Changes <span id="applyCountChip">(0)</span>
                </button>
            </div>
        </div>

        <div class="stats-bar">
            <div class="stat-chip">Total: <b id="statTotal">0</b></div>
            <div class="stat-chip stat-changed">Modified: <b id="statModified">0</b></div>
            <div class="stat-chip">Unchanged: <b id="statUnchanged">0</b></div>
            <div class="stat-chip stat-edit">Manual Edits: <b id="statEdited">0</b></div>
            <div class="stat-chip stat-warn">Whitespace Issues: <b id="statWarn">0</b></div>
            <div class="stat-chip">Selected for Apply: <b id="statSelected">0</b></div>
        </div>
    </header>

    <!-- Find & Replace Drawer -->
    <div id="findReplacePanel">
        <div class="fr-grid">
            <div>
                <select id="frScope">
                    <option value="all">All Tags</option>
                    <option value="Artist" selected>Artist Only</option>
                    <option value="Album">Album Only</option>
                    <option value="Title">Title Only</option>
                    <option value="selected">Selected Rows Only</option>
                </select>
            </div>
            <div>
                <input type="text" id="frFind" placeholder="Find text (e.g. an artist)" oninput="updateFrPreview()">
            </div>
            <div>
                <input type="text" id="frReplace" placeholder="Replace with (e.g. An Artist)">
            </div>
            <div>
                <button class="btn-blue" onclick="executeFindReplace()">Replace All Matching</button>
            </div>
        </div>
        <div class="fr-options">
            <label><input type="checkbox" id="frMatchCase" onchange="updateFrPreview()"> Match Case</label>
            <label><input type="checkbox" id="frWholeWord" onchange="updateFrPreview()"> Whole Word</label>
            <label><input type="checkbox" id="frRegex" onchange="updateFrPreview()"> Regex</label>
            <span class="fr-stats" id="frMatchesLabel">0 matches</span>
        </div>
    </div>

    <!-- Filters & Search Toolbar -->
    <div class="toolbar">
        <div class="filter-group">
            <button class="tab-btn active" onclick="setFilter('all', this)">All</button>
            <button class="tab-btn" onclick="setFilter('modified', this)">Modified Only</button>
            <button class="tab-btn" onclick="setFilter('unchanged', this)">Unchanged</button>
            <button class="tab-btn" onclick="setFilter('edited', this)">Manual Edits</button>
            <button class="tab-btn" onclick="setFilter('whitespace', this)">Whitespace</button>
            <span style="color: var(--border);">|</span>
            <button class="tab-btn" onclick="setTagFilter('all', this)">All Tags</button>
            <button class="tab-btn" onclick="setTagFilter('Title', this)">Title</button>
            <button class="tab-btn" onclick="setTagFilter('Album', this)">Album</button>
            <button class="tab-btn" onclick="setTagFilter('Artist', this)">Artist</button>
        </div>

        <div class="search-box">
            <input type="text" id="searchInput" placeholder="Search entries or paths (Press '/' to focus)" oninput="renderTable()">
        </div>
    </div>

    <!-- Batch Selection Toolbar -->
    <div class="batch-bar">
        <label style="display: flex; align-items: center; gap: 6px; cursor: pointer;">
            <input type="checkbox" id="selectAllCheckbox" onchange="toggleSelectAll(this.checked)">
            <span>Select Visible</span>
        </label>
        <button onclick="selectOnlyModified()">Select Modified Only</button>
        <button onclick="selectAllProposals()">Select All</button>
        <button onclick="deselectAll()">Deselect All</button>

        <span style="color: var(--border); margin: 0 4px;">|</span>

        <span style="font-size: 0.82rem; color: var(--text-muted);">Transform:</span>
        <select id="batchTransformScope" style="width: auto; padding: 4px 8px; font-size: 0.82rem; background: var(--surface); color: var(--text); border: 1px solid var(--border); border-radius: 6px;">
            <option value="all" selected>All Items</option>
            <option value="checked">Checked Rows</option>
            <option value="visible">Visible Filtered</option>
        </select>
        <select id="batchTransformSelect" onchange="applyBatchTransform(this.value); this.value='';">
            <option value="">Choose Case / Action...</option>
            <option value="sentence">Sentence case</option>
            <option value="title">Title Case</option>
            <option value="trim">Trim Whitespace Only</option>
            <option value="upper">UPPERCASE</option>
            <option value="lower">lowercase</option>
            <option value="revert">Revert Original</option>
        </select>
    </div>

    <!-- Table -->
    <div class="table-container">
        <div class="table-header">
            <div style="text-align: center;">Apply</div>
            <div>Tag</div>
            <div>Original</div>
            <div>Diff Preview</div>
            <div>New (Editable)</div>
            <div style="text-align: center;">Files</div>
        </div>

        <div id="rowsContainer">
            <!-- Rows injected by JavaScript -->
        </div>
    </div>

    <div id="toast">Message</div>

    <script>
        let state = {
            proposals: [],
            rootFolders: [],
            filter: 'all',
            tagFilter: 'all',
            search: '',
            hasDraft: false,
        };

        const STORAGE_KEY = 'aud_metadata_draft_' + window.location.port;

        // Initialize
        window.addEventListener('DOMContentLoaded', () => {
            fetchState();
            setupShortcuts();
        });

        function showToast(msg, type = '') {
            const toast = document.getElementById('toast');
            toast.textContent = msg;
            toast.className = 'show ' + (type ? 'toast-' + type : '');
            clearTimeout(window.toastTimer);
            window.toastTimer = setTimeout(() => {
                toast.className = '';
            }, 3000);
        }

        function escapeHtml(s) {
            return (s || '').replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#039;");
        }

        function formatWhitespace(s) {
            let escaped = escapeHtml(s);
            // Replace leading spaces
            escaped = escaped.replace(/^( +)/, m => `<span class="ws-glyph">${'·'.repeat(m.length)}</span>`);
            // Replace trailing spaces
            escaped = escaped.replace(/( +)$/, m => `<span class="ws-glyph">${'·'.repeat(m.length)}</span>`);
            // Replace internal multi-spaces
            escaped = escaped.replace(/(  +)/g, m => `<span class="ws-glyph">${'·'.repeat(m.length)}</span>`);
            return escaped;
        }

        // Tokenized Diff Algorithm
        function computeDiff(orig, curr) {
            if (orig === curr) {
                return `<span style="opacity: 0.5;">No change</span>`;
            }
            const tokenize = s => s.match(/[\\w]+|[^\\w\\s]+|\\s+/g) || [];
            const t1 = tokenize(orig);
            const t2 = tokenize(curr);

            const m = t1.length, n = t2.length;
            const dp = Array.from({length: m + 1}, () => new Uint16Array(n + 1));
            for (let i = 0; i < m; i++) {
                for (let j = 0; j < n; j++) {
                    if (t1[i] === t2[j]) dp[i+1][j+1] = dp[i][j] + 1;
                    else dp[i+1][j+1] = Math.max(dp[i+1][j], dp[i][j+1]);
                }
            }

            let i = m, j = n;
            const parts = [];
            while (i > 0 || j > 0) {
                if (i > 0 && j > 0 && t1[i-1] === t2[j-1]) {
                    parts.push({ type: 'same', val: t1[i-1] });
                    i--; j--;
                } else if (j > 0 && (i === 0 || dp[i][j-1] >= dp[i-1][j])) {
                    parts.push({ type: 'ins', val: t2[j-1] });
                    j--;
                } else if (i > 0 && (j === 0 || dp[i][j-1] < dp[i-1][j])) {
                    parts.push({ type: 'del', val: t1[i-1] });
                    i--;
                }
            }
            parts.reverse();

            return parts.map(p => {
                let esc = escapeHtml(p.val);
                if (/^\\s+$/.test(p.val)) {
                    esc = esc.replace(/ /g, '<span class="ws-glyph">·</span>');
                }
                if (p.type === 'ins') return `<ins class="diff-ins">${esc}</ins>`;
                if (p.type === 'del') return `<del class="diff-del">${esc}</del>`;
                return esc;
            }).join('');
        }

        async function fetchState() {
            try {
                const res = await fetch('/api/state');
                const data = await res.json();
                state.proposals = data.proposals || [];
                state.rootFolders = data.root_folders || [];

                document.getElementById('rootFoldersBadge').textContent = state.rootFolders.map(f => f.split('/').pop() || f).join(', ');
                document.getElementById('rootFoldersBadge').title = state.rootFolders.join('\\n');

                // Check localStorage draft
                if (data.clear_cache) {
                    localStorage.removeItem(STORAGE_KEY);
                } else {
                    restoreDraft();
                }

                if (data.case_mode && document.getElementById('headerCaseMode')) {
                    document.getElementById('headerCaseMode').value = data.case_mode;
                }

                updateStats();
                renderTable();
            } catch (err) {
                showToast('Error loading state: ' + err, 'error');
            }
        }

        function updateItemStatus(item) {
            if (item.current === item.original) {
                item.is_manually_edited = false;
                item.apply = (item.warnings && item.warnings.length > 0);
            } else if (item.current === item.proposed) {
                item.is_manually_edited = false;
                item.apply = true;
            } else {
                item.is_manually_edited = true;
                item.apply = true;
            }
        }

        function restoreDraft() {
            const saved = localStorage.getItem(STORAGE_KEY);
            if (!saved) return;
            try {
                const draftMap = JSON.parse(saved);
                let restoredCount = 0;
                state.proposals.forEach(p => {
                    if (draftMap[p.id]) {
                        p.current = draftMap[p.id].current;
                        p.apply = draftMap[p.id].apply;
                        updateItemStatus(p);
                        restoredCount++;
                    }
                });
                if (restoredCount > 0) {
                    showToast(`Restored ${restoredCount} edits from browser draft`, 'success');
                }
            } catch (e) {
                console.error("Draft restore error", e);
            }
        }

        function saveDraft() {
            const draftMap = {};
            state.proposals.forEach(p => {
                if (p.is_manually_edited || p.apply !== (p.original !== p.proposed)) {
                    draftMap[p.id] = {
                        current: p.current,
                        is_manually_edited: p.is_manually_edited,
                        apply: p.apply
                    };
                }
            });
            localStorage.setItem(STORAGE_KEY, JSON.stringify(draftMap));
        }

        function updateStats() {
            const total = state.proposals.length;
            let modified = 0, unchanged = 0, edited = 0, warn = 0, selected = 0;

            state.proposals.forEach(p => {
                const isMod = p.current !== p.original;
                if (isMod) modified++;
                else unchanged++;
                if (p.is_manually_edited && isMod) edited++;
                if (p.warnings && p.warnings.length > 0) warn++;
                if (p.apply) selected++;
            });

            document.getElementById('statTotal').textContent = total;
            document.getElementById('statModified').textContent = modified;
            document.getElementById('statUnchanged').textContent = unchanged;
            document.getElementById('statEdited').textContent = edited;
            document.getElementById('statWarn').textContent = warn;
            document.getElementById('statSelected').textContent = selected;
            document.getElementById('applyCountChip').textContent = `(${selected})`;
        }

        function setFilter(f, btn) {
            state.filter = f;
            document.querySelectorAll('.filter-group .tab-btn').forEach(b => {
                if (['all','modified','unchanged','edited','whitespace'].includes(b.getAttribute('onclick')?.match(/'([^']+)'/)?.[1])) {
                    b.classList.remove('active');
                }
            });
            btn.classList.add('active');
            renderTable();
        }

        function setTagFilter(tag, btn) {
            state.tagFilter = tag;
            document.querySelectorAll('.filter-group .tab-btn').forEach(b => {
                if (['all','Title','Album','Artist'].includes(b.getAttribute('onclick')?.match(/TagFilter\\('([^']+)'/)?.[1])) {
                    b.classList.remove('active');
                }
            });
            btn.classList.add('active');
            renderTable();
        }

        function renderTable() {
            const container = document.getElementById('rowsContainer');
            const search = document.getElementById('searchInput').value.toLowerCase().trim();

            const filtered = state.proposals.filter(item => {
                // Filter Tab
                if (state.filter === 'modified' && item.current === item.original) return false;
                if (state.filter === 'unchanged' && item.current !== item.original) return false;
                if (state.filter === 'edited' && (!item.is_manually_edited || item.current === item.original)) return false;
                if (state.filter === 'whitespace' && (!item.warnings || item.warnings.length === 0)) return false;

                // Tag Filter
                if (state.tagFilter !== 'all' && item.type !== state.tagFilter) return false;

                // Search
                if (search) {
                    const matchOrig = item.original.toLowerCase().includes(search);
                    const matchCurr = item.current.toLowerCase().includes(search);
                    const matchType = item.type.toLowerCase().includes(search);
                    const matchFile = item.files.some(f => f.toLowerCase().includes(search));
                    if (!matchOrig && !matchCurr && !matchType && !matchFile) return false;
                }
                return true;
            });

            if (filtered.length === 0) {
                container.innerHTML = `
                    <div class="empty-state">
                        <h3>No matching metadata entries</h3>
                        <p>Try clearing filters or search terms.</p>
                    </div>
                `;
                return;
            }

            container.innerHTML = filtered.map(item => {
                const isMod = item.current !== item.original;
                const isEdited = item.is_manually_edited && isMod;
                const warningsHtml = (item.warnings || []).map(w => `<span class="warn-badge">${w}</span>`).join(' ');
                const editedBadge = isEdited ? `<span class="edit-badge">Edited</span>` : '';
                const diffHtml = computeDiff(item.original, item.current);

                const fileRows = item.files.map(filePath => `
                    <div class="file-item">
                        <span>${escapeHtml(filePath)}</span>
                        <button class="detach-btn" onclick="detachFile('${item.id}', '${escapeHtml(filePath)}')">Detach / Edit</button>
                    </div>
                `).join('');

                return `
                    <div class="row-item ${isMod ? 'is-modified' : ''} ${isEdited ? 'is-edited' : ''}" id="row-${item.id}">
                        <div class="row-main">
                            <div style="text-align: center;">
                                <input type="checkbox" ${item.apply ? 'checked' : ''} onchange="toggleItemApply('${item.id}', this.checked)">
                            </div>
                            <div>
                                <span class="tag-badge tag-${item.type}">${item.type}</span>
                            </div>
                            <div>
                                <div style="font-family: monospace; font-size: 0.88rem;">${formatWhitespace(item.original)}</div>
                                <div>${warningsHtml}</div>
                            </div>
                            <div>
                                <div class="diff-container">${diffHtml}</div>
                            </div>
                            <div>
                                <div class="field-header">
                                    <span class="field-status">${editedBadge}</span>
                                    <select class="field-case-select" onchange="changeFieldCase('${item.id}', this.value); this.value='';" title="Change case style for this field">
                                        <option value="" disabled selected>Case</option>
                                        <option value="sentence">Sentence case</option>
                                        <option value="title">Title Case</option>
                                        <option value="upper">UPPERCASE</option>
                                        <option value="lower">lowercase</option>
                                        <option value="revert">Revert Original</option>
                                    </select>
                                </div>
                                <textarea class="edit-input" rows="1" oninput="handleInput('${item.id}', this)">${escapeHtml(item.current)}</textarea>
                            </div>
                            <div style="text-align: center;">
                                <button class="count-btn" onclick="toggleFiles('${item.id}')" title="Inspect files">
                                    ${item.count} files
                                </button>
                            </div>
                        </div>
                        <div class="file-list" id="files-${item.id}">
                            ${fileRows}
                        </div>
                    </div>
                `;
            }).join('');

            // Auto-expand textareas
            document.querySelectorAll('textarea.edit-input').forEach(ta => {
                ta.style.height = 'auto';
                ta.style.height = ta.scrollHeight + 'px';
            });
        }

        function handleInput(id, textarea) {
            textarea.style.height = 'auto';
            textarea.style.height = textarea.scrollHeight + 'px';

            const item = state.proposals.find(p => p.id === id);
            if (!item) return;

            item.current = textarea.value;
            updateItemStatus(item);

            // Update row style and diff
            const row = document.getElementById('row-' + id);
            if (row) {
                const isMod = item.current !== item.original;
                const isEdited = item.is_manually_edited && isMod;
                const cb = row.querySelector('input[type="checkbox"]');
                if (cb) cb.checked = item.apply;
                const diffContainer = row.querySelector('.diff-container');
                if (diffContainer) diffContainer.innerHTML = computeDiff(item.original, item.current);
                if (isMod) row.classList.add('is-modified');
                else row.classList.remove('is-modified');
                if (isEdited) row.classList.add('is-edited');
                else row.classList.remove('is-edited');
                const statusSpan = row.querySelector('.field-status');
                if (statusSpan) {
                    statusSpan.innerHTML = isEdited ? '<span class="edit-badge">Edited</span>' : '';
                }
            }

            saveDraft();
            updateStats();
        }

        function toggleItemApply(id, checked) {
            const item = state.proposals.find(p => p.id === id);
            if (item) {
                item.apply = checked;
                saveDraft();
                updateStats();
            }
        }

        function toggleFiles(id) {
            const el = document.getElementById('files-' + id);
            if (el) el.classList.toggle('open');
        }

        function detachFile(id, filePath) {
            const itemIndex = state.proposals.findIndex(p => p.id === id);
            if (itemIndex === -1) return;
            const item = state.proposals[itemIndex];

            if (item.files.length <= 1) {
                showToast("Only 1 file in group; cannot detach further.", "error");
                return;
            }

            // Remove file from group
            item.files = item.files.filter(f => f !== filePath);
            item.count = item.files.length;

            // Create new detached item
            const newId = 'detached_' + Math.random().toString(36).substr(2, 9);
            const newItem = {
                id: newId,
                type: item.type,
                original: item.original,
                proposed: item.proposed,
                current: item.current,
                files: [filePath],
                count: 1,
                is_english: item.is_english,
                is_manually_edited: true,
                apply: true,
                warnings: item.warnings,
            };

            state.proposals.splice(itemIndex + 1, 0, newItem);
            saveDraft();
            updateStats();
            renderTable();
            showToast("Detached file into separate edit row", "success");
        }

        // Selection & Batch Actions
        function toggleSelectAll(checked) {
            const visibleIds = new Set(
                Array.from(document.querySelectorAll('.row-item')).map(r => r.id.replace('row-', ''))
            );
            state.proposals.forEach(p => {
                if (visibleIds.has(p.id)) p.apply = checked;
            });
            saveDraft();
            updateStats();
            renderTable();
        }

        function selectOnlyModified() {
            state.proposals.forEach(p => {
                p.apply = (p.current !== p.original);
            });
            saveDraft();
            updateStats();
            renderTable();
            showToast("Selected all modified items");
        }

        function selectAllProposals() {
            state.proposals.forEach(p => p.apply = true);
            saveDraft();
            updateStats();
            renderTable();
            showToast("Selected all items");
        }

        function deselectAll() {
            state.proposals.forEach(p => p.apply = false);
            saveDraft();
            updateStats();
            renderTable();
            showToast("Deselected all");
        }

        function toSentenceCase(str) {
            if (!str) return str;
            let cleaned = str.trim().replace(/[ \\t]+/g, ' ');
            let lower = cleaned.toLowerCase();
            let res = lower.charAt(0).toUpperCase() + lower.slice(1);
            res = res.replace(/([.!?:]\\s+)(\\p{L})/gu, (m, p1, p2) => p1 + p2.toUpperCase());
            res = res.replace(/([(\\[{]\\s*)(\\p{L})/gu, (m, p1, p2) => p1 + p2.toUpperCase());
            res = res.replace(/([/\\\\–—]\\s*)(\\p{L})/gu, (m, p1, p2) => p1 + p2.toUpperCase());
            res = res.replace(/\\b(?<!')(?=[mdclxvi])(M{0,3}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3}))\\b/gi, (m) => {
                const ml = m.toLowerCase();
                if (['mix', 'dim', 'mid', 'did', 'lid'].includes(ml)) return m;
                if (m.length > 1 || ml === 'i') return m.toUpperCase();
                return m;
            });
            return res;
        }

        function toTitleCase(str) {
            if (!str) return str;
            const minorWords = new Set(['a', 'an', 'and', 'as', 'at', 'but', 'by', 'en', 'for', 'if', 'in', 'of', 'on', 'or', 'the', 'to', 'v', 'via', 'vs', 'b/w']);
            let words = str.toLowerCase().split(' ');
            const len = words.length;
            let res = words.map((w, idx) => {
                if (!w) return w;
                const isFirst = (idx === 0);
                const isLast = (idx === len - 1);
                const cleanW = w.replace(/^[^\\p{L}\\p{N}]+|[^\\p{L}\\p{N}]+$/gu, '');
                if (!isFirst && !isLast && minorWords.has(cleanW)) {
                    return w;
                }
                return w.replace(/\\p{L}/u, c => c.toUpperCase());
            }).join(' ');
            res = res.replace(/([(\\[{\\/\\\\–—:]\\s*)(\\p{L})/gu, (m, p1, p2) => p1 + p2.toUpperCase());
            res = res.replace(/\\b(\\p{L})(\\p{L}*)([^\\p{L}\\p{N}]*)$/gu, (m, p1, p2, p3) => p1.toUpperCase() + p2 + p3);
            res = res.replace(/\\b(?<!')(?=[mdclxvi])(M{0,3}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3}))\\b/gi, (m) => {
                const ml = m.toLowerCase();
                if (['mix', 'dim', 'mid', 'did', 'lid'].includes(ml)) return m;
                if (m.length > 1 || ml === 'i') return m.toUpperCase();
                return m;
            });
            res = res.replace(/\\b(?:feat|ft)\\b\\.?/gi, 'feat.');
            return res;
        }

        function changeFieldCase(id, caseType) {
            const item = state.proposals.find(p => p.id === id);
            if (!item) return;

            if (caseType === 'sentence') {
                item.current = toSentenceCase(item.current);
            } else if (caseType === 'title') {
                item.current = toTitleCase(item.current);
            } else if (caseType === 'upper') {
                item.current = item.current.toUpperCase();
            } else if (caseType === 'lower') {
                item.current = item.current.toLowerCase();
            } else if (caseType === 'revert') {
                item.current = item.original;
            }

            updateItemStatus(item);
            saveDraft();
            updateStats();
            renderTable();
            showToast(`Applied ${caseType} case to field`);
        }

        function applyBatchTransform(type) {
            if (!type) return;
            const scope = document.getElementById('batchTransformScope')?.value || 'all';
            let targets = [];

            if (scope === 'checked') {
                targets = state.proposals.filter(p => p.apply);
                if (targets.length === 0) {
                    showToast("No checked rows to transform", "error");
                    return;
                }
            } else if (scope === 'visible') {
                const search = document.getElementById('searchInput').value.toLowerCase().trim();
                targets = state.proposals.filter(item => {
                    if (state.filter === 'modified' && item.current === item.original) return false;
                    if (state.filter === 'unchanged' && item.current !== item.original) return false;
                    if (state.filter === 'edited' && (!item.is_manually_edited || item.current === item.original)) return false;
                    if (state.filter === 'whitespace' && (!item.warnings || item.warnings.length === 0)) return false;
                    if (state.tagFilter !== 'all' && item.type !== state.tagFilter) return false;
                    if (search) {
                        const matchOrig = item.original.toLowerCase().includes(search);
                        const matchCurr = item.current.toLowerCase().includes(search);
                        const matchType = item.type.toLowerCase().includes(search);
                        const matchFile = item.files.some(f => f.toLowerCase().includes(search));
                        if (!matchOrig && !matchCurr && !matchType && !matchFile) return false;
                    }
                    return true;
                });
            } else {
                targets = state.proposals;
            }

            targets.forEach(p => {
                if (type === 'trim') {
                    p.current = p.current.trim().replace(/[ \t]+/g, ' ');
                } else if (type === 'sentence') {
                    p.current = toSentenceCase(p.current);
                } else if (type === 'title') {
                    p.current = toTitleCase(p.current);
                } else if (type === 'upper') {
                    p.current = p.current.toUpperCase();
                } else if (type === 'lower') {
                    p.current = p.current.toLowerCase();
                } else if (type === 'revert') {
                    p.current = p.original;
                }
                updateItemStatus(p);
            });

            saveDraft();
            updateStats();
            renderTable();
            showToast(`Transformed ${targets.length} items to ${type}`, "success");
        }

        // Find & Replace Feature
        function toggleFindReplace() {
            const panel = document.getElementById('findReplacePanel');
            const btn = document.getElementById('toggleFrBtn');
            panel.classList.toggle('open');
            btn.classList.toggle('btn-active');
            if (panel.classList.contains('open')) {
                document.getElementById('frFind').focus();
                updateFrPreview();
            }
        }

        function getFrRegex() {
            const findText = document.getElementById('frFind').value;
            if (!findText) return null;
            const isMatchCase = document.getElementById('frMatchCase').checked;
            const isWholeWord = document.getElementById('frWholeWord').checked;
            const isRegex = document.getElementById('frRegex').checked;

            let pattern = findText;
            if (!isRegex) {
                pattern = pattern.replace(/[.*+?^${}()|[\\]\\\\]/g, '\\\\$&');
            }
            if (isWholeWord) {
                pattern = `\\\\b${pattern}\\\\b`;
            }
            const flags = isMatchCase ? 'g' : 'gi';
            try {
                return new RegExp(pattern, flags);
            } catch (e) {
                return null;
            }
        }

        function updateFrPreview() {
            const re = getFrRegex();
            const scope = document.getElementById('frScope').value;
            const label = document.getElementById('frMatchesLabel');
            if (!re) {
                label.textContent = "0 matches";
                return;
            }
            let matches = 0;
            state.proposals.forEach(p => {
                if (scope !== 'all' && scope !== 'selected' && p.type !== scope) return;
                if (scope === 'selected' && !p.apply) return;
                const m = p.current.match(re);
                if (m) matches += m.length;
            });
            label.textContent = `${matches} occurrence${matches === 1 ? '' : 's'} match`;
        }

        function executeFindReplace() {
            const re = getFrRegex();
            const replaceText = document.getElementById('frReplace').value;
            const scope = document.getElementById('frScope').value;
            if (!re) {
                showToast("Please enter a valid search query", "error");
                return;
            }
            let updatedCount = 0;
            state.proposals.forEach(p => {
                if (scope !== 'all' && scope !== 'selected' && p.type !== scope) return;
                if (scope === 'selected' && !p.apply) return;

                if (re.test(p.current)) {
                    const newCurrent = p.current.replace(re, replaceText);
                    if (newCurrent !== p.current) {
                        p.current = newCurrent;
                        updateItemStatus(p);
                        updatedCount++;
                    }
                }
            });

            saveDraft();
            updateStats();
            renderTable();
            updateFrPreview();
            showToast(`Updated ${updatedCount} items`, "success");
        }

        async function changeGlobalCaseMode(mode) {
            await recheckProposals(true, mode);
        }

        // Server Actions: Recheck, Rescan, Apply, Cancel
        async function recheckProposals(forceAll = false, caseMode = null) {
            const mode = caseMode || document.getElementById('headerCaseMode')?.value || 'title';
            showToast(`Rechecking rules (${mode} case)...`, "");
            try {
                const res = await fetch('/api/recheck', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        proposals: state.proposals,
                        force_all: forceAll,
                        case_mode: mode
                    })
                });
                const data = await res.json();
                state.proposals = data.proposals || [];
                saveDraft();
                updateStats();
                renderTable();
                showToast(`Recheck complete: ${state.proposals.length} items evaluated`, "success");
            } catch (err) {
                showToast("Error rechecking: " + err, "error");
            }
        }

        async function rescanDisk() {
            showToast("Rescanning files from disk...", "");
            try {
                const res = await fetch('/api/rescan', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ proposals: state.proposals })
                });
                const data = await res.json();
                state.proposals = data.proposals || [];
                saveDraft();
                updateStats();
                renderTable();
                showToast("Disk rescan complete", "success");
            } catch (err) {
                showToast("Error during rescan: " + err, "error");
            }
        }

        async function applyChanges() {
            const selectedItems = state.proposals.filter(p => p.apply);
            if (selectedItems.length === 0) {
                showToast("No items selected to apply", "error");
                return;
            }

            showToast("Applying changes...", "");
            try {
                const res = await fetch('/api/apply', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ items: selectedItems })
                });
                const result = await res.json();
                localStorage.removeItem(STORAGE_KEY);
                document.body.innerHTML = `
                    <div style="text-align: center; padding: 80px 20px;">
                        <h1 style="color: var(--success); font-size: 2rem; margin-bottom: 12px;">Changes Applied Successfully!</h1>
                        <p style="color: var(--text-muted); font-size: 1.1rem; margin-bottom: 24px;">
                            Updated ${result.applied} groups. (Errors: ${result.errors})
                        </p>
                        <p style="color: var(--text);">You may now close this browser tab and return to your terminal.</p>
                    </div>
                `;
            } catch (err) {
                showToast("Error applying changes: " + err, "error");
            }
        }

        async function cancelAndShutdown() {
            await fetch('/api/shutdown', { method: 'POST' });
            window.close();
            document.body.innerHTML = `
                <div style="text-align: center; padding: 80px 20px;">
                    <h1 style="color: var(--warning); margin-bottom: 12px;">Server Closed</h1>
                    <p style="color: var(--text-muted);">Operation was cancelled by user.</p>
                </div>
            `;
        }

        function setupShortcuts() {
            window.addEventListener('keydown', e => {
                if (e.key === '/' && document.activeElement.tagName !== 'INPUT' && document.activeElement.tagName !== 'TEXTAREA') {
                    e.preventDefault();
                    document.getElementById('searchInput').focus();
                } else if ((e.ctrlKey || e.metaKey) && (e.key === 'f' || e.key === 'h')) {
                    e.preventDefault();
                    toggleFindReplace();
                } else if ((e.ctrlKey || e.metaKey) && e.key === 's') {
                    e.preventDefault();
                    applyChanges();
                } else if (e.key === 'Escape') {
                    const panel = document.getElementById('findReplacePanel');
                    if (panel.classList.contains('open')) toggleFindReplace();
                }
            });
        }
    </script>
</body>
</html>
"""


class PreviewServer(BaseHTTPRequestHandler):
    processor = None
    no_save_cache = False
    clear_cache_flag = False

    def do_GET(self):
        if self.path == '/':
            self.send_response(200)
            self.send_header("Content-type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(HTML_SPA.encode('utf-8'))
        elif self.path == '/api/state':
            self.send_response(200)
            self.send_header("Content-type", "application/json; charset=utf-8")
            self.end_headers()
            payload = {
                'proposals': self.processor.proposals,
                'root_folders': self.processor.root_folders,
                'clear_cache': self.clear_cache_flag,
                'case_mode': self.processor.case_mode,
            }
            self.wfile.write(json.dumps(payload).encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        content_length = int(self.headers.get('Content-Length', 0))
        post_data = self.rfile.read(content_length) if content_length > 0 else b'{}'
        data = json.loads(post_data.decode('utf-8')) if post_data else {}

        if self.path == '/api/recheck':
            force_all = data.get('force_all', False)
            current_proposals = data.get('proposals', [])
            case_mode = data.get('case_mode', self.processor.case_mode)
            self.processor.generate_proposals(previous_state=None if force_all else current_proposals, case_mode=case_mode)
            self.send_response(200)
            self.send_header("Content-type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps({'proposals': self.processor.proposals, 'case_mode': self.processor.case_mode}).encode('utf-8'))

        elif self.path == '/api/rescan':
            current_proposals = data.get('proposals', [])
            case_mode = data.get('case_mode', self.processor.case_mode)
            self.processor.scan()
            self.processor.generate_proposals(previous_state=current_proposals, case_mode=case_mode)
            self.send_response(200)
            self.send_header("Content-type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps({'proposals': self.processor.proposals, 'case_mode': self.processor.case_mode}).encode('utf-8'))

        elif self.path == '/api/apply':
            items = data.get('items', [])
            applied, errors = self.processor.apply_changes(items, no_save_cache=self.no_save_cache)
            self.send_response(200)
            self.send_header("Content-type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps({'applied': applied, 'errors': errors}).encode('utf-8'))
            threading.Thread(target=self.server.shutdown).start()

        elif self.path == '/api/shutdown':
            self.send_response(200)
            self.end_headers()
            print(f"\n{Color.YELLOW}Operation cancelled by user.{Color.ENDC}")
            threading.Thread(target=self.server.shutdown).start()
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        return


def print_cli_diff_table(proposals):
    """Print proposed changes in terminal with colors and clear diffs."""
    print(f"\n{Color.BOLD}{'TAG':<8} {'ORIGINAL':<34} -> {'PROPOSED':<34} {'FILES':<6}{Color.ENDC}")
    print("=" * 86)
    for p in proposals:
        orig = p['original']
        new_val = p['proposed']
        t = p['type']
        count = p['count']
        warnings = p.get('warnings', [])

        orig_disp = (orig[:31] + '...') if len(orig) > 34 else orig
        new_disp = (new_val[:31] + '...') if len(new_val) > 34 else new_val

        color_tag = Color.HEADER if t == 'Title' else (Color.CYAN if t == 'Album' else Color.YELLOW)
        mod_color = Color.GREEN if orig != new_val else Color.DIM

        print(f"{color_tag}{t:<8}{Color.ENDC} {orig_disp:<34} -> {mod_color}{new_disp:<34}{Color.ENDC} {count:<6}")
        if warnings:
            print(f"  {Color.YELLOW}  Warning: {', '.join(warnings)}{Color.ENDC}")
    print("=" * 86)


def main():
    parser = argparse.ArgumentParser(
        prog="aud-fix-metadata",
        description="Batch capitalize, format, and trim audio file metadata.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  aud-fix-metadata                          # comprehensive scan of current dir with web preview
  aud-fix-metadata ~/Music/Artist           # scan specific directory
  aud-fix-metadata --list ~/Music/          # pick subdirectories interactively
  aud-fix-metadata --dry-run                # preview proposed changes in terminal without modifying
  aud-fix-metadata --no-preview             # run in terminal mode without launching web UI
  aud-fix-metadata --undo                   # revert the last applied batch change
  aud-fix-metadata --clear-cache            # wipe saved drafts and undo logs
  aud-fix-metadata --no-save-cache          # do not save draft cache
        """,
    )
    parser.add_argument("folder", nargs="?", default=None, help="Folder to scan (default: current directory)")
    parser.add_argument("--list", action="store_true", help="List subdirectories and interactively select which to process")
    parser.add_argument("-p", "--preview", action="store_true", default=True, help="Launch interactive web preview (default: enabled)")
    parser.add_argument("--no-preview", "-c", "--cli", action="store_false", dest="preview", help="Run in terminal CLI mode without launching web preview")
    parser.add_argument("--dry-run", action="store_true", help="Print proposed changes in terminal and exit without modifying files")
    parser.add_argument("-nt", "--no-title", action="store_true", help="Skip checking Titles")
    parser.add_argument("-a", "--album", action="store_true", default=True, help="Check Albums (default: enabled)")
    parser.add_argument("--no-album", action="store_true", help="Skip checking Albums")
    parser.add_argument("-at", "--artist", action="store_true", default=True, help="Check Artists (default: enabled)")
    parser.add_argument("--no-artist", action="store_true", help="Skip checking Artists")
    parser.add_argument("-il", "--ignore-lang", action="store_true", default=True, help="Process all languages without langdetect filter (default: enabled)")
    parser.add_argument("--detect-lang", action="store_false", dest="ignore_lang", help="Enable language detection filter to skip non-English")
    parser.add_argument("-sc", "--sentence-case", action="store_true", help="Format tags in Sentence case instead of Title Case")
    parser.add_argument("--undo", action="store_true", help="Revert the most recent batch modification")
    parser.add_argument("--clear-cache", action="store_true", help="Clear saved session drafts and undo logs")
    parser.add_argument("--no-save-cache", "--no-cache", action="store_true", help="Do not save session drafts to disk")
    args = parser.parse_args()

    if args.undo:
        execute_undo()
        return

    if args.clear_cache:
        clear_cache()
        if not args.folder and not args.list and not len(sys.argv) > 2:
            return

    if args.list:
        base = args.folder if args.folder else os.getcwd()
        if not os.path.isdir(base):
            print(f"{Color.RED}Error: '{base}' is not a directory.{Color.ENDC}")
            sys.exit(1)
        chosen = list_and_select(base)
        if not chosen:
            sys.exit(0)
        _run(chosen, args)
        return

    folder = args.folder if args.folder else os.getcwd()
    _run([folder], args)


def _run(folders, args):
    for folder in folders:
        if not os.path.exists(folder):
            print(f"{Color.RED}Error: Folder '{folder}' not found.{Color.ENDC}")
            sys.exit(1)

    check_title = not args.no_title
    check_album = not args.no_album
    check_artist = not args.no_artist

    if not check_title and not check_album and not check_artist:
        print(f"{Color.YELLOW}Warning: No checks enabled. Remove --no-title / --no-album / --no-artist.{Color.ENDC}")
        sys.exit(0)

    case_mode = 'sentence' if getattr(args, 'sentence_case', False) else 'title'
    processor = TitleProcessor(
        folders,
        check_title=check_title,
        check_album=check_album,
        check_artist=check_artist,
        ignore_lang=args.ignore_lang,
        case_mode=case_mode,
    )

    processor.scan()
    count = processor.generate_proposals()

    has_cover_desc = len(processor.files_with_cover_desc) > 0

    if count == 0 and not has_cover_desc:
        print(f"\n{Color.GREEN}No changes needed! Metadata appears correct.{Color.ENDC}")
        return

    modified_count = sum(1 for p in processor.proposals if p['proposed'] != p['original'] or p.get('warnings'))
    cover_desc_note = f", {len(processor.files_with_cover_desc)} with cover descriptions" if has_cover_desc else ""
    print(f"\n{Color.BOLD}Found {count} groups ({modified_count} with proposed improvements{cover_desc_note}).{Color.ENDC}")

    if args.dry_run:
        print_cli_diff_table(processor.proposals)
        if has_cover_desc:
            print(f"\n{Color.YELLOW}[Dry Run] {len(processor.files_with_cover_desc)} file(s) have cover art description metadata to clear.{Color.ENDC}")
        print(f"\n{Color.CYAN}[Dry Run] Exiting without modifying files.{Color.ENDC}")
        return

    if args.preview:
        port = find_available_port(8000, 8050)
        url = f"http://127.0.0.1:{port}"
        PreviewServer.processor = processor
        PreviewServer.no_save_cache = args.no_save_cache
        PreviewServer.clear_cache_flag = args.clear_cache
        server = HTTPServer(('127.0.0.1', port), PreviewServer)
        print(f"{Color.GREEN}Starting web preview at {url}... (Check your browser){Color.ENDC}")
        webbrowser.open(url)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print(f"\n{Color.YELLOW}Server interrupted.{Color.ENDC}")
        finally:
            server.server_close()
    else:
        print_cli_diff_table(processor.proposals)
        if modified_count == 0 and has_cover_desc:
            print(f"\n{Color.YELLOW}No tag changes needed, but found {len(processor.files_with_cover_desc)} file(s) with cover art description metadata.{Color.ENDC}")
        response = input("\nApply these changes automatically? [y/N]: ").strip().lower()
        if response == 'y':
            changes = [
                {
                    'original': p['original'],
                    'type': p['type'],
                    'current': p['proposed'],
                    'files': p['files'],
                    'apply': (p['proposed'] != p['original'] or bool(p.get('warnings'))),
                }
                for p in processor.proposals
            ]
            processor.apply_changes(changes, no_save_cache=args.no_save_cache)
        else:
            print("Cancelled.")


if __name__ == "__main__":
    main()
