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
aud-fix-metadata — Batch capitalize and trim audio file metadata.

Usage:
    aud-fix-metadata                        # scan current directory
    aud-fix-metadata ~/Music/Artist
    aud-fix-metadata --list                 # pick subdirs interactively, then fix
    aud-fix-metadata --list ~/Music/        # pick from ~/Music subdirs
    aud-fix-metadata -p ~/Music/            # interactive web preview
    aud-fix-metadata -a -at ~/Music/        # also check albums and artists
    aud-fix-metadata -il ~/Music/           # process non-English tags too
"""

import os
import sys
import json
import re
import html
import shutil
import threading
import webbrowser
import argparse
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
    """
    Parse a selection string like "1 3 5-8 10" into a sorted list of
    0-based indices.  Returns an empty list if any token is invalid.
    """
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

    num_w    = len(str(len(entries)))
    col_w    = max(len(e.name) for e in entries) + num_w + 4
    n_cols   = max(1, min(2, shutil.get_terminal_size(fallback=(80, 24)).columns // col_w))
    n_rows   = -(-len(entries) // n_cols)   # ceiling division

    for row in range(n_rows):
        line = ""
        for col in range(n_cols):
            idx = row + col * n_rows
            if idx >= len(entries):
                break
            label = f"{Color.CYAN}{idx + 1:>{num_w}}{Color.ENDC}  {Color.BOLD}{entries[idx].name}{Color.ENDC}"
            # Pad with raw (non-ANSI) width so columns stay aligned
            pad = col_w - (num_w + 2 + len(entries[idx].name))
            line += label + " " * pad
        print("  " + line)


def list_and_select(base):
    """
    Show a numbered grid of subdirectories under *base*, prompt for
    selection, and return the chosen directory paths as strings.
    """
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
        # parse_selection already printed the error; loop to re-prompt


def capitalize_roman_numerals(text):
    ROMAN_EXCEPTIONS = {'mix'}

    candidate_pattern = r"\b(?<!')[mdclxvi]+(?!')\b"

    validator = re.compile(r"""
        ^
        M{0,3}
        (CM|CD|D?C{0,3})?
        (XC|XL|L?X{0,3})?
        (IX|IV|V?I{0,3})?
        $
    """, re.VERBOSE | re.IGNORECASE)

    def replacer(match):
        word = match.group(0)
        if word.lower() in ROMAN_EXCEPTIONS:
            return word
        if validator.match(word):
            return word.upper()
        return word

    return re.sub(candidate_pattern, replacer, text, flags=re.IGNORECASE)


def ignore_is_verb(word, **kwargs):
    if word.lower() == 'is':
        return 'is'
    return None


def smart_format_text(text, ignore_language_filter=False):
    if not text:
        return text, False

    cleaned_text = text.strip()

    if cleaned_text.lower() in ('untitled', '[untitled]'):
        return '[untitled]', True

    try:
        is_english = True
        if len(cleaned_text.split()) > 2:
            try:
                lang = detect(cleaned_text)
                if lang != 'en':
                    is_english = False
            except:
                pass

        should_process = is_english or ignore_language_filter

        if should_process:
            formatted = titlecase(cleaned_text, callback=ignore_is_verb)

            if formatted:
                formatted = formatted[0].upper() + formatted[1:]

            def fix_slash_capitalization(m):
                return m.group(1) + m.group(2) + m.group(3).upper()

            formatted = re.sub(r'(/)(\s*)([a-z])', fix_slash_capitalization, formatted)
            formatted = capitalize_roman_numerals(formatted)
            return formatted, is_english

        return cleaned_text, is_english
    except Exception:
        return cleaned_text, True


class TitleProcessor:
    def __init__(self, root_folders, check_title=True, check_album=False, check_artist=False, ignore_lang=False):
        # Accept either a single path string or a list of paths
        if isinstance(root_folders, (str, bytes)):
            root_folders = [root_folders]
        self.root_folders = list(root_folders)
        self.check_title  = check_title
        self.check_album  = check_album
        self.check_artist = check_artist
        self.ignore_lang  = ignore_lang

        self.groups = {
            'Title':  defaultdict(list),
            'Album':  defaultdict(list),
            'Artist': defaultdict(list),
        }
        self.proposals = []

    def scan(self):
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
                                return audio.get(tag_name, [None])[0]

                            if self.check_title:
                                val = get_tag('title')
                                if val: self.groups['Title'][val].append(path)

                            if self.check_album:
                                val = get_tag('album')
                                if val: self.groups['Album'][val].append(path)

                            if self.check_artist:
                                val = get_tag('artist')
                                if val: self.groups['Artist'][val].append(path)

                        except Exception as e:
                            print(f"{Color.RED}Error reading {f}: {e}{Color.ENDC}")

    def generate_proposals(self):
        print(f"{Color.CYAN}Analyzing metadata and checking for whitespace...{Color.ENDC}")
        self.proposals = []

        for tag_type, group_dict in self.groups.items():
            for original, file_paths in group_dict.items():
                new_text, is_english = smart_format_text(original, self.ignore_lang)

                if new_text != original or tag_type == 'Artist' or self.ignore_lang:
                    self.proposals.append({
                        'id':         hash(original + tag_type),
                        'type':       tag_type,
                        'original':   original,
                        'new':        new_text,
                        'files':      file_paths,
                        'count':      len(file_paths),
                        'is_english': is_english,
                    })

        self.proposals.sort(key=lambda x: (x['type'], x['original']))
        return len(self.proposals)

    def apply_changes(self, change_list):
        count = 0
        print(f"\n{Color.HEADER}--- Applying Changes ---{Color.ENDC}")

        for item in change_list:
            if not item.get('apply'):
                continue

            original = item['original']
            new_text = item['new'].strip()
            tag_type = item.get('type')

            if not new_text or new_text == original:
                continue

            paths = self.groups[tag_type].get(original, [])

            for path in paths:
                try:
                    audio = MutagenFile(path, easy=True)
                    audio[tag_type.lower()] = new_text
                    audio.save()
                except Exception as e:
                    print(f"{Color.RED}Failed to update {os.path.basename(path)}: {e}{Color.ENDC}")

            count += 1
            print(f"[{Color.GREEN}OK{Color.ENDC}] {tag_type}: '{original}' -> '{Color.BOLD}{new_text}{Color.ENDC}'")

        print(f"\n{Color.GREEN}Done. Updated {count} groups.{Color.ENDC}")


HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Metadata Fixer Preview</title>
    <style>
        :root { --bg: #121212; --surface: #1e1e1e; --primary: #bb86fc; --text: #e0e0e0; --border: #333; }
        body { background: var(--bg); color: var(--text); font-family: sans-serif; margin: 0; padding: 20px; }
        h1 { color: var(--primary); }
        .controls { position: sticky; top: 0; background: var(--bg); padding: 15px 0; border-bottom: 1px solid var(--border); display: flex; justify-content: space-between; align-items: center; z-index: 10; }
        button { background: var(--primary); border: none; padding: 10px 20px; color: #000; font-weight: bold; cursor: pointer; border-radius: 4px; }
        button:hover { opacity: 0.9; }
        button.cancel { background: #cf6679; }

        .grid { display: grid; grid-template-columns: 40px 80px 1fr 1fr 60px; gap: 10px; margin-top: 20px; align-items: stretch; }
        .header { font-weight: bold; color: #888; border-bottom: 1px solid #444; padding-bottom: 5px; }

        .row { display: contents; }
        .row:hover .cell { background: #2c2c2c; }
        .cell { padding: 10px; background: var(--surface); border-radius: 4px; display: flex; align-items: center; word-break: break-word; min-width: 0; }

        input[type="text"], textarea.new-input { width: 100%; background: #333; border: 1px solid #555; color: #fff; padding: 5px; border-radius: 3px; box-sizing: border-box; min-width: 0; font-family: inherit; font-size: inherit; resize: none; overflow: hidden; }
        input[type="checkbox"] { transform: scale(1.5); accent-color: var(--primary); cursor: pointer; margin: 0 auto; }

        .badge { background: #333; padding: 4px 8px; border-radius: 4px; font-size: 0.75em; color: #aaa; font-weight: bold; text-transform: uppercase; width: 50px; text-align: center; display: inline-block; }

        .tag-title  { color: #bb86fc; background: rgba(187, 134, 252, 0.1); }
        .tag-album  { color: #03dac6; background: rgba(3, 218, 198, 0.1); }
        .tag-artist { color: #ffb74d; background: rgba(255, 183, 77, 0.1); }

        .whitespace-warn { border: 1px solid #cf6679; }
        .no-change { opacity: 0.5; }
        .count-badge { background: #444; color: #fff; padding: 2px 6px; border-radius: 10px; font-size: 0.8em; margin: 0 auto; }
        .lang-flag { font-size: 0.7em; margin-left: 5px; color: #777; }
    </style>
</head>
<body>
    <div class="controls">
        <div>
            <h1>Proposed Changes</h1>
            <span id="stats">Found {{TOTAL}} groups to review.</span>
        </div>
        <div style="gap: 10px; display: flex;">
            <button class="cancel" onclick="shutdown()">Cancel</button>
            <button onclick="submitChanges()">APPLY CHANGES</button>
        </div>
    </div>

    <div class="grid">
        <div class="header" style="text-align: center;">
            <input type="checkbox" onclick="toggleAll(this)" title="Check/Uncheck All">
        </div>
        <div class="header">Tag</div>
        <div class="header">Original</div>
        <div class="header">New (Editable)</div>
        <div class="header" style="text-align: center;">Files</div>

        {{ROWS}}
    </div>

    <script>
        function toggleAll(source) {
            const checkboxes = document.querySelectorAll('.apply-cb');
            checkboxes.forEach(cb => cb.checked = source.checked);
        }

        function submitChanges() {
            const rows = document.querySelectorAll('.data-row');
            const payload = [];

            rows.forEach(row => {
                const original = row.dataset.original;
                const tagType = row.dataset.type;
                const newText = row.querySelector('.new-input').value;
                const checked = row.querySelector('.apply-cb').checked;

                payload.push({
                    original: original,
                    type: tagType,
                    new: newText,
                    apply: checked
                });
            });

            fetch('/apply', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(payload)
            }).then(res => {
                document.body.innerHTML = "<div style='text-align:center; padding:50px;'><h1>Changes Applied!</h1><p>You can close this tab and return to the terminal.</p></div>";
            });
        }

        function shutdown() {
            fetch('/shutdown', {method: 'POST'}).then(() => {
                window.close();
            });
        }

        // Auto-resize all textareas to fit their initial content
        document.querySelectorAll('textarea.new-input').forEach(ta => {
            ta.style.height = 'auto';
            ta.style.height = ta.scrollHeight + 'px';
        });
    </script>
</body>
</html>
"""


class PreviewServer(BaseHTTPRequestHandler):
    processor = None

    def do_GET(self):
        if self.path == '/':
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()

            rows_html = ""
            for p in self.processor.proposals:
                t          = p['type']
                is_english = p.get('is_english', True)
                tag_class  = "tag-title" if t == 'Title' else ("tag-album" if t == 'Album' else "tag-artist")
                warn_style = "whitespace-warn" if p['original'] != p['original'].strip() else ""

                is_checked = "checked"
                if t == 'Artist':
                    is_checked = ""
                elif p['original'] == p['new']:
                    is_checked = ""
                elif not is_english and not self.processor.ignore_lang:
                    is_checked = ""

                lang_indicator = "" if is_english else "<span class='lang-flag'>(Non-EN)</span>"

                rows_html += f"""
                <div class="row data-row" data-original="{html.escape(p['original'])}" data-type="{p['type']}">
                    <div class="cell">
                        <input type="checkbox" class="apply-cb" {is_checked}>
                    </div>
                    <div class="cell">
                        <span class="badge {tag_class}">{p['type']}</span>
                    </div>
                    <div class="cell {warn_style}" title="Original Text">
                        <span style="opacity: 0.7; white-space: pre-wrap;">{html.escape(p['original'])}</span>
                        {lang_indicator}
                    </div>
                    <div class="cell">
                        <textarea
                               class="new-input"
                               rows="1"
                               oninput="this.closest('.row').querySelector('.apply-cb').checked = true; this.style.height='auto'; this.style.height=this.scrollHeight+'px';"
                               >{html.escape(p['new'])}</textarea>
                    </div>
                    <div class="cell">
                        <span class="count-badge">{p['count']}</span>
                    </div>
                </div>
                """

            output = HTML_TEMPLATE.replace('{{ROWS}}', rows_html)
            output = output.replace('{{TOTAL}}', str(len(self.processor.proposals)))
            self.wfile.write(output.encode('utf-8'))

    def do_POST(self):
        if self.path == '/apply':
            content_length = int(self.headers['Content-Length'])
            post_data      = self.rfile.read(content_length)
            data           = json.loads(post_data)

            self.processor.apply_changes(data)

            self.send_response(200)
            self.end_headers()
            threading.Thread(target=self.server.shutdown).start()

        elif self.path == '/shutdown':
            self.send_response(200)
            self.end_headers()
            print(f"\n{Color.YELLOW}Operation cancelled by user.{Color.ENDC}")
            threading.Thread(target=self.server.shutdown).start()

    def log_message(self, format, *args):
        return


def main():
    parser = argparse.ArgumentParser(
        prog="aud-fix-metadata",
        description="Batch capitalize and trim audio file metadata.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  aud-fix-metadata                          # current directory
  aud-fix-metadata ~/Music/Artist
  aud-fix-metadata --list                   # pick subdirs interactively
  aud-fix-metadata --list ~/Music/          # pick from ~/Music subdirs
  aud-fix-metadata -p ~/Music/              # interactive web preview
  aud-fix-metadata -a -at ~/Music/          # also check albums and artists
  aud-fix-metadata -il ~/Music/             # process non-English tags too
        """,
    )
    parser.add_argument("folder", nargs="?", default=None, help="Folder to scan (or base for --list)")
    parser.add_argument("--list", action="store_true", help="List subdirectories and interactively select which to process")
    parser.add_argument("-p",  "--preview",     action="store_true", help="Open interactive web preview")
    parser.add_argument("-nt", "--no-title",    action="store_true", help="Skip checking Titles")
    parser.add_argument("-a",  "--album",       action="store_true", help="Check Albums")
    parser.add_argument("-at", "--artist",      action="store_true", help="Check Artists (shows all for review)")
    parser.add_argument("-il", "--ignore-lang", action="store_true", help="Show all files and process non-English tags")
    args = parser.parse_args()

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

    if not check_title and not args.album and not args.artist:
        print(f"{Color.YELLOW}Warning: No checks enabled. Use -a or -at, or remove -nt.{Color.ENDC}")
        sys.exit(0)

    processor = TitleProcessor(
        folders,
        check_title=check_title,
        check_album=args.album,
        check_artist=args.artist,
        ignore_lang=args.ignore_lang,
    )

    processor.scan()
    count = processor.generate_proposals()

    if count == 0:
        print(f"\n{Color.GREEN}No changes needed! Metadata appears correct.{Color.ENDC}")
        return

    print(f"\n{Color.BOLD}Found {count} groups to review.{Color.ENDC}")

    if args.preview:
        print(f"{Color.GREEN}Starting web preview... (Check your browser){Color.ENDC}")
        PreviewServer.processor = processor
        server = HTTPServer(('localhost', 8000), PreviewServer)
        webbrowser.open("http://localhost:8000")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        server.server_close()
    else:
        print(f"{Color.YELLOW}Use -p or --preview to see details and edit specific items.{Color.ENDC}")
        response = input("Apply these changes automatically? [y/N]: ").lower()
        if response == 'y':
            changes = [{'original': p['original'], 'type': p['type'], 'new': p['new'], 'apply': True} for p in processor.proposals]
            processor.apply_changes(changes)
        else:
            print("Cancelled.")


if __name__ == "__main__":
    main()
