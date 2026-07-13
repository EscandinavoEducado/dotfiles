// ==UserScript==
// @name         RYM List: Duplicate Check + Sort Position Helper
// @namespace    https://rateyourmusic.com/
// @version      1.0
// @description  On the "add item to list" page, warns if the item is already on the list and suggests where it should go alphabetically. Colors follow RYM's active theme.
// @match        https://rateyourmusic.com/lists/new_item_a?*
// @run-at       document-idle
// @grant        none
// ==/UserScript==

(function () {
    'use strict';

    function parseQuery() {
        const params = new URLSearchParams(location.search);
        return {
            type: params.get('type'),
 assocId: params.get('assoc_id'),
 listId: params.get('list_id'),
        };
    }

    function extractNameAndTranslit(anchorEl) {
        const clone = anchorEl.cloneNode(true);
        const subtextEl = clone.querySelector('.subtext');
        let translit = null;
        if (subtextEl) {
            translit = subtextEl.textContent.trim().replace(/^\[/, '').replace(/\]$/, '').trim();
            subtextEl.remove();
        }
        const name = clone.textContent.trim();
        return { name, translit };
    }

    function idFromTitle(titleAttr) {
        if (!titleAttr) return null;
        const m = titleAttr.match(/\[[A-Za-z]+(\d+)\]/);
        return m ? m[1] : null;
    }

    function buildEntries() {
        const container = document.querySelector('div[style*="height:500px"][style*="overflow:auto"]');
        if (!container) return null;
        const table = container.querySelector('table.mbgen');
        if (!table) return null;

        const rows = table.querySelectorAll('tr');
        const entries = [];
        let lastPlaceholderId = null;

        rows.forEach((tr) => {
            const td = tr.querySelector('td');
            if (!td) return;

            if (td.id && td.id.startsWith('insert')) {
                lastPlaceholderId = td.id.replace('insert', '');
                return;
            }

            const a = td.querySelector('a[title]');
            if (!a) return;

            const id = idFromTitle(a.getAttribute('title'));
            const { name, translit } = extractNameAndTranslit(a);

            entries.push({
                id,
                name,
                translit,
                beforeId: lastPlaceholderId,
            });
        });

        return entries;
    }

    function sortKey(entry) {
        return (entry.translit || entry.name || '').trim();
    }

    function formatEntryLabel(entry) {
        return entry.translit ? `${entry.name} [${entry.translit}]` : entry.name;
    }

    function getNewItemInfo() {
        const h2Anchor = document.querySelector('#infobox h2 a');
        if (!h2Anchor) return null;
        return extractNameAndTranslit(h2Anchor);
    }

    function escapeHtml(str) {
        return (str || '').replace(/[&<>"']/g, (c) => (
            { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]
        ));
    }


    function injectPanel(html) {
        let target = null;
        document.querySelectorAll('.medium').forEach((div) => {
            if (div.querySelector('#description_step')) target = div;
        });
            if (!target) return null;

            target.style.display = 'inline-block';
        target.style.verticalAlign = 'top';

        const panel = document.createElement('div');
        panel.id = 'rym-helper-panel';
        panel.style.display = 'inline-block';
        panel.style.verticalAlign = 'top';
        panel.style.marginLeft = '20px';
        panel.style.padding = '8px 12px';
        panel.style.border = '1px solid var(--ui-divider-line, #999)';
        panel.style.borderRadius = '4px';
        panel.style.fontSize = '12px';
        panel.style.lineHeight = '1.4';
        panel.style.maxWidth = '420px';
        panel.style.background = 'var(--surface-secondary, #fafafa)';
        panel.style.color = 'var(--text-primary, #222)';
        panel.innerHTML = html;

        target.insertAdjacentElement('afterend', panel);
        return panel;
    }


    function main() {
        const { assocId } = parseQuery();
        if (!assocId) return;

        const entries = buildEntries();
        if (!entries) return;

        const dup = entries.find((e) => e.id === assocId);

        const newItem = getNewItemInfo();
        const newKey = newItem ? sortKey(newItem) : null;

        let html = '';
        if (dup) {
            html += `<div style="color:var(--alert-error-text,#fff);background:var(--alert-error-background,#b00000);font-weight:bold;padding:4px 8px;border-radius:3px;margin-bottom:6px;">&#9888; Possible duplicate</div>`;
            html += `<div style="margin-bottom:8px;">Already appears to be on this list as <b>${escapeHtml(formatEntryLabel(dup))}</b>.</div>`;
        } else {
            html += `<div style="color:#14401c;background:var(--alert-success-background,#207020);font-weight:bold;padding:4px 8px;border-radius:3px;margin-bottom:6px;">&#10003; No duplicate found</div>`;
        }

        let placeId = null;

        if (newKey) {
            const collator = new Intl.Collator(undefined, { numeric: true, sensitivity: 'base' });
            let suggestion = null;

            for (let i = 0; i < entries.length; i++) {
                const k = sortKey(entries[i]);
                if (!k) continue;
                if (collator.compare(newKey, k) < 0) {
                    suggestion = { index: i };
                    break;
                }
            }

            let beforeLabel = null;
            let afterLabel = null;

            if (suggestion) {
                beforeLabel = formatEntryLabel(entries[suggestion.index]);
                afterLabel = suggestion.index > 0 ? formatEntryLabel(entries[suggestion.index - 1]) : null;
                placeId = entries[suggestion.index].beforeId;
            } else if (entries.length) {
                afterLabel = formatEntryLabel(entries[entries.length - 1]);
                placeId = '0';
            }

            html += `<div style="margin-top:8px;">`;
            html += `<div style="font-weight:bold;margin-bottom:4px;color:var(--text-primary,#222);">Suggested position <span style="font-weight:normal;color:var(--text-secondary,#666);">(sorting by "${escapeHtml(newKey)}")</span>:</div>`;
            if (afterLabel && beforeLabel) {
                html += `<div>Between <b>${escapeHtml(afterLabel)}</b> and <b>${escapeHtml(beforeLabel)}</b></div>`;
            } else if (beforeLabel) {
                html += `<div>Before <b>${escapeHtml(beforeLabel)}</b> (start of list)</div>`;
            } else if (afterLabel) {
                html += `<div>After <b>${escapeHtml(afterLabel)}</b> (end of list)</div>`;
            } else {
                html += `<div>List appears empty.</div>`;
            }

            if (placeId !== null) {
                html += `<div style="margin-top:6px;"><a href="javascript:void(0)" id="rym-helper-place-btn" style="font-weight:bold;color:var(--link-text-default,#005);">&#10148; Place here</a></div>`;
            }
            html += `<div style="margin-top:6px;font-size:0.9em;color:var(--text-secondary,#777);">Note: non-Latin scripts, decorative symbols, and surname-first conventions are approximated.</div>`;
            html += `</div>`;
        }

        injectPanel(html);

        const btn = document.getElementById('rym-helper-place-btn');
        if (btn && placeId !== null) {
            btn.addEventListener('click', () => {
                if (typeof window.setOrder === 'function') {
                    window.setOrder(placeId);
                    btn.textContent = '✓ Placement set';
                    btn.style.color = 'var(--alert-success-background, #207020)';
                }
            });
        }
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', main);
    } else {
        main();
    }
})();
