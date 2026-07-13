// ==UserScript==
// @name         RYM List: Auto-Alphabetize
// @namespace    https://rateyourmusic.com/
// @version      1.0
// @description  Adds buttons on the RYM "reorder list" page to sort items alphabetically (using bracket transliterations where present), either the whole list, or just a range you click to select. Nothing is saved until you press the page's own "Save Order" button.
// @match        https://rateyourmusic.com/lists/reorder_list*
// @run-at       document-idle
// @grant        none
// ==/UserScript==

(function () {
    'use strict';

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

    function sortKey(entry) {
        return (entry.translit || entry.name || '').trim();
    }

    function buildItems() {
        const boxes = document.getElementById('boxes');
        if (!boxes) return null;
        const lis = Array.from(boxes.children).filter(
            (el) => el.tagName === 'LI' && el.classList.contains('box')
        );
        return lis.map((li) => {
            const a = li.querySelector('a[title]');
            const { name, translit } = a ? extractNameAndTranslit(a) : { name: '', translit: null };
            return { li, name, translit };
        });
    }

    const collator = new Intl.Collator(undefined, { numeric: true, sensitivity: 'base' });

    function sortSlice(itemSlice) {
        return itemSlice.slice().sort((a, b) => collator.compare(sortKey(a), sortKey(b)));
    }

    function reportChanges(before, after) {
        const beforeIds = before.map((it) => it.li.id);
        const afterIds = after.map((it) => it.li.id);
        return beforeIds.filter((id, i) => id !== afterIds[i]).length;
    }


    function alphabetize() {
        const boxes = document.getElementById('boxes');
        const items = buildItems();
        if (!boxes || !items || !items.length) {
            alert('Could not find the list on this page.');
            return;
        }

        const sorted = sortSlice(items);
        const changedCount = reportChanges(items, sorted);

        sorted.forEach((it) => boxes.appendChild(it.li));

        alert(
            `Sorted ${sorted.length} items — ${changedCount} changed position.\n\n` +
            `Nothing has been saved yet. Scroll through the list to sanity-check ` +
            `anything unusual (symbols, non-Latin names, translits), then click ` +
            `"Save Order" to commit, or just reload the page to discard.`
        );
    }


    let rangeMode = false;
    let rangeSelection = [];

    const HIGHLIGHT_BG = '#fff3a0';

    function clearHighlights() {
        rangeSelection.forEach((li) => {
            li.style.outline = '';
            li.style.backgroundColor = '';
        });
    }

    function resetRangeState() {
        clearHighlights();
        rangeSelection = [];
    }

    function boxesClickHandler(e) {
        if (!rangeMode) return;
        const li = e.target.closest('li.box');
        if (!li) return;

        e.preventDefault();
        e.stopPropagation();
        e.stopImmediatePropagation();

        const idx = rangeSelection.indexOf(li);
        if (idx !== -1) {
            li.style.outline = '';
            li.style.backgroundColor = '';
            rangeSelection.splice(idx, 1);
            updateRangeStatus();
            return;
        }

        if (rangeSelection.length >= 2) return;

        li.style.outline = '3px solid #e69500';
        li.style.backgroundColor = HIGHLIGHT_BG;
        rangeSelection.push(li);
        updateRangeStatus();

        if (rangeSelection.length === 2) {
            runRangeSort();
        }
    }

    let statusEl = null;

    function updateRangeStatus() {
        if (!statusEl) return;
        if (rangeSelection.length === 0) {
            statusEl.textContent = 'Range mode: click the first item of the range you want alphabetized.';
        } else if (rangeSelection.length === 1) {
            statusEl.textContent = 'Range mode: now click the last item of the range.';
        }
    }

    function runRangeSort() {
        const boxes = document.getElementById('boxes');
        const items = buildItems();
        if (!boxes || !items || !items.length) {
            alert('Could not find the list on this page.');
            exitRangeMode();
            return;
        }

        const [liA, liB] = rangeSelection;
        const idxA = items.findIndex((it) => it.li === liA);
        const idxB = items.findIndex((it) => it.li === liB);

        if (idxA === -1 || idxB === -1) {
            alert('Could not locate the selected items — please try again.');
            exitRangeMode();
            return;
        }

        const start = Math.min(idxA, idxB);
        const end = Math.max(idxA, idxB);

        const before = items.slice(start, end + 1);
        const sorted = sortSlice(before);
        const changedCount = reportChanges(before, sorted);

        liA.style.outline = '';
        liA.style.backgroundColor = '';
        liB.style.outline = '';
        liB.style.backgroundColor = '';

        const anchorLi = items[end + 1] ? items[end + 1].li : null;
        sorted.forEach((it) => {
            boxes.insertBefore(it.li, anchorLi);
        });

        rangeSelection = [];
        exitRangeMode();

        alert(
            `Sorted items ${start + 1}–${end + 1} (${sorted.length} items) — ${changedCount} changed position.\n\n` +
            `Everything outside that range was left untouched. Nothing has been ` +
            `saved yet — sanity-check the range, then click "Save Order" to commit, ` +
            `or reload the page to discard.`
        );
    }

    function enterRangeMode() {
        rangeMode = true;
        rangeSelection = [];
        const boxes = document.getElementById('boxes');
        if (boxes) {
            boxes.addEventListener('click', boxesClickHandler, true);
            boxes.style.cursor = 'crosshair';
        }
        if (statusEl) {
            statusEl.style.display = 'inline';
            updateRangeStatus();
        }
        if (rangeBtn) rangeBtn.textContent = 'Cancel range selection';
    }

    function exitRangeMode() {
        rangeMode = false;
        const boxes = document.getElementById('boxes');
        if (boxes) {
            boxes.removeEventListener('click', boxesClickHandler, true);
            boxes.style.cursor = '';
        }
        clearHighlights();
        rangeSelection = [];
        if (statusEl) statusEl.style.display = 'none';
        if (rangeBtn) rangeBtn.textContent = 'Sort range only…';
    }

    function toggleRangeMode() {
        if (rangeMode) {
            exitRangeMode();
        } else {
            enterRangeMode();
        }
    }


    let rangeBtn = null;

    function injectButtons() {
        const btn = document.createElement('button');
        btn.textContent = 'Auto-alphabetize list';
        btn.style.fontSize = 'large';
        btn.style.marginLeft = '12px';
        btn.style.cursor = 'pointer';
        btn.addEventListener('click', alphabetize);

        rangeBtn = document.createElement('button');
        rangeBtn.textContent = 'Sort range only…';
        rangeBtn.style.fontSize = 'large';
        rangeBtn.style.marginLeft = '12px';
        rangeBtn.style.cursor = 'pointer';
        rangeBtn.addEventListener('click', toggleRangeMode);

        statusEl = document.createElement('span');
        statusEl.style.marginLeft = '12px';
        statusEl.style.fontStyle = 'italic';
        statusEl.style.display = 'none';

        const boxOutline = document.getElementById('boxoutline');
        const instructions = boxOutline
        ? boxOutline.previousElementSibling
        : document.querySelector('p');

        if (instructions && instructions.tagName === 'P') {
            instructions.appendChild(btn);
            instructions.appendChild(rangeBtn);
            instructions.appendChild(statusEl);
        } else {
            document.body.insertBefore(btn, document.body.firstChild);
            document.body.insertBefore(rangeBtn, document.body.firstChild.nextSibling);
            document.body.insertBefore(statusEl, rangeBtn.nextSibling);
        }
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', injectButtons);
    } else {
        injectButtons();
    }
})();
