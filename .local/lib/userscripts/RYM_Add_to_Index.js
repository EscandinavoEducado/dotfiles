// ==UserScript==
// @name         RYM: Add to Index
// @namespace    https://rateyourmusic.com/
// @version      1.0
// @description  Adds an "Add to Index" toggle button on release pages. Adds the release to your collection, sets format to Digital (if not already set), and tags it "index".
// @match        https://rateyourmusic.com/release/*
// @run-at       document-idle
// @grant        none
// ==/UserScript==

(function () {
    'use strict';

    const INDEX_TAG = 'index';
    const OWNED_STATUSES = ['o', 'w', 'u'];

    function getCatalogWidgetId() {
        const btn = document.querySelector('[id^="catalog_"][class~="catalog_btn"]');
        if (!btn) return null;
        return btn.id.replace(/^catalog_/, '');
    }

    function getCatalogObject(widgetId) {
        return window['catalog_' + widgetId];
    }

    function isCurrentlyCatalogued(widgetId) {
        const btn = document.getElementById('catalog_' + widgetId);
        if (!btn) return false;
        return OWNED_STATUSES.some((s) => btn.classList.contains('catalog_' + s));
    }

    function getCurrentFormatText(widgetId) {
        const span = document.getElementById('format_text_' + widgetId);
        return span ? span.textContent.trim() : '';
    }

    function getCurrentTags() {
        const input = document.querySelector('input.tag_tags[id^="tags_"]');
        if (!input) return [];
        return input.value
        .split(',')
        .map((t) => t.trim())
        .filter(Boolean);
    }

    function setTagsInputValue(tagsArray) {
        const input = document.querySelector('input.tag_tags[id^="tags_"]');
        if (!input) return null;
        input.value = tagsArray.length ? tagsArray.join(', ') + (tagsArray.length ? ', ' : '') : '';
        return input;
    }

    function hasIndexTag() {
        return getCurrentTags()
        .map((t) => t.toLowerCase())
        .includes(INDEX_TAG.toLowerCase());
    }

    function saveTags() {
        if (window.tags && typeof window.tags.save === 'function') {
            window.tags.save();
        } else {
            console.warn('[AddToIndex] window.tags.save() not available.');
        }
    }

    function addIndexTag() {
        const current = getCurrentTags();
        if (current.map((t) => t.toLowerCase()).includes(INDEX_TAG.toLowerCase())) return;
        setTagsInputValue([...current, INDEX_TAG]);
        saveTags();
    }

    function removeIndexTag() {
        const current = getCurrentTags();
        const filtered = current.filter((t) => t.toLowerCase() !== INDEX_TAG.toLowerCase());
        if (filtered.length === current.length) return;
        setTagsInputValue(filtered);
        saveTags();
    }

    function performAdd(widgetId) {
        const catalogObj = getCatalogObject(widgetId);
        if (!catalogObj) {
            alert('[Add to Index] Could not find the catalog widget on this page.');
            return;
        }

        if (!isCurrentlyCatalogued(widgetId)) {
            catalogObj.setOwnership('o');
        }

        const currentFormat = getCurrentFormatText(widgetId);
        if (currentFormat.toLowerCase() !== 'digital') {
            catalogObj.setFormat('MP3');
        }

        addIndexTag();
    }

    function performRemove(widgetId) {
        const catalogObj = getCatalogObject(widgetId);
        if (!catalogObj) {
            alert('[Add to Index] Could not find the catalog widget on this page.');
            return;
        }

        catalogObj.setOwnership('n');
        removeIndexTag();
    }

    function currentIndexState(widgetId) {
        return isCurrentlyCatalogued(widgetId) && hasIndexTag();
    }

    function updateButtonLabel(btn, widgetId) {
        if (currentIndexState(widgetId)) {
            btn.textContent = 'Remove from Index';
            btn.classList.add('index_added');
            btn.classList.remove('index_removed');
        } else {
            btn.textContent = 'Add to Index';
            btn.classList.add('index_removed');
            btn.classList.remove('index_added');
        }
    }

    function injectButton() {
        const widgetId = getCatalogWidgetId();
        if (!widgetId) return;

        if (document.getElementById('add_to_index_btn')) return;

        const myCatalogSection = document.getElementById('my_catalog');
        const catalogSectionOuter = myCatalogSection
        ? myCatalogSection.closest('.section_my_catalog.section_outer')
        : null;
        const insertionAnchor = catalogSectionOuter || myCatalogSection;
        if (!insertionAnchor || !insertionAnchor.parentNode) return;

        const btn = document.createElement('button');
        btn.id = 'add_to_index_btn';
        btn.style.display = 'block';
        btn.style.margin = '0 0 10px 0';
        btn.style.padding = '.5em 1em';
        btn.style.fontSize = '1em';
        btn.style.fontWeight = 'bold';
        btn.style.cursor = 'pointer';
        btn.style.borderRadius = '4px';
        btn.style.border = '1px solid var(--ui-divider-line, #999)';
        btn.style.background = 'var(--surface-secondary, #eee)';
        btn.style.color = 'var(--text-primary, #222)';

        updateButtonLabel(btn, widgetId);

        btn.addEventListener('click', () => {
            btn.disabled = true;
            const wasIndexed = currentIndexState(widgetId);

            try {
                if (wasIndexed) {
                    performRemove(widgetId);
                } else {
                    performAdd(widgetId);
                }
            } catch (err) {
                console.error('[AddToIndex] Error:', err);
                alert('[Add to Index] Something went wrong — check the console for details.');
            }

            let attempts = 0;
            const poll = setInterval(() => {
                attempts++;
                const nowIndexed = currentIndexState(widgetId);
                if (nowIndexed !== wasIndexed || attempts > 20) {
                    clearInterval(poll);
                    updateButtonLabel(btn, widgetId);
                    btn.disabled = false;
                }
            }, 250);
        });

        insertionAnchor.parentNode.insertBefore(btn, insertionAnchor.nextSibling);
    }

    function init() {
        injectButton();
        let tries = 0;
        const retry = setInterval(() => {
            tries++;
            if (document.getElementById('add_to_index_btn') || tries > 10) {
                clearInterval(retry);
                return;
            }
            injectButton();
        }, 300);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
