// ==UserScript==
// @name         RYM Wishlist Red
// @namespace    https://rateyourmusic.com/
// @version      1.0
// @description  Colors "Wishlist" labels red on RYM artist discography pages so they're easier to tell apart from Collection entries.
// @match        https://rateyourmusic.com/artist/*
// @match        https://rateyourmusic.com/release/*
// @grant        none
// @run-at       document-idle
// ==/UserScript==

(function () {
    'use strict';

    const TARGET_TEXT = 'Wishlist';
    const RED = '#e05353';

    const LABEL_SELECTORS = [
        '.disco_cat_inner',
        '.disco_cat_inner_issue span[id^="disco_cat_catalog_msg_"]',
    ];

    function maybeColor(el) {
        if (el.textContent.trim() === TARGET_TEXT) {
            el.style.setProperty('color', RED, 'important');
        }
    }

    function colorWishlistLabels(root) {
        LABEL_SELECTORS.forEach((selector) => {
            root.querySelectorAll(selector).forEach(maybeColor);
        });
    }

    colorWishlistLabels(document);

    const observer = new MutationObserver((mutations) => {
        for (const mutation of mutations) {
            if (mutation.addedNodes.length) {
                mutation.addedNodes.forEach((node) => {
                    if (node.nodeType === Node.ELEMENT_NODE) {
                        const matchesLabel = LABEL_SELECTORS.some(
                            (selector) => node.matches && node.matches(selector)
                        );
                        if (matchesLabel) {
                            maybeColor(node);
                        } else {
                            colorWishlistLabels(node);
                        }
                    }
                });
            }
        }
    });

    observer.observe(document.body, { childList: true, subtree: true });
})();
