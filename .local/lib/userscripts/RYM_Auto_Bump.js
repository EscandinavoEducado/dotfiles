// ==UserScript==
// @name         RateYourMusic Auto-Bump
// @namespace    https://rateyourmusic.com/
// @version      1.0
// @description  Automatically clicks bump buttons and confirms the popup on RYM
// @match        https://rateyourmusic.com/*
// @grant        none
// @run-at       document-start
// ==/UserScript==

(function () {
    'use strict';

    window.confirm = function (message) {
        console.log('[AutoBump] confirm() intercepted:', message);
        return true;
    };

    function tryBump(btn) {
        if (
            btn.classList.contains('bumpable') &&
            btn.style.display === 'block'
        ) {
            console.log('[AutoBump] Bumping:', btn.id);
            btn.click();
        }
    }

    const observer = new MutationObserver(mutations => {
        for (const mutation of mutations) {
            if (
                mutation.type === 'attributes' &&
                (mutation.attributeName === 'style' || mutation.attributeName === 'class') &&
                mutation.target.classList.contains('bump_btn')
            ) {
                tryBump(mutation.target);
            }

            for (const node of mutation.addedNodes) {
                if (node.nodeType !== Node.ELEMENT_NODE) continue;
                if (node.classList.contains('bump_btn')) tryBump(node);
                node.querySelectorAll?.('.bump_btn').forEach(tryBump);
            }
        }
    });

    observer.observe(document.documentElement, {
        childList: true,
        subtree: true,
        attributes: true,
        attributeFilter: ['style', 'class'],
    });

})();
