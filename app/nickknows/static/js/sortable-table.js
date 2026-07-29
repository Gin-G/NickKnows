// Click-to-sort for any <table class="sortable">. Header cells opt in with
// data-sort="num" | "text". Cells may carry a data-val for the sort key
// (falls back to visible text). Reusable across NFL pages.
(function () {
    function sortTable(table, th) {
        const headerRow = th.parentNode;
        const colIndex = Array.prototype.indexOf.call(headerRow.children, th);
        const type = th.getAttribute('data-sort') || 'text';
        const tbody = table.tBodies[0];
        if (!tbody) return;
        const asc = !th.classList.contains('sort-asc');

        headerRow.querySelectorAll('th').forEach(function (h) {
            h.classList.remove('sort-asc', 'sort-desc');
        });
        th.classList.add(asc ? 'sort-asc' : 'sort-desc');

        const rows = Array.prototype.slice.call(tbody.rows);
        rows.sort(function (a, b) {
            const ca = a.cells[colIndex], cb = b.cells[colIndex];
            let x = ca ? (ca.getAttribute('data-val') ?? ca.innerText.trim()) : '';
            let y = cb ? (cb.getAttribute('data-val') ?? cb.innerText.trim()) : '';
            if (type === 'num') {
                x = parseFloat(x); y = parseFloat(y);
                if (isNaN(x)) x = -Infinity;
                if (isNaN(y)) y = -Infinity;
                return asc ? x - y : y - x;
            }
            return asc ? String(x).localeCompare(String(y)) : String(y).localeCompare(String(x));
        });
        rows.forEach(function (r) { tbody.appendChild(r); });
    }

    document.querySelectorAll('table.sortable').forEach(function (table) {
        table.querySelectorAll('thead th[data-sort]').forEach(function (th) {
            th.classList.add('sortable-th');
            th.addEventListener('click', function () { sortTable(table, th); });
        });
    });
})();
