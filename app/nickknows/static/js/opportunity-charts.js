/* Weekly opportunity charts.

   Everything drawn here, including which way a player is trending, comes from
   NFL-API GET /opportunities/weekly/. This file only draws it: the explorer on
   /NFL/Opportunities/Trends, and one player's charts on his stats page.

   Player names are data: they reach the DOM through textContent only. */
window.OppCharts = (function () {
    'use strict';

    var SVG = 'http://www.w3.org/2000/svg';
    var NOUN = { opportunities: 'opportunities', targets: 'targets',
                 carries: 'rush attempts', pass_attempts: 'pass attempts' };
    var TITLE = { opportunities: 'Opportunities', targets: 'Targets',
                  carries: 'Rush attempts', pass_attempts: 'Pass attempts' };
    var WORDS = { up: 'Up', down: 'Down', steady: 'Steady', too_few_games: 'Too few games',
                  interrupted: 'Back from absence' };
    // Less than one a game of the charted thing is a row of zeros, not a chart.
    var MIN_PER_GAME = 1;

    function el(tag, attrs, text) {
        var node = document.createElement(tag);
        for (var k in attrs || {}) node.setAttribute(k, attrs[k]);
        if (text != null) node.textContent = text;
        return node;
    }
    function svg(tag, attrs) {
        var node = document.createElementNS(SVG, tag);
        for (var k in attrs || {}) node.setAttribute(k, attrs[k]);
        return node;
    }
    function fmt(v) { return v == null ? '–' : (Math.round(v * 10) / 10).toString(); }
    function signed(v) { return (v > 0 ? '+' : v < 0 ? '−' : '') + fmt(Math.abs(v)); }
    function niceMax(v) {
        var steps = [4, 6, 8, 10, 12, 16, 20, 24, 28, 32, 36, 40, 50, 60, 80];
        for (var i = 0; i < steps.length; i++) if (v <= steps[i]) return steps[i];
        return Math.ceil(v / 20) * 20;
    }
    function seriesMax(p, metric) {
        return Math.max.apply(null, p.metrics[metric].values.map(function (v) { return v || 0; }));
    }

    function tooltip() {
        var tip = document.getElementById('opp-tip');
        if (!tip) {
            tip = el('div', { id: 'opp-tip', 'class': 'opp-tip', role: 'status' });
            document.body.appendChild(tip);
        }
        return tip;
    }

    function ruleText(rule) {
        var mc = rule.min_change || {};
        return 'How the trend is called: a player’s last ' + (rule.recent_games || 3) +
            ' games against the games before them. Up or down needs a change of at least ' +
            fmt(mc.opportunities) + ' opportunities a game (' + fmt(mc.targets) +
            ' for targets or rush attempts, ' + fmt(mc.pass_attempts) + ' for pass attempts) ' +
            'that is also bigger than his own week-to-week swings; anything less is Steady, and under ' +
            (rule.min_games || 5) + ' games there is no call. Weeks he did not play are skipped, not ' +
            'counted as zero, and if his last ' + (rule.recent_games || 3) + ' games reach back more than ' +
            (rule.recent_span_weeks || 5) + ' weeks he is marked back from an absence instead of given a ' +
            'direction. Opportunities are targets plus rush attempts, or pass attempts plus rush attempts ' +
            'for a quarterback. This describes what has happened — it is not a projection. Measured on ' +
            '2026 weeks 1–9, the game after a call kept roughly half of the change in opportunities and ' +
            'targets and about three quarters in rush attempts.';
    }

    function badge(m) {
        if (m.direction === 'up') return el('span', { 'class': 'opp-badge' }, '▲ Up ' + fmt(m.change) + ' a game');
        if (m.direction === 'down') return el('span', { 'class': 'opp-badge' }, '▼ Down ' + fmt(Math.abs(m.change)) + ' a game');
        return el('span', { 'class': 'opp-badge opp-badge--quiet' }, WORDS[m.direction] || m.direction);
    }

    // ── one chart ────────────────────────────────────────────────────────────
    var W = 300, H = 132, L = 24, R = 6, T = 8, B = 18;

    function chart(ctx, p, metric, yMax) {
        var weeks = ctx.weeks, m = p.metrics[metric];
        var root = svg('svg', { viewBox: '0 0 ' + W + ' ' + H, role: 'img' });
        root.setAttribute('aria-label', p.player_name + ', ' + NOUN[metric] + ' by week: ' +
            weeks.map(function (w, i) {
                return 'week ' + w + ' ' + (m.values[i] == null ? 'none' : fmt(m.values[i]));
            }).join(', '));

        var plotW = W - L - R, plotH = H - T - B;
        var band = plotW / Math.max(weeks.length, 1);
        var barW = Math.min(24, band - 2);
        function y(v) { return T + plotH - (v / yMax) * plotH; }

        [0, yMax / 2, yMax].forEach(function (tick) {
            root.appendChild(svg('line', { x1: L, x2: W - R, y1: y(tick), y2: y(tick), stroke: 'var(--grid)', 'stroke-width': 1 }));
            var t = svg('text', { x: L - 5, y: y(tick) + 3, 'text-anchor': 'end' });
            t.textContent = fmt(tick);
            root.appendChild(t);
        });

        var played = [];
        m.values.forEach(function (v, i) { if (v != null) played.push(i); });
        var recentN = ctx.rule.recent_games || 3;
        // Windows are only drawn where a direction could be called from them.
        var windows = m.recent != null && m.direction !== 'interrupted';
        var recentIdx = windows ? played.slice(-recentN) : [];
        var earlierIdx = windows ? played.slice(0, -recentN) : [];

        weeks.forEach(function (w, i) {
            var cx = L + band * i + band / 2;
            var wk = svg('text', { x: cx, y: H - 5, 'text-anchor': 'middle' });
            wk.textContent = w;
            root.appendChild(wk);

            var hit = svg('rect', { 'class': 'hit', x: L + band * i, y: T, width: band, height: plotH + 2 });
            hit.addEventListener('pointermove', function (ev) { showTip(ctx, ev, p, metric, w, i); });
            hit.addEventListener('pointerleave', hideTip);
            root.appendChild(hit);

            var v = m.values[i];
            if (v == null) {
                root.appendChild(svg('line', { x1: cx - 4, x2: cx + 4, y1: y(0) - 3, y2: y(0) - 3,
                    stroke: 'var(--text-muted)', 'stroke-width': 2, 'stroke-linecap': 'round', opacity: 0.6, 'pointer-events': 'none' }));
                return;
            }
            var top = y(v), base = y(0), x0 = cx - barW / 2, h = base - top;
            var r = Math.min(4, h, barW / 2);
            // Rounded at the data end, square on the baseline.
            var d = 'M' + x0 + ' ' + base + ' V' + (top + r) + ' Q' + x0 + ' ' + top + ' ' + (x0 + r) + ' ' + top +
                ' H' + (x0 + barW - r) + ' Q' + (x0 + barW) + ' ' + top + ' ' + (x0 + barW) + ' ' + (top + r) + ' V' + base + ' Z';
            root.appendChild(svg('path', { 'class': 'bar', d: d, 'pointer-events': 'none',
                fill: recentIdx.indexOf(i) >= 0 ? 'var(--bar-recent)' : 'var(--bar-earlier)' }));
        });

        function avgLine(idx, value) {
            if (!idx.length) return;
            var x1 = L + band * idx[0] + 1, x2 = L + band * (idx[idx.length - 1] + 1) - 1;
            // A surface-coloured underlay keeps the line readable across the bars.
            root.appendChild(svg('line', { x1: x1, x2: x2, y1: y(value), y2: y(value), stroke: 'var(--surface)', 'stroke-width': 5, 'stroke-linecap': 'round', 'pointer-events': 'none' }));
            root.appendChild(svg('line', { x1: x1, x2: x2, y1: y(value), y2: y(value), stroke: 'var(--avg-line)', 'stroke-width': 2, 'stroke-linecap': 'round', 'pointer-events': 'none' }));
        }
        if (windows) { avgLine(earlierIdx, m.earlier); avgLine(recentIdx, m.recent); }
        return root;
    }

    function showTip(ctx, ev, p, metric, week, i) {
        var tip = tooltip();
        tip.textContent = '';
        var v = p.metrics[metric].values[i];
        if (v == null) {
            tip.appendChild(el('strong', {}, 'Week ' + week));
            tip.appendChild(el('div', { 'class': 'sub' }, 'Did not play, or no look'));
        } else {
            tip.appendChild(el('strong', {}, fmt(v) + ' ' + NOUN[metric]));
            tip.appendChild(el('div', { 'class': 'sub' }, 'Week ' + week + ' · ' + p.player_name));
            var parts = [];
            if (p.position === 'QB') parts.push(fmt(p.metrics.pass_attempts.values[i]) + ' pass attempts');
            else parts.push(fmt(p.metrics.targets.values[i]) + ' targets');
            parts.push(fmt(p.metrics.carries.values[i]) + ' rush attempts');
            tip.appendChild(el('div', { 'class': 'sub' }, parts.join(', ')));
        }
        tip.style.display = 'block';
        var box = tip.getBoundingClientRect();
        var x = Math.min(ev.clientX + 14, window.innerWidth - box.width - 8);
        var yy = ev.clientY - box.height - 12;
        tip.style.left = Math.max(8, x) + 'px';
        tip.style.top = (yy < 8 ? ev.clientY + 16 : yy) + 'px';
    }
    function hideTip() { tooltip().style.display = 'none'; }

    // ── one card ─────────────────────────────────────────────────────────────
    function card(ctx, p, metric, yMax, opts) {
        opts = opts || {};
        var m = p.metrics[metric];
        var node = el('article', { 'class': 'opp-card' });
        var head = el('div', { 'class': 'opp-card__head' });
        var name = el('span', { 'class': 'opp-card__name', title: opts.title || p.player_name });
        if (!opts.title && ctx.playerUrl) {
            name.appendChild(el('a', { href: ctx.playerUrl.replace('__NAME__', encodeURIComponent(p.player_name)) }, p.player_name));
        } else {
            name.textContent = opts.title || p.player_name;
        }
        head.appendChild(name);
        head.appendChild(el('span', { 'class': 'opp-card__meta' },
            opts.meta != null ? opts.meta : [p.position, p.team].filter(Boolean).join(' · ')));
        node.appendChild(head);

        var trend = el('div', { 'class': 'opp-card__trend' });
        trend.appendChild(badge(m));
        trend.appendChild(el('span', {}, m.recent != null && m.direction !== 'interrupted'
            ? fmt(m.earlier) + ' → ' + fmt(m.recent) + ' a game'
            : fmt(m.per_game) + ' a game over ' + m.games + (m.games === 1 ? ' game' : ' games')));
        // A direction called on games from weeks ago is a fact about then.
        var lastWeek = ctx.weeks[ctx.weeks.length - 1];
        if (p.latest_week != null && p.latest_week < lastWeek) {
            trend.appendChild(el('span', {}, '· last played week ' + p.latest_week));
        }
        node.appendChild(trend);
        node.appendChild(chart(ctx, p, metric, yMax));
        return node;
    }

    function context(payload, extra) {
        var ctx = { weeks: payload.weeks || [], rule: payload.trend_rule || {} };
        for (var k in extra || {}) ctx[k] = extra[k];
        return ctx;
    }

    // ── the explorer ─────────────────────────────────────────────────────────
    function mountExplorer(root, payload, initial) {
        var ctx = context(payload, { playerUrl: root.dataset.playerUrl });
        var PLAYERS = payload.data || [];
        var state = { metric: initial.metric, direction: initial.direction,
                      position: initial.position, team: initial.team,
                      sort: 'movers', shown: 48 };
        var $ = function (sel) { return root.querySelector(sel); };
        var grid = $('#opp-grid');

        function inScope(p) {
            var m = p.metrics[state.metric];
            if (!m || (m.per_game || 0) < MIN_PER_GAME) return false;
            if (state.position !== 'ALL' && p.position !== state.position) return false;
            if (state.team !== 'ALL' && p.team !== state.team) return false;
            return true;
        }
        function ordered(list) {
            var mt = state.metric, out = list.slice();
            if (state.sort === 'name') {
                out.sort(function (a, b) { return a.player_name.localeCompare(b.player_name); });
            } else if (state.sort === 'volume') {
                out.sort(function (a, b) { return b.metrics[mt].per_game - a.metrics[mt].per_game; });
            } else {
                // Called movers first, by the size of the move; everyone else by volume.
                out.sort(function (a, b) {
                    var da = a.metrics[mt], db = b.metrics[mt];
                    var ca = (da.direction === 'up' || da.direction === 'down') ? Math.abs(da.change) : -1;
                    var cb = (db.direction === 'up' || db.direction === 'down') ? Math.abs(db.change) : -1;
                    if (ca !== cb) return cb - ca;
                    return db.per_game - da.per_game;
                });
            }
            return out;
        }

        function table(list) {
            var t = $('#opp-table');
            t.textContent = '';
            var head = el('tr');
            ['Player', 'Pos', 'Team', 'Games', 'Per game', 'Earlier', 'Last 3', 'Change', 'Trend']
                .concat(ctx.weeks.map(function (w) { return 'Wk ' + w; }))
                .forEach(function (h, i) { head.appendChild(el('th', i > 2 && i !== 8 ? { 'class': 'num' } : {}, h)); });
            var thead = el('thead'); thead.appendChild(head); t.appendChild(thead);
            var body = el('tbody');
            list.forEach(function (p) {
                var m = p.metrics[state.metric], tr = el('tr');
                tr.appendChild(el('td', {}, p.player_name));
                tr.appendChild(el('td', {}, p.position));
                tr.appendChild(el('td', {}, p.team || ''));
                [m.games, fmt(m.per_game), fmt(m.earlier), fmt(m.recent), m.change == null ? '–' : signed(m.change)]
                    .forEach(function (v) { tr.appendChild(el('td', { 'class': 'num' }, v)); });
                tr.appendChild(el('td', {}, WORDS[m.direction] || m.direction));
                m.values.forEach(function (v) { tr.appendChild(el('td', { 'class': 'num' }, fmt(v))); });
                body.appendChild(tr);
            });
            t.appendChild(body);
        }

        function render() {
            var scoped = PLAYERS.filter(inScope);
            var counts = { all: scoped.length, up: 0, down: 0, steady: 0 };
            scoped.forEach(function (p) {
                var d = p.metrics[state.metric].direction;
                if (d in counts) counts[d] += 1;
            });
            root.querySelectorAll('#opp-direction button').forEach(function (b) {
                b.setAttribute('aria-pressed', b.dataset.value === state.direction ? 'true' : 'false');
                b.querySelector('.n').textContent = counts[b.dataset.value];
            });
            root.querySelectorAll('#opp-metric button').forEach(function (b) {
                b.setAttribute('aria-pressed', b.dataset.value === state.metric ? 'true' : 'false');
            });
            var list = ordered(scoped.filter(function (p) {
                return state.direction === 'all' || p.metrics[state.metric].direction === state.direction;
            }));
            // One scale per position, so two backs can be compared by eye and a
            // tight end is not flattened by a workhorse's thirty touches.
            var maxByPos = {};
            list.forEach(function (p) {
                maxByPos[p.position] = Math.max(maxByPos[p.position] || 0, seriesMax(p, state.metric));
            });
            grid.textContent = '';
            list.slice(0, state.shown).forEach(function (p) {
                grid.appendChild(card(ctx, p, state.metric, niceMax(maxByPos[p.position] || 1)));
            });
            $('#opp-empty').hidden = list.length > 0;
            var more = $('#opp-more');
            more.hidden = list.length <= state.shown;
            more.textContent = 'Show all ' + list.length + ' players';
            table(list);
        }

        var teamSelect = $('#opp-team');
        var teams = {};
        PLAYERS.forEach(function (p) { if (p.team) teams[p.team] = true; });
        Object.keys(teams).sort().forEach(function (t) { teamSelect.appendChild(el('option', { value: t }, t)); });
        if (state.team !== 'ALL' && !teams[state.team]) state.team = 'ALL';
        teamSelect.value = state.team;
        $('#opp-position').value = state.position;

        function bindSeg(id, key) {
            root.querySelectorAll('#' + id + ' button').forEach(function (b) {
                b.addEventListener('click', function () {
                    state[key] = b.dataset.value;
                    // Only quarterbacks throw; asking for pass attempts narrows to them.
                    if (key === 'metric' && state.metric === 'pass_attempts' && state.position !== 'QB') {
                        state.position = 'QB'; $('#opp-position').value = 'QB';
                    }
                    state.shown = 48; render();
                });
            });
        }
        bindSeg('opp-metric', 'metric');
        bindSeg('opp-direction', 'direction');
        [['opp-position', 'position'], ['opp-team', 'team'], ['opp-sort', 'sort']].forEach(function (pair) {
            $('#' + pair[0]).addEventListener('change', function (ev) {
                state[pair[1]] = ev.target.value; state.shown = 48; render();
            });
        });
        $('#opp-more').addEventListener('click', function () { state.shown = Infinity; render(); });
        $('#opp-rule').textContent = ruleText(ctx.rule);
        render();
    }

    // ── one player ───────────────────────────────────────────────────────────
    function mountPlayer(root, payload) {
        var ctx = context(payload);
        var p = (payload.data || [])[0];
        var grid = root.querySelector('#opp-grid');
        if (!p) return;
        var metrics = p.position === 'QB'
            ? ['opportunities', 'pass_attempts', 'carries']
            : ['opportunities', 'targets', 'carries'];
        metrics.forEach(function (metric) {
            var m = p.metrics[metric];
            if (!m || (metric !== 'opportunities' && (m.per_game || 0) < MIN_PER_GAME)) return;
            grid.appendChild(card(ctx, p, metric, niceMax(seriesMax(p, metric) || 1),
                { title: TITLE[metric], meta: fmt(m.per_game) + ' a game' }));
        });
        var rule = root.querySelector('#opp-rule');
        if (rule) rule.textContent = ruleText(ctx.rule);
    }

    function boot() {
        var root = document.querySelector('[data-opp-charts]');
        if (!root) return;
        var payload = JSON.parse(document.getElementById('opp-data').textContent);
        if (root.dataset.oppCharts === 'player') {
            mountPlayer(root, payload);
        } else {
            mountExplorer(root, payload, JSON.parse(document.getElementById('opp-initial').textContent));
        }
    }
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
    else boot();

    return { mountExplorer: mountExplorer, mountPlayer: mountPlayer };
})();
