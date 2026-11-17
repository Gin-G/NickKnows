"""The AI Dev portfolio's FALLBACK list.

/ai-dev reads the portfolio from IdeaBRD's public page (see ideabrd_client):
flag an idea public in IdeaBRD and it appears there. This list renders only
while IdeaBRD is unreachable or has nothing flagged public. Field notes:

  status    one of IdeaBRD's: idea, active, paused, deployed, done.
  progress  0-100, taken from the repo's IDEA.md. None when the repo has no
            IDEA.md; the card then shows no bar rather than a made-up number.
  live      public URL, or None (private apps, Android-only builds).
  repo      only for PUBLIC GitHub repos. A link to a private repo 404s for
            every visitor but the owner.
  started   'YYYY-MM' of the first commit, used for ordering within a group.

Progress and status mirror each repo's IDEA.md as of 2026-11-13. IDEA.md is
the source of truth; update this list when those numbers move.
"""

CATEGORIES = [
    ('outdoors', 'Mountains & Water', 'Skiing, fishing and the weather that decides both.'),
    ('sports', 'Sports & Fantasy', 'NFL data, projections, and what to do with them.'),
    ('tools', 'Tools & Platforms', 'Things I wanted to exist, so I built them.'),
]

PROJECTS = [
    # ── Mountains & Water ──────────────────────────────────────────────
    {
        'name': 'Foreal Ski',
        'category': 'outdoors',
        'status': 'active', 'progress': 72, 'started': '2026-10',
        'summary': 'An Android ski game on real mountains: force-based ski physics on '
                   '2 m lidar terrain, with OpenStreetMap runs and lifts, a terrain park, '
                   'tricks, grabs and rails. Players can request a mountain, and a GitHub '
                   'workflow builds and publishes it.',
        'stack': ['Kotlin', 'OpenGL ES 3', 'Python terrain tools', 'GitHub Actions'],
        'live': None,
        'repo': 'https://github.com/Gin-G/forealski-game',
        'note': 'Android',
    },
    {
        'name': 'Rate the Slopes',
        'category': 'outdoors',
        'status': 'active', 'progress': 70, 'started': '2023-02',
        'summary': 'Yelp for ski runs. Every trail gets its own rating for difficulty, '
                   'scenery, grooming and crowding, so you can tell which black at a '
                   'resort is actually good. Web and Android.',
        'stack': ['Web', 'Android'],
        'live': 'https://www.ratetheslopes.com',
        'repo': None,
    },
    {
        'name': 'SkiTunes',
        'category': 'outdoors',
        'status': 'active', 'progress': 70, 'started': '2022-03',
        'summary': 'A searchable database of the songs used in ski and snowboard films, '
                   'with one-click export of any search into a real Spotify playlist.',
        'stack': ['Flask', 'Postgres', 'Spotify OAuth', 'Argo Rollouts'],
        'live': 'https://skimoviesongs.com',
        'repo': 'https://github.com/Gin-G/SkiTunes',
    },
    {
        'name': 'Trout Temps',
        'category': 'outdoors',
        'status': 'done', 'progress': 100, 'started': '2026-06',
        'summary': 'Live USGS water temperatures for fly fishers. Each river is flagged '
                   'safe, caution or stop against the 65°F trout-stress threshold, and '
                   'every river, gage and town has its own page.',
        'stack': ['Vanilla JS', 'Leaflet', 'USGS API', 'nginx'],
        'live': 'https://trout-temps.nickknows.net',
        'repo': 'https://github.com/Gin-G/trout_temps',
    },
    {
        'name': 'MV Derby Planner',
        'category': 'outdoors',
        'status': 'active', 'progress': None, 'started': '2026-10',
        'summary': "A fishing planner for the Martha's Vineyard Derby. It lines up NOAA "
                   'tides, currents and sun times by spot, so you know where to be and when.',
        'stack': ['Python', 'NOAA CO-OPS', 'Kustomize'],
        'live': 'https://mvderby.nickknows.net',
        'repo': 'https://github.com/Gin-G/mvderby',
    },
    {
        'name': 'PropStory',
        'category': 'outdoors',
        'status': 'active', 'progress': 65, 'started': '2026-06',
        'summary': "Enter an address and get that property's climate history (snow, wind, "
                   'temperature, records) from ERA5 reanalysis. It started with one '
                   'question: can you ski out the back yard of a ridgetop lot?',
        'stack': ['ERA5 / NCAR GDEX', 'Static web app'],
        'live': 'https://propstory.nickknows.net',
        'repo': 'https://github.com/Gin-G/propstory',
    },

    # ── Sports & Fantasy ───────────────────────────────────────────────
    {
        'name': 'NFL-API',
        'category': 'sports',
        'status': 'active', 'progress': None, 'started': '2025-09',
        'summary': 'The data backbone for everything NFL here. It ingests nflverse data, '
                   'grades players, coaches and coordinators, and serves projections. '
                   'The NFL Stats section of this site reads from it.',
        'stack': ['FastAPI', 'CloudNativePG', 'Kubernetes'],
        'live': None,
        'repo': 'https://github.com/Gin-G/NFL-API',
        'internal': 'NFL',
    },
    {
        'name': 'NFL Projections',
        'category': 'sports',
        'status': 'active', 'progress': None, 'started': '2024-07',
        'summary': 'A neural-net pipeline that projects next-week player stats and '
                   'FanDuel points, with floor, median and ceiling from a quantile model. '
                   'It also builds DFS player pools and lineups.',
        'stack': ['PyTorch', 'nflreadpy', 'pandas'],
        'live': None,
        'repo': 'https://github.com/Gin-G/nfl-data-py',
    },
    {
        'name': 'Sharp Edge',
        'category': 'sports',
        'status': 'active', 'progress': 65, 'started': '2026-04',
        'summary': 'Betting analytics. It syncs bet history, runs daily MLB screens and a '
                   'weekly NFL prop board, tracks every pick to a settled outcome, and '
                   'opens it all to Claude over chat and MCP.',
        'stack': ['Svelte', 'FastAPI', 'CloudNativePG', 'MCP'],
        'live': 'https://sharp-edge.nickknows.net',
        'repo': 'https://github.com/Gin-G/sharp-edge',
    },
    {
        'name': 'WhoYaGot',
        'category': 'sports',
        'status': 'active', 'progress': 94, 'started': '2026-08',
        'summary': 'This player or that player? Head-to-head votes build personal and '
                   'crowd draft boards, and you can export yours to the site you actually '
                   'draft on.',
        'stack': ['Google sign-in', 'Android', 'ArgoCD'],
        'live': 'https://whoyagot.nickknows.net',
        'repo': 'https://github.com/Gin-G/WhoYaGot',
    },
    {
        'name': 'DeFS',
        'category': 'sports',
        'status': 'active', 'progress': 70, 'started': '2025-06',
        'summary': 'Peer-to-peer daily fantasy sports settled on-chain. Entry is in USDC, '
                   'contracts hold escrow and pay the winners, and the house never takes '
                   'custody of anyone\'s funds.',
        'stack': ['Solidity', 'Monad', 'USDC'],
        'live': 'https://v1.defs.bet',
        'repo': None,
    },

    # ── Tools & Platforms ──────────────────────────────────────────────
    {
        'name': 'IdeaBRD',
        'category': 'tools',
        'status': 'active', 'progress': 95, 'started': '2026-06',
        'summary': 'The board that tracks all of these. Each idea is a tile with notes, '
                   'progress and to-dos backed by GitHub issues. The board lives in git, '
                   'and an Android app reads it from there.',
        'stack': ['SvelteKit', 'FastAPI', 'CNPG', 'Capacitor'],
        'live': 'https://ideabrd.nickknows.net',
        'repo': 'https://github.com/Gin-G/IdeaBRD',
    },
    {
        'name': 'WhoKan',
        'category': 'tools',
        'status': 'active', 'progress': 65, 'started': '2025-03',
        'summary': 'Internal skill discovery. People list what they know, colleagues '
                   'search by skill and ask for help, and reputation comes from help '
                   'actually given. Every skill has its own forum and chat room.',
        'stack': ['FastAPI', 'Alpine.js', 'CNPG', 'Helm'],
        'live': 'https://whokan.nickknows.net',
        'repo': 'https://github.com/Gin-G/whookan',
    },
    {
        'name': 'Notebook Web',
        'category': 'tools',
        'status': 'active', 'progress': 70, 'started': '2026-04',
        'summary': 'A Jupyter notebook gallery you can run. Launch spins up a Kubernetes '
                   'pod from a pre-built image and streams the kernel to a custom '
                   'in-browser renderer, with no JupyterLab and no Binder.',
        'stack': ['FastAPI', 'repo2docker', 'WebSockets', 'Kubernetes'],
        'live': 'https://notebooks.nickknows.net',
        'repo': 'https://github.com/Gin-G/notebook-web',
    },
    {
        'name': 'Hydrow Web',
        'category': 'tools',
        'status': 'active', 'progress': 60, 'started': '2026-05',
        'summary': "A web dashboard for Hydrow rowing data, built on the rower's "
                   'undocumented mobile API: stats, workout history, leaderboards and '
                   'rower profiles.',
        'stack': ['Flask', 'Redis sessions', 'Chart.js'],
        'live': 'https://hydrow.nickknows.net',
        'repo': 'https://github.com/Gin-G/hydrow-web',
    },
    {
        'name': 'Hench',
        'category': 'tools',
        'status': 'deployed', 'progress': 75, 'started': '2026-07',
        'summary': 'Self-hosted personal finance. Plaid pulls in transactions and a '
                   'monthly Sankey shows where the money went. It is growing into a '
                   'forward-looking bill and payday planner.',
        'stack': ['Svelte 5', 'ECharts', 'FastAPI', 'Plaid'],
        'live': None,
        'repo': 'https://github.com/Gin-G/hench',
        'note': 'Private',
    },
    {
        'name': 'Tiskit',
        'category': 'tools',
        'status': 'active', 'progress': 20, 'started': '2026-05',
        'summary': 'A last-mile form filler for addiction-treatment admissions. It gets '
                   'data that already exists into the payer questionnaire or prior-auth '
                   'PDF, so nobody retypes it. PDFs never touch disk.',
        'stack': ['FastAPI', 'PDF forms'],
        'live': 'https://tiskit.nickknows.net',
        'repo': 'https://github.com/Gin-G/tiskit',
    },
    {
        'name': 'BabyBoo',
        'category': 'tools',
        'status': 'active', 'progress': 60, 'started': '2022-03',
        'summary': 'A local-first word app for babies and toddlers. It shows picture cards '
                   'in the recorded voices and faces of their own family, and nothing '
                   'leaves the device.',
        'stack': ['React Native', 'Expo'],
        'live': None,
        'repo': None,
        'note': 'Android',
    },
    {
        'name': 'Simposal',
        'category': 'tools',
        'status': 'active', 'progress': 45, 'started': '2026-06',
        'summary': 'A provenance-first CLI for grant proposals. Every fact in a draft has '
                   'a receipt: a citation, a named person, or an explicit AI label.',
        'stack': ['Python CLI', 'NSF'],
        'live': None,
        'repo': None,
        'note': 'CLI',
    },
    {
        'name': 'Gingi',
        'category': 'tools',
        'status': 'active', 'progress': 20, 'started': '2026-05',
        'summary': 'A staged "digital me". It starts as a drafting assistant that writes '
                   'in my voice and is meant to grow into a voice agent with real context.',
        'stack': ['Claude Projects', 'Evals'],
        'live': None,
        'repo': 'https://github.com/Gin-G/gingi',
    },
]


def grouped_projects():
    """[(key, title, blurb, [projects newest-first]), ...] in CATEGORIES order."""
    groups = []
    for key, title, blurb in CATEGORIES:
        items = [p for p in PROJECTS if p['category'] == key]
        items.sort(key=lambda p: p['started'], reverse=True)
        groups.append((key, title, blurb, items))
    return groups
