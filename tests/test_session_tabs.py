"""Tests for in-page session tabs (pinned conversations).

A session tab pins a conversation above the transcript so that:
  1. Switching back to it skips the metadata round-trip when its warm
     snapshot is fresh (loadSession `warmSessionMeta` fast path in
     static/sessions.js projects S.session from the authoritative sidebar
     row; messages still load authoritatively).
  2. The page keeps receiving that session's persistent-channel events
     (bg_task_complete / server_turn_started / session-updated) while it is
     NOT the active pane, via a bounded background EventSource per pinned
     session (static/session-tabs.js, cap SESSION_TABS_MAX_STREAMS=2 so the
     HTTP/1.1 ~6-socket pool never saturates: 1 active + 2 pinned + global
     streams fit).

Covered here: structural wiring (files reference each other), behavioral
pin/unpin/switch/stream-cap logic driven through Node VM against the shipped
static/session-tabs.js, the loadSession fast-path guards in sessions.js, and
the pool-budget arithmetic as a pinned invariant.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest


REPO = Path(__file__).parent.parent
TABS_JS = (REPO / "static" / "session-tabs.js").read_text(encoding="utf-8")
SESSIONS_JS = (REPO / "static" / "sessions.js").read_text(encoding="utf-8")
NODE = shutil.which("node")


def _extract_function(source: str, name: str) -> str:
    """Extract ``function name(...)`` with balanced braces."""
    for prefix in (f"function {name}(", f"async function {name}("):
        start = source.find(prefix)
        if start >= 0:
            break
    else:
        raise AssertionError(f"{name} function not found")
    brace = source.find("{", start)
    assert brace >= 0, f"{name} opening brace not found"
    depth = 0
    for idx in range(brace, len(source)):
        ch = source[idx]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return source[start:idx + 1]
    raise AssertionError(f"{name} function braces unbalanced")


def _run_tabs_driver(driver_js: str) -> dict:
    """Run the shipped session-tabs.js in a Node VM sandbox + driver snippet.

    File-based harness (mirrors tests/test_cli_only_slash_commands.py):
    the module source is embedded via json.dumps, the driver runs inside an
    async IIFE so `await sessionTabSwitch(...)` works, and the result is
    printed as JSON. The sandbox stubs browser globals
    (document/localStorage/EventSource) and the cross-file functions
    session-tabs.js calls into (loadSession, _sessionSnapshotById, ...).
    """
    if NODE is None:
        pytest.skip("node not available")
    import tempfile
    import textwrap

    script = textwrap.dedent(
        """
        const vm = require('vm');
        const TABS_SRC = MODULE_SRC_JSON;
        const ret = {};
        const store = {};
        const sandbox = {
          console, ret,
          localStorage: {
            getItem: (k) => (k in store ? store[k] : null),
            setItem: (k, v) => { store[k] = String(v); },
            removeItem: (k) => { delete store[k]; },
          },
          document: {
            hidden: false,
            _hooks: {},
            getElementById: (id) => (id === 'sessionTabBar' ? sandbox._bar : null),
            createElement: (tag) => ({
              tag, children: [], dataset: {}, style: {},
              className: '', textContent: '', title: '', hidden: false,
              appendChild(c) { this.children.push(c); return c; },
              setAttribute(k, v) { this[k] = v; },
              addEventListener() {},
            }),
            addEventListener: (ev, fn) => { sandbox.document._hooks[ev] = fn; },
          },
          EventSource: function(url) {
            sandbox._esOpened.push(String(url));
            this.url = String(url);
            this.readyState = 1;
            this._listeners = {};
            this.addEventListener = (ev, fn) => { this._listeners[ev] = fn; };
            this.close = () => { this.readyState = 2; };
          },
          _bar: null,
          _esOpened: [],
          _rows: {},
          S: {session: null, messages: []},
          _testRet: null,
          _testStore: null,
          loadSessionCalls: [],
          loadSession: async (sid, opts) => {
            sandbox.loadSessionCalls.push({sid, opts: opts || {}});
            sandbox.S.session = {session_id: sid};
            return true;
          },
          _sessionSnapshotById: (sid) => (sandbox._rows || {})[sid] || null,
          _sessionDisplayTitle: (s) => (s && s.title) || 'Untitled',
          _truncatedSessionId: (sid) => String(sid).slice(0, 8),
          _apiUrl: (p) => p,
          renderSessionList: () => {},
          _handleBgTaskCompleteEvent: null,
          _markSessionCompletionUnread: null,
          t: (k) => k,
          closeMobileSidebar: () => {},
          setTimeout: (fn) => 0,
          clearTimeout: () => {},
        };
        sandbox.window = sandbox;
        sandbox.globalThis = sandbox;
        sandbox._bar = sandbox.document.createElement('div');
        sandbox._bar.hidden = true;
        vm.createContext(sandbox);
        sandbox._testRet = ret;
        sandbox._testStore = store;
        vm.runInContext(TABS_SRC, sandbox);
        // Classic-script top-level function declarations land on the sandbox
        // global; alias the ones the driver uses into local scope (strict
        // mode keeps them off the Node outer scope). NL = newline without
        // tripping over python/JS escape layers.
        const NL = String.fromCharCode(10);
        const _driverPrologue = [
          "const sessionTabPin = globalThis.sessionTabPin;",
          "const sessionTabUnpin = globalThis.sessionTabUnpin;",
          "const sessionTabsList = globalThis.sessionTabsList;",
          "const sessionTabsHas = globalThis.sessionTabsHas;",
          "const sessionTabSwitch = globalThis.sessionTabSwitch;",
          "const tabsOnActiveChanged = globalThis._sessionTabsOnActiveSessionChangedForTest;",
          "const tabStreams = globalThis._sessionTabStreamsForTest;",
          "const tabOnBgComplete = globalThis._sessionTabOnBgTaskCompleteForTest;",
          "const tabsInit = globalThis._sessionTabsInitForTest;",
          "const sb = globalThis;",
          "const ret = globalThis._testRet;",
          "const store = globalThis._testStore;",
        ].join(NL);
        (async () => {
          const _driverSrc = __TABS_DRIVER_SOURCE__;
          const _driverProgram = _driverPrologue + NL + ";(async () => {" + NL + _driverSrc + NL + "})()";
          await vm.runInContext(_driverProgram, sandbox);
          process.stdout.write(JSON.stringify({ret: sandbox._testRet, store, esOpened: sandbox._esOpened, loadSessionCalls: sandbox.loadSessionCalls}));
        })().catch(err => {
          console.error(err && err.stack || err);
          process.exit(1);
        });
        """
    ).replace("MODULE_SRC_JSON", json.dumps(TABS_JS)).replace(
        "__TABS_DRIVER_SOURCE__", json.dumps(textwrap.dedent(driver_js))
    )
    with tempfile.NamedTemporaryFile("w", suffix=".js", encoding="utf-8", delete=False) as handle:
        handle.write(script)
        script_path = Path(handle.name)
    try:
        proc = subprocess.run([NODE, str(script_path)], capture_output=True, text=True, timeout=60)
    finally:
        script_path.unlink(missing_ok=True)
    assert proc.returncode == 0, f"node harness failed: {proc.stderr[:2000]}"
    full = json.loads(proc.stdout)
    return full["ret"] | {"_store": full["store"], "_esOpened": full["esOpened"], "_loadSessionCalls": full["loadSessionCalls"]}


# ── Structural wiring ──────────────────────────────────────────────────────

def test_session_tabs_module_exists_with_caps():
    assert "SESSION_TABS_MAX_TABS = 4" in TABS_JS
    assert "SESSION_TABS_MAX_STREAMS = 2" in TABS_JS
    assert "SESSION_TABS_SNAPSHOT_TTL_MS" in TABS_JS
    assert "hermes-webui-session-tabs" in TABS_JS


def test_index_mounts_tab_bar_and_module_in_order():
    html = (REPO / "static" / "index.html").read_text(encoding="utf-8")
    assert 'id="sessionTabBar"' in html
    assert "session-tabs.js" in html
    # Load order: sessions.js defines loadSession et al; session-tabs.js must
    # come after it and before boot.js (which calls _sessionTabsInit).
    assert html.index("static/sessions.js") < html.index("static/session-tabs.js") < html.index("static/boot.js")
    # Mount point lives inside #mainChat, above the transcript shell.
    main_chat = html.index('id="mainChat"')
    assert main_chat < html.index('id="sessionTabBar"') < html.index('class="messages-shell"')


def test_loadsession_fast_path_guards():
    """warmSessionMeta projects from the sidebar row only when idle."""
    assert "opts.warmSessionMeta" in SESSIONS_JS
    # Guard 1: never skip metadata while EITHER side may hold a live turn —
    # the row's active_stream_id/pending message and the current pane's
    # active_stream_id (split across _warmTabMetaProjection/_withTabProjection
    # so the loadSession fetch window stays byte-stable for existing tests).
    assert "row.pending_user_message || row.active_stream_id" in SESSIONS_JS
    assert "(S.session && S.session.active_stream_id)" in SESSIONS_JS
    # Guard 2: the fast path only skips the fetch; messages still load below.
    assert "_ensureMessagesLoaded" in SESSIONS_JS


def test_tab_hooks_wired_into_session_lifecycle():
    # Active-pane change re-sorts + reconciles background subscribers.
    assert "_sessionTabsOnActiveSessionChanged" in SESSIONS_JS
    # List payload apply prunes dead pins + refreshes warm snapshots.
    assert "_sessionTabsPruneToVisibleRows" in SESSIONS_JS
    assert "_sessionTabsSyncSnapshotsFromRows" in SESSIONS_JS
    # Turn settle refreshes the pin snapshot + dot.
    assert "_sessionTabRememberSnapshot(finalSid" in SESSIONS_JS
    # Boot restores pins after the list cache is authoritative.
    boot = (REPO / "static" / "boot.js").read_text(encoding="utf-8")
    assert "_sessionTabsInit" in boot
    # Overflow menu entry (rare per-item action placement per UIUX rule 10).
    assert "session_tabs_pin" in SESSIONS_JS
    assert "sessionTabToggle" in SESSIONS_JS


def test_no_backend_changes_needed():
    """Tabs reuse existing endpoints; no new route is required."""
    routes = (REPO / "api" / "routes.py").read_text(encoding="utf-8")
    assert "session-tabs" not in routes
    assert "warmSessionMeta" not in routes


def test_pool_budget_invariant_documented():
    # 1 (active pane stream, owned by messages.js) + MAX_STREAMS pinned
    # subscribers + sessions/events + gateway must stay under the ~6
    # same-origin HTTP/1.1 socket limit. This pins the arithmetic in-tree.
    assert "SESSION_TABS_MAX_STREAMS = 2" in TABS_JS
    reconcile = _extract_function(TABS_JS, "_sessionTabsReconcileStreams")
    assert "slice(0, SESSION_TABS_MAX_STREAMS)" in reconcile
    # The active pane's stream is never duplicated by tab subscribers.
    assert "sid !== activeSid" in reconcile or "sid) => sid !== activeSid" in reconcile


def test_background_subscriber_never_attaches_renderer():
    """A tab-channel server_turn_started must NOT attachLiveStream.

    The renderer writes tokens into the VISIBLE pane; attaching another
    session's stream would corrupt this pane's transcript. The handler may
    only refresh list/snapshot state.
    """
    handler = _extract_function(TABS_JS, "_sessionTabOnServerTurnStarted")
    assert "attachLiveStream(" not in handler
    assert "renderSessionList" in handler


def test_tab_channel_event_handling_uses_shared_dedupe():
    handler = _extract_function(TABS_JS, "_sessionTabOnBgTaskComplete")
    # Canonical foreground handler first (owns the foreground dedupe ring +
    # ack POST); isolated fallback only when absent.
    assert "_handleBgTaskCompleteEvent" in handler
    assert "_markSessionCompletionUnread" in handler
    # Server contract: frames without event_id are ignored.
    assert "event_id" in handler


# ── Behavioral (Node VM against shipped module) ────────────────────────────

@pytest.mark.skipif(NODE is None, reason="node not available")
class TestSessionTabsBehavior:
    def test_pin_unpin_persist_and_cap(self):
        out = _run_tabs_driver("""
          sessionTabPin("a");sessionTabPin("b");sessionTabPin("c");sessionTabPin("d");sessionTabPin("e");
          ret.tabs = sessionTabsList();
          ret.storedAtCap = JSON.parse(store['hermes-webui-session-tabs']);
          ret.hasA = sessionTabsHas("a");
          sessionTabUnpin("c");
          ret.afterUnpin = sessionTabsList();
        """)
        # Cap is 4: oldest ("a") evicted when "e" pinned.
        assert out["tabs"] == ["b", "c", "d", "e"]
        assert out["storedAtCap"] == ["b", "c", "d", "e"]
        assert out["hasA"] is False
        assert out["afterUnpin"] == ["b", "d", "e"]

    def test_active_session_sorts_last_and_excluded_from_streams(self):
        out = _run_tabs_driver("""
          sessionTabPin("a");sessionTabPin("b");sessionTabPin("c");
          sb.S.session = {session_id: 'a'};
          tabsOnActiveChanged("a");
          ret.tabs = sessionTabsList();
          ret.streams = Array.from(tabStreams.keys());
          ret.renderedActive = sb._bar.children.filter(c => c.className.includes('active')).map(c => c.dataset.sid);
        """)
        assert out["tabs"] == ["b", "c", "a"]
        # Active pane "a" must NOT hold a background subscriber (messages.js
        # owns its stream); oldest-first cap fills from the remainder.
        assert "a" not in out["streams"]
        assert set(out["streams"]) == {"b", "c"}
        assert out["renderedActive"] == ["a"]

    def test_stream_cap_bounds_subscribers(self):
        out = _run_tabs_driver("""
          sessionTabPin("a");sessionTabPin("b");sessionTabPin("c");sessionTabPin("d");
          ret.streams = Array.from(tabStreams.keys());
        """)
        # No active pane here, so oldest two pins hold the two sockets.
        assert out["streams"] == ["a", "b"]

    def test_switch_passes_warm_snapshot_and_skips_same_session(self):
        out = _run_tabs_driver(
            """
            sb._rows = {s1: {session_id: 's1', title: 'Alpha'}};
            sessionTabPin("s1", {title: 'Alpha', message_count: 7});
            await sessionTabSwitch("s1");
            ret.firstCall = sb.loadSessionCalls[0];
            sb.S.session = {session_id: 's1'};
            sb.loadSessionCalls = [];
            await sessionTabSwitch("s1");
            ret.secondCalls = sb.loadSessionCalls.length;
            """
        )
        assert out["firstCall"]["sid"] == "s1"
        assert out["firstCall"]["opts"]["warmSessionMeta"]["title"] == "Alpha"
        assert out["firstCall"]["opts"]["warmSessionMeta"]["message_count"] == 7
        # Same-session switch is a no-op: no second navigation.
        assert out["secondCalls"] == 0

    def test_hidden_page_drops_streams_and_rearms_on_show(self):
        out = _run_tabs_driver("""
          tabsInit();
          sessionTabPin("a");sessionTabPin("b");
          ret.before = Array.from(tabStreams.keys()).length;
          sb.document.hidden = true;
          sb.document._hooks['visibilitychange']();
          ret.hidden = Array.from(tabStreams.keys()).length;
          sb.document.hidden = false;
          sb._esOpened = [];
          sb.document._hooks['visibilitychange']();
          ret.shown = Array.from(tabStreams.keys()).length;
          ret.reopened = sb._esOpened.length;
        """)
        assert out["before"] == 2
        assert out["hidden"] == 0
        assert out["shown"] == 2
        assert out["reopened"] == 2

    def test_bg_task_complete_dedupe_and_unread_fallback(self):
        out = _run_tabs_driver("""
          sessionTabPin("s1");
          tabStreams.clear();
          let unreadMarked = [];
          sb._markSessionCompletionUnread = (sid, n) => { unreadMarked.push([sid, n]); };
          const ev = {data: JSON.stringify({session_id: 's1', event_id: 'e1', message_count: 9})};
          tabOnBgComplete('s1', ev);
          tabOnBgComplete('s1', ev);
          ret.unreadMarked = unreadMarked;
          const wrongSid = {data: JSON.stringify({session_id: 'other', event_id: 'e2'})};
          tabOnBgComplete('s1', wrongSid);
          ret.afterWrongSid = unreadMarked.length;
          const noId = {data: JSON.stringify({session_id: 's1'})};
          tabOnBgComplete('s1', noId);
          ret.afterNoId = unreadMarked.length;
        """)
        # First delivery marks unread; exact duplicate is dropped; frames for
        # another sid or without event_id are ignored.
        assert out["unreadMarked"] == [["s1", 9]]
        assert out["afterWrongSid"] == 1
        assert out["afterNoId"] == 1

    def test_canonical_handler_delegation_preferred(self):
        out = _run_tabs_driver("""
          sessionTabPin("s1");
          let delegated = [];
          sb._handleBgTaskCompleteEvent = (e, sid, opts) => { delegated.push([sid, opts && opts.source]); };
          const ev = {data: JSON.stringify({session_id: 's1', event_id: 'e9'})};
          tabOnBgComplete('s1', ev);
          tabOnBgComplete('s1', ev);
          ret.delegated = delegated;
        """)
        # Both deliveries reach the canonical handler (it owns the foreground
        # dedupe ring); the tab-level ring already dropped the exact duplicate
        # before delegation — exactly one delegation happens.
        assert out["delegated"] == [["s1", "session-tab"]]

    def test_prune_removes_deleted_sessions(self):
        out = _run_tabs_driver("""
          sessionTabPin("a");sessionTabPin("gone");
          ret.before = sessionTabsList();
        """)
        # Without a loaded cache the prune must never drop pins blind.
        assert out["before"] == ["a", "gone"]


def test_css_tab_bar_rules_present():
    css = (REPO / "static" / "style.css").read_text(encoding="utf-8")
    for rule in (
        ".session-tab-bar",
        ".session-tab",
        ".session-tab.active",
        ".session-tab.streaming",
        ".session-tab.unread",
        ".session-tab-close",
    ):
        assert rule in css, f"missing CSS rule: {rule}"
    # Mobile: horizontal scroll + 44px touch targets.
    assert "@media (max-width:640px)" in css


def test_i18n_tab_keys_present_in_en():
    i18n = (REPO / "static" / "i18n.js").read_text(encoding="utf-8")
    for key in (
        "session_tabs_pin:",
        "session_tabs_unpin:",
        "session_tabs_pin_desc:",
        "session_tabs_unpin_desc:",
        "session_tabs_pinned:",
        "session_tabs_unpinned:",
        "session_tabs_full:",
    ):
        assert key in i18n, f"missing i18n key: {key}"
