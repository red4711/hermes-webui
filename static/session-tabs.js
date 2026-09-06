// ── Session tabs (pinned conversations) ──────────────────────────────────────
// One browser tab = one active session pane (S.session). A *session tab* is an
// in-page pin: the pinned sessions stay subscribed to their persistent
// per-session SSE channel (/api/session/stream) via a lightweight EventSource
// each, so server-initiated activity (bg_task_complete, server_turn_started,
// session-updated) reaches this page even when the session is NOT the active
// pane. Switching back to a pinned session then skips the metadata round-trip
// when its snapshot is warm (bounded TTL), making the switch near-instant.
//
// Connection-pool budget (HTTP/1.1, ~6 sockets/origin; see api/routes.py
// _handle_session_sse_stream + static/messages.js closeOtherLiveStreams):
//   - At most SESSION_TABS_MAX_STREAMS (2) pinned subscribers are held open.
//   - The ACTIVE pane's own stream is owned by messages.js startSessionStream
//     and never duplicated here (we skip the active sid when arming).
//   - The active pane's live token stream (/api/chat/stream) is never touched:
//     tab subscribers listen ONLY to the session channel and ignore token
//     traffic (they share the dedupe ring with the foreground handlers).
// Lifecycle: load order is sessions.js -> session-tabs.js (classic scripts, see
// static/index.html). This module owns its own state (SESSION_TABS array,
// _tabStreams map) and calls into sessions.js/messages.js globals only through
// typeof-guarded lookups so the scope_undef gate stays green when files load
// in isolation.
'use strict';

// Cap on pinned sessions (product surface) — small enough to stay readable on
// mobile and to keep the pool math obvious.
const SESSION_TABS_MAX_TABS = 4;
// Cap on concurrent background channel subscribers. The active pane's own
// stream (owned by messages.js) is separate, so worst case is 1 (active) +
// 2 (pinned) = 3 session-channel sockets, plus sessions/events + gateway =
// 5 < 6 pool slots, with fetch() able to reuse keep-alive slots.
const SESSION_TABS_MAX_STREAMS = 2;
// Warm-snapshot TTL: a pinned session whose metadata snapshot is younger than
// this skips the metadata fetch on switch (messages still load authoritatively).
const SESSION_TABS_SNAPSHOT_TTL_MS = 5 * 60 * 1000;
// localStorage persistence key (ordered sid array only — no titles/messages).
const SESSION_TABS_STORE_KEY = 'hermes-webui-session-tabs';

// Ordered pinned session ids. Plain array of strings; persisted verbatim.
let SESSION_TABS = [];
// sid -> {title, messageCount, updatedAt, fetchedAt} warm metadata snapshot.
// Titles come from the sidebar _allSessions cache or completed-session
// payloads — never from DOM. Entries expire after SNAPSHOT_TTL_MS.
const _sessionTabSnapshots = new Map();
// sid -> {es, backoff, timer} background channel subscriber handles. Bounded
// to MAX_STREAMS entries; never contains the active pane sid.
const _sessionTabStreams = new Map();
// Dedupe ring for tab-channel bg_task_complete frames, shared with the
// foreground handler semantics: (sid, event_id) pairs already consumed are
// dropped. Bounded so a hot session cannot grow it without limit.
const _sessionTabBgSeen = new Map();
const _SESSION_TAB_BG_SEEN_MAX = 200;
// Re-entrancy guard: true while a tab-initiated loadSession is in flight so a
// second tab click/close cannot interleave a second navigation.
let _sessionTabSwitching = false;

// ── Persistence ─────────────────────────────────────────────────────────────
function _sessionTabsPersist() {
  try {
    localStorage.setItem(SESSION_TABS_STORE_KEY, JSON.stringify(SESSION_TABS));
  } catch (_) { /* storage may be unavailable (private mode) — tabs stay in-memory */ }
}

function _sessionTabsRestore() {
  let raw = null;
  try { raw = localStorage.getItem(SESSION_TABS_STORE_KEY); } catch (_) { raw = null; }
  if (!raw) return;
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return;
    SESSION_TABS = parsed
      .map((v) => String(v || '').trim())
      .filter((sid, i, arr) => sid && arr.indexOf(sid) === i)
      .slice(0, SESSION_TABS_MAX_TABS);
  } catch (_) { SESSION_TABS = []; }
}

function sessionTabsList() {
  return [...SESSION_TABS];
}

function sessionTabsHas(sid) {
  return !!sid && SESSION_TABS.indexOf(String(sid)) !== -1;
}

// ── Snapshots (warm metadata for fast switching) ────────────────────────────
function _sessionTabSnapshotValid(entry) {
  if (!entry || typeof entry.fetchedAt !== 'number') return false;
  return (Date.now() - entry.fetchedAt) <= SESSION_TABS_SNAPSHOT_TTL_MS;
}

// Record a warm snapshot. `meta` may carry {title, message_count, updated_at}.
// Sources: sidebar cache rows, completed-session payloads, loadSession data.
// Never reads DOM; never stores messages.
function _sessionTabRememberSnapshot(sid, meta) {
  if (!sid || !sessionTabsHas(sid)) return;
  const prev = _sessionTabSnapshots.get(String(sid)) || {};
  const title = (meta && (meta.title || meta.display_title)) || prev.title || '';
  const messageCount = (meta && Number.isFinite(Number(meta.message_count)))
    ? Number(meta.message_count)
    : (Number.isFinite(Number(prev.messageCount)) ? Number(prev.messageCount) : null);
  const updatedAt = (meta && (meta.updated_at || meta.last_message_at))
    ? Number(meta.updated_at || meta.last_message_at) || prev.updatedAt || 0
    : (prev.updatedAt || 0);
  _sessionTabSnapshots.set(String(sid), {
    title: String(title || ''),
    messageCount,
    updatedAt,
    fetchedAt: Date.now(),
  });
}

function _sessionTabSnapshotFor(sid) {
  const entry = _sessionTabSnapshots.get(String(sid));
  return _sessionTabSnapshotValid(entry) ? entry : null;
}

// Resolve a display title without touching the DOM: warm snapshot first, then
// the sidebar _allSessions cache, then a truncated id. Mirrors the
// _sessionDisplayTitle fallback chain (Untitled) without duplicating it.
function _sessionTabTitleFor(sid) {
  const snap = _sessionTabSnapshotFor(sid);
  if (snap && snap.title) return snap.title;
  try {
    if (typeof _sessionSnapshotById === 'function') {
      const row = _sessionSnapshotById(sid);
      if (row) {
        if (typeof _sessionDisplayTitle === 'function') {
          const t = _sessionDisplayTitle(row);
          if (t) return t;
        } else if (row.title) {
          return String(row.title);
        }
      }
    }
  } catch (_) { /* fall through to truncated id */ }
  if (typeof _truncatedSessionId === 'function') {
    try { return _truncatedSessionId(sid); } catch (_) { /* fall through */ }
  }
  return String(sid || '').slice(0, 8) || 'Untitled';
}

// ── Membership ──────────────────────────────────────────────────────────────
function _sessionTabsEnforceCap() {
  while (SESSION_TABS.length > SESSION_TABS_MAX_TABS) {
    const dropped = SESSION_TABS.shift();
    _sessionTabDropStream(dropped);
    _sessionTabSnapshots.delete(String(dropped));
  }
}

// Pin a session. Returns true when the set changed. New pins go to the END
// (rightmost); the ACTIVE session is always last so it renders rightmost.
function sessionTabPin(sid, meta) {
  sid = String(sid || '').trim();
  if (!sid) return false;
  if (sessionTabsHas(sid)) {
    if (meta) _sessionTabRememberSnapshot(sid, meta);
    _sessionTabsRender();
    return false;
  }
  SESSION_TABS.push(sid);
  if (meta) _sessionTabRememberSnapshot(sid, meta);
  else _sessionTabRememberSnapshot(sid, null);
  _sessionTabsEnforceCap();
  _sessionTabsPersist();
  _sessionTabsRender();
  _sessionTabsReconcileStreams();
  return true;
}

function sessionTabUnpin(sid) {
  sid = String(sid || '').trim();
  if (!sid) return false;
  const idx = SESSION_TABS.indexOf(sid);
  if (idx === -1) return false;
  SESSION_TABS.splice(idx, 1);
  _sessionTabDropStream(sid);
  _sessionTabSnapshots.delete(sid);
  _sessionTabsPersist();
  _sessionTabsRender();
  _sessionTabsReconcileStreams();
  return true;
}

function sessionTabToggle(sid, meta) {
  return sessionTabsHas(sid) ? !sessionTabUnpin(sid) && false : sessionTabPin(sid, meta);
}

// Called after the ACTIVE session changes (loadSession success, newSession,
// delete): the active sid sorts last, deleted/archived sids are pruned.
function _sessionTabsOnActiveSessionChanged(activeSid) {
  let changed = false;
  if (activeSid) {
    const idx = SESSION_TABS.indexOf(String(activeSid));
    if (idx !== -1 && idx !== SESSION_TABS.length - 1) {
      SESSION_TABS.splice(idx, 1);
      SESSION_TABS.push(String(activeSid));
      changed = true;
    }
  }
  if (changed) _sessionTabsPersist();
  _sessionTabsRender();
  _sessionTabsReconcileStreams();
}

// Prune pins whose sessions no longer exist in the sidebar cache. Runs on
// every session-list payload apply (cheap: one pass over <=4 ids).
function _sessionTabsPruneToVisibleRows() {
  if (!SESSION_TABS.length) return;
  let rows = null;
  try {
    if (typeof _allSessions !== 'undefined' && Array.isArray(_allSessions)) rows = _allSessions;
  } catch (_) { rows = null; }
  if (!rows) return; // cache not loaded yet — never prune blind
  const visible = new Set(rows.map((s) => s && s.session_id).filter(Boolean));
  const before = SESSION_TABS.length;
  SESSION_TABS = SESSION_TABS.filter((sid) => visible.has(sid));
  if (SESSION_TABS.length !== before) {
    for (const sid of [..._sessionTabStreams.keys()]) {
      if (SESSION_TABS.indexOf(sid) === -1) _sessionTabDropStream(sid);
    }
    for (const sid of [..._sessionTabSnapshots.keys()]) {
      if (SESSION_TABS.indexOf(sid) === -1) _sessionTabSnapshots.delete(sid);
    }
    _sessionTabsPersist();
    _sessionTabsRender();
    _sessionTabsReconcileStreams();
  } else {
    // Titles may have changed (rename/LLM title) — repaint cheaply.
    _sessionTabsRender();
  }
}

// ── Rendering ───────────────────────────────────────────────────────────────
function _sessionTabsBar() {
  if (typeof document === 'undefined') return null;
  return document.getElementById('sessionTabBar');
}

// Streaming/unread/attention lookup for a pinned sid: reuse the sidebar's own
// predicates so the tab dot can never disagree with the row dot.
// Streaming = sidebar effective-streaming OR locally-rendering this pane.
// Unread = sidebar unread predicate (completion markers + viewed counts).
// Attention (approval/clarify) wins over both.
function _sessionTabStateFor(sid) {
  const state = {streaming: false, unread: false, attention: null};
  if (!sid) return state;
  try {
    const row = (typeof _sessionSnapshotById === 'function') ? _sessionSnapshotById(sid) : null;
    if (row) {
      if (typeof _isSessionEffectivelyStreaming === 'function' && _isSessionEffectivelyStreaming(row)) {
        state.streaming = true;
      }
      if (typeof _hasUnreadForSession === 'function' && _hasUnreadForSession(row)) {
        state.unread = true;
      }
      if (typeof _sessionAttentionState === 'function') {
        const att = _sessionAttentionState(row);
        if (att && att.kind) state.attention = att.kind;
      }
    }
  } catch (_) { /* predicates are best-effort — a tab never fails to render */ }
  try {
    if (typeof S !== 'undefined' && S && S.session && S.session.session_id === sid && S.busy) {
      state.streaming = true;
    }
  } catch (_) { /* ignore */ }
  return state;
}

function _sessionTabsRender() {
  const bar = _sessionTabsBar();
  if (!bar) return;
  let activeSid = null;
  try {
    activeSid = (typeof S !== 'undefined' && S && S.session) ? S.session.session_id : null;
  } catch (_) { activeSid = null; }
  // Empty state: hide the strip entirely (no chrome cost for non-users).
  if (!SESSION_TABS.length) {
    bar.hidden = true;
    bar.innerHTML = '';
    return;
  }
  bar.hidden = false;
  // Rebuild is cheap (<=4 chips) and keeps ordering trivially correct.
  bar.innerHTML = '';
  for (const sid of SESSION_TABS) {
    const chip = document.createElement('div');
    const isActive = sid === activeSid;
    chip.className = 'session-tab' + (isActive ? ' active' : '');
    chip.dataset.sid = sid;
    chip.setAttribute('role', 'tab');
    chip.setAttribute('aria-selected', isActive ? 'true' : 'false');
    const st = _sessionTabStateFor(sid);
    if (st.streaming) chip.classList.add('streaming');
    if (st.unread && !isActive) chip.classList.add('unread');
    if (st.attention) chip.classList.add('needs-attention', 'attention-' + st.attention);
    const label = document.createElement('button');
    label.type = 'button';
    label.className = 'session-tab-label';
    label.textContent = _sessionTabTitleFor(sid);
    label.title = _sessionTabTitleFor(sid);
    label.setAttribute('aria-label', 'Switch to ' + _sessionTabTitleFor(sid));
    label.onclick = (e) => {
      e.stopPropagation();
      void sessionTabSwitch(sid);
    };
    const close = document.createElement('button');
    close.type = 'button';
    close.className = 'session-tab-close';
    close.textContent = '×';
    close.setAttribute('aria-label', 'Unpin ' + _sessionTabTitleFor(sid));
    close.title = (typeof t === 'function' ? t('session_tabs_unpin_menu') : 'Unpin conversation');
    close.onclick = (e) => {
      e.stopPropagation();
      sessionTabUnpin(sid);
    };
    const dot = document.createElement('span');
    dot.className = 'session-tab-dot';
    dot.setAttribute('aria-hidden', 'true');
    chip.appendChild(dot);
    chip.appendChild(label);
    chip.appendChild(close);
    bar.appendChild(chip);
  }
}

// ── Switching ───────────────────────────────────────────────────────────────
// Switch the active pane to a pinned session. Uses the existing single-pane
// loadSession path (the chokepoint every sidebar row funnels through), so all
// lineage resolution, profile switching, draft save/restore, INFLIGHT reattach
// and stream re-arm semantics are identical to a sidebar click. The ONLY
// addition: when the pinned snapshot is warm we pass it through so the
// metadata fetch can be skipped (messages still load authoritatively).
async function sessionTabSwitch(sid) {
  sid = String(sid || '').trim();
  if (!sid || _sessionTabSwitching) return false;
  let currentSid = null;
  try {
    currentSid = (typeof S !== 'undefined' && S && S.session) ? S.session.session_id : null;
  } catch (_) { currentSid = null; }
  if (currentSid === sid) return true; // already there — no-op, no stream churn
  if (typeof loadSession !== 'function') return false;
  _sessionTabSwitching = true;
  try {
    const snap = _sessionTabSnapshotFor(sid);
    const opts = {_preloadNotified: true};
    if (snap) {
      opts.warmSessionMeta = {
        title: snap.title,
        message_count: snap.messageCount,
        updated_at: snap.updatedAt,
      };
    }
    if (typeof closeMobileSidebar === 'function') {
      try { closeMobileSidebar(); } catch (_) { /* desktop: no-op */ }
    }
    await loadSession(sid, opts);
    return true;
  } catch (_) {
    return false;
  } finally {
    _sessionTabSwitching = false;
  }
}

// ── Background channel subscribers ──────────────────────────────────────────
// One EventSource per pinned (non-active) session on /api/session/stream —
// the SAME persistent channel the foreground pane uses, so the page receives
// bg_task_complete / server_turn_started / session-updated for pinned sessions
// while the user works elsewhere. Bounded to MAX_STREAMS subscribers, oldest
// pins first, active pane always excluded (its stream is owned by
// messages.js startSessionStream — duplicating it would double-socket one
// channel and burn a pool slot for zero new information).
//
// Each subscriber reuses the foreground event semantics by delegating to the
// canonical handlers when they exist:
//   - bg_task_complete -> _handleBgTaskCompleteEvent (shared dedupe ring)
//   - server_turn_started -> renderSessionList refresh (NOT attachLiveStream:
//     the stream belongs to a background pane; attaching it to the visible
//     composer would inject another session's tokens into this pane's
//     transcript — the attach path is strictly foreground-only)
//   - session-updated -> warm-snapshot refresh + list repaint
// When the canonical handlers are absent (isolated test harness), frames are
// applied to the local snapshot/unread state directly.
function _sessionTabBgSeenAdd(sid, eventId) {
  const key = String(sid) + '|' + String(eventId);
  if (_sessionTabBgSeen.has(key)) return true; // duplicate
  _sessionTabBgSeen.set(key, Date.now());
  if (_sessionTabBgSeen.size > _SESSION_TAB_BG_SEEN_MAX) {
    const oldest = _sessionTabBgSeen.keys().next().value;
    _sessionTabBgSeen.delete(oldest);
  }
  return false;
}

function _sessionTabsReconcileStreams() {
  if (typeof EventSource === 'undefined') return;
  let activeSid = null;
  try {
    activeSid = (typeof S !== 'undefined' && S && S.session) ? S.session.session_id : null;
  } catch (_) { activeSid = null; }
  // Desired set: pinned sids minus the active pane, oldest-first, capped.
  const wanted = SESSION_TABS.filter((sid) => sid !== activeSid).slice(0, SESSION_TABS_MAX_STREAMS);
  const wantedSet = new Set(wanted);
  for (const sid of [..._sessionTabStreams.keys()]) {
    if (!wantedSet.has(sid)) _sessionTabDropStream(sid);
  }
  for (const sid of wanted) {
    if (!_sessionTabStreams.has(sid)) _sessionTabArmStream(sid);
  }
}

function _sessionTabDropStream(sid) {
  const handle = _sessionTabStreams.get(String(sid));
  if (!handle) return;
  _sessionTabStreams.delete(String(sid));
  try { if (handle.timer) clearTimeout(handle.timer); } catch (_) { /* ignore */ }
  try { if (handle.es && handle.es.readyState !== 2) handle.es.close(); } catch (_) { /* ignore */ }
}

function _sessionTabArmStream(sid) {
  sid = String(sid);
  if (!sid || _sessionTabStreams.has(sid)) return;
  if (typeof EventSource === 'undefined') return;
  // Never duplicate the foreground pane's own stream.
  try {
    if (typeof S !== 'undefined' && S && S.session && S.session.session_id === sid) return;
  } catch (_) { /* ignore */ }
  const handle = {es: null, timer: 0, backoff: 1000};
  _sessionTabStreams.set(sid, handle);
  _sessionTabOpenStream(sid, handle);
}

function _sessionTabStreamUrl(sid) {
  let knownCount = '';
  try {
    const snap = _sessionTabSnapshots.get(sid);
    if (snap && Number.isFinite(Number(snap.messageCount))) knownCount = String(Number(snap.messageCount));
    else if (typeof _sessionSnapshotById === 'function') {
      const row = _sessionSnapshotById(sid);
      if (row && Number.isFinite(Number(row.message_count))) knownCount = String(Number(row.message_count));
    }
  } catch (_) { /* omit known_count — server treats absent as no self-heal basis */ }
  const base = (typeof _apiUrl === 'function')
    ? _apiUrl('api/session/stream?session_id=' + encodeURIComponent(sid))
    : ('api/session/stream?session_id=' + encodeURIComponent(sid));
  return knownCount ? base + '&known_count=' + encodeURIComponent(knownCount) : base;
}

function _sessionTabOpenStream(sid, handle) {
  if (!handle || _sessionTabStreams.get(sid) !== handle) return;
  // The pin may have become the active pane while we were backing off — the
  // foreground owns that stream; stand down until reconcile re-arms us.
  try {
    if (typeof S !== 'undefined' && S && S.session && S.session.session_id === sid) return;
  } catch (_) { /* ignore */ }
  let es = null;
  try {
    es = new EventSource(_sessionTabStreamUrl(sid));
  } catch (_) {
    _sessionTabScheduleReconnect(sid, handle);
    return;
  }
  handle.es = es;
  handle.backoff = 1000;
  es.addEventListener('bg_task_complete', (e) => {
    _sessionTabOnBgTaskComplete(sid, e);
  });
  // Legacy alias (dual-emit shim, see api/background_process.py
  // _emit_bg_task_complete_events_now): same dedupe key, harmless when both
  // arrive; ignored once the server drops the alias.
  es.addEventListener('process_complete', (e) => {
    _sessionTabOnBgTaskComplete(sid, e);
  });
  es.addEventListener('server_turn_started', (e) => {
    _sessionTabOnServerTurnStarted(sid, e);
  });
  es.addEventListener('session-updated', (e) => {
    _sessionTabOnSessionUpdated(sid, e);
  });
  es.onerror = () => {
    // Browser auto-reconnects transient drops. Only intervene on a hard close
    // (readyState CLOSED) — drop the dead handle so reconcile builds fresh,
    // with backoff so a dead session id cannot spin the reconnect loop.
    if (es.readyState === 2 && _sessionTabStreams.get(sid) === handle && handle.es === es) {
      try { es.close(); } catch (_) { /* ignore */ }
      handle.es = null;
      _sessionTabScheduleReconnect(sid, handle);
    }
  };
}

function _sessionTabScheduleReconnect(sid, handle) {
  if (_sessionTabStreams.get(sid) !== handle) return;
  if (handle.timer) return;
  const delay = Math.min(handle.backoff || 1000, 30000);
  handle.backoff = Math.min(delay * 2, 30000);
  handle.timer = setTimeout(() => {
    handle.timer = 0;
    _sessionTabOpenStream(sid, handle);
  }, delay);
}

function _sessionTabOnBgTaskComplete(sid, e) {
  let d = null;
  try { d = JSON.parse((e && e.data) || '{}'); } catch (_) { return; }
  const evSid = d.session_id || sid;
  if (evSid !== sid) return;
  const eventId = d.event_id ? String(d.event_id) : '';
  if (!eventId) return; // server contract requires event_id (see messages.js)
  if (_sessionTabBgSeenAdd(sid, eventId)) return; // duplicate
  // Delegate to the canonical foreground handler when present — it owns the
  // foreground dedupe ring, toasts, viewed-marking and ack POST. Guarded so
  // the scope_undef gate stays green in isolation.
  if (typeof _handleBgTaskCompleteEvent === 'function') {
    try {
      _handleBgTaskCompleteEvent(e, sid, {source: 'session-tab'});
      return;
    } catch (_) { /* fall through to local handling */ }
  }
  // Isolated fallback: mark unread + repaint the tab dot.
  try {
    if (typeof _markSessionCompletionUnread === 'function') {
      _markSessionCompletionUnread(sid, Number(d.message_count) || 0);
    }
  } catch (_) { /* ignore */ }
  _sessionTabsRender();
}

function _sessionTabOnServerTurnStarted(sid, e) {
  let d = null;
  try { d = JSON.parse((e && e.data) || '{}'); } catch (_) { return; }
  const evSid = d.session_id || sid;
  const streamId = String(d.stream_id || '');
  if (!streamId || evSid !== sid) return;
  // Foreground-only attach rule: NEVER attachLiveStream from a background tab
  // subscriber — the renderer writes tokens into the VISIBLE pane, so
  // attaching another session's stream would corrupt this pane's transcript.
  // Record liveness in the snapshot and repaint (spinner dot), and refresh
  // the list so the sidebar row shows running state.
  _sessionTabRememberSnapshot(sid, null);
  _sessionTabsRender();
  try {
    if (typeof renderSessionList === 'function') void renderSessionList({deferWhileInteracting: true});
  } catch (_) { /* ignore */ }
}

function _sessionTabOnSessionUpdated(sid, e) {
  let d = null;
  try { d = JSON.parse((e && e.data) || '{}'); } catch (_) { return; }
  const evSid = d.session_id || sid;
  if (evSid !== sid) return;
  const serverCount = Number(d.message_count);
  if (Number.isFinite(serverCount)) {
    _sessionTabRememberSnapshot(sid, {message_count: serverCount});
  }
  try {
    if (typeof renderSessionList === 'function') void renderSessionList({deferWhileInteracting: true});
  } catch (_) { /* ignore */ }
  _sessionTabsRender();
}

// ── Sidebar hooks (called from sessions.js; typeof-guarded there) ───────────
// Refresh warm snapshots from a freshly-applied session-list payload so pins
// track renames/LLM titles without any extra fetch.
function _sessionTabsSyncSnapshotsFromRows(rows) {
  if (!Array.isArray(rows) || !SESSION_TABS.length) return;
  const byId = new Map();
  for (const s of rows) {
    if (s && s.session_id) byId.set(String(s.session_id), s);
  }
  for (const sid of SESSION_TABS) {
    const row = byId.get(sid);
    if (row) _sessionTabRememberSnapshot(sid, row);
  }
}

// ── Boot ────────────────────────────────────────────────────────────────────
function _sessionTabsInit() {
  _sessionTabsRestore();
  _sessionTabsRender();
  _sessionTabsReconcileStreams();
  if (typeof document !== 'undefined' && !document._hermesSessionTabsVisibilityHook) {
    document.addEventListener('visibilitychange', () => {
      // Mirror the foreground stream lifecycle: hidden pages hold no SSE
      // (pool budget), visible pages re-arm. Reconcile is idempotent.
      if (document.hidden) {
        for (const sid of [..._sessionTabStreams.keys()]) _sessionTabDropStream(sid);
      } else {
        _sessionTabsReconcileStreams();
      }
    });
    document._hermesSessionTabsVisibilityHook = true;
  }
}

// Test seam: expose module internals the behavioral suite drives through a
// Node VM (read-only inspection — never mutated by product code).
if (typeof window !== 'undefined') {
  window.sessionTabsList = sessionTabsList;
  window.sessionTabsHas = sessionTabsHas;
  window.sessionTabPin = sessionTabPin;
  window.sessionTabUnpin = sessionTabUnpin;
  window.sessionTabToggle = sessionTabToggle;
  window.sessionTabSwitch = sessionTabSwitch;
  window._sessionTabStreamsForTest = _sessionTabStreams;
  window._sessionTabsInitForTest = _sessionTabsInit;
  window._sessionTabsOnActiveSessionChangedForTest = _sessionTabsOnActiveSessionChanged;
  window._sessionTabOnBgTaskCompleteForTest = _sessionTabOnBgTaskComplete;
}
