/* Bharathi Meraki analytics tracker
 * Single <script src="/_analytics/tracker.js" defer></script> in base template.
 * Tracks: pageview, tab_view, heartbeat (active time only), session_end.
 */
(function () {
  "use strict";

  var ENDPOINT = "/_analytics/event";
  var HEARTBEAT_MS = 15000; // 15s
  var SESSION_ID = (function () {
    // Per-page-load session id, used to group events server-side.
    return Math.random().toString(36).slice(2) + Date.now().toString(36);
  })();

  var activeSeconds = 0;
  var lastTickAt = Date.now();
  var isVisible = !document.hidden;
  var hasFocus = document.hasFocus();
  var heartbeatTimer = null;

  function send(eventType, extra) {
    var body = Object.assign(
      { event_type: eventType, session_id: SESSION_ID },
      extra || {}
    );
    try {
      // sendBeacon for unload paths, fetch otherwise
      if (eventType === "session_end" && navigator.sendBeacon) {
        var blob = new Blob([JSON.stringify(body)], { type: "application/json" });
        navigator.sendBeacon(ENDPOINT, blob);
      } else {
        fetch(ENDPOINT, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
          credentials: "same-origin",
          keepalive: true,
        }).catch(function () {});
      }
    } catch (e) {}
  }

  function isActive() {
    return isVisible && hasFocus;
  }

  function tick() {
    var now = Date.now();
    var elapsed = Math.round((now - lastTickAt) / 1000);
    lastTickAt = now;

    if (isActive() && elapsed > 0 && elapsed <= 30) {
      activeSeconds += elapsed;
      send("heartbeat", {
        active_seconds: elapsed,
        tab_name: getCurrentTab(),
      });
    }
  }

  function startHeartbeat() {
    if (heartbeatTimer) return;
    lastTickAt = Date.now();
    heartbeatTimer = setInterval(tick, HEARTBEAT_MS);
  }

  function stopHeartbeat() {
    if (heartbeatTimer) {
      clearInterval(heartbeatTimer);
      heartbeatTimer = null;
    }
  }

  // ----- Tab detection -----
  // Looks for elements with [data-analytics-tab] OR .nav-tab.active OR
  // [role=tab][aria-selected=true]. Configurable below.
  function getCurrentTab() {
    var el =
      document.querySelector("[data-analytics-tab].active") ||
      document.querySelector(".tab-btn.active") ||
      document.querySelector(".nav-tab.active") ||
      document.querySelector("[role='tab'][aria-selected='true']");
    if (!el) return null;
    return (
      el.getAttribute("data-analytics-tab") ||
      el.getAttribute("data-tab") ||
      el.textContent.trim().slice(0, 40) ||
      null
    );
  }

  function watchTabClicks() {
    document.addEventListener(
      "click",
      function (e) {
        var t = e.target.closest(
          "[data-analytics-tab], .tab-btn, .nav-tab, [role='tab']"
        );
        if (!t) return;
        // Defer so the active class flip happens first
        setTimeout(function () {
          var name = getCurrentTab();
          if (name) send("tab_view", { tab_name: name });
        }, 50);
      },
      true
    );
  }

  // ----- Lifecycle -----
  function init() {
    send("pageview", { tab_name: getCurrentTab() });
    startHeartbeat();
    watchTabClicks();

    document.addEventListener("visibilitychange", function () {
      isVisible = !document.hidden;
      lastTickAt = Date.now();
    });
    window.addEventListener("focus", function () {
      hasFocus = true;
      lastTickAt = Date.now();
    });
    window.addEventListener("blur", function () {
      hasFocus = false;
    });
    window.addEventListener("pagehide", function () {
      tick();
      send("session_end", { active_seconds: 0, tab_name: getCurrentTab() });
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
