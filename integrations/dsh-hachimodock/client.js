/*
 * DSH Client half: close the native approval composer when the HachimoDock
 * device answers first.
 *
 * The Host bridge answers /events on loopback. The native DSH approval UI keeps
 * its pending request in `uiSession.sessionStatus` even when the user focuses a
 * different session, so a DOM click on the visible buttons is not enough: the
 * background approval must be settled through that projection. The client
 * plugin therefore resolves the matching `PendingApproval` after the device
 * decision is audited, and only falls back to the DOM click when this DSH
 * version does not expose the projection. This module is a browser
 * module-loader factory.
 */
window.__ModuleLoader__.load({
  id: '@hachimodock/dsh-bridge',
  factory(require) {
    const React = require('react')
    const BRIDGE_ORIGIN = 'http://127.0.0.1:17892'
    const EVENT_URL = `${BRIDGE_ORIGIN}/events?role=client`
    const FOCUS_URL = `${BRIDGE_ORIGIN}/focus`
    let composerActions = null

    function VoiceDock(props) {
      React.useEffect(() => {
        composerActions = props.inputActions ?? null
        return () => {
          if (composerActions === props.inputActions) composerActions = null
        }
      }, [props.inputActions])
      return null
    }

    /**
     * Extract an opaque main-session id from a DSH selection snapshot.
     * Never reads titles, prompts, or transcript content.
     * @param {unknown} value - selection or binding snapshot.
     * @returns {string|null} trimmed session id, or null for a non-ordinary/empty main.
     */
    function focusSessionId(value) {
      if (typeof value === 'string') return value.trim() || null
      if (!value || typeof value !== 'object') return null
      if (Object.prototype.hasOwnProperty.call(value, 'subagentAddress')) return null
      if (typeof value.sessionId === 'string') return value.sessionId.trim() || null
      if (typeof value.key === 'string') return value.key.trim() || null
      return null
    }

    /**
     * Fire-and-forget POST to the Host focus endpoint. A failed loopback call
     * must never affect the DSH UI or approval handling.
     * @param {object} body - bounded focus/observer fields (opaque id only).
     * @returns {void}
     */
    function postFocus(body) {
      try {
        const response = fetch(FOCUS_URL, {
          method: 'POST',
          headers: { 'content-type': 'text/plain' },
          body: JSON.stringify(body),
          keepalive: true,
        })
        if (response && typeof response.catch === 'function') response.catch(() => {})
      } catch {
        /* focus sync is best-effort */
      }
    }

    /**
     * Publish one focus change, including the explicit user/restore origin.
     * @param {string|null} sessionId - opaque id or null to clear.
     * @param {'user'|'restore'} origin - whether this is a user action.
     * @returns {void}
     */
    function publishFocus(sessionId, origin) {
      postFocus({ sessionId, origin, observer: 'v1', capable: true })
    }

    /**
     * Observe the DSH main-session selection and mirror it to the Host bridge.
     * Primary source is the Workspace selection store; the Session main-binding
     * source is a fallback. Both may be absent in a minimal profile, in which
     * case focus sync stays disabled without affecting the rest of the plugin.
     * @param {object} ctx - DSH client Cordis context.
     * @returns {void}
     */
    function installFocusSync(ctx) {
      /** @type {string|null|undefined} undefined means "nothing reported yet". */
      let lastReported
      // Workspace is authoritative once it has published a main selection.
      // The Session binding fallback is ignored afterwards so a stale fallback
      // can never overwrite the user's current Workspace choice.
      let workspaceSeen = false
      let capable = false
      let observerTimer = null
      const cleanups = []
      const announceObserver = () => {
        postFocus({ observer: 'v1', capable })
      }
      const report = (value, kind, origin) => {
        // `{sessionId: undefined}` is a partially hydrated store, not a
        // Workspace main selection; do not suppress the uiSession fallback.
        const explicitWorkspaceSelection = value && typeof value === 'object'
          && (typeof value.sessionId === 'string'
            || Object.prototype.hasOwnProperty.call(value, 'subagentAddress'))
        if (kind === 'workspace' && explicitWorkspaceSelection) workspaceSeen = true
        if (kind === 'session' && workspaceSeen) return
        const next = focusSessionId(value)
        // A fresh client must not clear the Host's last confirmed focus just
        // because its initial selection is still restoring.
        if (next === null && lastReported === undefined) return
        // A user re-selecting the same DSH session must still refresh the Host
        // timestamp; restore/reload repeats stay idempotent.
        if (next === lastReported && origin !== 'user') return
        lastReported = next
        publishFocus(next, origin)
      }
      const attach = (source, kind) => {
        if (!source || typeof source.getSnapshot !== 'function' || typeof source.subscribe !== 'function') return
        let active = true
        let restorePending = true
        const update = () => {
          if (!active) return
          try {
            const value = source.getSnapshot()
            const next = focusSessionId(value)
            const origin = restorePending && next !== null ? 'restore' : 'user'
            if (next !== null) restorePending = false
            report(value, kind, origin)
          } catch {
            /* a changed selection shape must not break the client */
          }
        }
        let dispose
        try {
          dispose = source.subscribe(update)
        } catch {
          return
        }
        capable = true
        announceObserver()
        cleanups.push(() => {
          active = false
          try {
            if (typeof dispose === 'function') dispose()
          } catch {
            /* ignore */
          }
        })
        update()
      }
      const tryInject = (serviceName, kind, pick) => {
        try {
          if (!ctx || typeof ctx.inject !== 'function') return
          ctx.inject([serviceName], (scope) => {
            try {
              const source = pick(scope)
              if (source) attach(source, kind)
            } catch {
              /* unavailable service shape */
            }
          })
        } catch {
          /* focus sync unavailable in this profile */
        }
      }
      // Announce observer liveness separately from SSE before probing
      // services: without a source it reports capable:false so the PC can fail
      // closed rather than assume an SSE-attached but old/cached client can
      // observe focus. A successful attach immediately announces capable:true.
      announceObserver()
      // Workspace selection is authoritative for the main conversation; the
      // Session main-binding source covers clients that do not load Workspace.
      tryInject('uiWorkspace', 'workspace', (scope) => scope?.uiWorkspace?.selection)
      tryInject('uiSession', 'session', (scope) => scope?.uiSession?.adapter?.current)
      try {
        observerTimer = setInterval(announceObserver, 5000)
      } catch {
        observerTimer = null
      }
      try {
        ctx.effect(() => () => {
          if (observerTimer !== null) clearInterval(observerTimer)
          for (const dispose of cleanups.splice(0)) dispose()
        }, 'hachimodock: DSH focus sync')
      } catch {
        if (observerTimer !== null) clearInterval(observerTimer)
        /* effects unavailable in a test double */
      }
    }

    function findComposer() {
      const candidates = Array.from(document.querySelectorAll('[contenteditable="true"]'))
        .filter((element) => element.offsetParent !== null)
      return candidates.find((element) => element.getAttribute('role') === 'textbox')
        || candidates[candidates.length - 1]
        || null
    }

    function insertIntoComposer(text, attempt = 0) {
      if (composerActions && typeof composerActions.captureInsertion === 'function'
          && typeof composerActions.insertText === 'function') {
        try {
          const span = composerActions.captureInsertion()
          if (composerActions.insertText(text, span)) return true
        } catch {
          /* fall through to the DOM adapter */
        }
      }
      const composer = findComposer()
      if (!composer) {
        if (attempt < 20) setTimeout(() => insertIntoComposer(text, attempt + 1), 150)
        return false
      }
      composer.focus()
      const selection = window.getSelection()
      if (!selection) return false
      const range = document.createRange()
      range.selectNodeContents(composer)
      range.collapse(false)
      selection.removeAllRanges()
      selection.addRange(range)
      let inserted = false
      try {
        inserted = document.execCommand('insertText', false, text)
      } catch {
        inserted = false
      }
      if (!inserted) {
        // Fallback for editors that ignore execCommand; dispatch the same
        // input event shape a paste would produce.
        const event = new InputEvent('beforeinput', {
          bubbles: true,
          cancelable: true,
          inputType: 'insertText',
          data: text,
        })
        composer.dispatchEvent(event)
      }
      return true
    }

    function submitComposer() {
      if (composerActions && typeof composerActions.submit === 'function') {
        try {
          composerActions.submit()
          return true
        } catch {
          /* fall through to the DOM adapter */
        }
      }
      const composer = findComposer()
      if (!composer) return false
      composer.focus()
      const selection = window.getSelection()
      if (selection) {
        const range = document.createRange()
        range.selectNodeContents(composer)
        range.collapse(false)
        selection.removeAllRanges()
        selection.addRange(range)
      }
      const enter = new KeyboardEvent('keydown', {
        key: 'Enter',
        code: 'Enter',
        keyCode: 13,
        which: 13,
        bubbles: true,
        cancelable: true,
      })
      composer.dispatchEvent(enter)
      // Some composer versions route Enter through a form/button rather than
      // the editor key handler; fall back to the visible send control.
      setTimeout(() => {
        const buttons = Array.from(document.querySelectorAll('button'))
        const send = buttons.find((button) => {
          const label = `${button.getAttribute('aria-label') || ''} ${button.title || ''} ${button.textContent || ''}`
          return /发送|send|submit/i.test(label) && !button.disabled
        })
        if (send) send.click()
      }, 250)
      return true
    }

    /** DSH `uiSession` service, when this profile loads the client session UI. */
    let uiSessionService = null

    /**
     * Read the native DSH approval projection for one session.
     *
     * `dsh-client-ui-session` publishes every process-local pending interaction
     * in `uiSession.sessionStatus`, including sessions that are not currently
     * focused. The approval projection exposes `kind`, `sessionId`,
     * `answerable`, `answer(outcome)`, and `abort(reason)`.
     *
     * @param {string} sessionId - opaque DSH session id from the bridge event.
     * @returns {object|null} the pending approval, or null when absent/settled.
     */
    function nativePendingApproval(sessionId) {
      if (!uiSessionService || typeof sessionId !== 'string' || sessionId === '') return null
      try {
        const snapshot = uiSessionService.sessionStatus?.getSnapshot?.()
        const entry = snapshot?.get?.(sessionId)
        const pending = entry?.pendingInteraction
        if (!pending || pending.kind !== 'approval' || pending.sessionId !== sessionId) return null
        if (pending.answerable === false) return null
        return pending
      } catch {
        return null
      }
    }

    /**
     * Settle the native DSH approval projection after the device decided.
     *
     * This works for a background session whose approval panel is not mounted,
     * which is the stale-modal case the DOM fallback cannot reach. It is
     * best-effort: a changed DSH client projection must never break the bridge.
     *
     * @param {string} sessionId - opaque DSH session id.
     * @param {string} outcome - `allowed-once` / `rejected` / other device outcome.
     * @returns {boolean} whether a pending native approval was settled.
     */
    function settleNativeApproval(sessionId, outcome) {
      const pending = nativePendingApproval(sessionId)
      if (!pending) return false
      try {
        if (outcome === 'allowed-once' || outcome === 'rejected') {
          const result = pending.answer(outcome)
          if (result && typeof result.catch === 'function') result.catch(() => {})
        } else if (typeof pending.abort === 'function') {
          pending.abort(new Error('device approval resolved without a native decision'))
        } else {
          return false
        }
        return true
      } catch {
        return false
      }
    }

    function dismissNativeApproval() {
      // Timeout/abort has no native cancel API. Prefer the visible reject
      // control; fall back to Escape so a stale composer cannot stay open.
      if (clickApproval('rejected', 0, true)) return
      try {
        document.dispatchEvent(new KeyboardEvent('keydown', {
          key: 'Escape', code: 'Escape', keyCode: 27, which: 27,
          bubbles: true, cancelable: true,
        }))
      } catch {
        /* no DOM focus available */
      }
    }

    function clickApproval(outcome, attempt = 0) {
      const allow = outcome === 'allowed-once'
      const labels = allow
        ? ['允许一次', 'Allow once']
        : ['拒绝', 'Reject']
      const buttons = Array.from(document.querySelectorAll('button'))
      for (const button of buttons) {
        const text = (button.textContent || '').replace(/\s+/g, ' ').trim()
        if (labels.some((label) => text === label || text.includes(label))) {
          button.click()
          return true
        }
      }
      if (attempt < 20) {
        setTimeout(() => clickApproval(outcome, attempt + 1), 100)
      }
      return false
    }

    return {
      inject: ['slots'],
      apply(ctx) {
        if (typeof EventSource !== 'function') return
        try {
          ctx.slots.inject('conversation.composer.dock', () => ctx.slots.register({
            name: 'conversation.composer.dock',
            id: 'hachimodock-voice-dock',
            order: 999,
          }, VoiceDock))
        } catch (error) {
          // A missing/changed slot must never take down DSH web boot.
          console.warn('[hachimodock] composer dock unavailable', error)
        }
        installFocusSync(ctx)
        try {
          ctx.inject(['uiSession'], (scope) => {
            uiSessionService = scope?.uiSession ?? null
          })
        } catch {
          uiSessionService = null
        }
        const source = new EventSource(EVENT_URL)
        const onResolved = (event) => {
          try {
            const payload = JSON.parse(event.data)
            if (!payload) return
            const outcome = payload.outcome
            // Settle the DSH-owned projection first: it reaches a background
            // session whose approval panel is not mounted. Only clients without
            // the projection fall back to clicking the visible button.
            const canProject = typeof uiSessionService?.sessionStatus?.getSnapshot === 'function'
            const settled = canProject && settleNativeApproval(payload.sessionId, outcome)
            if (settled || canProject) return
            if (outcome === 'allowed-once' || outcome === 'rejected') {
              clickApproval(outcome)
            }
          } catch {
            /* malformed events must not break the client */
          }
        }
        const onVoiceInsert = (event) => {
          try {
            const payload = JSON.parse(event.data)
            if (payload && typeof payload.text === 'string' && payload.text.trim()) {
              insertIntoComposer(payload.text)
            }
          } catch {
            /* malformed events must not break the client */
          }
        }
        source.addEventListener('approval-resolved', onResolved)
        const onVoiceSubmit = () => {
          try { submitComposer() } catch { /* editor fallback must not throw */ }
        }
        source.addEventListener('voice-insert', onVoiceInsert)
        source.addEventListener('voice-submit', onVoiceSubmit)
        const onApprovalExpired = () => {
          try { dismissNativeApproval() } catch { /* best effort */ }
        }
        source.addEventListener('approval-expired', onApprovalExpired)
        ctx.effect(() => () => {
          source.removeEventListener('approval-resolved', onResolved)
          source.removeEventListener('voice-insert', onVoiceInsert)
          source.removeEventListener('voice-submit', onVoiceSubmit)
          source.removeEventListener('approval-expired', onApprovalExpired)
          source.close()
        }, 'hachimodock: device approval dismissal')
      },
    }
  },
})
