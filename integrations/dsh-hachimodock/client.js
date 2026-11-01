/*
 * DSH Client half: close the native approval composer when the HachimoDock
 * device answers first.
 *
 * The Host bridge answers /events on loopback. The native DSH approval UI has
 * no host-side withdrawal API, but its visible buttons are the only active
 * approval controls, so the client plugin clicks the matching button after the
 * device decision is audited. This module is a browser module-loader factory.
 */
window.__ModuleLoader__.load({
  id: '@hachimodock/dsh-bridge',
  factory(require) {
    const EVENT_URL = 'http://127.0.0.1:17892/events?role=client'

    function findComposer() {
      const candidates = Array.from(document.querySelectorAll('[contenteditable="true"]'))
        .filter((element) => element.offsetParent !== null)
      return candidates.find((element) => element.getAttribute('role') === 'textbox')
        || candidates[candidates.length - 1]
        || null
    }

    function insertIntoComposer(text, attempt = 0) {
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
      apply(ctx) {
        if (typeof EventSource !== 'function') return
        const source = new EventSource(EVENT_URL)
        const onResolved = (event) => {
          try {
            const payload = JSON.parse(event.data)
            if (payload && (payload.outcome === 'allowed-once' || payload.outcome === 'rejected')) {
              clickApproval(payload.outcome)
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
        ctx.effect(() => () => {
          source.removeEventListener('approval-resolved', onResolved)
          source.removeEventListener('voice-insert', onVoiceInsert)
          source.removeEventListener('voice-submit', onVoiceSubmit)
          source.close()
        }, 'hachimodock: device approval dismissal')
      },
    }
  },
})
