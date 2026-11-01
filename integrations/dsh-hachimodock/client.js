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
        source.addEventListener('approval-resolved', onResolved)
        ctx.effect(() => () => {
          source.removeEventListener('approval-resolved', onResolved)
          source.close()
        }, 'hachimodock: device approval dismissal')
      },
    }
  },
})
