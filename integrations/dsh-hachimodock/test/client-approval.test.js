'use strict'

/**
 * DSH client approval dismissal: settle the native approval projection for a
 * background session, not just the visible DOM button.
 */

const test = require('node:test')
const assert = require('node:assert/strict')
const { readFileSync } = require('node:fs')
const { join } = require('node:path')
const vm = require('node:vm')

const source = readFileSync(join(__dirname, '..', 'client.js'), 'utf8')

function createEventSourceClass() {
  const instances = []
  class FakeEventSource {
    constructor(url) {
      this.url = url
      this.listeners = new Map()
      this.closed = false
      instances.push(this)
    }

    addEventListener(type, listener) {
      if (!this.listeners.has(type)) this.listeners.set(type, new Set())
      this.listeners.get(type).add(listener)
    }

    removeEventListener(type, listener) {
      this.listeners.get(type)?.delete(listener)
    }

    close() {
      this.closed = true
    }

    emit(type, payload) {
      const data = typeof payload === 'string' ? payload : JSON.stringify(payload)
      for (const listener of [...(this.listeners.get(type) ?? [])]) listener({ data })
    }
  }
  return { FakeEventSource, instances }
}

function loadPlugin({ services = {}, buttons = [] } = {}) {
  const { FakeEventSource, instances } = createEventSourceClass()
  let factory
  const keyboardEvents = []
  const sandbox = {
    window: {
      __ModuleLoader__: {
        load: (definition) => {
          factory = definition.factory
        },
      },
    },
    document: {
      querySelectorAll: (selector) => (selector === 'button' ? buttons : []),
      dispatchEvent: (event) => keyboardEvents.push(event),
    },
    EventSource: FakeEventSource,
    fetch: () => Promise.resolve({ catch() {} }),
    console: { warn: () => {} },
    setTimeout,
    setInterval: (fn, ms) => {
      const timer = setInterval(fn, ms)
      if (typeof timer.unref === 'function') timer.unref()
      return timer
    },
    clearInterval,
    InputEvent: class {},
    KeyboardEvent: class {
      constructor(type, init) {
        this.type = type
        this.key = init?.key
      }
    },
  }
  vm.runInNewContext(source, sandbox)
  assert.equal(typeof factory, 'function')
  const plugin = factory((name) => (name === 'react' ? { useEffect: () => {} } : {}))
  const cleanups = []
  const ctx = {
    slots: {
      inject: (_name, factoryFn) => factoryFn(),
      register: () => () => {},
    },
    effect: (effect) => {
      const cleanup = effect()
      if (typeof cleanup === 'function') cleanups.push(cleanup)
      return () => {}
    },
    inject: (names, callback) => {
      const scope = {}
      let any = false
      for (const name of names) {
        if (services[name] !== undefined) {
          scope[name] = services[name]
          any = true
        }
      }
      if (any) callback(scope)
    },
    dispose: () => cleanups.splice(0).forEach((cleanup) => cleanup()),
  }
  plugin.apply(ctx)
  assert.equal(instances.length, 1)
  return { ctx, source: instances[0], keyboardEvents }
}

function pendingApproval(sessionId, { answerable = true } = {}) {
  const pending = {
    kind: 'approval',
    sessionId,
    answerable,
    answers: [],
    aborts: 0,
    answer(outcome) {
      this.answers.push(outcome)
      this.answerable = false
      return Promise.resolve()
    },
    abort() {
      this.aborts += 1
      this.answerable = false
    },
  }
  return pending
}

function uiSessionWith(entries) {
  return {
    sessionStatus: {
      getSnapshot: () => new Map(entries),
    },
  }
}

test('device allow settles a background native approval without a DOM click', () => {
  const pending = pendingApproval('session-bg')
  let clicks = 0
  const buttons = [{
    textContent: '允许一次',
    click: () => { clicks += 1 },
  }]
  const selection = {
    getSnapshot: () => ({ sessionId: 'session-focused' }),
    subscribe: () => () => {},
  }
  const { source: events, ctx } = loadPlugin({
    services: {
      uiSession: uiSessionWith([['session-bg', { pendingInteraction: pending }]]),
      uiWorkspace: { selection },
    },
    buttons,
  })
  events.emit('approval-resolved', { sessionId: 'session-bg', outcome: 'allowed-once' })
  assert.deepEqual(pending.answers, ['allowed-once'])
  assert.equal(pending.aborts, 0)
  assert.equal(clicks, 0, 'background approval must not depend on the visible button')
  ctx.dispose()
})

test('device reject settles the background native approval as rejected', () => {
  const pending = pendingApproval('session-bg')
  const { source: events, ctx } = loadPlugin({
    services: { uiSession: uiSessionWith([['session-bg', { pendingInteraction: pending }]]) },
  })
  events.emit('approval-resolved', { sessionId: 'session-bg', outcome: 'rejected' })
  assert.deepEqual(pending.answers, ['rejected'])
  ctx.dispose()
})

test('device timeout/unavailable aborts the native projection instead of leaving it pending', () => {
  const pending = pendingApproval('session-bg')
  const { source: events, ctx } = loadPlugin({
    services: { uiSession: uiSessionWith([['session-bg', { pendingInteraction: pending }]]) },
  })
  events.emit('approval-resolved', { sessionId: 'session-bg', outcome: 'unavailable' })
  assert.equal(pending.aborts, 1)
  assert.deepEqual(pending.answers, [])
  ctx.dispose()
})

test('an already settled or absent native approval is not clicked through the DOM', () => {
  const settled = pendingApproval('session-bg', { answerable: false })
  let clicks = 0
  const buttons = [{ textContent: '允许一次', click: () => { clicks += 1 } }]
  const { source: events, ctx } = loadPlugin({
    services: { uiSession: uiSessionWith([['session-bg', { pendingInteraction: settled }]]) },
    buttons,
  })
  events.emit('approval-resolved', { sessionId: 'session-bg', outcome: 'allowed-once' })
  assert.deepEqual(settled.answers, [])
  assert.equal(clicks, 0)
  ctx.dispose()
})

test('a decision for another session does not settle the background approval', () => {
  const pending = pendingApproval('session-bg')
  const { source: events, ctx } = loadPlugin({
    services: { uiSession: uiSessionWith([['session-bg', { pendingInteraction: pending }]]) },
  })
  events.emit('approval-resolved', { sessionId: 'session-other', outcome: 'allowed-once' })
  assert.deepEqual(pending.answers, [])
  ctx.dispose()
})

test('without uiSession the visible-button fallback is retained for older clients', () => {
  let clicks = 0
  const buttons = [{ textContent: '允许一次', click: () => { clicks += 1 } }]
  const { source: events, ctx } = loadPlugin({ buttons })
  events.emit('approval-resolved', { sessionId: 'session-bg', outcome: 'allowed-once' })
  assert.equal(clicks, 1)
  ctx.dispose()
})

test('a uiSession without the pending projection still uses the visible-button fallback', () => {
  let clicks = 0
  const buttons = [{ textContent: '允许一次', click: () => { clicks += 1 } }]
  const { source: events, ctx } = loadPlugin({ services: { uiSession: {} }, buttons })
  events.emit('approval-resolved', { sessionId: 'session-bg', outcome: 'allowed-once' })
  assert.equal(clicks, 1)
  ctx.dispose()
})

test('malformed approval-resolved payloads do not throw', () => {
  const { source: events, ctx } = loadPlugin()
  assert.doesNotThrow(() => events.emit('approval-resolved', '{not json'))
  assert.doesNotThrow(() => events.emit('approval-resolved', { sessionId: 42, outcome: 'allowed-once' }))
  ctx.dispose()
})
