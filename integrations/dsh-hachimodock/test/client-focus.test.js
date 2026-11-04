'use strict'

/**
 * DSH client focus observer: load the real browser module in a VM with fake
 * React/EventSource/fetch, then drive a fake main-selection store.
 */

const test = require('node:test')
const assert = require('node:assert/strict')
const { readFileSync } = require('node:fs')
const { join } = require('node:path')
const vm = require('node:vm')

const source = readFileSync(join(__dirname, '..', 'client.js'), 'utf8')

function createStore(initial) {
  let value = initial
  const listeners = new Set()
  return {
    getSnapshot: () => value,
    subscribe: (listener) => {
      listeners.add(listener)
      return () => listeners.delete(listener)
    },
    set: (next) => {
      value = next
      for (const listener of [...listeners]) listener()
    },
  }
}

function createSandbox(fetchImpl) {
  const sandbox = {
    window: {
      __ModuleLoader__: {
        load: () => {},
      },
    },
    document: {
      querySelectorAll: () => [],
      dispatchEvent: () => {},
    },
    EventSource: class {
      addEventListener() {}
      removeEventListener() {}
      close() {}
    },
    fetch: fetchImpl,
    console: { warn: () => {} },
    setTimeout,
    setInterval: (fn, ms) => {
      const timer = setInterval(fn, ms)
      if (typeof timer.unref === 'function') timer.unref()
      return timer
    },
    clearInterval,
    InputEvent: class {},
    KeyboardEvent: class {},
  }
  let factory
  sandbox.window.__ModuleLoader__.load = (definition) => {
    factory = definition.factory
  }
  vm.runInNewContext(source, sandbox)
  assert.equal(typeof factory, 'function')
  return factory((name) => (name === 'react' ? { useEffect: () => {} } : {}))
}

function loadPlugin({ services = {}, fetchCalls = [], fetchImpl } = {}) {
  const plugin = createSandbox(fetchImpl ?? ((url, options) => {
    fetchCalls.push({ url, options, body: JSON.parse(options.body) })
    return Promise.resolve({ catch() {} })
  }))
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
  return { ctx, fetchCalls }
}

function selectionCalls(fetchCalls) {
  return fetchCalls.filter((call) => Object.prototype.hasOwnProperty.call(call.body, 'sessionId'))
}

function observerCalls(fetchCalls) {
  return fetchCalls.filter((call) => call.body.observer === 'v1'
    && !Object.prototype.hasOwnProperty.call(call.body, 'sessionId'))
}

test('the client half mirrors the Workspace main selection, origins, and user re-selection', () => {
  const selection = createStore({})
  const fetchCalls = []
  const { ctx } = loadPlugin({
    services: { uiWorkspace: { selection } },
    fetchCalls,
  })
  assert.equal(selectionCalls(fetchCalls).length, 0, 'restoring empty selection must not clear focus')
  assert.equal(observerCalls(fetchCalls).length >= 1, true)
  assert.equal(observerCalls(fetchCalls)[0].body.capable, false)
  assert.equal(observerCalls(fetchCalls).some((call) => call.body.capable === true), true)

  selection.set({ sessionId: 'session-a' })
  let calls = selectionCalls(fetchCalls)
  assert.equal(calls.length, 1)
  assert.equal(calls[0].url, 'http://127.0.0.1:17892/focus')
  assert.equal(calls[0].options.method, 'POST')
  assert.equal(calls[0].options.headers['content-type'], 'text/plain')
  assert.deepEqual(calls[0].body, {
    sessionId: 'session-a', origin: 'restore', observer: 'v1', capable: true,
  })

  // Same-id user re-selection is a new action and must be published.
  selection.set({ sessionId: 'session-a' })
  calls = selectionCalls(fetchCalls)
  assert.equal(calls.length, 2)
  assert.equal(calls[1].body.origin, 'user')

  selection.set({ sessionId: 'session-a', subagentAddress: { sessionId: 'child' } })
  calls = selectionCalls(fetchCalls)
  assert.equal(calls.length, 3, 'subagent main must clear the ordinary target')
  assert.equal(calls[2].body.sessionId, null)

  selection.set({ sessionId: 'session-b' })
  calls = selectionCalls(fetchCalls)
  assert.equal(calls.length, 4)
  assert.equal(calls[3].body.sessionId, 'session-b')

  selection.set({})
  calls = selectionCalls(fetchCalls)
  assert.equal(calls.length, 5)
  assert.equal(calls[4].body.sessionId, null)

  ctx.dispose()
  selection.set({ sessionId: 'session-c' })
  assert.equal(selectionCalls(fetchCalls).length, 5, 'disposed client must stop publishing')
})

test('a partially hydrated Workspace value does not suppress the uiSession fallback', () => {
  const selection = createStore({ sessionId: undefined })
  const current = createStore({ key: 'session-fallback' })
  const fetchCalls = []
  const { ctx } = loadPlugin({
    services: {
      uiWorkspace: { selection },
      uiSession: { adapter: { current } },
    },
    fetchCalls,
  })
  let calls = selectionCalls(fetchCalls)
  assert.equal(calls.length, 1)
  assert.equal(calls[0].body.sessionId, 'session-fallback')

  selection.set({ sessionId: 'session-workspace' })
  calls = selectionCalls(fetchCalls)
  assert.equal(calls.length, 2)
  assert.equal(calls[1].body.sessionId, 'session-workspace')

  // A late/stale fallback update must not overwrite the Workspace choice.
  current.set({ key: 'session-fallback' })
  assert.equal(selectionCalls(fetchCalls).length, 2)
  ctx.dispose()
})

test('Workspace selection wins over a stale Session-binding fallback', () => {
  const selection = createStore({})
  const current = createStore({ key: 'session-fallback' })
  const fetchCalls = []
  const { ctx } = loadPlugin({
    services: {
      uiWorkspace: { selection },
      uiSession: { adapter: { current } },
    },
    fetchCalls,
  })
  assert.equal(selectionCalls(fetchCalls).length, 1)
  assert.equal(selectionCalls(fetchCalls)[0].body.sessionId, 'session-fallback')

  selection.set({ sessionId: 'session-workspace' })
  assert.equal(selectionCalls(fetchCalls).length, 2)
  assert.equal(selectionCalls(fetchCalls)[1].body.sessionId, 'session-workspace')

  current.set({ key: 'session-fallback' })
  assert.equal(selectionCalls(fetchCalls).length, 2)
  ctx.dispose()
})

test('the uiSession main binding is used when Workspace is unavailable', () => {
  const current = createStore({ key: 'session-c' })
  const fetchCalls = []
  const { ctx } = loadPlugin({ services: { uiSession: { adapter: { current } } }, fetchCalls })
  assert.equal(selectionCalls(fetchCalls).length, 1)
  assert.equal(selectionCalls(fetchCalls)[0].body.sessionId, 'session-c')
  ctx.dispose()
})

test('missing focus services announces incapable instead of pretending to observe', () => {
  const fetchCalls = []
  const { ctx } = loadPlugin({ services: {}, fetchCalls })
  assert.equal(selectionCalls(fetchCalls).length, 0)
  assert.equal(observerCalls(fetchCalls).length, 1)
  assert.equal(observerCalls(fetchCalls)[0].body.capable, false)
  ctx.dispose()
})

test('failed loopback fetch never breaks plugin apply', () => {
  const ctxRef = []
  assert.doesNotThrow(() => {
    const { ctx } = loadPlugin({ fetchImpl: () => { throw new Error('loopback unavailable') } })
    ctxRef.push(ctx)
  })
  ctxRef[0].dispose()
})
