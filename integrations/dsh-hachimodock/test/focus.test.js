'use strict'

/**
 * DSH UI focus mirror: opaque main-session id only, bounded, loopback, and
 * safe for the PC to merge with the explicit P4 selection by timestamp.
 */

const test = require('node:test')
const assert = require('node:assert/strict')

const {
  startTestServer,
  requestJson,
  postJson,
  openSse,
  waitFor,
} = require('./helpers')

test('GET /focus is empty before the client half publishes a selection', async (t) => {
  const { server, url } = await startTestServer(t)
  const { status, body } = await requestJson(`${url}/focus`)
  assert.equal(status, 200)
  assert.equal(body.ok, true)
  assert.equal(body.focus, null)
  assert.equal(body.clientAttached, false)
  assert.equal(body.observerAttached, false)
  assert.equal(body.observerCapable, false)
  assert.equal(typeof body.instanceId, 'string')
  assert.equal(body.instanceId.length > 0, true)
  assert.equal(server.focusState(), null)
})

test('POST /focus accepts text/plain JSON, then GET exposes only the opaque id', async (t) => {
  const { url } = await startTestServer(t)
  const response = await fetch(`${url}/focus`, {
    method: 'POST',
    headers: { 'content-type': 'text/plain' },
    body: JSON.stringify({ sessionId: 'session-a' }),
  })
  assert.equal(response.status, 200)
  const accepted = await response.json()
  assert.equal(accepted.focus.sessionId, 'session-a')
  assert.equal(accepted.focus.source, 'dsh-ui')
  assert.equal(Number.isSafeInteger(accepted.focus.updatedAt), true)
  assert.equal(accepted.focus.updatedAt > 0, true)
  assert.equal(accepted.focus.seq > 0, true)

  const lookup = await requestJson(`${url}/focus`)
  assert.equal(lookup.body.focus.sessionId, 'session-a')
  assert.equal(lookup.body.focus.updatedAt, accepted.focus.updatedAt)
  assert.equal(lookup.body.focus.seq, accepted.focus.seq)
  // No title/content field is ever exposed.
  assert.deepEqual(Object.keys(lookup.body.focus).sort(), ['origin', 'seq', 'sessionId', 'source', 'updatedAt'])
})

test('same-id restore keeps the timestamp; same-id user re-selection refreshes it', async (t) => {
  const { url } = await startTestServer(t)
  const first = await postJson(`${url}/focus`, { sessionId: 'session-a', origin: 'user' })
  await new Promise((resolve) => setTimeout(resolve, 5))
  const restored = await postJson(`${url}/focus`, { sessionId: 'session-a', origin: 'restore' })
  assert.equal(restored.body.focus.updatedAt, first.body.focus.updatedAt)
  assert.equal(restored.body.focus.seq, first.body.focus.seq)
  assert.equal(restored.body.focus.origin, 'user')

  const repeated = await postJson(`${url}/focus`, { sessionId: 'session-a', origin: 'user' })
  assert.equal(repeated.body.focus.updatedAt >= first.body.focus.updatedAt, true)
  assert.ok(repeated.body.focus.seq > first.body.focus.seq)
  assert.equal(repeated.body.focus.origin, 'user')

  const changed = await postJson(`${url}/focus`, { sessionId: 'session-b', origin: 'user' })
  assert.equal(changed.body.focus.sessionId, 'session-b')
  assert.equal(changed.body.focus.updatedAt >= repeated.body.focus.updatedAt, true)
  assert.ok(changed.body.focus.seq > repeated.body.focus.seq)
})

test('observer capability is independent of SSE attachment and has a heartbeat TTL', async (t) => {
  const { server, url } = await startTestServer(t)
  // SSE client attached but an old/cached client half never announces focus.
  const sse = await openSse(`${url}/events?role=client`)
  try {
    await waitFor(() => server.hasClientSubscriber())
    const sseOnly = await requestJson(`${url}/focus`)
    assert.equal(sseOnly.body.clientAttached, true)
    assert.equal(sseOnly.body.observerAttached, false)
    assert.equal(sseOnly.body.observerCapable, false)
  } finally {
    sse.close()
  }
  await waitFor(() => !server.hasClientSubscriber())

  const capable = await postJson(`${url}/focus`, { observer: 'v1', capable: true })
  assert.equal(capable.status, 200)
  assert.equal(capable.body.observerAttached, true)
  assert.equal(capable.body.observerCapable, true)
  assert.equal(capable.body.focus, null)

  // Heartbeat refresh keeps it alive; an aged marker expires.
  server.focusObserver.attachedAt = server.now() - 16000
  const expired = await requestJson(`${url}/focus`)
  assert.equal(expired.body.observerAttached, false)
  assert.equal(expired.body.observerCapable, false)

  const incapable = await postJson(`${url}/focus`, { observer: 'v1', capable: false })
  assert.equal(incapable.body.observerAttached, true)
  assert.equal(incapable.body.observerCapable, false)
  const invalid = await postJson(`${url}/focus`, { observer: 'v2', capable: true })
  assert.equal(invalid.status, 400)
})

test('null or empty sessionId clears the focus; invalid values are rejected', async (t) => {
  const { url } = await startTestServer(t)
  await postJson(`${url}/focus`, { sessionId: 'session-a' })
  const cleared = await postJson(`${url}/focus`, { sessionId: null })
  assert.equal(cleared.status, 200)
  assert.equal(cleared.body.focus, null)
  assert.equal((await postJson(`${url}/focus`, { sessionId: 'session-b' })).status, 200)
  assert.equal((await postJson(`${url}/focus`, { sessionId: '' })).body.focus, null)

  for (const sessionId of [42, true, 'x'.repeat(513), 'bad\u0000id', 'line\nbreak']) {
    const { status, body } = await postJson(`${url}/focus`, { sessionId })
    assert.equal(status, 400, JSON.stringify(sessionId))
    assert.equal(body.error, 'invalid-request')
  }
  const arrayBody = await postJson(`${url}/focus`, [])
  assert.equal(arrayBody.status, 400)
  const malformed = await requestJson(`${url}/focus`, {
    method: 'POST',
    headers: { 'content-type': 'text/plain' },
    body: '{not json',
  })
  assert.equal(malformed.status, 400)
})

test('clientAttached reflects a role=client /events subscriber', async (t) => {
  const { server, url } = await startTestServer(t)
  const sse = await openSse(`${url}/events?role=client`)
  try {
    await waitFor(() => server.hasClientSubscriber())
    const attached = await requestJson(`${url}/focus`)
    assert.equal(attached.body.clientAttached, true)
    assert.equal(attached.body.observerAttached, false)
  } finally {
    sse.close()
  }
  await waitFor(() => !server.hasClientSubscriber())
  const detached = await requestJson(`${url}/focus`)
  assert.equal(detached.body.clientAttached, false)
})

test('wrong methods answer 405 and an unknown route stays 404', async (t) => {
  const { url } = await startTestServer(t)
  for (const method of ['PUT', 'DELETE']) {
    const { status } = await requestJson(`${url}/focus`, { method })
    assert.equal(status, 405)
  }
  const missing = await requestJson(`${url}/focus/extra`)
  assert.equal(missing.status, 404)
})
