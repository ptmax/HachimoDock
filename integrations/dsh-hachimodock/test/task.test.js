'use strict'

/**
 * POST /task and GET /task: tracked task acceptance, idempotency, failure,
 * cancellation, turn correlation and approval propagation.
 *
 * The transport is exercised against a real BridgeServer with the fake Cordis
 * context from helpers.js; no DSH process is required.
 */

const test = require('node:test')
const assert = require('node:assert/strict')
const net = require('node:net')

const {
  createFakeSession,
  createFakeAgent,
  createFakeCtx,
  createApprovalRequest,
  openSse,
  postJson,
  requestJson,
  waitFor,
  startTestBridge,
} = require('./helpers')

const TASK_ID = '64b2c8d4-0000-4000-8000-000000000001'

function makeBridge(t) {
  const session = createFakeSession('session-task')
  const agent = createFakeAgent(session)
  const ctx = createFakeCtx({ sessions: [session], agents: [agent] })
  return startTestBridge(t, ctx).then(({ bridge, url }) => ({ bridge, url, ctx, session, agent }))
}

test('POST /task accepts one followup and GET /task reports it', async (t) => {
  const { url, agent, session } = await makeBridge(t)
  const { status, body } = await postJson(`${url}/task`, {
    taskId: TASK_ID,
    sessionId: 'auto',
    text: '把项目的测试修好',
    origin: 'realtime-voice',
  })
  assert.equal(status, 200)
  assert.equal(body.ok, true)
  assert.equal(body.taskId, TASK_ID)
  assert.equal(body.sessionId, session.id)
  assert.equal(body.state, 'accepted')
  assert.equal(body.duplicate, false)
  assert.equal(agent.followups.length, 1)
  assert.equal(agent.followups[0].role, 'user')
  assert.equal(agent.followups[0].content[0].text, '把项目的测试修好')
  assert.equal(agent.followups[0].source.kind, 'user')

  const lookup = await requestJson(`${url}/task?taskId=${encodeURIComponent(TASK_ID)}`)
  assert.equal(lookup.status, 200)
  assert.equal(lookup.body.ok, true)
  assert.equal(lookup.body.task.taskId, TASK_ID)
  assert.equal(lookup.body.task.state, 'accepted')
})

test('the same taskId is idempotent and a different payload conflicts', async (t) => {
  const { url, agent } = await makeBridge(t)
  const first = await postJson(`${url}/task`, {
    taskId: TASK_ID,
    sessionId: 'auto',
    text: '修好测试',
  })
  assert.equal(first.status, 200)
  const repeated = await postJson(`${url}/task`, {
    taskId: TASK_ID,
    sessionId: 'auto',
    text: '修好测试',
  })
  assert.equal(repeated.status, 200)
  assert.equal(repeated.body.duplicate, true)
  assert.equal(agent.followups.length, 1, 'retrying one task must not re-prompt DSH')

  const conflict = await postJson(`${url}/task`, {
    taskId: TASK_ID,
    sessionId: 'auto',
    text: '另一个任务',
  })
  assert.equal(conflict.status, 409)
  assert.equal(conflict.body.error, 'duplicate-conflict')
})

test('invalid, missing-session and missing-agent dispatch fail closed', async (t) => {
  const session = createFakeSession('session-live')
  const ctx = createFakeCtx({ sessions: [session], agents: [] })
  const { bridge, url } = await startTestBridge(t, ctx)
  assert.ok(bridge)

  const emptyText = await postJson(`${url}/task`, { taskId: TASK_ID, text: '   ' })
  assert.equal(emptyText.status, 400)
  assert.equal(emptyText.body.error, 'invalid-request')

  const emptyId = await postJson(`${url}/task`, { taskId: '', text: '任务' })
  assert.equal(emptyId.status, 400)

  const missingAgent = await postJson(`${url}/task`, { taskId: TASK_ID, sessionId: 'session-live', text: '任务' })
  assert.equal(missingAgent.status, 409)
  assert.equal(missingAgent.body.error, 'agent-not-live')

  const unknownSession = await postJson(`${url}/task`, {
    taskId: TASK_ID,
    sessionId: 'session-missing',
    text: '任务',
  })
  assert.equal(unknownSession.status, 409)
  assert.equal(unknownSession.body.error, 'session-not-found')

  const unknownTask = await requestJson(`${url}/task?taskId=missing`)
  assert.equal(unknownTask.status, 404)
  assert.equal(unknownTask.body.error, 'task-not-found')
})

test('explicit session targeting beats auto and never falls back from a stale id', async (t) => {
  const newer = createFakeSession('session-newer', { createdAt: 2000 })
  const older = createFakeSession('session-older', { createdAt: 1000 })
  const newerAgent = createFakeAgent(newer)
  const olderAgent = createFakeAgent(older)
  const ctx = createFakeCtx({ sessions: [older, newer], agents: [olderAgent, newerAgent] })
  const { url } = await startTestBridge(t, ctx)

  const explicit = await postJson(`${url}/task`, {
    taskId: TASK_ID,
    sessionId: 'session-older',
    text: '发到指定会话',
  })
  assert.equal(explicit.status, 200)
  assert.equal(explicit.body.sessionId, 'session-older')
  assert.equal(olderAgent.followups.length, 1)
  assert.equal(newerAgent.followups.length, 0)

  const auto = await postJson(`${url}/task`, {
    taskId: '64b2c8d4-0000-4000-8000-000000000002',
    sessionId: 'auto',
    text: '发到最近会话',
  })
  assert.equal(auto.status, 200)
  assert.equal(auto.body.sessionId, 'session-newer')
  assert.equal(newerAgent.followups.length, 1)

  const stale = await postJson(`${url}/task`, {
    taskId: '64b2c8d4-0000-4000-8000-000000000003',
    sessionId: 'session-gone',
    text: '不能落到别的会话',
  })
  assert.equal(stale.status, 409)
  assert.equal(stale.body.error, 'session-not-found')
  assert.equal(olderAgent.followups.length, 1)
  assert.equal(newerAgent.followups.length, 1)
})

test('a running DSH agent queues the task instead of claiming it started', async (t) => {
  const { url, agent } = await makeBridge(t)
  agent.status = 'running'
  const { status, body } = await postJson(`${url}/task`, {
    taskId: TASK_ID,
    sessionId: 'auto',
    text: '追加任务',
  })
  assert.equal(status, 200)
  assert.equal(body.state, 'queued')
  assert.equal(body.queued, true)
})

test('inbox claim binds the turn; assistant text and completed turn finish the task', async (t) => {
  const { bridge, url, ctx, session, agent } = await makeBridge(t)
  const accepted = await postJson(`${url}/task`, {
    taskId: TASK_ID,
    sessionId: 'auto',
    text: '整理文档',
  })
  assert.equal(accepted.status, 200)
  const message = agent.followups[0]
  ctx.dispatchSync('agent/inbox/claimed', {
    message,
    turn: 9,
    agent: { id: session.id, session },
  })
  const claimed = await requestJson(`${url}/task?taskId=${TASK_ID}`)
  assert.equal(claimed.body.task.state, 'working')
  assert.equal(claimed.body.task.turn, 9)

  ctx.dispatchSync('session/event', session, { type: 'turn/start', time: 1000, data: { turn: 9 } })
  ctx.dispatchSync('session/event', session, {
    type: 'assistant/message',
    time: 1001,
    data: { message: { content: [{ type: 'thinking', text: 'secret reasoning' }, { type: 'text', text: '已完成文档整理' }] } },
  })
  const midway = await requestJson(`${url}/task?taskId=${TASK_ID}`)
  assert.equal(midway.body.task.state, 'working')
  assert.equal(midway.body.task.resultExcerpt, '已完成文档整理')

  ctx.dispatchSync('session/event', session, {
    type: 'turn/end',
    time: 1002,
    data: { turn: 9, reason: { kind: 'completed' } },
  })
  const done = await requestJson(`${url}/task?taskId=${TASK_ID}`)
  assert.equal(done.body.task.state, 'done')
  assert.equal(done.body.task.reason, 'completed')
  assert.equal(bridge.server.events.some((event) => event.type === 'turn' && event.phase === 'end'), true)
})

test('aborted and failed turn endings are not reported as done', async (t) => {
  const { url, ctx, session, agent } = await makeBridge(t)
  await postJson(`${url}/task`, { taskId: TASK_ID, sessionId: 'auto', text: '任务' })
  ctx.dispatchSync('agent/inbox/claimed', {
    message: agent.followups[0],
    turn: 3,
    agent: { session },
  })
  ctx.dispatchSync('session/event', session, { type: 'turn/start', time: 1, data: { turn: 3 } })
  ctx.dispatchSync('session/event', session, {
    type: 'turn/end',
    time: 2,
    data: { turn: 3, reason: { kind: 'aborted' } },
  })
  const stopped = await requestJson(`${url}/task?taskId=${TASK_ID}`)
  assert.equal(stopped.body.task.state, 'stopped')
  assert.equal(stopped.body.task.reason, 'aborted')
})

test('interrupt cancels the running task and marks it stopped', async (t) => {
  const { bridge, url, session, agent, ctx } = await makeBridge(t)
  agent.status = 'running'
  await postJson(`${url}/task`, { taskId: TASK_ID, sessionId: 'auto', text: '长任务' })
  ctx.dispatchSync('agent/inbox/claimed', {
    message: agent.followups[0],
    turn: 4,
    agent: { session },
  })
  assert.equal(bridge.tasks.get(TASK_ID).state, 'working')
  const response = await postJson(`${url}/interrupt`, { sessionId: session.id })
  assert.equal(response.status, 200)
  assert.equal(response.body.interrupted, true)
  assert.deepEqual(agent.cancelled, { kind: 'user' })
  const task = await requestJson(`${url}/task?taskId=${TASK_ID}`)
  assert.equal(task.body.task.state, 'stopped')
  assert.equal(task.body.task.reason, 'user')
  assert.ok(bridge)
})


test('the 32K character contract works for CJK text that exceeds the generic 64KB body cap', async (t) => {
  const { url } = await makeBridge(t)
  const text = '汉'.repeat(22000) // ~66 KB UTF-8, still under the 32K-char contract
  const accepted = await postJson(`${url}/task`, { taskId: TASK_ID, sessionId: 'auto', text })
  assert.equal(accepted.status, 200)
  assert.equal(accepted.body.ok, true)

  const tooLong = await postJson(`${url}/task`, {
    taskId: '64b2c8d4-0000-4000-8000-0000000000ff',
    sessionId: 'auto',
    text: '汉'.repeat(32 * 1024 + 1),
  })
  assert.equal(tooLong.status, 400)
  assert.equal(tooLong.body.error, 'invalid-request')
})

test('a lost POST response still leaves one followup for the takeover probe', async (t) => {
  const { url, agent } = await makeBridge(t)
  const body = JSON.stringify({ taskId: TASK_ID, sessionId: 'auto', text: '连接丢失任务' })
  const port = Number(new URL(url).port)
  await new Promise((resolve) => {
    const socket = net.connect(port, '127.0.0.1', () => {
      socket.end(
        `POST /task HTTP/1.1\r\nHost: 127.0.0.1:${port}\r\n` +
        `Content-Type: application/json\r\nContent-Length: ${Buffer.byteLength(body)}\r\n` +
        `Connection: close\r\n\r\n${body}`,
      )
    })
    socket.once('finish', () => {
      socket.destroy()
      resolve()
    })
    socket.on('error', () => {
      socket.destroy()
      resolve()
    })
  })
  await waitFor(() => agent.followups.length === 1)
  const probe = await requestJson(`${url}/task?taskId=${TASK_ID}`)
  assert.equal(probe.status, 200)
  assert.equal(probe.body.task.taskId, TASK_ID)
  assert.equal(probe.body.task.state, 'accepted')
  assert.equal(agent.followups.length, 1, 'the lost response must not lead to a duplicate followup')
})

test('concurrent duplicate taskId submissions follow up exactly once', async (t) => {
  const { url, agent } = await makeBridge(t)
  const responses = await Promise.all(
    Array.from({ length: 10 }, () => postJson(`${url}/task`, {
      taskId: TASK_ID,
      sessionId: 'auto',
      text: '同一个任务',
    })),
  )
  assert.equal(responses.every((response) => response.status === 200), true)
  assert.equal(responses.filter((response) => response.body.duplicate === false).length, 1)
  assert.equal(responses.filter((response) => response.body.duplicate === true).length, 9)
  assert.equal(agent.followups.length, 1, 'parallel retries must never duplicate the DSH prompt')
})

test('a discarded inbox message marks its task cancelled', async (t) => {
  const { url, ctx, agent } = await makeBridge(t)
  await postJson(`${url}/task`, { taskId: TASK_ID, sessionId: 'auto', text: '排队任务' })
  ctx.dispatchSync('agent/inbox/discarded', { message: agent.followups[0] })
  const task = await requestJson(`${url}/task?taskId=${TASK_ID}`)
  assert.equal(task.body.task.state, 'cancelled')
  assert.equal(task.body.task.reason, 'discarded')
})

test('interrupt preserves accepted/queued task records while idle', async (t) => {
  const { url, session, agent } = await makeBridge(t)
  agent.status = 'running'
  await postJson(`${url}/task`, { taskId: TASK_ID, sessionId: 'auto', text: '尚未开始的任务' })
  const before = await requestJson(`${url}/task?taskId=${TASK_ID}`)
  assert.equal(before.body.task.state, 'queued')
  agent.status = 'idle'
  const response = await postJson(`${url}/interrupt`, { sessionId: session.id })
  assert.equal(response.status, 200)
  assert.equal(response.body.interrupted, false)
  const task = await requestJson(`${url}/task?taskId=${TASK_ID}`)
  assert.equal(task.body.task.state, before.body.task.state)
  assert.notEqual(task.body.task.state, 'stopped')
})

test('an approval marks the bound task waiting_user and clears after a decision', async (t) => {
  const { bridge, url, session, agent, ctx } = await makeBridge(t)
  await postJson(`${url}/task`, { taskId: TASK_ID, sessionId: 'auto', text: '需要权限的任务' })
  ctx.dispatchSync('agent/inbox/claimed', {
    message: agent.followups[0],
    turn: 2,
    agent: { session },
  })
  ctx.dispatchSync('session/event', session, { type: 'turn/start', time: 1, data: { turn: 2 } })

  const events = await openSse(`${url}/events`)
  try {
    const pending = bridge.handleApprovalRequest(createApprovalRequest(session), async () => 'unavailable')
    const frame = await events.next()
    assert.equal(frame.event, 'approval')
    await waitFor(() => bridge.tasks.get(TASK_ID)?.state === 'waiting_user')
    const decision = await postJson(`${url}/approval/decision`, {
      requestId: frame.data.requestId,
      decision: 'allow',
    })
    assert.equal(decision.status, 200)
    assert.equal(await pending, 'allowed-once')
    await waitFor(() => bridge.tasks.get(TASK_ID)?.state === 'working')
  } finally {
    events.close()
  }
})
