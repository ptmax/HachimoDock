'use strict'

/**
 * Pure in-memory task registry for HachimoDock-dispatched DSH tasks.
 *
 * The bridge receives a `POST /task`, calls `agent.followup()` on a live DSH
 * agent, and then keeps observing the agent's session events. This module maps
 * those observations to one bounded task record:
 *
 *   accepted -> queued -> working -> tool_running -> waiting_user
 *            -> done | stopped | error | cancelled
 *
 * It knows nothing about Cordis or HTTP so `node --test` can drive it directly.
 * Tasks are process-local and deliberately not persisted: after a DSH restart
 * `POST /task` can no longer be queried, and the PC must treat the result as
 * unknown instead of retrying the prompt.
 *
 * @module @hachimodock/dsh-bridge/tasks
 */

/** Maximum retained task records (oldest terminal entries are evicted first). */
const DEFAULT_TASK_LIMIT = 128
/** How long a terminal task stays queryable. */
const DEFAULT_TASK_TTL_MS = 15 * 60 * 1000
/** Longest result excerpt exposed to the PC/device body path. */
const MAX_RESULT_CHARS = 383
/** States that can still transition. */
const ACTIVE_STATES = new Set(['accepted', 'queued', 'working', 'tool_running', 'waiting_user'])
/** States after which a task is immutable. */
const TERMINAL_STATES = new Set(['done', 'stopped', 'error', 'cancelled'])
/**
 * Truncate one string field.
 * @param {unknown} value - candidate.
 * @param {number} max - maximum characters.
 * @returns {string} bounded string, or an empty string.
 */
function boundedText(value, max) {
  if (typeof value !== 'string') return ''
  const text = value.trim()
  return text.length <= max ? text : `${text.slice(0, max - 1)}…`
}

/**
 * Map one `turn/end` reason kind to a terminal task state.
 * Older DSH hosts omit the reason; preserve their historical completed shape.
 * @param {unknown} reasonKind - `data.reason.kind` from `turn/end`.
 * @returns {'done'|'stopped'|'error'} terminal state.
 */
function terminalStateForReason(reasonKind) {
  if (reasonKind === 'error') return 'error'
  if (reasonKind === 'completed') return 'done'
  // Older hosts omit the reason entirely; preserve their historical completed
  // shape. Any other reported ending (aborted/interrupted/blocked/max-tokens/
  // forked/unknown future kinds) must never look like a completed task.
  if (typeof reasonKind === 'string' && reasonKind !== '') return 'stopped'
  return 'done'
}

/**
 * Create one bounded task registry.
 * @param {object} [options] - injectable bounds.
 * @param {() => number} [options.now] - clock in epoch milliseconds.
 * @param {number} [options.ttlMs] - terminal retention.
 * @param {number} [options.limit] - maximum task records.
 * @returns {DshTaskRegistry} registry.
 */
function createDshTaskRegistry({ now = () => Date.now(), ttlMs = DEFAULT_TASK_TTL_MS, limit = DEFAULT_TASK_LIMIT } = {}) {
  return new DshTaskRegistry({ now, ttlMs, limit })
}

class DshTaskRegistry {
  /**
   * @param {object} [options] - injectable bounds.
   * @param {() => number} [options.now] - clock.
   * @param {number} [options.ttlMs] - terminal retention.
   * @param {number} [options.limit] - maximum task records.
   */
  constructor({ now = () => Date.now(), ttlMs = DEFAULT_TASK_TTL_MS, limit = DEFAULT_TASK_LIMIT } = {}) {
    this.now = now
    this.ttlMs = ttlMs
    this.limit = limit
    /** @type {Map<string, object>} taskId -> task (insertion order = age). */
    this.tasks = new Map()
    /** @type {Map<string, string>} messageId -> taskId. */
    this.byMessage = new Map()
    /** @type {Map<string, number>} sessionId -> latest observed turn. */
    this.currentTurn = new Map()
  }

  /**
   * Store one newly accepted task. Repeated ids return the original record.
   * @param {object} args - accepted task facts.
   * @param {string} args.taskId - client-generated id.
   * @param {string} args.messageId - DSH user message id used for inbox correlation.
   * @param {string} args.sessionId - resolved live session id.
   * @param {string} args.requestedSessionId - `auto` or the explicit id asked for.
   * @param {string} args.textHash - request hash used to detect conflicting retries.
   * @param {boolean} args.queued - whether the agent was already running.
   * @returns {{task: object, duplicate: boolean}} public task summary.
   */
  accept({ taskId, messageId, sessionId, requestedSessionId, textHash, queued }) {
    this.sweep()
    const existing = this.tasks.get(taskId)
    if (existing) {
      return { task: this.summary(taskId), duplicate: true }
    }
    const stamp = this.now()
    const task = {
      taskId,
      messageId,
      sessionId,
      requestedSessionId,
      textHash,
      state: queued === true ? 'queued' : 'accepted',
      queued: queued === true,
      turn: null,
      reason: '',
      resultExcerpt: '',
      acceptedAt: stamp,
      updatedAt: stamp,
    }
    this.tasks.set(taskId, task)
    this.byMessage.set(messageId, taskId)
    this.trim()
    return { task: this.summary(taskId), duplicate: false }
  }

  /**
   * @param {string} taskId - task id.
   * @returns {object|undefined} internal record.
   */
  record(taskId) {
    return this.tasks.get(taskId)
  }

  /**
   * @param {string} taskId - task id.
   * @returns {object|null} public summary, or null when unknown/expired.
   */
  get(taskId) {
    this.sweep()
    const task = this.tasks.get(taskId)
    return task ? this.publicSummary(task) : null
  }

  /**
   * Public bounded view of one task.
   * @param {string} taskId - task id.
   * @returns {object|null} summary or null.
   */
  summary(taskId) {
    return this.get(taskId)
  }

  /**
   * Turn the internal record into the `GET /task` shape.
   * @param {object} task - internal record.
   * @returns {object} public summary.
   */
  publicSummary(task) {
    return {
      taskId: task.taskId,
      sessionId: task.sessionId,
      state: task.state,
      queued: task.queued,
      turn: Number.isSafeInteger(task.turn) ? task.turn : null,
      reason: task.reason || '',
      resultExcerpt: task.resultExcerpt,
      acceptedAt: task.acceptedAt,
      updatedAt: task.updatedAt,
    }
  }

  /**
   * Bind the inbox message to the turn that claimed it.
   * @param {object} args - claim facts.
   * @param {string} args.messageId - claimed DSH message id.
   * @param {number} [args.turn] - claimed turn number.
   * @param {string} [args.sessionId] - owning session id when available.
   * @returns {object|null} updated summary.
   */
  claim({ messageId, turn, sessionId }) {
    const taskId = this.byMessage.get(messageId)
    if (!taskId) return null
    const task = this.tasks.get(taskId)
    if (!task || !ACTIVE_STATES.has(task.state)) return this.publicSummary(task)
    if (typeof sessionId === 'string' && sessionId !== '') task.sessionId = sessionId
    if (Number.isSafeInteger(turn)) task.turn = turn
    task.state = 'working'
    this.touch(task)
    return this.publicSummary(task)
  }

  /**
   * An inbox message was discarded before it could run.
   * @param {object} args - discard facts.
   * @param {string} args.messageId - discarded message id.
   * @returns {object|null} updated summary.
   */
  discard({ messageId }) {
    const taskId = this.byMessage.get(messageId)
    if (!taskId) return null
    const task = this.tasks.get(taskId)
    if (!task || !ACTIVE_STATES.has(task.state)) return this.publicSummary(task)
    task.state = 'cancelled'
    task.reason = 'discarded'
    this.touch(task)
    return this.publicSummary(task)
  }

  /**
   * Observe a turn boundary for queued/bound tasks.
   * @param {string} sessionId - session id.
   * @param {number} turn - turn number when available.
   * @returns {void}
   */
  noteTurnStart(sessionId, turn) {
    if (typeof sessionId !== 'string' || sessionId === '') return
    if (Number.isSafeInteger(turn)) this.currentTurn.set(sessionId, turn)
    for (const task of this.tasks.values()) {
      if (task.sessionId !== sessionId || !ACTIVE_STATES.has(task.state)) continue
      // Only a task already bound by `agent/inbox/claimed` belongs to this
      // turn. A queued task must never inherit a pre-existing turn.
      if (task.turn === null) continue
      if (Number.isSafeInteger(turn) && task.turn !== turn) continue
      task.state = 'working'
      this.touch(task)
    }
  }

  /**
   * Observe tool activity.
   * @param {string} sessionId - session id.
   * @param {'start'|'end'} phase - tool phase.
   * @returns {void}
   */
  noteTool(sessionId, phase) {
    if (typeof sessionId !== 'string' || sessionId === '') return
    const turn = this.currentTurn.get(sessionId)
    for (const task of this.tasks.values()) {
      if (task.sessionId !== sessionId || !ACTIVE_STATES.has(task.state)) continue
      if (task.turn === null) continue
      if (Number.isSafeInteger(turn) && task.turn !== turn) continue
      task.state = phase === 'start' ? 'tool_running' : 'working'
      this.touch(task)
    }
  }

  /**
   * Remember the latest assistant text for the task's current turn.
   * @param {string} sessionId - session id.
   * @param {string} text - bounded assistant text.
   * @returns {void}
   */
  noteAssistant(sessionId, text) {
    if (typeof sessionId !== 'string' || sessionId === '' || typeof text !== 'string') return
    const excerpt = boundedText(text, MAX_RESULT_CHARS)
    if (excerpt === '') return
    const turn = this.currentTurn.get(sessionId)
    for (const task of this.tasks.values()) {
      if (task.sessionId !== sessionId || !ACTIVE_STATES.has(task.state)) continue
      if (task.turn === null || !Number.isSafeInteger(turn) || task.turn !== turn) continue
      task.resultExcerpt = excerpt
      this.touch(task)
    }
  }

  /**
   * Reflect an approval prompt that belongs to a task's session.
   * @param {string} sessionId - session id.
   * @param {boolean} pending - whether an approval is currently pending.
   * @returns {void}
   */
  noteApproval(sessionId, pending) {
    if (typeof sessionId !== 'string' || sessionId === '') return
    for (const task of this.tasks.values()) {
      if (task.sessionId !== sessionId || !ACTIVE_STATES.has(task.state)) continue
      // A queued task behind someone else's turn must not become waiting_user
      // because another task in the same session asked for approval.
      if (task.turn === null && task.state !== 'working' && task.state !== 'tool_running' && task.state !== 'waiting_user') {
        continue
      }
      if (pending) {
        task.state = 'waiting_user'
      } else if (task.state === 'waiting_user') {
        task.state = 'working'
      }
      this.touch(task)
    }
  }

  /**
   * Close the task bound to one completed turn.
   * @param {string} sessionId - session id.
   * @param {number|undefined} turn - turn number when reported.
   * @param {unknown} reasonKind - `turn/end` reason kind.
   * @returns {void}
   */
  finishTurn(sessionId, turn, reasonKind) {
    if (typeof sessionId !== 'string' || sessionId === '') return
    // Without a turn number there is no safe way to attribute this ending to a
    // specific queued task; leave queued/unbound tasks untouched and let the
    // PC time out to unknown rather than claiming a false completion.
    if (!Number.isSafeInteger(turn)) return
    const state = terminalStateForReason(reasonKind)
    const reason = typeof reasonKind === 'string' ? reasonKind : ''
    for (const task of this.tasks.values()) {
      if (task.sessionId !== sessionId || !ACTIVE_STATES.has(task.state)) continue
      if (task.turn === null || task.turn !== turn) continue
      task.state = state
      task.reason = reason
      this.touch(task)
    }
  }

  /**
   * Mark every active task in a session as stopped by the user.
   * @param {string} sessionId - session id.
   * @returns {void}
   */
  cancelSession(sessionId) {
    if (typeof sessionId !== 'string' || sessionId === '') return
    for (const task of this.tasks.values()) {
      if (task.sessionId !== sessionId || !ACTIVE_STATES.has(task.state)) continue
      // A device stop applies to the turn that is actually executing. Do not
      // erase accepted/queued follow-up messages; they belong to the user and
      // must remain available for the next turn. Explicit task cancellation
      // (discard/cancel) handles those separately.
      if (task.state === 'queued') continue
      task.state = 'stopped'
      task.reason = 'user'
      this.touch(task)
    }
  }

  /**
   * Mark a task failed before it was accepted.
   * @param {string} taskId - task id.
   * @param {string} reason - bounded failure code.
   * @returns {void}
   */
  fail(taskId, reason) {
    const task = this.tasks.get(taskId)
    if (!task || !ACTIVE_STATES.has(task.state)) return
    task.state = 'error'
    task.reason = boundedText(reason, 80)
    this.touch(task)
  }

  /**
   * @param {object} task - internal record.
   * @returns {void}
   */
  touch(task) {
    task.updatedAt = this.now()
  }

  /**
   * Remove expired terminal tasks, then enforce the record limit.
   * @returns {void}
   */
  sweep() {
    const cutoff = this.now() - this.ttlMs
    for (const [taskId, task] of [...this.tasks]) {
      if (TERMINAL_STATES.has(task.state) && task.updatedAt < cutoff) this.remove(taskId)
    }
    this.trim()
  }

  /**
   * Keep the registry bounded: drop terminal records first.
   * @returns {void}
   */
  trim() {
    if (this.tasks.size <= this.limit) return
    for (const [taskId, task] of [...this.tasks]) {
      if (this.tasks.size <= this.limit) return
      if (TERMINAL_STATES.has(task.state)) this.remove(taskId)
    }
    for (const taskId of [...this.tasks.keys()]) {
      if (this.tasks.size <= this.limit) return
      this.remove(taskId)
    }
  }

  /**
   * @param {string} taskId - task id.
   * @returns {void}
   */
  remove(taskId) {
    const task = this.tasks.get(taskId)
    if (!task) return
    this.tasks.delete(taskId)
    if (task.messageId && this.byMessage.get(task.messageId) === taskId) {
      this.byMessage.delete(task.messageId)
    }
  }
}

module.exports = {
  createDshTaskRegistry,
  DshTaskRegistry,
  terminalStateForReason,
  DEFAULT_TASK_LIMIT,
  DEFAULT_TASK_TTL_MS,
  MAX_RESULT_CHARS,
  ACTIVE_STATES,
  TERMINAL_STATES,
}
