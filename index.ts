/**
 * PersonaOS ADE Plugin for Orca Desktop & Mobile.
 *
 * Implements the Orca visual interface to PersonaOS persistent workforce:
 * - Worker roster & status inspection
 * - Task submission with active worktree context
 * - Review queue decisioning
 * - Strict prohibition on direct external agent CLI execution
 */

export interface PersonaOSWorker {
  id: string
  name: string
  role: string
  status: string
  current_profile_version: number
  capabilities: string[]
  permissions: string[]
}

export interface TaskSubmitOptions {
  workerId: string
  title: string
  objective: string
  worktreePath?: string
  worktreeId?: string
  branch?: string
  dependencies?: string[]
  runId?: string
  inputData?: Record<string, unknown>
}

export interface TaskReviewContext {
  task_id: string
  status: string
  review_notes?: string
  worker_id?: string
  generation?: number
}

/**
 * External coordination run representation in Orca ADE (ADR 0040).
 */
export interface OrchestrationRun {
  run_id: string
  objective: string
  coordinator_handle?: string
  consumer_generation: number
  legacy?: boolean
  created_at?: string
  updated_at?: string
}

/**
 * Task representation projected to/from Orca Orchestration DAG.
 */
export interface OrchestrationTask {
  task_id: string
  spec: string
  task_title?: string
  display_name?: string
  status: 'pending' | 'ready' | 'claimed' | 'running' | 'review' | 'completed' | 'failed'
  deps: string[]
  parent_id?: string
  run_id?: string
  personaos_task_id?: string
}

/**
 * Decision gate in Orca Orchestration mirroring PersonaOS review queue.
 */
export interface DecisionGate {
  gate_id: string
  task_id: string
  question: string
  options: string[]
  status: 'pending' | 'resolved'
  resolution?: string
  run_id?: string
}

/**
 * Inter-agent or coordinator lifecycle message.
 */
export interface InterAgentMessage {
  message_id: string
  msg_type: string
  subject: string
  from_handle?: string
  to_handle?: string
  body?: string
  task_id?: string
  dispatch_id?: string
  outcome?: 'succeeded' | 'failed'
  files_modified?: string[]
  phase?: string
  created_at?: string
}

/**
 * Server-Sent Events lifecycle event types and subscription models (ADR 0041 M6).
 */
export type GatewayEventType =
  | 'task_status_changed'
  | 'ready_wave_advanced'
  | 'gate_created'
  | 'gate_resolved'
  | 'heartbeat'
  | string

export interface GatewayEventMessage {
  event_id: string
  event_type: GatewayEventType
  payload: Record<string, unknown>
  timestamp: string
}

export type ConnectionState = 'connecting' | 'connected' | 'reconnecting' | 'disconnected'

export interface EventStreamOptions {
  runId?: string
  heartbeatInterval?: number
  initialLastEventId?: string
  maxReconnectAttempts?: number
  reconnectDelayMs?: number
  maxReconnectDelayMs?: number
  backoffMultiplier?: number
  heartbeatTimeoutMs?: number
  onEvent?: (event: GatewayEventMessage) => void
  onTaskStatusChanged?: (payload: Record<string, unknown>, eventId: string) => void
  onReadyWaveAdvanced?: (payload: Record<string, unknown>, eventId: string) => void
  onGateCreated?: (payload: Record<string, unknown>, eventId: string) => void
  onGateResolved?: (payload: Record<string, unknown>, eventId: string) => void
  onHeartbeat?: (timestamp: string) => void
  onConnectionStateChange?: (state: ConnectionState, error?: Error) => void
  onError?: (error: Error) => void
}

export interface EventStreamSubscription {
  unsubscribe: () => void
  close: () => void
  isConnected: () => boolean
  getConnectionState: () => ConnectionState
  getLastEventId: () => string | null
}

/**
 * Robust Server-Sent Events streaming manager with auto-reconnection and Last-Event-ID resumption.
 */
class SSEStreamManager implements EventStreamSubscription {
  private abortController: AbortController | null = null
  private connected = false
  private connectionState: ConnectionState = 'disconnected'
  private lastEventId: string | null = null
  private reconnectAttempts = 0
  private closed = false
  private watchdogTimer: any = null
  private reconnectTimer: any = null
  private retryDelayMs = 3000

  constructor(
    private baseUrl: string,
    private token: string | undefined,
    private options: EventStreamOptions
  ) {
    if (options.initialLastEventId) {
      this.lastEventId = options.initialLastEventId
    }
    this.retryDelayMs = options.reconnectDelayMs ?? 1000
  }

  start() {
    this.closed = false
    this.connect()
  }

  unsubscribe() {
    this.close()
  }

  close() {
    this.closed = true
    this.clearWatchdog()
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
    if (this.abortController) {
      this.abortController.abort()
      this.abortController = null
    }
    this.connected = false
    this.setConnectionState('disconnected')
  }

  isConnected(): boolean {
    return this.connected
  }

  getConnectionState(): ConnectionState {
    return this.connectionState
  }

  getLastEventId(): string | null {
    return this.lastEventId
  }

  private setConnectionState(state: ConnectionState, error?: Error) {
    this.connectionState = state
    this.connected = state === 'connected'
    this.options.onConnectionStateChange?.(state, error)
  }

  private resetWatchdog() {
    this.clearWatchdog()
    const timeoutMs = this.options.heartbeatTimeoutMs ?? 30000
    this.watchdogTimer = setTimeout(() => {
      // Half-open connection detected (no frame received within watchdog window)
      if (!this.closed && this.connected) {
        if (this.abortController) {
          this.abortController.abort()
        }
        this.handleDisconnect(new Error(`Watchdog timeout: no SSE frame received within ${timeoutMs}ms`))
      }
    }, timeoutMs)
  }

  private clearWatchdog() {
    if (this.watchdogTimer) {
      clearTimeout(this.watchdogTimer)
      this.watchdogTimer = null
    }
  }

  private async connect() {
    if (this.closed) return

    const isReconnect = this.reconnectAttempts > 0
    this.setConnectionState(isReconnect ? 'reconnecting' : 'connecting')

    this.abortController = new AbortController()
    const interval = this.options.heartbeatInterval ?? 15.0
    const queryParts: string[] = [`heartbeat_interval=${interval}`]
    if (this.options.runId) {
      queryParts.push(`run_id=${encodeURIComponent(this.options.runId)}`)
    }
    if (this.lastEventId) {
      queryParts.push(`last_event_id=${encodeURIComponent(this.lastEventId)}`)
    }

    const url = `${this.baseUrl}/integration/events?${queryParts.join('&')}`
    const headers: Record<string, string> = {
      Accept: 'text/event-stream',
      'Cache-Control': 'no-cache',
    }
    if (this.token) {
      headers['Authorization'] = `Bearer ${this.token}`
    }
    if (this.lastEventId) {
      headers['Last-Event-ID'] = this.lastEventId
    }

    try {
      const response = await fetch(url, {
        method: 'GET',
        headers,
        signal: this.abortController.signal,
      })

      if (!response.ok) {
        const errText = await response.text().catch(() => '')
        throw new Error(`SSE endpoint returned status ${response.status}: ${errText}`)
      }

      if (!response.body) {
        throw new Error('Response body is null, readable stream required for SSE')
      }

      this.reconnectAttempts = 0
      this.setConnectionState('connected')
      this.resetWatchdog()

      const reader = response.body.getReader()
      const decoder = new TextDecoder()
      let buffer = ''
      let currentEventId: string | null = null
      let currentEventType = 'message'
      let currentDataLines: string[] = []

      while (!this.closed) {
        const { value, done } = await reader.read()
        if (done) break

        this.resetWatchdog()
        buffer += decoder.decode(value, { stream: true })
        const lines = buffer.split(/\r\n|\r|\n/)
        // Last element may be a partial line, keep in buffer
        buffer = lines.pop() ?? ''

        for (const line of lines) {
          if (line === '') {
            // End of message block -> dispatch
            if (currentDataLines.length > 0 || currentEventType === 'heartbeat') {
              const dataStr = currentDataLines.join('\n')
              this.dispatchEvent(currentEventId, currentEventType, dataStr)
            }
            // Reset block fields
            currentEventId = null
            currentEventType = 'message'
            currentDataLines = []
            continue
          }

          if (line.startsWith(':')) {
            // Comment frame (e.g. ": connected")
            continue
          }

          if (line.startsWith('id:')) {
            currentEventId = line.substring(3).trim()
            if (currentEventId) {
              this.lastEventId = currentEventId
            }
          } else if (line.startsWith('event:')) {
            currentEventType = line.substring(6).trim()
          } else if (line.startsWith('data:')) {
            currentDataLines.push(line.substring(5).trim())
          } else if (line.startsWith('retry:')) {
            const parsedRetry = parseInt(line.substring(6).trim(), 10)
            if (!isNaN(parsedRetry) && parsedRetry > 0) {
              this.retryDelayMs = parsedRetry
            }
          }
        }
      }

      if (!this.closed) {
        this.handleDisconnect(new Error('SSE stream closed by server'))
      }
    } catch (err: any) {
      if (this.closed) return
      if (err.name === 'AbortError') return
      this.handleDisconnect(err instanceof Error ? err : new Error(String(err)))
    }
  }

  private dispatchEvent(eventId: string | null, eventType: string, rawData: string) {
    let payload: Record<string, unknown> = {}
    if (rawData) {
      try {
        const parsed = JSON.parse(rawData)
        if (typeof parsed === 'object' && parsed !== null) {
          // If the server wrapped the payload inside GatewayEventMessage, unwrap
          if ('payload' in parsed && typeof parsed.payload === 'object' && parsed.payload !== null) {
            payload = parsed.payload as Record<string, unknown>
            if ('event_id' in parsed && typeof parsed.event_id === 'string') {
              eventId = parsed.event_id
              this.lastEventId = eventId
            }
            if ('event_type' in parsed && typeof parsed.event_type === 'string') {
              eventType = parsed.event_type
            }
          } else {
            payload = parsed as Record<string, unknown>
          }
        } else {
          payload = { value: parsed }
        }
      } catch {
        payload = { raw: rawData }
      }
    }

    const eventMessage: GatewayEventMessage = {
      event_id: eventId ?? `evt-${Date.now()}`,
      event_type: eventType,
      payload,
      timestamp: new Date().toISOString(),
    }

    // Callback notifications
    if (eventType === 'heartbeat') {
      const hbTime = (payload.timestamp as string) || eventMessage.timestamp
      this.options.onHeartbeat?.(hbTime)
    } else if (eventType === 'task_status_changed') {
      this.options.onTaskStatusChanged?.(payload, eventMessage.event_id)
    } else if (eventType === 'ready_wave_advanced') {
      this.options.onReadyWaveAdvanced?.(payload, eventMessage.event_id)
    } else if (eventType === 'gate_created') {
      this.options.onGateCreated?.(payload, eventMessage.event_id)
    } else if (eventType === 'gate_resolved') {
      this.options.onGateResolved?.(payload, eventMessage.event_id)
    }

    this.options.onEvent?.(eventMessage)
  }

  private handleDisconnect(error: Error) {
    this.clearWatchdog()
    this.connected = false

    const maxAttempts = this.options.maxReconnectAttempts ?? Infinity
    if (this.reconnectAttempts >= maxAttempts) {
      this.setConnectionState('disconnected', error)
      this.options.onError?.(new Error(`Max reconnection attempts (${maxAttempts}) exceeded: ${error.message}`))
      return
    }

    this.setConnectionState('reconnecting', error)
    this.options.onError?.(error)

    const baseDelay = this.options.reconnectDelayMs ?? this.retryDelayMs
    const multiplier = this.options.backoffMultiplier ?? 1.5
    const maxDelay = this.options.maxReconnectDelayMs ?? 30000

    // Exponential backoff with 20% jitter
    const expDelay = Math.min(baseDelay * Math.pow(multiplier, this.reconnectAttempts), maxDelay)
    const jitter = 0.8 + Math.random() * 0.4
    const delay = Math.round(expDelay * jitter)

    this.reconnectAttempts++
    this.reconnectTimer = setTimeout(() => {
      this.connect()
    }, delay)
  }
}

export class PersonaOSOrcaPluginClient {
  private apiBaseUrl: string
  private token?: string

  constructor(apiBaseUrl = 'http://localhost:8000', token?: string) {
    this.apiBaseUrl = apiBaseUrl.replace(/\/+$/, '')
    this.token = token
  }

  private getHeaders(): Record<string, string> {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
    }
    if (this.token) {
      headers['Authorization'] = `Bearer ${this.token}`
    }
    return headers
  }

  /**
   * Fetch list of available persistent workers from PersonaOS.
   */
  async listWorkers(): Promise<PersonaOSWorker[]> {
    const res = await fetch(`${this.apiBaseUrl}/workers`, {
      method: 'GET',
      headers: this.getHeaders(),
    })
    if (!res.ok) {
      throw new Error(`Failed to list PersonaOS workers: ${res.status} ${res.statusText}`)
    }
    return (await res.json()) as PersonaOSWorker[]
  }

  /**
   * Enqueue a task to PersonaOS with active worktree binding and optional DAG dependencies.
   * Note: This NEVER spawns agent processes directly. Dispatch is handled
   * by PersonaOS WorkerExecutionKernel through the PAB Control Plane.
   */
  async submitTask(options: TaskSubmitOptions): Promise<{ id: string; status: string }> {
    const body = {
      worker_id: options.workerId,
      title: options.title,
      objective: options.objective,
      dependencies: options.dependencies || [],
      metadata: options.runId ? { run_id: options.runId } : undefined,
      input: {
        ...(options.inputData || {}),
        worktree: options.worktreeId ? {
          worktree_id: options.worktreeId,
          path: options.worktreePath,
          branch: options.branch,
        } : undefined,
      },
      side_effect_classification: 'UNKNOWN',
    }

    const res = await fetch(`${this.apiBaseUrl}/tasks`, {
      method: 'POST',
      headers: this.getHeaders(),
      body: JSON.stringify(body),
    })

    if (!res.ok) {
      const errText = await res.text()
      throw new Error(`Failed to submit task to PersonaOS: ${res.status} ${errText}`)
    }

    return (await res.json()) as { id: string; status: string }
  }

  /**
   * Retrieve human-in-the-loop diagnostic context for a task.
   */
  async getTaskReviewContext(taskId: string): Promise<TaskReviewContext> {
    const res = await fetch(`${this.apiBaseUrl}/tasks/${taskId}/review-context`, {
      method: 'GET',
      headers: this.getHeaders(),
    })
    if (!res.ok) {
      throw new Error(`Failed to get review context for task ${taskId}: ${res.status}`)
    }
    return (await res.json()) as TaskReviewContext
  }

  /**
   * Approve task in review queue.
   */
  async approveTask(taskId: string, notes?: string): Promise<{ ok: boolean }> {
    const res = await fetch(`${this.apiBaseUrl}/tasks/${taskId}/approve`, {
      method: 'POST',
      headers: this.getHeaders(),
      body: JSON.stringify({ review_notes: notes }),
    })
    if (!res.ok) {
      throw new Error(`Failed to approve task ${taskId}: ${res.status}`)
    }
    return (await res.json()) as { ok: boolean }
  }

  /**
   * Reject task in review queue.
   */
  async rejectTask(taskId: string, reason: string): Promise<{ ok: boolean }> {
    const res = await fetch(`${this.apiBaseUrl}/tasks/${taskId}/reject`, {
      method: 'POST',
      headers: this.getHeaders(),
      body: JSON.stringify({ reason }),
    })
    if (!res.ok) {
      throw new Error(`Failed to reject task ${taskId}: ${res.status}`)
    }
    return (await res.json()) as { ok: boolean }
  }

  /**
   * Resolve an Orca Decision Gate through PersonaOS Review Queue (ADR 0040).
   * Orca presents the decision gate UX, but resolution must invoke PersonaOS
   * with explicit generation fencing and operator audit.
   */
  async resolveDecisionGateThroughReview(
    taskId: string,
    expectedGeneration: number,
    operatorId: string,
    action: 'approve' | 'reject',
    notesOrReason: string
  ): Promise<{ ok: boolean }> {
    const url = `${this.apiBaseUrl}/tasks/${taskId}/${action}`
    const body = action === 'approve'
      ? { expected_generation: expectedGeneration, operator_id: operatorId, review_notes: notesOrReason }
      : { expected_generation: expectedGeneration, operator_id: operatorId, reason: notesOrReason }

    const res = await fetch(url, {
      method: 'POST',
      headers: this.getHeaders(),
      body: JSON.stringify(body),
    })
    if (!res.ok) {
      throw new Error(`Failed to resolve decision gate for task ${taskId}: ${res.status}`)
    }
    return (await res.json()) as { ok: boolean }
  }

  /**
   * Subscribe to the Integration Gateway Server-Sent Events lifecycle stream.
   * Features:
   * - Automatic reconnection with exponential backoff and jitter
   * - Last-Event-ID resumption (zero dropped events)
   * - Heartbeat watchdog to detect silent half-open connections
   * - Typed event callbacks
   */
  subscribeLifecycleEvents(options: EventStreamOptions = {}): EventStreamSubscription {
    const manager = new SSEStreamManager(this.apiBaseUrl, this.token, options)
    manager.start()
    return manager
  }

  // --------------------------------------------------------------------------
  // Standardized Gateway API Methods (/integration/*) - ADR 0041
  // --------------------------------------------------------------------------

  async getHealth(): Promise<{
    status: string
    gateway_version: string
    personaos_connected: boolean
    personaos_url: string
    pab_connected: boolean
    timestamp: string
  }> {
    const res = await fetch(`${this.apiBaseUrl}/integration/health`, {
      method: 'GET',
      headers: this.getHeaders(),
    })
    if (!res.ok) throw new Error(`Gateway health check failed: ${res.status}`)
    return await res.json()
  }

  async getDiagnostics(targetUrl?: string): Promise<Record<string, unknown>> {
    const query = targetUrl ? `?target_url=${encodeURIComponent(targetUrl)}` : ''
    const res = await fetch(`${this.apiBaseUrl}/integration/diagnostics${query}`, {
      method: 'GET',
      headers: this.getHeaders(),
    })
    if (!res.ok) throw new Error(`Diagnostics probe failed: ${res.status}`)
    return await res.json()
  }

  async listRuns(): Promise<OrchestrationRun[]> {
    const res = await fetch(`${this.apiBaseUrl}/integration/runs`, {
      method: 'GET',
      headers: this.getHeaders(),
    })
    if (!res.ok) throw new Error(`Failed to list coordination runs: ${res.status}`)
    return await res.json()
  }

  async createRun(run: {
    run_id: string
    objective: string
    coordinator_handle?: string
    metadata?: Record<string, unknown>
  }): Promise<OrchestrationRun> {
    const res = await fetch(`${this.apiBaseUrl}/integration/runs`, {
      method: 'POST',
      headers: this.getHeaders(),
      body: JSON.stringify(run),
    })
    if (!res.ok) throw new Error(`Failed to create coordination run: ${res.status}`)
    return await res.json()
  }

  async getRunDag(runId: string): Promise<{
    run_id: string
    nodes: Array<{
      task_id: string
      title: string
      worker_id?: string
      status: string
      dependencies: string[]
      wave: number
      generation: number
    }>
    edges: Array<{ from: string; to: string }>
    total_tasks: number
    completed_tasks: number
    ready_tasks: number
  }> {
    const res = await fetch(`${this.apiBaseUrl}/integration/runs/${encodeURIComponent(runId)}/dag`, {
      method: 'GET',
      headers: this.getHeaders(),
    })
    if (!res.ok) throw new Error(`Failed to get DAG for run ${runId}: ${res.status}`)
    return await res.json()
  }

  async getRunReadyWaves(runId: string): Promise<{
    run_id: string
    current_wave: number
    waves: Array<{
      wave_number: number
      ready_tasks: Array<{
        task_id: string
        title: string
        worker_id?: string
        priority: string
        generation: number
      }>
    }>
  }> {
    const res = await fetch(`${this.apiBaseUrl}/integration/runs/${encodeURIComponent(runId)}/ready-waves`, {
      method: 'GET',
      headers: this.getHeaders(),
    })
    if (!res.ok) throw new Error(`Failed to get ready waves for run ${runId}: ${res.status}`)
    return await res.json()
  }

  async listDecisionGates(runId?: string): Promise<DecisionGate[]> {
    const query = runId ? `?run_id=${encodeURIComponent(runId)}` : ''
    const res = await fetch(`${this.apiBaseUrl}/integration/decision-gates${query}`, {
      method: 'GET',
      headers: this.getHeaders(),
    })
    if (!res.ok) throw new Error(`Failed to list decision gates: ${res.status}`)
    return await res.json()
  }

  async getDecisionGate(gateId: string): Promise<DecisionGate> {
    const res = await fetch(`${this.apiBaseUrl}/integration/decision-gates/${encodeURIComponent(gateId)}`, {
      method: 'GET',
      headers: this.getHeaders(),
    })
    if (!res.ok) throw new Error(`Failed to get decision gate ${gateId}: ${res.status}`)
    return await res.json()
  }

  async resolveDecisionGate(
    gateId: string,
    decision: 'approve' | 'reject',
    expectedGeneration: number,
    reviewNotes?: string,
    selectedOption?: string
  ): Promise<{
    gate_id: string
    task_id: string
    resolved: boolean
    status: string
    new_generation: number
    timestamp: string
  }> {
    const res = await fetch(`${this.apiBaseUrl}/integration/decision-gates/${encodeURIComponent(gateId)}/resolve`, {
      method: 'POST',
      headers: this.getHeaders(),
      body: JSON.stringify({
        decision,
        expected_generation: expectedGeneration,
        review_notes: reviewNotes,
        selected_option: selectedOption,
      }),
    })
    if (!res.ok) {
      const errText = await res.text()
      throw new Error(`Failed to resolve decision gate ${gateId}: ${res.status} ${errText}`)
    }
    return await res.json()
  }

  async publishEvent(
    eventType: GatewayEventType,
    payload: Record<string, unknown> = {},
    eventId?: string
  ): Promise<GatewayEventMessage> {
    const res = await fetch(`${this.apiBaseUrl}/integration/events/publish`, {
      method: 'POST',
      headers: this.getHeaders(),
      body: JSON.stringify({
        event_id: eventId ?? `evt-${Date.now()}`,
        event_type: eventType,
        payload,
        timestamp: new Date().toISOString(),
      }),
    })
    if (!res.ok) throw new Error(`Failed to publish event: ${res.status}`)
    return await res.json()
  }

  /**
   * Guard verifying that no external agent binary is directly executed by the UI.
   * Invariant (ADR 0038 / ADR 0040): UI and ADE layers must NEVER spawn raw agent binaries.
   */
  assertZeroDirectAgentExecution(targetCommand: string): void {
    const prohibited = [
      'openclaw',
      'codex',
      'claude',
      'agy',
      'mcode',
      'dsh',
      'pi',
      'gemini',
      'cursor',
      'opencode',
      'opencode2',
    ]
    const firstWord = targetCommand.trim().split(/\s+/)[0].toLowerCase()
    for (const agent of prohibited) {
      if (firstWord === agent || firstWord.endsWith(`/${agent}`)) {
        throw new Error(
          `Direct execution of agent CLI '${agent}' is forbidden. All agent requests must be routed via PersonaOS REST API.`
        )
      }
    }
  }
}
