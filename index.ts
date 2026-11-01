/**
 * PersonaOS ADE Plugin for Orca Desktop & Mobile.
 *
 * Implements the Orca visual interface to PersonaOS persistent workforce:
 * - Worker roster & status inspection
 * - Task submission with active worktree context
 * - Review queue decisioning
 * - Strict prohibition on direct external agent CLI execution
 */

export * from './contracts'
import type {
  GatewayDagProjection,
  GatewayDiagnosticsResponse,
  GatewayHealthResponse,
  GatewayReadyWavesResponse,
  GatewayRunCreateRequest,
  GatewayRunSummary,
  GatewayWorkerSummary,
} from './contracts'

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
   * Run end-to-end connectivity diagnostics against the Integration Gateway (ADR 0041).
   */
  async checkDiagnostics(targetUrl?: string): Promise<GatewayDiagnosticsResponse> {
    const url = new URL(`${this.apiBaseUrl}/integration/diagnostics`)
    if (targetUrl) {
      url.searchParams.set('target_url', targetUrl)
    }
    const res = await fetch(url.toString(), {
      method: 'GET',
      headers: this.getHeaders(),
    })
    if (!res.ok) {
      throw new Error(`Diagnostics probe failed: ${res.status} ${res.statusText}`)
    }
    return (await res.json()) as GatewayDiagnosticsResponse
  }

  /**
   * Check Integration Gateway operational health.
   */
  async getIntegrationHealth(): Promise<GatewayHealthResponse> {
    const res = await fetch(`${this.apiBaseUrl}/integration/health`, {
      method: 'GET',
      headers: this.getHeaders(),
    })
    if (!res.ok) {
      throw new Error(`Gateway health probe failed: ${res.status} ${res.statusText}`)
    }
    return (await res.json()) as GatewayHealthResponse
  }

  /**
   * Fetch list of available persistent workers from PersonaOS Gateway (/integration/workers),
   * with automatic fallback to /workers endpoint.
   */
  async listWorkers(): Promise<GatewayWorkerSummary[]> {
    try {
      const res = await fetch(`${this.apiBaseUrl}/integration/workers`, {
        method: 'GET',
        headers: this.getHeaders(),
      })
      if (res.ok) {
        return (await res.json()) as GatewayWorkerSummary[]
      }
    } catch (_) {}

    const fallbackRes = await fetch(`${this.apiBaseUrl}/workers`, {
      method: 'GET',
      headers: this.getHeaders(),
    })
    if (!fallbackRes.ok) {
      throw new Error(`Failed to list PersonaOS workers: ${fallbackRes.status} ${fallbackRes.statusText}`)
    }
    return (await fallbackRes.json()) as GatewayWorkerSummary[]
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
        run_id: options.runId,
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
   * List active external coordination runs (ADR 0040 / ADR 0041).
   */
  async listRuns(): Promise<GatewayRunSummary[]> {
    const res = await fetch(`${this.apiBaseUrl}/integration/runs`, {
      method: 'GET',
      headers: this.getHeaders(),
    })
    if (!res.ok) {
      throw new Error(`Failed to list coordination runs: ${res.status} ${res.statusText}`)
    }
    return (await res.json()) as GatewayRunSummary[]
  }

  /**
   * Create or register an external coordination run.
   */
  async createRun(req: GatewayRunCreateRequest): Promise<GatewayRunSummary> {
    const res = await fetch(`${this.apiBaseUrl}/integration/runs`, {
      method: 'POST',
      headers: this.getHeaders(),
      body: JSON.stringify(req),
    })
    if (!res.ok) {
      const errText = await res.text()
      throw new Error(`Failed to create coordination run: ${res.status} ${errText}`)
    }
    return (await res.json()) as GatewayRunSummary
  }

  /**
   * Fetch pre-evaluated directed acyclic graph (DAG) projection for a coordination run.
   */
  async getRunDag(runId = 'default'): Promise<GatewayDagProjection> {
    const res = await fetch(`${this.apiBaseUrl}/integration/runs/${encodeURIComponent(runId)}/dag`, {
      method: 'GET',
      headers: this.getHeaders(),
    })
    if (!res.ok) {
      throw new Error(`Failed to fetch DAG for run ${runId}: ${res.status} ${res.statusText}`)
    }
    return (await res.json()) as GatewayDagProjection
  }

  /**
   * Fetch ready-to-execute tasks grouped by topological wave.
   */
  async getReadyWaves(runId = 'default'): Promise<GatewayReadyWavesResponse> {
    const res = await fetch(`${this.apiBaseUrl}/integration/runs/${encodeURIComponent(runId)}/ready-waves`, {
      method: 'GET',
      headers: this.getHeaders(),
    })
    if (!res.ok) {
      throw new Error(`Failed to fetch ready waves for run ${runId}: ${res.status} ${res.statusText}`)
    }
    return (await res.json()) as GatewayReadyWavesResponse
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
