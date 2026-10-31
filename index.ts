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
  inputData?: Record<string, unknown>
}

export interface TaskReviewContext {
  task_id: string
  status: string
  review_notes?: string
  worker_id?: string
  generation?: number
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
   * Enqueue a task to PersonaOS with active worktree binding.
   * Note: This NEVER spawns agent processes directly. Dispatch is handled
   * by PersonaOS WorkerExecutionKernel through the PAB Control Plane.
   */
  async submitTask(options: TaskSubmitOptions): Promise<{ id: string; status: string }> {
    const body = {
      worker_id: options.workerId,
      title: options.title,
      objective: options.objective,
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
   * Guard verifying that no external agent binary is directly executed by the UI.
   */
  assertZeroDirectAgentExecution(targetCommand: string): void {
    const prohibited = ['openclaw', 'codex', 'claude', 'agy', 'mcode', 'dsh', 'pi']
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
