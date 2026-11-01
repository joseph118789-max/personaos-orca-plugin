/**
 * PersonaOS–Orca Integration Gateway Type Contracts (ADR 0041).
 *
 * Defines the normalized, type-safe API contract for the PersonaOS Integration Gateway
 * (/integration/*) and Orca ADE integration layer.
 *
 * Invariants:
 * - Direct browser fetch() from Orca ADE to external PersonaOS backend ports is prohibited.
 * - All communication flows through the Gateway (Option B/C BFF).
 * - Zero Shadow State: The Gateway maintains no independent queue or scheduler.
 * - Generation Fencing: Gate resolution must supply expected_generation.
 */

/**
 * Structured diagnostic classification codes for ADE <-> PersonaOS connectivity.
 */
export enum ConnectionDiagnosticCode {
  CONNECTED = 'CONNECTED',
  NETWORK_UNREACHABLE = 'NETWORK_UNREACHABLE',
  CORS_BLOCKED = 'CORS_BLOCKED',
  AUTHENTICATION_FAILED = 'AUTHENTICATION_FAILED',
  ENDPOINT_NOT_FOUND = 'ENDPOINT_NOT_FOUND',
  API_VERSION_MISMATCH = 'API_VERSION_MISMATCH',
  INVALID_RESPONSE = 'INVALID_RESPONSE',
  SERVICE_UNAVAILABLE = 'SERVICE_UNAVAILABLE',
}

/**
 * Health check response from Integration Gateway (/integration/health).
 */
export interface GatewayHealthResponse {
  status: 'healthy' | 'degraded' | 'down' | string
  gateway_version: string
  personaos_connected: boolean
  personaos_url: string
  pab_connected: boolean
  timestamp: string
}

/**
 * Comprehensive diagnostic probe report for Orca ADE (/integration/diagnostics).
 */
export interface GatewayDiagnosticsResponse {
  code: ConnectionDiagnosticCode
  status: 'ok' | 'degraded' | 'error' | string
  latency_ms: number
  upstream_url: string
  cors_supported: boolean
  pna_supported: boolean
  auth_status: 'authenticated' | 'unauthenticated' | 'invalid' | string
  api_version_match: boolean
  remediation?: string | null
  details?: Record<string, unknown>
}

/**
 * Persistent worker summary for Orca ADE workforce roster (/integration/workers).
 */
export interface GatewayWorkerSummary {
  id: string
  name: string
  role: string
  status: 'idle' | 'running' | 'quarantined' | 'paused' | string
  current_profile_version: number
  capabilities: string[]
  permissions: string[]
  updated_at?: string | null
}

/**
 * Request payload to create an external coordination run (POST /integration/runs).
 */
export interface GatewayRunCreateRequest {
  run_id: string
  objective: string
  coordinator_handle?: string | null
  metadata?: Record<string, unknown>
}

/**
 * External coordination run summary (/integration/runs).
 */
export interface GatewayRunSummary {
  run_id: string
  objective: string
  coordinator_handle?: string | null
  status: 'active' | 'completed' | 'failed' | string
  consumer_generation: number
  legacy?: boolean
  created_at?: string | null
  updated_at?: string | null
}

/**
 * Projected task node in the orchestration DAG (/integration/runs/{run_id}/dag).
 */
export interface GatewayDagNode {
  task_id: string
  title: string
  worker_id?: string | null
  status: 'pending' | 'ready' | 'claimed' | 'running' | 'review' | 'completed' | 'failed' | string
  dependencies: string[]
  wave: number
  generation: number
}

/**
 * Directed dependency edge in the task DAG.
 */
export interface GatewayDagEdge {
  from: string
  to: string
}

/**
 * Pre-evaluated DAG projection returned to Orca ADE (/integration/runs/{run_id}/dag).
 */
export interface GatewayDagProjection {
  run_id: string
  nodes: GatewayDagNode[]
  edges: GatewayDagEdge[]
  total_tasks: number
  completed_tasks: number
  ready_tasks: number
}

/**
 * Task ready for execution within a ready wave.
 */
export interface GatewayReadyTask {
  task_id: string
  title: string
  worker_id?: string | null
  priority: 'low' | 'medium' | 'high' | 'urgent' | string
  generation: number
}

/**
 * Tier of ready tasks whose dependencies are fully resolved.
 */
export interface GatewayReadyWave {
  wave_number: number
  ready_tasks: GatewayReadyTask[]
}

/**
 * Evaluated ready waves for an orchestration run (/integration/runs/{run_id}/ready-waves).
 */
export interface GatewayReadyWavesResponse {
  run_id: string
  current_wave: number
  waves: GatewayReadyWave[]
}

/**
 * Interactive decision gate (HITL) mapped from a task in review status (/integration/decision-gates).
 */
export interface GatewayDecisionGate {
  gate_id: string
  task_id: string
  run_id?: string | null
  title: string
  question: string
  options: string[]
  status: 'pending' | 'approved' | 'rejected' | string
  execution_generation: number
  review_context: Record<string, unknown>
  created_at?: string | null
}

/**
 * Request payload to resolve an interactive decision gate (POST /integration/decision-gates/{gate_id}/resolve).
 */
export interface GatewayGateResolveRequest {
  decision: 'approve' | 'reject'
  expected_generation: number
  review_notes?: string | null
  selected_option?: string | null
}

/**
 * Response returned upon resolving a decision gate.
 */
export interface GatewayGateResolveResponse {
  gate_id: string
  task_id: string
  resolved: boolean
  status: 'approved' | 'rejected' | string
  new_generation: number
  timestamp: string
}

/**
 * Server-Sent Event message payload (/integration/events).
 */
export interface GatewayEventMessage {
  event_id: string
  event_type: 'task_status_changed' | 'ready_wave_advanced' | 'gate_created' | 'gate_resolved' | 'heartbeat' | string
  payload: Record<string, unknown>
  timestamp: string
}
