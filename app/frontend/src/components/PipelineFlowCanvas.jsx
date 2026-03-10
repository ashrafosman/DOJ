import React, { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import StageDetailPanel from './StageDetailPanel';

// ─── Layout constants ─────────────────────────────────────────────────────────
const CANVAS_WIDTH = 1280;
const CANVAS_HEIGHT = 320;
const NODE_W = 120;
const NODE_H = 80;
const NODE_Y = 140;
const NODE_SPACING = 160;
const STAGES = ['Upload', 'Bronze', 'Mapping', 'Silver', 'Gold', 'Staging', 'Complete'];
const STAGE_NODES = STAGES.map((name, i) => ({
  id: name.toLowerCase(),
  label: name,
  x: 80 + i * NODE_SPACING,
  y: NODE_Y,
}));

const SYSTEM_COLORS = {
  LegacyCase: { hex: '#8b5cf6', track_y_offset: -18, label: 'LC' },
  OpenJustice: { hex: '#06b6d4', track_y_offset: 0, label: 'OJ' },
  AdHocExports: { hex: '#f97316', track_y_offset: 18, label: 'AH' },
};

const STAGE_ICONS = {
  Upload: '↑',
  Bronze: '⬡',
  Silver: '◈',
  Mapping: '↔',
  Gold: '★',
  Staging: '⊞',
  Complete: '✓',
};

// ─── Demo data ────────────────────────────────────────────────────────────────
function generateDemoSummary() {
  return {
    stages: [
      { id: 'upload', label: 'Upload', job_count: 1, status: 'complete', avg_duration_s: 22, sla_ok: true, active_jobs: [] },
      { id: 'bronze', label: 'Bronze', job_count: 2, status: 'running', avg_duration_s: 88, sla_ok: true, active_jobs: ['JOB-D2C4E9'] },
      { id: 'mapping', label: 'Mapping', job_count: 1, status: 'review', avg_duration_s: 180, sla_ok: false, active_jobs: ['JOB-B7F1A8'] },
      { id: 'silver', label: 'Silver', job_count: 1, status: 'failed', avg_duration_s: 310, sla_ok: false, active_jobs: [] },
      { id: 'gold', label: 'Gold', job_count: 1, status: 'running', avg_duration_s: 210, sla_ok: true, active_jobs: ['JOB-A4F2B1'] },
      { id: 'staging', label: 'Staging', job_count: 0, status: 'idle', avg_duration_s: 180, sla_ok: true, active_jobs: [] },
      { id: 'complete', label: 'Complete', job_count: 2, status: 'complete', avg_duration_s: 5, sla_ok: true, active_jobs: [] },
    ],
    active_jobs: [
      { id: 'JOB-A4F2B1', system: 'LegacyCase', stage_index: 4, status: 'running', file_name: 'cases_2024_q4.csv', rows: 142856 },
      { id: 'JOB-D2C4E9', system: 'AdHocExports', stage_index: 1, status: 'running', file_name: 'adhoc_parole_data.xlsx', rows: 8750 },
      { id: 'JOB-B7F1A8', system: 'OpenJustice', stage_index: 2, status: 'review', file_name: 'oj_contacts_2024.csv', rows: 29100 },
      { id: 'JOB-C9E3D7', system: 'LegacyCase', stage_index: 6, status: 'complete', file_name: 'defendants_export_jan.xlsx', rows: 58340 },
      { id: 'JOB-F5A3C2', system: 'OpenJustice', stage_index: 3, status: 'failed', file_name: 'oj_charges_batch7.csv', rows: 51200 },
    ],
    connector_flows: [
      { from: 'upload', to: 'bronze', job_count: 2, systems: ['LegacyCase', 'AdHocExports'] },
      { from: 'bronze', to: 'mapping', job_count: 1, systems: ['OpenJustice'] },
      { from: 'mapping', to: 'silver', job_count: 1, systems: ['OpenJustice'] },
      { from: 'gold', to: 'staging', job_count: 0, systems: [] },
    ],
  };
}

// ─── Utilities ────────────────────────────────────────────────────────────────
function statusColor(status) {
  switch (status) {
    case 'complete': return '#22c55e';
    case 'running': return '#f59e0b';
    case 'failed': return '#ef4444';
    case 'review': return '#ef4444';
    case 'upload': return '#3b82f6';
    default: return '#475569';
  }
}

function statusBorderGlow(status) {
  switch (status) {
    case 'complete': return 'drop-shadow(0 0 8px #22c55e60)';
    case 'running': return 'drop-shadow(0 0 10px #f59e0b80)';
    case 'failed': return 'drop-shadow(0 0 10px #ef444480)';
    case 'review': return 'drop-shadow(0 0 8px #ef444460)';
    default: return 'none';
  }
}

function formatDuration(seconds) {
  if (!seconds) return '—';
  if (seconds < 60) return `${seconds}s`;
  return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
}

// ─── CSS Keyframes injected once ─────────────────────────────────────────────
const ANIM_STYLE = `
  @keyframes moveDot {
    0%   { offset-distance: 0%; opacity: 0.2; }
    10%  { opacity: 1; }
    90%  { opacity: 1; }
    100% { offset-distance: 100%; opacity: 0.2; }
  }
  @keyframes nodeRunningPulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.6; }
  }
  @keyframes dashFlow {
    0%   { stroke-dashoffset: 24; }
    100% { stroke-dashoffset: 0; }
  }
  @keyframes trackDotMove {
    0%   { transform: translateX(0); opacity: 0; }
    5%   { opacity: 1; }
    95%  { opacity: 1; }
    100% { transform: translateX(var(--dot-travel)); opacity: 0; }
  }
`;

// ─── Particle dots on a connector ────────────────────────────────────────────
function FlowParticles({ x1, x2, y, systems, count }) {
  const pathId = `conn-path-${x1}-${x2}`;
  const pathD = `M${x1},${y} L${x2},${y}`;
  return (
    <>
      <defs>
        <path id={pathId} d={pathD} />
      </defs>
      {systems.slice(0, 3).map((sys, si) => {
        const sysColor = SYSTEM_COLORS[sys]?.hex || '#64748b';
        return [0, 1, 2].map(dotI => (
          <circle
            key={`${sys}-${dotI}`}
            r={3.5}
            fill={sysColor}
            style={{
              offsetPath: `path('${pathD}')`,
              offsetDistance: '0%',
              animation: `moveDot ${1.8 + si * 0.4}s linear ${dotI * 0.6 + si * 0.2}s infinite`,
              filter: `drop-shadow(0 0 4px ${sysColor})`,
            }}
          />
        ));
      })}
    </>
  );
}

// ─── Stage node ───────────────────────────────────────────────────────────────
function StageNode({ node, summary, isSelected, onClick, onHover, onLeave }) {
  const stageSummary = summary?.stages?.find(s => s.id === node.id);
  const status = stageSummary?.status || 'idle';
  const jobCount = stageSummary?.job_count || 0;
  const color = statusColor(status);
  const glow = statusBorderGlow(status);
  const isRunning = status === 'running';
  const isActive = status === 'running' || status === 'review';

  return (
    <g
      onClick={() => onClick(node)}
      onMouseEnter={(e) => onHover(node, stageSummary, e)}
      onMouseLeave={onLeave}
      style={{ cursor: 'pointer' }}
    >
      {/* Glow halo for active states */}
      {isActive && (
        <rect
          x={node.x - NODE_W / 2 - 4}
          y={node.y - NODE_H / 2 - 4}
          width={NODE_W + 8}
          height={NODE_H + 8}
          rx={14}
          fill={color}
          opacity={0.08}
          style={{ animation: isRunning ? 'nodeRunningPulse 1.5s ease-in-out infinite' : 'none' }}
        />
      )}

      {/* Main node rect */}
      <rect
        x={node.x - NODE_W / 2}
        y={node.y - NODE_H / 2}
        width={NODE_W}
        height={NODE_H}
        rx={10}
        fill={isSelected ? '#1e2d4a' : '#1a1f2e'}
        stroke={color}
        strokeWidth={isSelected ? 2.5 : isActive ? 2 : 1.5}
        style={{ filter: glow, transition: 'all 0.2s' }}
      />

      {/* Selected indicator */}
      {isSelected && (
        <rect
          x={node.x - NODE_W / 2}
          y={node.y - NODE_H / 2}
          width={NODE_W}
          height={NODE_H}
          rx={10}
          fill={color}
          opacity={0.06}
        />
      )}

      {/* Stage icon */}
      <text
        x={node.x}
        y={node.y - 14}
        textAnchor="middle"
        fontSize={18}
        fill={color}
        style={{ filter: isActive ? `drop-shadow(0 0 6px ${color})` : 'none' }}
      >
        {STAGE_ICONS[node.label] || '◆'}
      </text>

      {/* Stage label */}
      <text
        x={node.x}
        y={node.y + 4}
        textAnchor="middle"
        fontSize={11}
        fontWeight="600"
        fill="#f1f5f9"
        fontFamily="system-ui, sans-serif"
      >
        {node.label}
      </text>

      {/* Status text */}
      <text
        x={node.x}
        y={node.y + 18}
        textAnchor="middle"
        fontSize={9}
        fill={color}
        fontFamily="system-ui, sans-serif"
        style={{ animation: isRunning ? 'nodeRunningPulse 1.5s ease-in-out infinite' : 'none' }}
      >
        {status.toUpperCase()}
      </text>

      {/* Job count badge */}
      {jobCount > 0 && (
        <>
          <circle
            cx={node.x + NODE_W / 2 - 8}
            cy={node.y - NODE_H / 2 + 8}
            r={9}
            fill={color}
            opacity={0.9}
          />
          <text
            x={node.x + NODE_W / 2 - 8}
            y={node.y - NODE_H / 2 + 12}
            textAnchor="middle"
            fontSize={9}
            fontWeight="bold"
            fill="#fff"
            fontFamily="system-ui, sans-serif"
          >
            {jobCount}
          </text>
        </>
      )}
    </g>
  );
}

// ─── Connector between two nodes ──────────────────────────────────────────────
function Connector({ from, to, flow }) {
  const x1 = from.x + NODE_W / 2;
  const x2 = to.x - NODE_W / 2;
  const y = NODE_Y + NODE_H / 2 - NODE_H / 2;
  const midY = NODE_Y;

  const hasFlow = flow && flow.job_count > 0;
  const systems = flow?.systems || [];
  const thickness = Math.min(8, 2 + (flow?.job_count || 0));
  const activeColor = systems[0] ? SYSTEM_COLORS[systems[0]]?.hex || '#3b82f6' : '#3b82f680';

  return (
    <g>
      {/* Base connector line */}
      <line
        x1={x1} y1={midY}
        x2={x2} y2={midY}
        stroke="#2d3748"
        strokeWidth={2}
      />

      {/* Active flow line */}
      {hasFlow && (
        <line
          x1={x1} y1={midY}
          x2={x2} y2={midY}
          stroke={activeColor}
          strokeWidth={thickness}
          strokeOpacity={0.5}
          strokeDasharray="8 4"
          style={{
            animation: 'dashFlow 0.8s linear infinite',
            filter: `drop-shadow(0 0 4px ${activeColor})`,
          }}
        />
      )}

      {/* Arrow head */}
      <polygon
        points={`${x2},${midY - 5} ${x2 + 7},${midY} ${x2},${midY + 5}`}
        fill={hasFlow ? activeColor : '#2d3748'}
        opacity={hasFlow ? 0.9 : 0.5}
      />

      {/* Animated particles */}
      {hasFlow && (
        <FlowParticles x1={x1} x2={x2 - 7} y={midY} systems={systems} count={flow.job_count} />
      )}
    </g>
  );
}

// ─── Per-system job track ─────────────────────────────────────────────────────
function SystemTrack({ system, jobs, allNodes }) {
  const sysConfig = SYSTEM_COLORS[system];
  if (!sysConfig) return null;

  const sysJobs = jobs.filter(j => j.system === system);
  const trackY = NODE_Y + sysConfig.track_y_offset;
  const trackColor = sysConfig.hex;

  return (
    <g>
      {/* Track line */}
      <line
        x1={allNodes[0].x - NODE_W / 2}
        y1={trackY}
        x2={allNodes[allNodes.length - 1].x + NODE_W / 2}
        y2={trackY}
        stroke={trackColor}
        strokeWidth={1}
        strokeOpacity={0.2}
        strokeDasharray="4 8"
      />

      {/* Job dots */}
      {sysJobs.map((job, i) => {
        const node = allNodes[job.stage_index];
        if (!node) return null;
        const dotX = node.x;
        const dotColor = statusColor(job.status);
        const isActive = job.status === 'running';

        return (
          <g key={job.id}>
            {/* Pulse ring for active jobs */}
            {isActive && (
              <circle
                cx={dotX}
                cy={trackY}
                r={10}
                fill={trackColor}
                opacity={0.15}
                style={{ animation: 'nodeRunningPulse 1.2s ease-in-out infinite' }}
              />
            )}
            {/* Main dot */}
            <circle
              cx={dotX}
              cy={trackY}
              r={5}
              fill={trackColor}
              stroke={dotColor}
              strokeWidth={1.5}
              style={{ filter: `drop-shadow(0 0 4px ${trackColor})` }}
            />
            {/* Job label */}
            <text
              x={dotX}
              y={trackY - 9}
              textAnchor="middle"
              fontSize={8}
              fill={trackColor}
              fontFamily="monospace"
              opacity={0.85}
            >
              {job.id.split('-')[1]}
            </text>
          </g>
        );
      })}
    </g>
  );
}

// ─── Tooltip ──────────────────────────────────────────────────────────────────
function NodeTooltip({ node, stageSummary, pos, visible }) {
  if (!visible || !node) return null;
  const status = stageSummary?.status || 'idle';
  const color = statusColor(status);

  return (
    <div
      className="fixed z-50 pointer-events-none bg-doj-surface border border-doj-border rounded-xl shadow-2xl p-3 min-w-[180px]"
      style={{ left: pos.x + 12, top: pos.y - 10 }}
    >
      <div className="flex items-center gap-2 mb-2">
        <span className="text-sm font-bold text-doj-text">{node.label}</span>
        <span
          className="px-1.5 py-0.5 rounded text-[10px] font-bold uppercase"
          style={{ backgroundColor: color + '30', color, border: `1px solid ${color}60` }}
        >
          {status}
        </span>
      </div>
      <div className="space-y-1 text-xs">
        <div className="flex justify-between gap-4">
          <span className="text-doj-muted">Active jobs</span>
          <span className="font-mono text-doj-text">{stageSummary?.job_count || 0}</span>
        </div>
        <div className="flex justify-between gap-4">
          <span className="text-doj-muted">Avg time</span>
          <span className="font-mono text-doj-text">{formatDuration(stageSummary?.avg_duration_s)}</span>
        </div>
        <div className="flex justify-between gap-4">
          <span className="text-doj-muted">SLA</span>
          <span className={`font-mono font-bold ${stageSummary?.sla_ok ? 'text-doj-green' : 'text-doj-red'}`}>
            {stageSummary?.sla_ok ? 'OK' : 'BREACHED'}
          </span>
        </div>
      </div>
      <div className="mt-2 pt-2 border-t border-doj-border/50 text-[10px] text-doj-muted">
        Click to drill down
      </div>
    </div>
  );
}

// ─── Legend ───────────────────────────────────────────────────────────────────
function Legend() {
  const statusItems = [
    { label: 'Complete', color: '#22c55e' },
    { label: 'Running', color: '#f59e0b' },
    { label: 'Failed/Review', color: '#ef4444' },
    { label: 'Idle', color: '#475569' },
  ];
  const systemItems = Object.entries(SYSTEM_COLORS).map(([k, v]) => ({ label: k, color: v.hex }));

  return (
    <div className="flex items-center justify-between px-4 py-3 bg-doj-surface-2 border-t border-doj-border">
      <div className="flex items-center gap-4">
        <span className="text-[10px] text-doj-muted uppercase tracking-wider">Status:</span>
        {statusItems.map(s => (
          <div key={s.label} className="flex items-center gap-1.5">
            <div className="w-2.5 h-2.5 rounded-sm" style={{ backgroundColor: s.color }} />
            <span className="text-xs text-doj-muted">{s.label}</span>
          </div>
        ))}
      </div>
      <div className="flex items-center gap-4">
        <span className="text-[10px] text-doj-muted uppercase tracking-wider">Systems:</span>
        {systemItems.map(s => (
          <div key={s.label} className="flex items-center gap-1.5">
            <div className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: s.color, boxShadow: `0 0 4px ${s.color}` }} />
            <span className="text-xs text-doj-muted">{s.label}</span>
          </div>
        ))}
      </div>
      <div className="flex items-center gap-1.5">
        <div className="w-8 h-0.5 border-t-2 border-dashed border-doj-blue opacity-60" />
        <span className="text-xs text-doj-muted">Active flow</span>
        <div className="w-2 h-2 rounded-full bg-doj-blue" style={{ animation: 'nodeRunningPulse 1s infinite' }} />
        <span className="text-xs text-doj-muted">Particle</span>
      </div>
    </div>
  );
}

// ─── Datasource status cards ──────────────────────────────────────────────────
function DatasourceCards({ activeJobs }) {
  const systems = Object.entries(SYSTEM_COLORS);
  return (
    <div className="grid grid-cols-3 gap-3 mb-4">
      {systems.map(([sysName, cfg]) => {
        const sysJobs = activeJobs.filter(j => j.system === sysName);
        const runningJob = sysJobs.find(j => j.status === 'running');
        const latestJob = runningJob || sysJobs[sysJobs.length - 1];
        const overallStatus = sysJobs.some(j => j.status === 'running') ? 'running'
          : sysJobs.some(j => j.status === 'failed') ? 'failed'
          : sysJobs.some(j => j.status === 'review') ? 'review'
          : sysJobs.some(j => j.status === 'complete') ? 'complete'
          : 'idle';
        const statusColor2 = statusColor(overallStatus);
        const isRunning = overallStatus === 'running';

        return (
          <div
            key={sysName}
            className="bg-doj-surface border rounded-xl px-4 py-3 flex flex-col gap-1.5"
            style={{ borderColor: cfg.hex + '40' }}
          >
            {/* Header row */}
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <span
                  className="w-2.5 h-2.5 rounded-full"
                  style={{ backgroundColor: cfg.hex, boxShadow: `0 0 6px ${cfg.hex}` }}
                />
                <span className="text-sm font-semibold" style={{ color: cfg.hex }}>{sysName}</span>
              </div>
              <span
                className="text-[10px] font-bold uppercase px-1.5 py-0.5 rounded"
                style={{ backgroundColor: statusColor2 + '25', color: statusColor2, border: `1px solid ${statusColor2}50` }}
              >
                {overallStatus}
                {isRunning && <span style={{ animation: 'nodeRunningPulse 1s infinite', display: 'inline-block', marginLeft: 3 }}>•</span>}
              </span>
            </div>

            {/* Current stage / file */}
            {latestJob ? (
              <>
                <div className="text-xs text-doj-muted truncate" title={latestJob.file_name}>
                  {latestJob.file_name}
                </div>
                <div className="flex items-center justify-between text-xs">
                  <span className="text-doj-muted">
                    Stage: <span className="text-doj-text font-medium">{STAGES[latestJob.stage_index]}</span>
                  </span>
                  {latestJob.rows > 0 && (
                    <span className="font-mono text-doj-muted">{latestJob.rows.toLocaleString()} rows</span>
                  )}
                </div>
              </>
            ) : (
              <div className="text-xs text-doj-muted">No active jobs</div>
            )}
          </div>
        );
      })}
    </div>
  );
}

// ─── Main Component ───────────────────────────────────────────────────────────
export function PipelineFlowCanvas() {
  const [summary, setSummary] = useState(null);
  const [selectedStage, setSelectedStage] = useState(null);
  const [tooltip, setTooltip] = useState({ visible: false, node: null, stageSummary: null, pos: { x: 0, y: 0 } });
  const [triggering, setTriggering] = useState(false);
  const [triggerMsg, setTriggerMsg] = useState(null);
  const tooltipTimer = useRef(null);

  const fetchSummary = useCallback(async () => {
    try {
      const res = await fetch('/api/stages/summary');
      if (res.ok) {
        const data = await res.json();
        // Backend returns a dict with stages/active_jobs/connector_flows
        if (data && data.stages) {
          // Normalize active_jobs to ensure all values are primitives (guards against
          // unexpected object types that would cause React error #31)
          data.active_jobs = (Array.isArray(data.active_jobs) ? data.active_jobs : []).map(j => ({
            id: String(j.id ?? ''),
            system: String(j.system ?? ''),
            stage_index: Number(j.stage_index ?? 0),
            status: String(j.status ?? 'idle'),
            file_name: String(j.file_name ?? ''),
            rows: Number(j.rows ?? 0),
          }));
          setSummary(data);
        } else {
          setSummary(generateDemoSummary());
        }
      } else {
        setSummary(generateDemoSummary());
      }
    } catch {
      setSummary(generateDemoSummary());
    }
  }, []);

  const handleTrigger = useCallback(async () => {
    setTriggering(true);
    setTriggerMsg(null);
    try {
      const res = await fetch('/api/pipeline/trigger', { method: 'POST' });
      if (res.ok) {
        const data = await res.json();
        setTriggerMsg({ type: 'success', text: `Started run ${data.run_id} — ${data.job_ids.length} datasource jobs queued` });
        await fetchSummary();
      } else {
        const err = await res.json().catch(() => ({}));
        setTriggerMsg({ type: 'error', text: err.detail || 'Trigger failed' });
      }
    } catch (e) {
      setTriggerMsg({ type: 'error', text: e.message || 'Network error' });
    } finally {
      setTriggering(false);
      setTimeout(() => setTriggerMsg(null), 6000);
    }
  }, [fetchSummary]);

  useEffect(() => {
    fetchSummary();
    const interval = setInterval(fetchSummary, 10000);
    return () => clearInterval(interval);
  }, [fetchSummary]);

  const handleNodeClick = useCallback((node) => {
    setSelectedStage(prev => prev?.id === node.id ? null : node);
  }, []);

  const handleNodeHover = useCallback((node, stageSummary, e) => {
    clearTimeout(tooltipTimer.current);
    tooltipTimer.current = setTimeout(() => {
      setTooltip({ visible: true, node, stageSummary, pos: { x: e.clientX, y: e.clientY } });
    }, 200);
  }, []);

  const handleNodeLeave = useCallback(() => {
    clearTimeout(tooltipTimer.current);
    setTooltip(prev => ({ ...prev, visible: false }));
  }, []);

  // Build connector flows
  const connectorFlows = useMemo(() => {
    const flows = summary?.connector_flows || [];
    return STAGES.slice(0, -1).map((s, i) => {
      const fromNode = STAGE_NODES[i];
      const toNode = STAGE_NODES[i + 1];
      const flow = flows.find(f => f.from === fromNode.id && f.to === toNode.id);
      return { from: fromNode, to: toNode, flow };
    });
  }, [summary]);

  const activeJobs = summary?.active_jobs || [];

  return (
    <div className="flex gap-0 h-full">
      {/* ── Main canvas area ── */}
      <div className={`flex-1 transition-all duration-300 ${selectedStage ? 'mr-0' : ''}`}>
        <div className="mb-4 flex items-start justify-between">
          <div>
            <h1 className="text-xl font-bold text-doj-text">Pipeline Flow</h1>
            <p className="text-sm text-doj-muted mt-0.5">
              Live data flow visualization — click any stage to drill down
            </p>
          </div>
          <div className="flex flex-col items-end gap-2">
            <button
              onClick={handleTrigger}
              disabled={triggering}
              className="flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-semibold transition-all"
              style={{
                backgroundColor: triggering ? '#1e2d4a' : '#1a56db',
                color: triggering ? '#64748b' : '#fff',
                border: '1px solid',
                borderColor: triggering ? '#2d3748' : '#2563eb',
                cursor: triggering ? 'not-allowed' : 'pointer',
                opacity: triggering ? 0.7 : 1,
              }}
            >
              {triggering ? (
                <>
                  <span style={{ animation: 'nodeRunningPulse 0.8s ease-in-out infinite', display: 'inline-block' }}>◌</span>
                  Starting…
                </>
              ) : (
                <>▶ Start Ingestion</>
              )}
            </button>
            {triggerMsg && (
              <div
                className="text-xs px-3 py-1.5 rounded-lg max-w-xs text-right"
                style={{
                  backgroundColor: triggerMsg.type === 'success' ? '#14532d30' : '#7f1d1d30',
                  color: triggerMsg.type === 'success' ? '#22c55e' : '#ef4444',
                  border: `1px solid ${triggerMsg.type === 'success' ? '#22c55e40' : '#ef444440'}`,
                }}
              >
                {triggerMsg.text}
              </div>
            )}
          </div>
        </div>

        {/* Datasource status cards */}
        <DatasourceCards activeJobs={activeJobs} />

        <div className="bg-doj-surface border border-doj-border rounded-xl overflow-hidden">
          {/* Injected animation styles */}
          <style>{ANIM_STYLE}</style>

          {/* SVG Canvas */}
          <div className="relative overflow-x-auto">
            <svg
              viewBox={`0 0 ${CANVAS_WIDTH} ${CANVAS_HEIGHT}`}
              width="100%"
              height={CANVAS_HEIGHT}
              style={{ minWidth: 900 }}
              className="bg-doj-bg"
            >
              {/* Grid pattern */}
              <defs>
                <pattern id="grid" width="40" height="40" patternUnits="userSpaceOnUse">
                  <path d="M 40 0 L 0 0 0 40" fill="none" stroke="#1e2434" strokeWidth="0.5" />
                </pattern>
                <linearGradient id="canvasGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#0f1117" />
                  <stop offset="100%" stopColor="#0c0e14" />
                </linearGradient>
              </defs>
              <rect width={CANVAS_WIDTH} height={CANVAS_HEIGHT} fill="url(#canvasGrad)" />
              <rect width={CANVAS_WIDTH} height={CANVAS_HEIGHT} fill="url(#grid)" />

              {/* Stage labels above nodes */}
              {STAGE_NODES.map(node => (
                <text
                  key={`label-${node.id}`}
                  x={node.x}
                  y={NODE_Y - NODE_H / 2 - 14}
                  textAnchor="middle"
                  fontSize={10}
                  fill="#475569"
                  fontFamily="monospace"
                  letterSpacing="0.5"
                >
                  {(node.label).toUpperCase()}
                </text>
              ))}

              {/* System tracks (behind nodes) */}
              {Object.keys(SYSTEM_COLORS).map(sys => (
                <SystemTrack
                  key={sys}
                  system={sys}
                  jobs={activeJobs}
                  allNodes={STAGE_NODES}
                />
              ))}

              {/* Connectors */}
              {connectorFlows.map(({ from, to, flow }, i) => (
                <Connector key={i} from={from} to={to} flow={flow} />
              ))}

              {/* Stage nodes */}
              {STAGE_NODES.map(node => (
                <StageNode
                  key={node.id}
                  node={node}
                  summary={summary}
                  isSelected={selectedStage?.id === node.id}
                  onClick={handleNodeClick}
                  onHover={handleNodeHover}
                  onLeave={handleNodeLeave}
                />
              ))}

              {/* Stage duration labels below nodes */}
              {STAGE_NODES.map((node, i) => {
                const s = summary?.stages?.[i];
                return s ? (
                  <text
                    key={`dur-${node.id}`}
                    x={node.x}
                    y={NODE_Y + NODE_H / 2 + 18}
                    textAnchor="middle"
                    fontSize={9}
                    fill="#475569"
                    fontFamily="monospace"
                  >
                    avg {formatDuration(s.avg_duration_s)}
                  </text>
                ) : null;
              })}

              {/* Title watermark */}
              <text
                x={CANVAS_WIDTH - 16}
                y={CANVAS_HEIGHT - 10}
                textAnchor="end"
                fontSize={9}
                fill="#1e2434"
                fontFamily="monospace"
                letterSpacing="1"
              >
                DOJ MIGRATION MONITOR • PIPELINE VIEW
              </text>
            </svg>
          </div>

          {/* Legend */}
          <Legend />
        </div>

        {/* Active jobs mini-table */}
        {activeJobs.length > 0 && (
          <div className="mt-4 bg-doj-surface border border-doj-border rounded-xl overflow-hidden">
            <div className="px-4 py-3 border-b border-doj-border bg-doj-surface-2 flex items-center justify-between">
              <span className="text-sm font-semibold text-doj-text">Active Jobs in Pipeline</span>
              <span className="text-xs text-doj-muted">{activeJobs.filter(j => j.status === 'running').length} running</span>
            </div>
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-doj-border/50">
                  {['Job ID', 'File', 'System', 'Stage', 'Status', 'Rows'].map(h => (
                    <th key={h} className="px-4 py-2 text-left text-doj-muted font-medium">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {activeJobs.map((job, i) => {
                  const sysConfig = SYSTEM_COLORS[job.system];
                  const sColor = statusColor(job.status);
                  return (
                    <tr key={job.id} className={`hover:bg-white/2 ${i < activeJobs.length - 1 ? 'border-b border-doj-border/30' : ''}`}>
                      <td className="px-4 py-2 font-mono text-doj-blue">{job.id}</td>
                      <td className="px-4 py-2 text-doj-muted max-w-[160px] truncate">{job.file_name}</td>
                      <td className="px-4 py-2">
                        <span className="flex items-center gap-1.5">
                          <span className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: sysConfig?.hex }} />
                          <span style={{ color: sysConfig?.hex }}>{job.system}</span>
                        </span>
                      </td>
                      <td className="px-4 py-2 text-doj-text">{STAGES[job.stage_index]}</td>
                      <td className="px-4 py-2">
                        <span className="font-medium" style={{ color: sColor }}>{job.status}</span>
                      </td>
                      <td className="px-4 py-2 font-mono text-doj-muted">{job.rows?.toLocaleString() || '—'}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* ── Stage detail panel ── */}
      {selectedStage && (
        <StageDetailPanel
          stage={selectedStage}
          summary={summary}
          onClose={() => setSelectedStage(null)}
        />
      )}

      {/* Tooltip */}
      <NodeTooltip
        node={tooltip.node}
        stageSummary={tooltip.stageSummary}
        pos={tooltip.pos}
        visible={tooltip.visible}
      />
    </div>
  );
}

export default PipelineFlowCanvas;
