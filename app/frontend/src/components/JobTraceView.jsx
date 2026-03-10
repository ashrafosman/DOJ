import React, { useState, useEffect, useCallback } from 'react';

// ─── Constants ────────────────────────────────────────────────────────────────
const STAGES = ['Upload', 'Bronze', 'Silver', 'Mapping', 'Gold', 'Staging', 'Complete'];

const STATUS_COLORS = {
  complete: { fill: '#22c55e', bg: 'rgba(34,197,94,0.15)', border: '#22c55e', text: '#22c55e', label: 'Complete' },
  running: { fill: '#f59e0b', bg: 'rgba(245,158,11,0.15)', border: '#f59e0b', text: '#f59e0b', label: 'Running' },
  failed: { fill: '#ef4444', bg: 'rgba(239,68,68,0.15)', border: '#ef4444', text: '#ef4444', label: 'Failed' },
  review: { fill: '#ef4444', bg: 'rgba(239,68,68,0.12)', border: '#ef4444', text: '#ef4444', label: 'Review' },
  blocked: { fill: '#475569', bg: 'rgba(71,85,105,0.12)', border: '#475569', text: '#475569', label: 'Blocked' },
  idle: { fill: '#2d3748', bg: 'rgba(45,55,72,0.3)', border: '#2d3748', text: '#475569', label: 'Idle' },
  upload: { fill: '#3b82f6', bg: 'rgba(59,130,246,0.15)', border: '#3b82f6', text: '#3b82f6', label: 'Uploading' },
};

const SYSTEM_COLORS = {
  LegacyCase: '#8b5cf6',
  OpenJustice: '#06b6d4',
  AdHocExports: '#f97316',
};

// ─── Demo data ────────────────────────────────────────────────────────────────
function buildDemoTrace(jobId) {
  return {
    job_id: jobId || 'JOB-A4F2B1',
    file_name: 'cases_2024_q4.csv',
    system: 'LegacyCase',
    overall_status: 'running',
    total_duration_s: 1395,
    stages: [
      {
        name: 'Upload', status: 'complete', duration_s: 45,
        started_at: '2024-01-15T09:00:00Z', ended_at: '2024-01-15T09:00:45Z',
        rows_in: 142856, rows_out: 142856, issues: 0,
      },
      {
        name: 'Bronze', status: 'complete', duration_s: 120,
        started_at: '2024-01-15T09:00:45Z', ended_at: '2024-01-15T09:02:45Z',
        rows_in: 142856, rows_out: 142820, issues: 0,
      },
      {
        name: 'Silver', status: 'complete', duration_s: 310,
        started_at: '2024-01-15T09:02:45Z', ended_at: '2024-01-15T09:07:55Z',
        rows_in: 142820, rows_out: 141990, issues: 2,
      },
      {
        name: 'Mapping', status: 'complete', duration_s: 200,
        started_at: '2024-01-15T09:07:55Z', ended_at: '2024-01-15T09:11:15Z',
        rows_in: 141990, rows_out: 141750, issues: 3,
      },
      {
        name: 'Gold', status: 'running', duration_s: 720,
        started_at: '2024-01-15T09:11:15Z', ended_at: null,
        rows_in: 141750, rows_out: null, issues: 1,
      },
      {
        name: 'Staging', status: 'idle', duration_s: null,
        started_at: null, ended_at: null,
        rows_in: null, rows_out: null, issues: 0,
      },
      {
        name: 'Complete', status: 'idle', duration_s: null,
        started_at: null, ended_at: null,
        rows_in: null, rows_out: null, issues: 0,
      },
    ],
    reconciliation_issues: [
      {
        stage: 'Silver',
        items: [
          { id: 'QR-001', type: 'Quality Rule', desc: 'NULL values in required field CaseID (12 rows)', severity: 'error' },
          { id: 'QR-002', type: 'Quality Rule', desc: 'Date format mismatch in FilingDate (3 rows)', severity: 'warn' },
        ],
      },
      {
        stage: 'Mapping',
        items: [
          { id: 'MAP-001', type: 'Low Confidence', desc: 'StatusCode → case_status_cd (72%)', severity: 'warn' },
          { id: 'MAP-003', type: 'Low Confidence', desc: 'Value → sentence_length_days (61%)', severity: 'error' },
          { id: 'UNM-001', type: 'Unmapped Code', desc: 'ADJCONT not in code table', severity: 'warn' },
        ],
      },
      {
        stage: 'Gold',
        items: [
          { id: 'DUP-001', type: 'Duplicate', desc: 'Michael Torres — 94% match across LC/OJ', severity: 'warn' },
        ],
      },
    ],
  };
}

// ─── Utilities ────────────────────────────────────────────────────────────────
function formatDuration(seconds) {
  if (seconds == null) return '—';
  if (seconds < 60) return `${seconds}s`;
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  return s > 0 ? `${m}m ${s}s` : `${m}m`;
}

function formatRows(n) {
  if (n == null) return '—';
  return n.toLocaleString();
}

function formatTime(isoStr) {
  if (!isoStr) return '—';
  return new Date(isoStr).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

// ─── Tooltip for timeline segment ────────────────────────────────────────────
function SegmentTooltip({ stage, visible, pos }) {
  if (!visible || !stage) return null;
  const sc = STATUS_COLORS[stage.status] || STATUS_COLORS.idle;
  return (
    <div
      className="fixed z-50 pointer-events-none bg-doj-surface border border-doj-border rounded-xl shadow-2xl p-4 min-w-[220px]"
      style={{ left: pos.x + 12, top: pos.y - 10 }}
    >
      <div className="flex items-center gap-2 mb-3">
        <span className="text-sm font-bold text-doj-text">{stage.name}</span>
        <span
          className="px-1.5 py-0.5 rounded text-[10px] font-bold uppercase"
          style={{ backgroundColor: sc.bg, color: sc.text, border: `1px solid ${sc.border}60` }}
        >
          {sc.label}
        </span>
      </div>
      <div className="space-y-1.5 text-xs">
        <div className="flex justify-between gap-6">
          <span className="text-doj-muted">Start</span>
          <span className="font-mono text-doj-text">{formatTime(stage.started_at)}</span>
        </div>
        <div className="flex justify-between gap-6">
          <span className="text-doj-muted">End</span>
          <span className="font-mono text-doj-text">{formatTime(stage.ended_at)}</span>
        </div>
        <div className="flex justify-between gap-6">
          <span className="text-doj-muted">Duration</span>
          <span className="font-mono text-doj-text">{formatDuration(stage.duration_s)}</span>
        </div>
        <div className="pt-1.5 border-t border-doj-border/50 space-y-1">
          <div className="flex justify-between gap-6">
            <span className="text-doj-muted">Rows In</span>
            <span className="font-mono text-doj-text">{formatRows(stage.rows_in)}</span>
          </div>
          <div className="flex justify-between gap-6">
            <span className="text-doj-muted">Rows Out</span>
            <span className="font-mono text-doj-text">{formatRows(stage.rows_out)}</span>
          </div>
          {stage.issues > 0 && (
            <div className="flex justify-between gap-6">
              <span className="text-doj-amber">Issues</span>
              <span className="font-mono text-doj-amber font-bold">{stage.issues}</span>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

// ─── Reconciliation issues accordion ─────────────────────────────────────────
function IssueAccordion({ group }) {
  const [open, setOpen] = useState(group.items.some(i => i.severity === 'error'));
  return (
    <div className="border border-doj-border rounded-xl overflow-hidden mb-2">
      <button
        onClick={() => setOpen(p => !p)}
        className="w-full flex items-center justify-between px-4 py-3 bg-doj-surface-2 hover:bg-white/2 transition-colors"
      >
        <div className="flex items-center gap-2">
          <span className="text-sm font-semibold text-doj-text">{group.stage}</span>
          <span className={`px-1.5 py-0.5 rounded text-[10px] font-bold ${group.items.some(i => i.severity === 'error') ? 'bg-doj-red/20 text-doj-red border border-doj-red/30' : 'bg-doj-amber/20 text-doj-amber border border-doj-amber/30'}`}>
            {group.items.length}
          </span>
        </div>
        <span className={`text-doj-muted transition-transform ${open ? 'rotate-180' : ''}`}>▼</span>
      </button>
      {open && (
        <div className="divide-y divide-doj-border/30">
          {group.items.map(issue => (
            <div
              key={issue.id}
              className={`px-4 py-3 ${issue.severity === 'error' ? 'bg-doj-red/5' : 'bg-doj-amber/5'}`}
            >
              <div className="flex items-center justify-between mb-0.5">
                <span className={`text-[10px] font-bold uppercase tracking-wider ${issue.severity === 'error' ? 'text-doj-red' : 'text-doj-amber'}`}>
                  {issue.type}
                </span>
                <span className="text-[10px] font-mono text-doj-muted">{issue.id}</span>
              </div>
              <p className="text-xs text-doj-muted">{issue.desc}</p>
              <a href="/review" className="text-[10px] text-doj-blue hover:underline mt-1 inline-block">
                Review →
              </a>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ─── Main Component ───────────────────────────────────────────────────────────
export default function JobTraceView({ jobId }) {
  const [trace, setTrace] = useState(null);
  const [loading, setLoading] = useState(true);
  const [tooltip, setTooltip] = useState({ visible: false, stage: null, pos: { x: 0, y: 0 } });

  const fetchTrace = useCallback(async () => {
    try {
      const res = await fetch(`/api/jobs/${jobId}/trace`);
      if (res.ok) {
        const data = await res.json();
        setTrace(data);
      } else {
        setTrace(buildDemoTrace(jobId));
      }
    } catch {
      setTrace(buildDemoTrace(jobId));
    } finally {
      setLoading(false);
    }
  }, [jobId]);

  useEffect(() => {
    fetchTrace();
  }, [fetchTrace]);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="flex items-center gap-3 text-doj-muted text-sm">
          <svg className="w-5 h-5 animate-spin text-doj-blue" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0l3.181 3.183a8.25 8.25 0 0013.803-3.7M4.031 9.865a8.25 8.25 0 0113.803-3.7l3.181 3.182m0-4.991v4.99" />
          </svg>
          Loading trace...
        </div>
      </div>
    );
  }

  if (!trace) return null;

  // Calculate timeline widths
  const totalDuration = trace.stages.reduce((sum, s) => sum + (s.duration_s || 0), 0) || 1;
  const activeStage = trace.stages.find(s => s.status === 'running' || s.status === 'upload');

  const sysColor = SYSTEM_COLORS[trace.system] || '#64748b';

  return (
    <div className="flex gap-6">
      {/* ── Main timeline area ── */}
      <div className="flex-1 min-w-0">
        {/* Header */}
        <div className="flex items-center justify-between mb-5">
          <div className="flex items-center gap-3">
            <a
              href="/"
              className="flex items-center gap-1.5 text-xs text-doj-muted hover:text-doj-text transition-colors border border-doj-border rounded-lg px-3 py-1.5 hover:border-doj-border/80"
            >
              <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" d="M10.5 19.5L3 12m0 0l7.5-7.5M3 12h18" />
              </svg>
              Back to Board
            </a>
            <div>
              <div className="flex items-center gap-2">
                <h2 className="text-lg font-bold text-doj-text font-mono">{trace.job_id}</h2>
                <span
                  className="px-2 py-0.5 rounded text-xs font-medium border"
                  style={{
                    backgroundColor: sysColor + '20',
                    color: sysColor,
                    borderColor: sysColor + '40',
                  }}
                >
                  {trace.system}
                </span>
              </div>
              <p className="text-xs text-doj-muted mt-0.5">{trace.file_name}</p>
            </div>
          </div>
          <div className="text-right">
            <div className="text-xs text-doj-muted">Total time</div>
            <div className="font-mono text-sm font-bold text-doj-text">{formatDuration(trace.total_duration_s)}</div>
          </div>
        </div>

        {/* Stage name labels above timeline */}
        <div className="flex mb-1">
          {trace.stages.map((stage, i) => {
            const widthPct = stage.duration_s
              ? Math.max(3, Math.round((stage.duration_s / totalDuration) * 100))
              : stage.status === 'idle' ? 5 : 5;
            return (
              <div
                key={stage.name}
                className="text-center overflow-hidden"
                style={{ width: `${widthPct}%`, flexShrink: 0 }}
              >
                <span className="text-[9px] text-doj-muted uppercase tracking-wider truncate block">{stage.name}</span>
              </div>
            );
          })}
        </div>

        {/* Timeline bar */}
        <div className="flex h-16 rounded-xl overflow-hidden border border-doj-border shadow-inner">
          {trace.stages.map((stage, i) => {
            const sc = STATUS_COLORS[stage.status] || STATUS_COLORS.idle;
            const widthPct = stage.duration_s
              ? Math.max(3, Math.round((stage.duration_s / totalDuration) * 100))
              : stage.status === 'idle' ? 5 : 5;
            const isActive = stage.status === 'running';
            const isBlocked = stage.status === 'idle' && i > 0;

            return (
              <div
                key={stage.name}
                className={`relative flex items-center justify-center border-r border-black/20 cursor-pointer overflow-hidden transition-all group
                  ${isActive ? 'animate-pulse-amber' : ''}
                `}
                style={{
                  width: `${widthPct}%`,
                  flexShrink: 0,
                  backgroundColor: sc.bg,
                  borderLeft: `2px solid ${sc.border}60`,
                }}
                onMouseEnter={(e) => setTooltip({ visible: true, stage, pos: { x: e.clientX, y: e.clientY } })}
                onMouseLeave={() => setTooltip(prev => ({ ...prev, visible: false }))}
              >
                {/* Hatching for blocked stages */}
                {isBlocked && (
                  <div
                    className="absolute inset-0 opacity-20"
                    style={{
                      backgroundImage: `repeating-linear-gradient(
                        45deg,
                        transparent,
                        transparent 4px,
                        ${sc.fill} 4px,
                        ${sc.fill} 5px
                      )`,
                    }}
                  />
                )}

                {/* Running pulse */}
                {isActive && (
                  <div
                    className="absolute inset-0 opacity-10 animate-pulse"
                    style={{ backgroundColor: sc.fill }}
                  />
                )}

                {/* Active stage indicator */}
                {isActive && (
                  <div
                    className="absolute top-1 right-1 w-2 h-2 rounded-full"
                    style={{ backgroundColor: sc.fill, boxShadow: `0 0 6px ${sc.fill}`, animation: 'pulse 1s infinite' }}
                  />
                )}

                {/* Issue badge */}
                {stage.issues > 0 && (
                  <div className="absolute top-1 left-1 w-4 h-4 rounded-full bg-doj-red flex items-center justify-center text-[8px] font-bold text-white">
                    {stage.issues}
                  </div>
                )}

                {/* Stage icon/label */}
                <span
                  className="text-xs font-bold z-10"
                  style={{ color: sc.text, textShadow: `0 0 8px ${sc.fill}` }}
                >
                  {stage.duration_s && stage.duration_s > 30 ? formatDuration(stage.duration_s) : ''}
                </span>
              </div>
            );
          })}
        </div>

        {/* Duration labels below timeline */}
        <div className="flex mt-1">
          {trace.stages.map((stage, i) => {
            const widthPct = stage.duration_s
              ? Math.max(3, Math.round((stage.duration_s / totalDuration) * 100))
              : 5;
            const sc = STATUS_COLORS[stage.status] || STATUS_COLORS.idle;
            return (
              <div
                key={stage.name}
                className="text-center overflow-hidden"
                style={{ width: `${widthPct}%`, flexShrink: 0 }}
              >
                <span className="text-[9px] font-mono block truncate" style={{ color: sc.text }}>
                  {formatDuration(stage.duration_s)}
                </span>
              </div>
            );
          })}
        </div>

        {/* Per-stage detail cards */}
        <div className="mt-6 grid grid-cols-4 gap-3 xl:grid-cols-7">
          {trace.stages.map((stage, i) => {
            const sc = STATUS_COLORS[stage.status] || STATUS_COLORS.idle;
            const rowDelta = stage.rows_in != null && stage.rows_out != null
              ? stage.rows_out - stage.rows_in
              : null;

            return (
              <div
                key={stage.name}
                className="p-3 rounded-xl border"
                style={{ backgroundColor: sc.bg + '80', borderColor: sc.border + '40' }}
              >
                <div className="flex items-center justify-between mb-1.5">
                  <span className="text-[10px] font-bold uppercase tracking-wider" style={{ color: sc.text }}>
                    {stage.name}
                  </span>
                  {stage.issues > 0 && (
                    <span className="text-[9px] text-doj-red font-bold">⚠{stage.issues}</span>
                  )}
                </div>
                <div className="space-y-0.5 text-[10px]">
                  <div className="flex justify-between">
                    <span className="text-doj-muted">Duration</span>
                    <span className="font-mono text-doj-text">{formatDuration(stage.duration_s)}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-doj-muted">In</span>
                    <span className="font-mono text-doj-text">{formatRows(stage.rows_in)}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-doj-muted">Out</span>
                    <span className="font-mono text-doj-text">{formatRows(stage.rows_out)}</span>
                  </div>
                  {rowDelta !== null && rowDelta < 0 && (
                    <div className="flex justify-between">
                      <span className="text-doj-muted">Lost</span>
                      <span className="font-mono text-doj-red">{Math.abs(rowDelta)}</span>
                    </div>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* ── Issues sidebar ── */}
      <div className="w-72 flex-shrink-0">
        <div className="bg-doj-surface border border-doj-border rounded-xl overflow-hidden">
          <div className="px-4 py-3 border-b border-doj-border bg-doj-surface-2">
            <h3 className="text-sm font-semibold text-doj-text">Reconciliation Issues</h3>
            <p className="text-xs text-doj-muted mt-0.5">
              {trace.reconciliation_issues.reduce((sum, g) => sum + g.items.length, 0)} total
            </p>
          </div>
          <div className="p-3">
            {trace.reconciliation_issues.length === 0 ? (
              <div className="py-8 text-center">
                <div className="text-2xl mb-2">✓</div>
                <p className="text-sm text-doj-green">No reconciliation issues</p>
              </div>
            ) : (
              trace.reconciliation_issues.map(group => (
                <IssueAccordion key={group.stage} group={group} />
              ))
            )}
          </div>
        </div>
      </div>

      {/* Tooltip */}
      <SegmentTooltip
        stage={tooltip.stage}
        visible={tooltip.visible}
        pos={tooltip.pos}
      />
    </div>
  );
}
