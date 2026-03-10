import React, { useState, useEffect, useCallback } from 'react';

const REFRESH_INTERVAL = 30_000; // 30s

// ─── Helpers ──────────────────────────────────────────────────────────────────

function fmt(n) {
  if (n == null) return '—';
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toLocaleString();
}

function fmtDur(s) {
  if (s == null) return '—';
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60), rem = s % 60;
  return rem ? `${m}m ${rem}s` : `${m}m`;
}

function fmtPct(n, total) {
  if (!total) return '0%';
  return `${Math.round((n / total) * 100)}%`;
}

// colour helpers
const STATUS_COLOR = {
  running:  { bg: '#3b82f615', border: '#3b82f640', text: '#3b82f6', dot: '#3b82f6' },
  complete: { bg: '#22c55e15', border: '#22c55e40', text: '#22c55e', dot: '#22c55e' },
  failed:   { bg: '#ef444415', border: '#ef444440', text: '#ef4444', dot: '#ef4444' },
  review:   { bg: '#f59e0b15', border: '#f59e0b40', text: '#f59e0b', dot: '#f59e0b' },
  idle:     { bg: '#2d374815', border: '#2d374840', text: '#64748b', dot: '#2d3748' },
};

const SYSTEM_COLORS = {
  LegacyCase:   '#8b5cf6',
  OpenJustice:  '#06b6d4',
  AdHocExports: '#f97316',
};

const STAGE_ORDER = ['Upload', 'Bronze', 'Mapping', 'Silver', 'Gold', 'Staging', 'Complete'];
const SLA_THRESHOLDS = [60, 300, 400, 600, 600, 200, 10];

// ─── Sub-components ───────────────────────────────────────────────────────────

function KpiCard({ label, value, sub, accent = '#3b82f6', icon, trend }) {
  return (
    <div className="bg-doj-surface border border-doj-border rounded-xl p-4 flex flex-col gap-1 min-w-0">
      <div className="flex items-center justify-between mb-0.5">
        <span className="text-[11px] font-semibold uppercase tracking-widest text-doj-muted">{label}</span>
        {icon && <span style={{ color: accent }} className="opacity-70">{icon}</span>}
      </div>
      <div className="flex items-end gap-2">
        <span className="font-mono text-2xl font-bold" style={{ color: accent }}>{value}</span>
        {trend != null && (
          <span className={`text-xs mb-1 font-medium ${trend >= 0 ? 'text-doj-green' : 'text-doj-red'}`}>
            {trend >= 0 ? '▲' : '▼'} {Math.abs(trend)}%
          </span>
        )}
      </div>
      {sub && <span className="text-[11px] text-doj-muted leading-tight">{sub}</span>}
    </div>
  );
}

function StatusDot({ status }) {
  const c = STATUS_COLOR[status] || STATUS_COLOR.idle;
  return (
    <span
      className="inline-block w-2 h-2 rounded-full flex-shrink-0"
      style={{ backgroundColor: c.dot, boxShadow: status === 'running' ? `0 0 6px ${c.dot}` : 'none' }}
    />
  );
}

function StageHealthRow({ stages }) {
  return (
    <div className="grid grid-cols-7 gap-2">
      {stages.map((s, i) => {
        const c = STATUS_COLOR[s.status] || STATUS_COLOR.idle;
        const slaLabel = s.avg_duration_s != null
          ? (s.sla_ok ? `${fmtDur(s.avg_duration_s)} ✓` : `${fmtDur(s.avg_duration_s)} ✗`)
          : '—';
        const slaColor = s.avg_duration_s == null ? '#64748b' : s.sla_ok ? '#22c55e' : '#ef4444';
        return (
          <div
            key={s.id}
            className="rounded-lg border flex flex-col gap-1.5 px-3 py-2.5 relative overflow-hidden"
            style={{ background: c.bg, borderColor: c.border }}
          >
            {/* running shimmer */}
            {s.status === 'running' && (
              <div
                className="absolute inset-0 opacity-10"
                style={{
                  background: `linear-gradient(90deg, transparent, ${c.dot}, transparent)`,
                  animation: 'shimmer 1.5s ease-in-out infinite',
                }}
              />
            )}
            <div className="flex items-center gap-1.5">
              <StatusDot status={s.status} />
              <span className="text-[11px] font-bold text-doj-text truncate">{s.label}</span>
            </div>
            <div className="font-mono text-lg font-bold leading-none" style={{ color: c.text }}>
              {s.job_count}
            </div>
            <div className="text-[10px] font-mono" style={{ color: slaColor }}>
              {slaLabel}
            </div>
            <div className="text-[9px] uppercase tracking-wider text-doj-muted capitalize">
              {s.status}
            </div>
          </div>
        );
      })}
    </div>
  );
}

function HourlyBarChart({ jobs }) {
  // Build counts per hour for the last 12 hours
  const now = new Date();
  const hours = Array.from({ length: 12 }, (_, i) => {
    const d = new Date(now);
    d.setHours(d.getHours() - (11 - i), 0, 0, 0);
    return d;
  });

  const buckets = hours.map(h => {
    const next = new Date(h); next.setHours(next.getHours() + 1);
    const count = jobs.filter(j => {
      const t = new Date(j.uploaded_at);
      return t >= h && t < next;
    }).length;
    return { label: `${String(h.getHours()).padStart(2, '0')}:00`, count };
  });

  const maxCount = Math.max(...buckets.map(b => b.count), 1);
  const barW = 100 / buckets.length;

  return (
    <div className="bg-doj-surface border border-doj-border rounded-xl p-4">
      <div className="text-xs font-semibold text-doj-muted uppercase tracking-wider mb-3">
        Jobs / Hour (Last 12h)
      </div>
      <div className="relative h-28">
        <svg viewBox={`0 0 ${buckets.length * 20} 60`} preserveAspectRatio="none" className="w-full h-full">
          {buckets.map((b, i) => {
            const h = b.count > 0 ? Math.max((b.count / maxCount) * 56, 4) : 0;
            const isNow = i === buckets.length - 1;
            return (
              <g key={i}>
                <rect
                  x={i * 20 + 2}
                  y={58 - h}
                  width={16}
                  height={h}
                  rx={2}
                  fill={isNow ? '#3b82f6' : '#3b82f640'}
                />
              </g>
            );
          })}
        </svg>
        {/* x-axis labels — show every 3rd */}
        <div className="flex mt-1">
          {buckets.map((b, i) => (
            <div key={i} className="flex-1 text-center text-[9px] text-doj-muted font-mono">
              {i % 3 === 0 ? b.label.slice(0, 2) : ''}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function SystemBreakdown({ jobs }) {
  const counts = {};
  jobs.forEach(j => {
    counts[j.system] = (counts[j.system] || 0) + 1;
  });
  const total = jobs.length || 1;
  const rows = Object.entries(counts).sort((a, b) => b[1] - a[1]);

  return (
    <div className="bg-doj-surface border border-doj-border rounded-xl p-4">
      <div className="text-xs font-semibold text-doj-muted uppercase tracking-wider mb-3">
        Jobs by System
      </div>
      <div className="space-y-3">
        {rows.length === 0 && <div className="text-xs text-doj-muted">No jobs today</div>}
        {rows.map(([sys, cnt]) => {
          const color = SYSTEM_COLORS[sys] || '#64748b';
          const pct = (cnt / total) * 100;
          return (
            <div key={sys} className="space-y-1">
              <div className="flex items-center justify-between text-xs">
                <div className="flex items-center gap-1.5">
                  <span className="w-2 h-2 rounded-full" style={{ backgroundColor: color }} />
                  <span className="text-doj-text font-medium">{sys}</span>
                </div>
                <span className="font-mono text-doj-muted">{cnt}</span>
              </div>
              <div className="h-1.5 rounded-full bg-doj-surface-2 overflow-hidden">
                <div
                  className="h-full rounded-full transition-all duration-700"
                  style={{ width: `${pct}%`, backgroundColor: color, boxShadow: `0 0 6px ${color}60` }}
                />
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function StageDurationChart({ stages }) {
  const items = stages
    .map((s, i) => ({ label: s.label, dur: s.avg_duration_s, sla: SLA_THRESHOLDS[i], ok: s.sla_ok }))
    .filter(x => x.dur != null);

  if (items.length === 0) {
    return (
      <div className="bg-doj-surface border border-doj-border rounded-xl p-4">
        <div className="text-xs font-semibold text-doj-muted uppercase tracking-wider mb-3">
          Avg Stage Duration vs SLA
        </div>
        <div className="text-xs text-doj-muted">No timing data yet</div>
      </div>
    );
  }

  const maxVal = Math.max(...items.flatMap(x => [x.dur, x.sla]), 1);

  return (
    <div className="bg-doj-surface border border-doj-border rounded-xl p-4">
      <div className="text-xs font-semibold text-doj-muted uppercase tracking-wider mb-3">
        Avg Stage Duration vs SLA
      </div>
      <div className="space-y-2.5">
        {items.map(x => {
          const durPct = Math.min((x.dur / maxVal) * 100, 100);
          const slaPct = Math.min((x.sla / maxVal) * 100, 100);
          const color = x.ok ? '#22c55e' : '#ef4444';
          return (
            <div key={x.label}>
              <div className="flex items-center justify-between text-[11px] mb-1">
                <span className="text-doj-muted w-16">{x.label}</span>
                <span className="font-mono" style={{ color }}>{fmtDur(x.dur)}</span>
              </div>
              <div className="h-2 rounded-full bg-doj-surface-2 relative overflow-visible">
                {/* actual bar */}
                <div
                  className="h-full rounded-full transition-all duration-700"
                  style={{ width: `${durPct}%`, backgroundColor: color, opacity: 0.8 }}
                />
                {/* SLA marker */}
                <div
                  className="absolute top-1/2 -translate-y-1/2 w-0.5 h-3 rounded"
                  style={{ left: `${slaPct}%`, backgroundColor: '#f59e0b', boxShadow: '0 0 4px #f59e0b' }}
                  title={`SLA: ${fmtDur(x.sla)}`}
                />
              </div>
            </div>
          );
        })}
        <div className="flex items-center gap-4 mt-3 text-[10px] text-doj-muted">
          <div className="flex items-center gap-1"><span className="w-3 h-1 rounded bg-doj-green inline-block" /> Actual</div>
          <div className="flex items-center gap-1"><span className="w-0.5 h-3 rounded bg-doj-amber inline-block" /> SLA limit</div>
        </div>
      </div>
    </div>
  );
}

function StatusBadge({ status }) {
  const c = STATUS_COLOR[status] || STATUS_COLOR.idle;
  return (
    <span
      className="text-[10px] font-bold uppercase tracking-wider px-1.5 py-0.5 rounded border"
      style={{ background: c.bg, borderColor: c.border, color: c.text }}
    >
      {status}
    </span>
  );
}

function RecentActivityTable({ jobs }) {
  const recent = [...jobs]
    .sort((a, b) => new Date(b.uploaded_at) - new Date(a.uploaded_at))
    .slice(0, 10);

  return (
    <div className="bg-doj-surface border border-doj-border rounded-xl overflow-hidden">
      <div className="px-4 py-3 border-b border-doj-border flex items-center justify-between">
        <span className="text-xs font-semibold text-doj-muted uppercase tracking-wider">Recent Activity</span>
        <span className="text-[11px] text-doj-muted">Last 10 jobs</span>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-doj-border/50">
              <th className="text-left px-4 py-2 text-doj-muted font-medium">File</th>
              <th className="text-left px-4 py-2 text-doj-muted font-medium">System</th>
              <th className="text-left px-4 py-2 text-doj-muted font-medium">Stage</th>
              <th className="text-right px-4 py-2 text-doj-muted font-medium">Rows</th>
              <th className="text-right px-4 py-2 text-doj-muted font-medium">Issues</th>
              <th className="text-center px-4 py-2 text-doj-muted font-medium">Status</th>
              <th className="text-right px-4 py-2 text-doj-muted font-medium">Time</th>
            </tr>
          </thead>
          <tbody>
            {recent.length === 0 && (
              <tr>
                <td colSpan={7} className="px-4 py-6 text-center text-doj-muted">No jobs today</td>
              </tr>
            )}
            {recent.map(j => {
              const sysColor = SYSTEM_COLORS[j.system] || '#64748b';
              const t = new Date(j.uploaded_at);
              const timeStr = t.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
              return (
                <tr key={j.id} className="border-b border-doj-border/30 hover:bg-white/[0.02] transition-colors">
                  <td className="px-4 py-2 font-mono text-doj-text max-w-[180px] truncate" title={j.file_name}>
                    {j.file_name}
                  </td>
                  <td className="px-4 py-2">
                    <span className="flex items-center gap-1.5">
                      <span className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: sysColor }} />
                      <span style={{ color: sysColor }} className="font-medium">{j.system}</span>
                    </span>
                  </td>
                  <td className="px-4 py-2 text-doj-muted">{j.stage}</td>
                  <td className="px-4 py-2 font-mono text-right text-doj-text">{fmt(j.rows)}</td>
                  <td className="px-4 py-2 font-mono text-right">
                    <span className={j.issues > 0 ? 'text-doj-amber' : 'text-doj-muted'}>{j.issues ?? 0}</span>
                  </td>
                  <td className="px-4 py-2 text-center">
                    <StatusBadge status={j.status} />
                  </td>
                  <td className="px-4 py-2 font-mono text-right text-doj-muted">{timeStr}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function IssueBreakdown({ issues }) {
  const bySys = {};
  issues.forEach(i => {
    const sys = i.source_system || 'Unknown';
    bySys[sys] = (bySys[sys] || 0) + 1;
  });

  const byType = {};
  issues.forEach(i => {
    const t = i.issue_type || 'unknown';
    byType[t] = (byType[t] || 0) + 1;
  });

  return (
    <div className="bg-doj-surface border border-doj-border rounded-xl p-4">
      <div className="text-xs font-semibold text-doj-muted uppercase tracking-wider mb-3">
        Open Issues
      </div>
      {issues.length === 0 ? (
        <div className="text-xs text-doj-green flex items-center gap-1.5">
          <span>✓</span> No open issues
        </div>
      ) : (
        <div className="space-y-2">
          {Object.entries(byType).map(([type, cnt]) => (
            <div key={type} className="flex items-center justify-between text-xs">
              <span className="text-doj-muted capitalize">{type.replace(/_/g, ' ')}</span>
              <span className="font-mono font-bold text-doj-amber">{cnt}</span>
            </div>
          ))}
          <div className="border-t border-doj-border/50 pt-2 flex items-center justify-between text-xs font-bold">
            <span className="text-doj-muted">Total</span>
            <span className="font-mono text-doj-red">{issues.length}</span>
          </div>
        </div>
      )}
    </div>
  );
}

// ─── Main Dashboard ───────────────────────────────────────────────────────────

export default function PipelineDashboard() {
  const [summary, setSummary] = useState(null);
  const [jobs, setJobs] = useState([]);
  const [issues, setIssues] = useState([]);
  const [loading, setLoading] = useState(true);
  const [lastRefresh, setLastRefresh] = useState(null);
  const [error, setError] = useState(null);

  const fetchAll = useCallback(async () => {
    try {
      const [sumRes, jobsRes, issuesRes] = await Promise.all([
        fetch('/api/stages/summary'),
        fetch('/api/jobs'),
        fetch('/api/reconciliation/issues'),
      ]);
      if (sumRes.ok) setSummary(await sumRes.json());
      if (jobsRes.ok) setJobs(await jobsRes.json());
      if (issuesRes.ok) setIssues(await issuesRes.json());
      setLastRefresh(new Date());
      setError(null);
    } catch (e) {
      setError('Failed to load data');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchAll();
    const t = setInterval(fetchAll, REFRESH_INTERVAL);
    return () => clearInterval(t);
  }, [fetchAll]);

  // ── Compute KPIs ──
  const today = new Date().toDateString();
  const todayJobs = jobs.filter(j => new Date(j.uploaded_at).toDateString() === today);
  const totalRows = todayJobs.reduce((s, j) => s + (j.rows || 0), 0);
  const activeCount = todayJobs.filter(j => j.status === 'running').length;
  const completeCount = todayJobs.filter(j => j.status === 'complete').length;
  const failedCount = todayJobs.filter(j => j.status === 'failed' || j.status === 'review').length;
  const successRate = todayJobs.length
    ? Math.round((completeCount / todayJobs.length) * 100)
    : 100;
  const openIssues = issues.filter(i => i.status === 'PENDING' || i.status === 'pending');
  const stages = summary?.stages ?? [];
  const slaViolations = stages.filter(s => s.avg_duration_s != null && !s.sla_ok).length;

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-doj-muted text-sm animate-pulse">Loading dashboard…</div>
      </div>
    );
  }

  return (
    <div className="space-y-5">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-lg font-bold text-doj-text">Pipeline Health Dashboard</h1>
          <p className="text-xs text-doj-muted mt-0.5">
            Today's migration run · {new Date().toLocaleDateString([], { weekday: 'long', month: 'long', day: 'numeric' })}
          </p>
        </div>
        <div className="flex items-center gap-3">
          {error && (
            <span className="text-xs text-doj-red bg-doj-red/10 border border-doj-red/30 px-2 py-1 rounded">
              {error}
            </span>
          )}
          <div className="text-[11px] text-doj-muted">
            Last refresh:{' '}
            <span className="font-mono text-doj-text">
              {lastRefresh ? lastRefresh.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }) : '—'}
            </span>
          </div>
          <button
            onClick={fetchAll}
            className="flex items-center gap-1.5 px-3 py-1.5 bg-doj-blue/15 border border-doj-blue/40 text-doj-blue rounded-lg text-xs font-medium hover:bg-doj-blue/25 transition-all"
          >
            <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0l3.181 3.183a8.25 8.25 0 0013.803-3.7M4.031 9.865a8.25 8.25 0 0113.803-3.7l3.181 3.182m0-4.991v4.99" />
            </svg>
            Refresh
          </button>
        </div>
      </div>

      {/* KPI Row */}
      <div className="grid grid-cols-6 gap-3">
        <KpiCard
          label="Jobs Today"
          value={todayJobs.length}
          sub={`${activeCount} running`}
          accent="#3b82f6"
          icon={<svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" d="M3.375 19.5h17.25m-17.25 0a1.125 1.125 0 01-1.125-1.125M3.375 19.5h7.5c.621 0 1.125-.504 1.125-1.125m-9.75 0V5.625m0 12.75v-1.5c0-.621.504-1.125 1.125-1.125m18.375 2.625V5.625m0 12.75c0 .621-.504 1.125-1.125 1.125m1.125-1.125v-1.5c0-.621-.504-1.125-1.125-1.125m0 3.75h-7.5A1.125 1.125 0 0112 18.375m9.75-12.75c0-.621-.504-1.125-1.125-1.125H3.375c-.621 0-1.125.504-1.125 1.125m19.5 0v1.5c0 .621-.504 1.125-1.125 1.125M2.25 5.625v1.5c0 .621.504 1.125 1.125 1.125m0 0h17.25m-17.25 0c0 .621.504 1.125 1.125 1.125h15c.621 0 1.125-.504 1.125-1.125m-3 7.5V5.625m0 0a3 3 0 00-3-3h-6a3 3 0 00-3 3" /></svg>}
        />
        <KpiCard
          label="Rows Processed"
          value={fmt(totalRows)}
          sub="across all stages"
          accent="#22c55e"
          icon={<svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" d="M20.25 6.375c0 2.278-3.694 4.125-8.25 4.125S3.75 8.653 3.75 6.375m16.5 0c0-2.278-3.694-4.125-8.25-4.125S3.75 4.097 3.75 6.375m16.5 0v11.25c0 2.278-3.694 4.125-8.25 4.125s-8.25-1.847-8.25-4.125V6.375m16.5 5.625c0 2.278-3.694 4.125-8.25 4.125s-8.25-1.847-8.25-4.125" /></svg>}
        />
        <KpiCard
          label="Active Now"
          value={activeCount}
          sub={`${completeCount} completed`}
          accent={activeCount > 0 ? '#f59e0b' : '#64748b'}
          icon={<svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" d="M3.75 13.5l10.5-11.25L12 10.5h8.25L9.75 21.75 12 13.5H3.75z" /></svg>}
        />
        <KpiCard
          label="Success Rate"
          value={`${successRate}%`}
          sub={`${failedCount} failed / review`}
          accent={successRate >= 90 ? '#22c55e' : successRate >= 70 ? '#f59e0b' : '#ef4444'}
          icon={<svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" d="M9 12.75L11.25 15 15 9.75M21 12a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>}
        />
        <KpiCard
          label="Open Issues"
          value={openIssues.length}
          sub="pending SME review"
          accent={openIssues.length > 0 ? '#f59e0b' : '#22c55e'}
          icon={<svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126zM12 15.75h.007v.008H12v-.008z" /></svg>}
        />
        <KpiCard
          label="SLA Violations"
          value={slaViolations}
          sub={`${stages.filter(s => s.sla_ok && s.avg_duration_s != null).length} stages within SLA`}
          accent={slaViolations > 0 ? '#ef4444' : '#22c55e'}
          icon={<svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" d="M12 6v6h4.5m4.5 0a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>}
        />
      </div>

      {/* Stage Health Row */}
      <div className="bg-doj-surface border border-doj-border rounded-xl p-4">
        <div className="text-xs font-semibold text-doj-muted uppercase tracking-wider mb-3">
          Stage Health
        </div>
        {stages.length > 0 ? (
          <StageHealthRow stages={stages} />
        ) : (
          <div className="text-xs text-doj-muted">No stage data</div>
        )}
      </div>

      {/* Charts row */}
      <div className="grid grid-cols-3 gap-4">
        <div className="col-span-1">
          <HourlyBarChart jobs={todayJobs} />
        </div>
        <div className="col-span-1">
          <SystemBreakdown jobs={todayJobs} />
        </div>
        <div className="col-span-1">
          <IssueBreakdown issues={openIssues} />
        </div>
      </div>

      {/* Stage duration chart */}
      <StageDurationChart stages={stages} />

      {/* Recent Activity */}
      <RecentActivityTable jobs={todayJobs} />
    </div>
  );
}
