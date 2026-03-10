import React, { useState, useEffect, useCallback } from 'react';

// ─── Stage-specific KPI definitions ──────────────────────────────────────────
const STAGE_METRICS_DEF = {
  upload: [
    { key: 'files_received', label: 'Files Received', format: 'num', icon: '↑' },
    { key: 'total_rows', label: 'Total Rows', format: 'rows', icon: '⊞' },
    { key: 'file_size_mb', label: 'Total Size', format: 'mb', icon: '◼' },
    { key: 'schema_detected', label: 'Schema Detected', format: 'bool', icon: '✓' },
  ],
  bronze: [
    { key: 'rows_landed', label: 'Rows Landed', format: 'rows', icon: '⬡' },
    { key: 'parse_errors', label: 'Parse Errors', format: 'num', icon: '✗' },
    { key: 'schema_cols', label: 'Columns Detected', format: 'num', icon: '⊞' },
    { key: 'autoloader_lag_s', label: 'Auto Loader Lag', format: 'sec', icon: '⏱' },
  ],
  silver: [
    { key: 'rows_cleaned', label: 'Rows Cleaned', format: 'rows', icon: '◈' },
    { key: 'rows_rejected', label: 'Rows Rejected', format: 'num', icon: '✗' },
    { key: 'quality_rule_failures', label: 'Quality Rule Failures', format: 'num', icon: '⚠' },
    { key: 'null_rate_pct', label: 'Null Rate', format: 'pct', icon: '○' },
  ],
  mapping: [
    { key: 'columns_mapped', label: 'Columns Mapped', format: 'num', icon: '↔' },
    { key: 'avg_confidence', label: 'Avg Confidence', format: 'pct', icon: '◈' },
    { key: 'low_confidence_count', label: 'Low Confidence', format: 'num', icon: '⚠' },
    { key: 'unmapped_count', label: 'Unmapped', format: 'num', icon: '?' },
  ],
  gold: [
    { key: 'duplicates_resolved', label: 'Duplicates Resolved', format: 'num', icon: '⊕' },
    { key: 'entities_merged', label: 'Entities Merged', format: 'num', icon: '⊞' },
    { key: 'rows_conformed', label: 'Rows Conformed', format: 'rows', icon: '★' },
    { key: 'merge_conflicts', label: 'Merge Conflicts', format: 'num', icon: '✗' },
  ],
  staging: [
    { key: 'rows_inserted', label: 'Rows Inserted', format: 'rows', icon: '⊞' },
    { key: 'upserts', label: 'Upserts', format: 'num', icon: '↑' },
    { key: 'jdbc_errors', label: 'JDBC Errors', format: 'num', icon: '✗' },
    { key: 'commit_time_s', label: 'Commit Time', format: 'sec', icon: '⏱' },
  ],
  complete: [
    { key: 'final_row_count', label: 'Final Row Count', format: 'rows', icon: '✓' },
    { key: 'tables_populated', label: 'Tables Populated', format: 'num', icon: '⊞' },
    { key: 'total_duration_s', label: 'Total Duration', format: 'sec', icon: '⏱' },
    { key: 'pipeline_status', label: 'Pipeline Status', format: 'str', icon: '◆' },
  ],
};

// ─── Demo data generators ─────────────────────────────────────────────────────
function demoMetrics(stageId) {
  const map = {
    upload: { files_received: 3, total_rows: 180750, file_size_mb: 24.8, schema_detected: true },
    bronze: { rows_landed: 180714, parse_errors: 36, schema_cols: 22, autoloader_lag_s: 4.2 },
    silver: { rows_cleaned: 179880, rows_rejected: 834, quality_rule_failures: 12, null_rate_pct: 3.4 },
    mapping: { columns_mapped: 19, avg_confidence: 0.87, low_confidence_count: 3, unmapped_count: 0 },
    gold: { duplicates_resolved: 31, entities_merged: 47, rows_conformed: 141750, merge_conflicts: 2 },
    staging: { rows_inserted: 141200, upserts: 550, jdbc_errors: 0, commit_time_s: 12 },
    complete: { final_row_count: 141200, tables_populated: 6, total_duration_s: 1247, pipeline_status: 'SUCCESS' },
  };
  return map[stageId] || {};
}

function demoJobs(stageId) {
  const allJobs = [
    { id: 'JOB-A4F2B1', file: 'cases_2024_q4.csv', system: 'LegacyCase', rows: 142856, time_in_stage: 840, status: 'running' },
    { id: 'JOB-D2C4E9', file: 'adhoc_parole_data.xlsx', system: 'AdHocExports', rows: 8750, time_in_stage: 120, status: 'running' },
    { id: 'JOB-B7F1A8', file: 'oj_contacts_2024.csv', system: 'OpenJustice', rows: 29100, time_in_stage: 480, status: 'review' },
    { id: 'JOB-C9E3D7', file: 'defendants_export_jan.xlsx', system: 'LegacyCase', rows: 58340, time_in_stage: 720, status: 'complete' },
    { id: 'JOB-F5A3C2', file: 'oj_charges_batch7.csv', system: 'OpenJustice', rows: 51200, time_in_stage: 1080, status: 'failed' },
  ];
  const stageToJobs = {
    upload: [allJobs[0], allJobs[1]],
    bronze: [allJobs[1], allJobs[0]],
    silver: [allJobs[4]],
    mapping: [allJobs[2]],
    gold: [allJobs[0]],
    staging: [],
    complete: [allJobs[3]],
  };
  return stageToJobs[stageId] || [];
}

function demoIssues(stageId) {
  const issues = {
    mapping: [
      { id: 'MAP-001', type: 'Low Confidence', desc: 'StatusCode → case_status_cd (72%)', severity: 'warn' },
      { id: 'MAP-003', type: 'Low Confidence', desc: 'Value → sentence_length_days (61%)', severity: 'error' },
      { id: 'UNM-001', type: 'Unmapped Code', desc: 'ADJCONT not found in code table', severity: 'warn' },
    ],
    silver: [
      { id: 'QR-001', type: 'Quality Rule', desc: 'NULL values in required field CaseID (12 rows)', severity: 'error' },
      { id: 'QR-002', type: 'Quality Rule', desc: 'Date format mismatch in FilingDate (3 rows)', severity: 'warn' },
    ],
    gold: [
      { id: 'DUP-001', type: 'Duplicate', desc: 'Michael Torres — 94% match across LC/OJ', severity: 'warn' },
    ],
  };
  return issues[stageId] || [];
}

function demoLogs(stageId) {
  const now = new Date();
  const ts = (minsAgo) => new Date(now - minsAgo * 60000).toISOString().slice(11, 19);
  return [
    { level: 'INFO', time: ts(0.1), msg: `Stage ${stageId} — heartbeat OK` },
    { level: 'INFO', time: ts(0.5), msg: 'Processing batch 847/1200' },
    { level: 'WARN', time: ts(1.2), msg: 'Slow read detected on partition 3 (4.2s lag)' },
    { level: 'INFO', time: ts(2.0), msg: 'Checkpoint saved at offset 84700' },
    { level: 'INFO', time: ts(3.5), msg: 'Quality checks passed for batch 840-846' },
    { level: 'ERROR', time: ts(4.1), msg: 'Retry attempt 1/3 on JDBC write for row 84301' },
    { level: 'INFO', time: ts(4.2), msg: 'Retry succeeded — row 84301 written' },
    { level: 'INFO', time: ts(5.0), msg: 'Schema validation OK — 22 columns confirmed' },
    { level: 'WARN', time: ts(6.3), msg: 'Null rate threshold exceeded for column DispositionCode (8.1%)' },
    { level: 'INFO', time: ts(7.8), msg: 'Auto Loader: 3 new files detected' },
    { level: 'INFO', time: ts(9.0), msg: 'Deduplication pass complete — 31 resolved' },
    { level: 'INFO', time: ts(10.5), msg: `Stage ${stageId} running — ETA 4m 20s` },
  ].reverse();
}

// ─── Format helpers ───────────────────────────────────────────────────────────
function formatMetricValue(value, format) {
  if (value == null) return '—';
  switch (format) {
    case 'rows': return typeof value === 'number' ? value.toLocaleString() : value;
    case 'mb': return `${value} MB`;
    case 'sec': return value < 60 ? `${value}s` : `${Math.floor(value / 60)}m ${value % 60}s`;
    case 'pct': return typeof value === 'number' && value <= 1 ? `${(value * 100).toFixed(1)}%` : `${value}%`;
    case 'bool': return value ? '✓ Yes' : '✗ No';
    default: return value.toString();
  }
}

function formatTimeInStage(seconds) {
  if (!seconds) return '—';
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}

function statusColor(status) {
  switch (status) {
    case 'complete': return 'text-doj-green';
    case 'running': return 'text-doj-amber';
    case 'failed': case 'review': return 'text-doj-red';
    default: return 'text-doj-muted';
  }
}

const SYSTEM_COLORS_MAP = {
  LegacyCase: '#8b5cf6',
  OpenJustice: '#06b6d4',
  AdHocExports: '#f97316',
};

// ─── Sub-components ───────────────────────────────────────────────────────────
function TabBar({ tabs, active, onSelect }) {
  return (
    <div className="flex border-b border-doj-border">
      {tabs.map(tab => (
        <button
          key={tab.id}
          onClick={() => onSelect(tab.id)}
          className={`px-4 py-2.5 text-xs font-medium border-b-2 transition-all -mb-px
            ${active === tab.id
              ? 'border-doj-blue text-doj-blue'
              : 'border-transparent text-doj-muted hover:text-doj-text hover:border-doj-border'
            }`}
        >
          {tab.label}
          {tab.count != null && tab.count > 0 && (
            <span className="ml-1.5 px-1 py-0.5 rounded text-[10px] bg-doj-red/20 text-doj-red border border-doj-red/30">
              {tab.count}
            </span>
          )}
        </button>
      ))}
    </div>
  );
}

function MetricCard({ def, value }) {
  const formatted = formatMetricValue(value, def.format);
  const isError = def.format === 'num' && typeof value === 'number' && value > 0 &&
    ['parse_errors', 'rows_rejected', 'quality_rule_failures', 'low_confidence_count',
      'jdbc_errors', 'merge_conflicts', 'unmapped_count'].includes(def.key);

  return (
    <div className="bg-doj-bg border border-doj-border rounded-xl p-3">
      <div className="flex items-center gap-1.5 mb-1.5">
        <span className="text-sm">{def.icon}</span>
        <span className="text-[10px] text-doj-muted uppercase tracking-wider leading-tight">{def.label}</span>
      </div>
      <div className={`text-xl font-bold font-mono ${isError ? 'text-doj-red' : 'text-doj-text'}`}>
        {formatted}
      </div>
    </div>
  );
}

function LogLine({ log }) {
  const colors = { INFO: 'text-doj-muted', WARN: 'text-doj-amber', ERROR: 'text-doj-red' };
  const bgColors = { INFO: '', WARN: 'bg-doj-amber/5', ERROR: 'bg-doj-red/5' };
  return (
    <div className={`flex items-start gap-3 px-3 py-1.5 hover:bg-white/2 font-mono text-xs ${bgColors[log.level] || ''}`}>
      <span className="text-doj-muted flex-shrink-0 w-16">{log.time}</span>
      <span className={`font-bold flex-shrink-0 w-10 ${colors[log.level] || 'text-doj-muted'}`}>{log.level}</span>
      <span className={colors[log.level] || 'text-doj-muted'}>{log.msg}</span>
    </div>
  );
}

// ─── Main Component ───────────────────────────────────────────────────────────
export default function StageDetailPanel({ stage, summary, onClose }) {
  const [activeTab, setActiveTab] = useState('jobs');
  const [jobs, setJobs] = useState([]);
  const [metrics, setMetrics] = useState({});
  const [issues, setIssues] = useState([]);
  const [logs, setLogs] = useState([]);
  const [loadingLogs, setLoadingLogs] = useState(false);

  const fetchStageData = useCallback(async () => {
    try {
      const [jobsRes, logsRes] = await Promise.allSettled([
        fetch(`/api/stages/${stage.id}/jobs`),
        fetch(`/api/stages/${stage.id}/logs`),
      ]);

      if (jobsRes.status === 'fulfilled' && jobsRes.value.ok) {
        const data = await jobsRes.value.json();
        // Fall back to demo data when API returns empty results (no active pipeline)
        setJobs(data.jobs?.length ? data.jobs : demoJobs(stage.id));
        setMetrics(Object.keys(data.metrics || {}).length ? data.metrics : demoMetrics(stage.id));
        setIssues(data.issues?.length ? data.issues : demoIssues(stage.id));
      } else {
        setJobs(demoJobs(stage.id));
        setMetrics(demoMetrics(stage.id));
        setIssues(demoIssues(stage.id));
      }

      if (logsRes.status === 'fulfilled' && logsRes.value.ok) {
        const data = await logsRes.value.json();
        setLogs(data.logs?.length ? data.logs : demoLogs(stage.id));
      } else {
        setLogs(demoLogs(stage.id));
      }
    } catch {
      setJobs(demoJobs(stage.id));
      setMetrics(demoMetrics(stage.id));
      setIssues(demoIssues(stage.id));
      setLogs(demoLogs(stage.id));
    }
  }, [stage.id]);

  useEffect(() => {
    fetchStageData();
  }, [fetchStageData]);

  const stageSummary = summary?.stages?.find(s => s.id === stage.id);
  const metricDefs = STAGE_METRICS_DEF[stage.id] || [];

  const TABS = [
    { id: 'jobs', label: 'Jobs', count: null },
    { id: 'metrics', label: 'Metrics', count: null },
    { id: 'issues', label: 'Issues', count: issues.length },
    { id: 'logs', label: 'Logs', count: null },
  ];

  const STAGE_ICONS_MAP = {
    upload: '↑', bronze: '⬡', silver: '◈', mapping: '↔',
    gold: '★', staging: '⊞', complete: '✓',
  };

  return (
    <div
      className="w-96 flex-shrink-0 bg-doj-surface border-l border-doj-border flex flex-col"
      style={{ animation: 'slideInRight 0.25s ease-out forwards' }}
    >
      <style>{`@keyframes slideInRight { from { transform: translateX(100%); opacity: 0; } to { transform: translateX(0); opacity: 1; } }`}</style>

      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-doj-border bg-doj-surface-2">
        <div className="flex items-center gap-2.5">
          <div
            className="w-8 h-8 rounded-lg flex items-center justify-center text-sm font-bold"
            style={{
              backgroundColor: (stageSummary?.status === 'running' ? '#f59e0b' :
                stageSummary?.status === 'complete' ? '#22c55e' :
                stageSummary?.status === 'failed' ? '#ef4444' : '#475569') + '25',
              color: stageSummary?.status === 'running' ? '#f59e0b' :
                stageSummary?.status === 'complete' ? '#22c55e' :
                stageSummary?.status === 'failed' ? '#ef4444' : '#475569',
            }}
          >
            {STAGE_ICONS_MAP[stage.id] || '◆'}
          </div>
          <div>
            <div className="text-sm font-bold text-doj-text">{stage.label}</div>
            <div className="text-[10px] text-doj-muted uppercase tracking-wider">
              {stageSummary?.status || 'idle'} · {jobs.length} job{jobs.length !== 1 ? 's' : ''}
            </div>
          </div>
        </div>
        <button
          onClick={onClose}
          className="w-7 h-7 rounded-lg bg-doj-bg border border-doj-border flex items-center justify-center text-doj-muted hover:text-doj-red hover:border-doj-red/50 transition-all"
        >
          <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" strokeWidth={2.5} stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>

      {/* Tabs */}
      <TabBar tabs={TABS} active={activeTab} onSelect={setActiveTab} />

      {/* Content */}
      <div className="flex-1 overflow-y-auto">
        {/* ── Jobs Tab ── */}
        {activeTab === 'jobs' && (
          <div>
            {jobs.length === 0 ? (
              <div className="p-8 text-center text-doj-muted text-sm">No jobs at this stage</div>
            ) : (
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b border-doj-border/50 bg-doj-surface-2">
                    {['Job ID', 'System', 'Rows', 'Time', 'Status'].map(h => (
                      <th key={h} className="px-3 py-2 text-left font-medium text-doj-muted">{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {jobs.map((job, i) => {
                    const sColor = SYSTEM_COLORS_MAP[job.system] || '#64748b';
                    return (
                      <tr key={job.id} className={`hover:bg-white/2 ${i < jobs.length - 1 ? 'border-b border-doj-border/30' : ''}`}>
                        <td className="px-3 py-2 font-mono text-doj-blue">{job.id}</td>
                        <td className="px-3 py-2">
                          <span className="flex items-center gap-1">
                            <span className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: sColor }} />
                            <span style={{ color: sColor }} className="text-[10px]">{job.system?.slice(0, 2)}</span>
                          </span>
                        </td>
                        <td className="px-3 py-2 font-mono text-doj-muted">{job.rows?.toLocaleString() || '—'}</td>
                        <td className="px-3 py-2 font-mono text-doj-muted">{formatTimeInStage(job.time_in_stage)}</td>
                        <td className="px-3 py-2">
                          <span className={`font-medium ${statusColor(job.status)}`}>{job.status}</span>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            )}

            {/* Action buttons */}
            <div className="p-3 border-t border-doj-border flex gap-2">
              <button
                onClick={fetchStageData}
                className="flex-1 flex items-center justify-center gap-1.5 py-1.5 bg-doj-surface-2 border border-doj-border text-doj-muted rounded-lg text-xs hover:text-doj-text hover:border-doj-border/80 transition-all"
              >
                <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0l3.181 3.183a8.25 8.25 0 0013.803-3.7M4.031 9.865a8.25 8.25 0 0113.803-3.7l3.181 3.182m0-4.991v4.99" />
                </svg>
                Retry Failed
              </button>
              <button
                className="flex-1 flex items-center justify-center gap-1.5 py-1.5 bg-doj-blue/10 border border-doj-blue/30 text-doj-blue/70 rounded-lg text-xs hover:bg-doj-blue/15 transition-all"
              >
                <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" d="M13.19 8.688a4.5 4.5 0 011.242 7.244l-4.5 4.5a4.5 4.5 0 01-6.364-6.364l1.757-1.757m13.35-.622l1.757-1.757a4.5 4.5 0 00-6.364-6.364l-4.5 4.5a4.5 4.5 0 001.242 7.244" />
                </svg>
                View Lineage
              </button>
            </div>
          </div>
        )}

        {/* ── Metrics Tab ── */}
        {activeTab === 'metrics' && (
          <div className="p-3 space-y-2">
            {/* SLA indicator */}
            {stageSummary && (
              <div className={`flex items-center justify-between px-3 py-2 rounded-lg border ${stageSummary.sla_ok ? 'bg-doj-green/10 border-doj-green/30' : 'bg-doj-red/10 border-doj-red/30'}`}>
                <span className="text-xs text-doj-muted">SLA Status</span>
                <span className={`text-xs font-bold ${stageSummary.sla_ok ? 'text-doj-green' : 'text-doj-red'}`}>
                  {stageSummary.sla_ok ? '✓ Within SLA' : '✗ SLA Breached'}
                </span>
              </div>
            )}
            <div className="grid grid-cols-2 gap-2">
              {metricDefs.map(def => (
                <MetricCard key={def.key} def={def} value={metrics[def.key]} />
              ))}
            </div>
            {stageSummary && (
              <div className="flex items-center justify-between px-3 py-2 bg-doj-bg border border-doj-border rounded-xl">
                <span className="text-xs text-doj-muted">Avg Duration</span>
                <span className="font-mono text-xs text-doj-text">
                  {stageSummary.avg_duration_s < 60
                    ? `${stageSummary.avg_duration_s}s`
                    : `${Math.floor(stageSummary.avg_duration_s / 60)}m ${stageSummary.avg_duration_s % 60}s`
                  }
                </span>
              </div>
            )}
          </div>
        )}

        {/* ── Issues Tab ── */}
        {activeTab === 'issues' && (
          <div className="p-3 space-y-2">
            {issues.length === 0 ? (
              <div className="py-8 text-center">
                <div className="text-2xl mb-2">✓</div>
                <p className="text-sm text-doj-green">No issues at this stage</p>
              </div>
            ) : (
              <>
                {issues.map(issue => (
                  <div
                    key={issue.id}
                    className={`p-3 rounded-xl border ${issue.severity === 'error' ? 'bg-doj-red/8 border-doj-red/30' : 'bg-doj-amber/8 border-doj-amber/30'}`}
                  >
                    <div className="flex items-center justify-between mb-1">
                      <span className={`text-[10px] font-bold uppercase tracking-wider ${issue.severity === 'error' ? 'text-doj-red' : 'text-doj-amber'}`}>
                        {issue.type}
                      </span>
                      <span className="text-[10px] font-mono text-doj-muted">{issue.id}</span>
                    </div>
                    <p className="text-xs text-doj-muted">{issue.desc}</p>
                    <a
                      href="/review"
                      className="inline-flex items-center gap-1 mt-2 text-[10px] text-doj-blue hover:underline"
                    >
                      Review in queue →
                    </a>
                  </div>
                ))}
              </>
            )}
          </div>
        )}

        {/* ── Logs Tab ── */}
        {activeTab === 'logs' && (
          <div>
            <div className="flex items-center justify-between px-3 py-2 border-b border-doj-border/50 bg-doj-surface-2">
              <span className="text-[10px] text-doj-muted uppercase tracking-wider">Last {logs.length} log entries</span>
              <button
                onClick={fetchStageData}
                className="text-[10px] text-doj-blue hover:underline"
              >
                Refresh
              </button>
            </div>
            <div className="overflow-y-auto font-mono divide-y divide-doj-border/20">
              {logs.map((log, i) => (
                <LogLine key={i} log={log} />
              ))}
              {logs.length === 0 && (
                <div className="p-6 text-center text-xs text-doj-muted">No logs available</div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
