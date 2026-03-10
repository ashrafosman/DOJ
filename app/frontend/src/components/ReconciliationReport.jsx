import React, { useState, useEffect, useCallback } from 'react';

// ─── Demo report data ─────────────────────────────────────────────────────────
function buildDemoReport(jobId) {
  return {
    job_id: jobId,
    file_name: jobId === 'JOB-A4F2B1' ? 'cases_2024_q4.csv' : 'defendants_export_jan.xlsx',
    system: jobId === 'JOB-A4F2B1' ? 'LegacyCase' : 'LegacyCase',
    completed_at: new Date(Date.now() - 3600000).toISOString(),
    summary: {
      total_rows_ingested: 142856,
      rows_mapped: 141200,
      rows_rejected: 1656,
      mapping_coverage_pct: 98.8,
      duplicates_identified: 47,
      duplicates_resolved: 31,
    },
    mapping_coverage: [
      { table: 'Stg_Case', expected: 24, mapped: 24, coverage: 100 },
      { table: 'Stg_Contact', expected: 18, mapped: 17, coverage: 94.4 },
      { table: 'Stg_Charge', expected: 12, mapped: 11, coverage: 91.7 },
      { table: 'Stg_Attorney', expected: 8, mapped: 8, coverage: 100 },
      { table: 'Stg_CourtEvent', expected: 15, mapped: 13, coverage: 86.7 },
      { table: 'Stg_Sentence', expected: 10, mapped: 9, coverage: 90.0 },
    ],
    row_reconciliation: [
      { stage: 'Source File', rows: 142856, delta: null },
      { stage: 'Bronze Landed', rows: 142820, delta: -36 },
      { stage: 'Silver Cleaned', rows: 141990, delta: -830 },
      { stage: 'Gold Conformed', rows: 141750, delta: -240 },
      { stage: 'Staged', rows: 141200, delta: -550 },
    ],
    issues_resolved: [
      { type: 'Low-Confidence Mappings', total: 12, resolved: 10 },
      { type: 'Duplicate Contacts', total: 47, resolved: 31 },
      { type: 'Unmapped Codes', total: 8, resolved: 6 },
      { type: 'Schema Drift', total: 3, resolved: 3 },
    ],
  };
}

const DEMO_JOBS = [
  { id: 'JOB-A4F2B1', label: 'JOB-A4F2B1 — cases_2024_q4.csv' },
  { id: 'JOB-C9E3D7', label: 'JOB-C9E3D7 — defendants_export_jan.xlsx' },
  { id: 'JOB-B7F1A8', label: 'JOB-B7F1A8 — oj_contacts_2024.csv' },
];

// ─── Sub-components ───────────────────────────────────────────────────────────
function KPICard({ label, value, sub, color = 'text-doj-text', icon }) {
  return (
    <div className="bg-doj-surface-2 border border-doj-border rounded-xl p-4">
      <div className="flex items-start justify-between mb-2">
        <span className="text-xs font-semibold text-doj-muted uppercase tracking-wider leading-tight">{label}</span>
        {icon && <span className="text-lg">{icon}</span>}
      </div>
      <div className={`text-2xl font-bold font-mono ${color}`}>{value}</div>
      {sub && <div className="text-xs text-doj-muted mt-1">{sub}</div>}
    </div>
  );
}

function CoverageBar({ pct }) {
  const color = pct >= 95 ? '#22c55e' : pct >= 85 ? '#f59e0b' : '#ef4444';
  const textColor = pct >= 95 ? 'text-doj-green' : pct >= 85 ? 'text-doj-amber' : 'text-doj-red';
  return (
    <div className="flex items-center gap-3">
      <div className="flex-1 h-1.5 bg-doj-border rounded-full overflow-hidden">
        <div
          className="h-full rounded-full transition-all duration-700"
          style={{ width: `${pct}%`, backgroundColor: color }}
        />
      </div>
      <span className={`font-mono text-xs font-bold ${textColor} w-12 text-right`}>{pct.toFixed(1)}%</span>
    </div>
  );
}

function SectionHeader({ title, description }) {
  return (
    <div className="mb-4">
      <h3 className="text-sm font-bold text-doj-text">{title}</h3>
      {description && <p className="text-xs text-doj-muted mt-0.5">{description}</p>}
    </div>
  );
}

function formatRows(n) {
  if (n == null) return '—';
  return n.toLocaleString();
}

// ─── CSV export ───────────────────────────────────────────────────────────────
function exportCSV(report) {
  const lines = [
    ['Section', 'Metric', 'Value'],
    ['Summary', 'Total Rows Ingested', report.summary.total_rows_ingested],
    ['Summary', 'Rows Mapped', report.summary.rows_mapped],
    ['Summary', 'Rows Rejected', report.summary.rows_rejected],
    ['Summary', 'Mapping Coverage %', report.summary.mapping_coverage_pct],
    ['Summary', 'Duplicates Identified', report.summary.duplicates_identified],
    ['Summary', 'Duplicates Resolved', report.summary.duplicates_resolved],
    [],
    ['Mapping Coverage', 'Table', 'Expected', 'Mapped', 'Coverage %'],
    ...report.mapping_coverage.map(r => ['Mapping Coverage', r.table, r.expected, r.mapped, r.coverage]),
    [],
    ['Row Reconciliation', 'Stage', 'Rows', 'Delta'],
    ...report.row_reconciliation.map(r => ['Row Reconciliation', r.stage, r.rows, r.delta ?? '']),
    [],
    ['Issues', 'Type', 'Total', 'Resolved'],
    ...report.issues_resolved.map(r => ['Issues', r.type, r.total, r.resolved]),
  ];

  const csv = lines.map(row => row.join(',')).join('\n');
  const blob = new Blob([csv], { type: 'text/csv' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `reconciliation_report_${report.job_id}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

// ─── Main Component ───────────────────────────────────────────────────────────
export default function ReconciliationReport() {
  const [selectedJob, setSelectedJob] = useState('JOB-A4F2B1');
  const [jobs, setJobs] = useState(DEMO_JOBS);
  const [report, setReport] = useState(null);
  const [loading, setLoading] = useState(false);

  const fetchJobs = useCallback(async () => {
    try {
      const res = await fetch('/api/jobs');
      if (res.ok) {
        const data = await res.json();
        if (data.length > 0) {
          setJobs(data.map(j => ({ id: j.id, label: `${j.id} — ${j.file_name}` })));
        }
      }
    } catch { /* use demo */ }
  }, []);

  const fetchReport = useCallback(async (jobId) => {
    setLoading(true);
    try {
      const res = await fetch(`/api/jobs/${jobId}/report`);
      if (res.ok) {
        const data = await res.json();
        setReport(data);
      } else {
        setReport(buildDemoReport(jobId));
      }
    } catch {
      setReport(buildDemoReport(jobId));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchJobs();
  }, [fetchJobs]);

  useEffect(() => {
    if (selectedJob) fetchReport(selectedJob);
  }, [selectedJob, fetchReport]);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="flex items-center gap-3 text-doj-muted">
          <svg className="w-5 h-5 animate-spin text-doj-blue" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0l3.181 3.183a8.25 8.25 0 0013.803-3.7M4.031 9.865a8.25 8.25 0 0113.803-3.7l3.181 3.182m0-4.991v4.99" />
          </svg>
          Loading report...
        </div>
      </div>
    );
  }

  return (
    <div className="print:bg-white print:text-black">
      {/* Header */}
      <div className="flex items-center justify-between mb-6 print:mb-4">
        <div>
          <h1 className="text-xl font-bold text-doj-text print:text-black">Reconciliation Report</h1>
          <p className="text-sm text-doj-muted print:text-gray-600 mt-0.5">Per-job data migration summary</p>
        </div>
        <div className="flex items-center gap-3 print:hidden">
          <select
            value={selectedJob}
            onChange={e => setSelectedJob(e.target.value)}
            className="bg-doj-surface border border-doj-border rounded-lg px-3 py-2 text-sm text-doj-text focus:outline-none focus:border-doj-blue/50"
          >
            {jobs.map(j => (
              <option key={j.id} value={j.id}>{j.label}</option>
            ))}
          </select>
          <button
            onClick={() => window.print()}
            className="flex items-center gap-2 px-4 py-2 bg-doj-surface-2 border border-doj-border text-doj-muted rounded-lg text-sm hover:text-doj-text transition-all"
          >
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" d="M6.72 13.829c-.24.03-.48.062-.72.096m.72-.096a42.415 42.415 0 0110.56 0m-10.56 0L6.34 18m10.94-4.171c.24.03.48.062.72.096m-.72-.096L17.66 18m0 0l.229 2.523a1.125 1.125 0 01-1.12 1.227H7.231c-.662 0-1.18-.568-1.12-1.227L6.34 18m11.318 0h1.091A2.25 2.25 0 0021 15.75V9.456c0-1.081-.768-2.015-1.837-2.175a48.055 48.055 0 00-1.913-.247M6.34 18H5.25A2.25 2.25 0 013 15.75V9.456c0-1.081.768-2.015 1.837-2.175a48.056 48.056 0 011.913-.247m10.5 0a48.536 48.536 0 00-10.5 0m10.5 0V3.375c0-.621-.504-1.125-1.125-1.125h-8.25c-.621 0-1.125.504-1.125 1.125v3.659M18 10.5h.008v.008H18V10.5zm-3 0h.008v.008H15V10.5z" />
            </svg>
            Export PDF
          </button>
          {report && (
            <button
              onClick={() => exportCSV(report)}
              className="flex items-center gap-2 px-4 py-2 bg-doj-green/15 border border-doj-green/40 text-doj-green rounded-lg text-sm hover:bg-doj-green/25 transition-all"
            >
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" d="M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5M16.5 12L12 16.5m0 0L7.5 12m4.5 4.5V3" />
              </svg>
              Export Excel (CSV)
            </button>
          )}
        </div>
      </div>

      {report && (
        <div className="space-y-6">
          {/* ── Section 1: Summary KPIs ── */}
          <div className="bg-doj-surface border border-doj-border rounded-xl p-5">
            <SectionHeader title="1. Summary KPIs" description={`Job ${report.job_id} — ${report.file_name}`} />
            <div className="grid grid-cols-3 gap-3 xl:grid-cols-6">
              <KPICard
                label="Rows Ingested"
                value={report.summary.total_rows_ingested.toLocaleString()}
                color="text-doj-blue"
                icon="📥"
              />
              <KPICard
                label="Rows Mapped"
                value={report.summary.rows_mapped.toLocaleString()}
                color="text-doj-green"
                icon="✓"
              />
              <KPICard
                label="Rows Rejected"
                value={report.summary.rows_rejected.toLocaleString()}
                color={report.summary.rows_rejected > 0 ? 'text-doj-red' : 'text-doj-muted'}
                icon="✗"
              />
              <KPICard
                label="Mapping Coverage"
                value={`${report.summary.mapping_coverage_pct}%`}
                color={report.summary.mapping_coverage_pct >= 95 ? 'text-doj-green' : 'text-doj-amber'}
                icon="↔"
              />
              <KPICard
                label="Duplicates Found"
                value={report.summary.duplicates_identified}
                color="text-doj-amber"
                icon="⊕"
              />
              <KPICard
                label="Duplicates Resolved"
                value={report.summary.duplicates_resolved}
                sub={`${Math.round(report.summary.duplicates_resolved / Math.max(report.summary.duplicates_identified, 1) * 100)}% resolution rate`}
                color="text-doj-green"
                icon="✓"
              />
            </div>
          </div>

          {/* ── Section 2: Mapping Coverage ── */}
          <div className="bg-doj-surface border border-doj-border rounded-xl p-5">
            <SectionHeader title="2. Mapping Coverage by Destination Table" />
            <div className="overflow-x-auto rounded-lg border border-doj-border">
              <table className="w-full">
                <thead>
                  <tr className="bg-doj-surface-2 border-b border-doj-border">
                    {['Destination Table', 'Expected Columns', 'Mapped Columns', 'Coverage', 'Status'].map(h => (
                      <th key={h} className="px-4 py-3 text-left text-xs font-semibold text-doj-muted uppercase tracking-wider">{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {report.mapping_coverage.map((row, i) => (
                    <tr key={row.table} className={`${i < report.mapping_coverage.length - 1 ? 'border-b border-doj-border/50' : ''} hover:bg-white/2`}>
                      <td className="px-4 py-3 font-mono text-sm text-doj-blue">{row.table}</td>
                      <td className="px-4 py-3 font-mono text-sm text-doj-text">{row.expected}</td>
                      <td className="px-4 py-3 font-mono text-sm text-doj-text">{row.mapped}</td>
                      <td className="px-4 py-3 min-w-[180px]">
                        <CoverageBar pct={row.coverage} />
                      </td>
                      <td className="px-4 py-3">
                        <span className={`px-2 py-0.5 rounded text-xs font-medium border
                          ${row.coverage === 100 ? 'bg-doj-green/15 border-doj-green/40 text-doj-green'
                          : row.coverage >= 85 ? 'bg-doj-amber/15 border-doj-amber/40 text-doj-amber'
                          : 'bg-doj-red/15 border-doj-red/40 text-doj-red'}`}
                        >
                          {row.coverage === 100 ? 'Complete' : row.coverage >= 85 ? 'Partial' : 'Incomplete'}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* ── Section 3: Row Reconciliation ── */}
          <div className="bg-doj-surface border border-doj-border rounded-xl p-5">
            <SectionHeader title="3. Row Reconciliation" description="Rows at each pipeline stage" />
            <div className="flex items-end gap-0 overflow-x-auto pb-2">
              {report.row_reconciliation.map((stage, i) => {
                const maxRows = report.row_reconciliation[0].rows;
                const heightPct = Math.max(20, Math.round((stage.rows / maxRows) * 100));
                const isLast = i === report.row_reconciliation.length - 1;
                return (
                  <React.Fragment key={stage.stage}>
                    <div className="flex flex-col items-center min-w-[120px]">
                      {/* Delta */}
                      {stage.delta != null && (
                        <div className={`text-xs font-mono mb-1 ${stage.delta < 0 ? 'text-doj-red' : 'text-doj-green'}`}>
                          {stage.delta > 0 ? '+' : ''}{stage.delta.toLocaleString()}
                        </div>
                      )}
                      {!stage.delta && <div className="text-xs mb-1 text-transparent">0</div>}
                      {/* Bar */}
                      <div className="relative w-20 flex flex-col justify-end" style={{ height: '120px' }}>
                        <div
                          className="w-full rounded-t-sm bg-doj-blue/40 border border-doj-blue/30 transition-all duration-700"
                          style={{ height: `${heightPct}%` }}
                        />
                      </div>
                      {/* Row count */}
                      <div className="font-mono text-xs text-doj-text mt-1">{formatRows(stage.rows)}</div>
                      {/* Stage name */}
                      <div className="text-[10px] text-doj-muted mt-1 text-center leading-tight">{stage.stage}</div>
                    </div>
                    {!isLast && (
                      <div className="flex items-center pb-6 text-doj-muted text-sm mx-1">→</div>
                    )}
                  </React.Fragment>
                );
              })}
            </div>
          </div>

          {/* ── Section 4: Issues Resolved ── */}
          <div className="bg-doj-surface border border-doj-border rounded-xl p-5">
            <SectionHeader title="4. Issues Resolved by Type" />
            <div className="grid grid-cols-2 gap-3 xl:grid-cols-4">
              {report.issues_resolved.map(item => {
                const pct = Math.round((item.resolved / Math.max(item.total, 1)) * 100);
                return (
                  <div key={item.type} className="bg-doj-surface-2 border border-doj-border rounded-xl p-4">
                    <div className="text-xs font-semibold text-doj-muted mb-3 leading-tight">{item.type}</div>
                    <div className="flex items-baseline gap-1 mb-2">
                      <span className="text-2xl font-bold font-mono text-doj-text">{item.resolved}</span>
                      <span className="text-sm text-doj-muted">/ {item.total}</span>
                    </div>
                    <div className="h-1.5 bg-doj-border rounded-full overflow-hidden">
                      <div
                        className="h-full rounded-full transition-all duration-700"
                        style={{
                          width: `${pct}%`,
                          backgroundColor: pct === 100 ? '#22c55e' : pct >= 75 ? '#f59e0b' : '#ef4444',
                        }}
                      />
                    </div>
                    <div className="text-xs text-doj-muted mt-1">{pct}% resolved</div>
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      )}

      {/* Print styles */}
      <style>{`
        @media print {
          body { background: white; color: black; }
          .bg-doj-surface, .bg-doj-surface-2, .bg-doj-bg { background: white !important; }
          .border-doj-border { border-color: #e5e7eb !important; }
          .text-doj-text { color: #111827 !important; }
          .text-doj-muted { color: #6b7280 !important; }
        }
      `}</style>
    </div>
  );
}
