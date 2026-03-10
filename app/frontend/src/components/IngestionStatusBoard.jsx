import React, { useState, useEffect, useMemo, useCallback, useRef } from 'react';
import { useFilterContext, SYSTEM_COLORS } from '../App';

// ─── Constants ────────────────────────────────────────────────────────────────
const STAGES = ['Upload', 'Bronze', 'Silver', 'Mapping', 'Gold', 'Staging', 'Complete'];

const STAGE_COLORS = {
  complete: { bg: 'bg-doj-green/20', text: 'text-doj-green', border: 'border-doj-green/40', fill: '#22c55e', label: 'Complete' },
  running: { bg: 'bg-doj-amber/20', text: 'text-doj-amber', border: 'border-doj-amber/40', fill: '#f59e0b', label: 'Running' },
  failed: { bg: 'bg-doj-red/20', text: 'text-doj-red', border: 'border-doj-red/40', fill: '#ef4444', label: 'Failed' },
  review: { bg: 'bg-doj-red/20', text: 'text-doj-red', border: 'border-doj-red/40', fill: '#ef4444', label: 'Review' },
  upload: { bg: 'bg-doj-blue/20', text: 'text-doj-blue', border: 'border-doj-blue/40', fill: '#3b82f6', label: 'Uploading' },
  idle: { bg: 'bg-doj-muted/10', text: 'text-doj-muted', border: 'border-doj-muted/20', fill: '#64748b', label: 'Idle' },
};

// ─── Demo data ────────────────────────────────────────────────────────────────
function generateDemoJobs() {
  const now = Date.now();
  return [
    {
      id: 'JOB-A4F2B1',
      file_name: 'cases_2024_q4.csv',
      system: 'LegacyCase',
      uploaded_at: new Date(now - 1000 * 60 * 12).toISOString(),
      stage: 'Gold',
      stage_index: 4,
      status: 'running',
      rows: 142856,
      issues: 3,
      stage_timings: [
        { stage: 'Upload', duration: 45, status: 'complete', rows_in: 142856, rows_out: 142856 },
        { stage: 'Bronze', duration: 120, status: 'complete', rows_in: 142856, rows_out: 142820 },
        { stage: 'Silver', duration: 310, status: 'complete', rows_in: 142820, rows_out: 141990 },
        { stage: 'Mapping', duration: 200, status: 'complete', rows_in: 141990, rows_out: 141750 },
        { stage: 'Gold', duration: null, status: 'running', rows_in: 141750, rows_out: null },
        { stage: 'Staging', duration: null, status: 'idle', rows_in: null, rows_out: null },
        { stage: 'Complete', duration: null, status: 'idle', rows_in: null, rows_out: null },
      ],
    },
    {
      id: 'JOB-C9E3D7',
      file_name: 'defendants_export_jan.xlsx',
      system: 'LegacyCase',
      uploaded_at: new Date(now - 1000 * 60 * 45).toISOString(),
      stage: 'Complete',
      stage_index: 6,
      status: 'complete',
      rows: 58340,
      issues: 0,
      stage_timings: [
        { stage: 'Upload', duration: 22, status: 'complete', rows_in: 58340, rows_out: 58340 },
        { stage: 'Bronze', duration: 67, status: 'complete', rows_in: 58340, rows_out: 58310 },
        { stage: 'Silver', duration: 145, status: 'complete', rows_in: 58310, rows_out: 58200 },
        { stage: 'Mapping', duration: 98, status: 'complete', rows_in: 58200, rows_out: 58180 },
        { stage: 'Gold', duration: 210, status: 'complete', rows_in: 58180, rows_out: 58000 },
        { stage: 'Staging', duration: 180, status: 'complete', rows_in: 58000, rows_out: 57990 },
        { stage: 'Complete', duration: 5, status: 'complete', rows_in: 57990, rows_out: 57990 },
      ],
    },
    {
      id: 'JOB-B7F1A8',
      file_name: 'oj_contacts_2024.csv',
      system: 'OpenJustice',
      uploaded_at: new Date(now - 1000 * 60 * 8).toISOString(),
      stage: 'Mapping',
      stage_index: 3,
      status: 'review',
      rows: 29100,
      issues: 14,
      stage_timings: [
        { stage: 'Upload', duration: 18, status: 'complete', rows_in: 29100, rows_out: 29100 },
        { stage: 'Bronze', duration: 44, status: 'complete', rows_in: 29100, rows_out: 29088 },
        { stage: 'Silver', duration: 95, status: 'complete', rows_in: 29088, rows_out: 28900 },
        { stage: 'Mapping', duration: null, status: 'review', rows_in: 28900, rows_out: null },
        { stage: 'Gold', duration: null, status: 'idle', rows_in: null, rows_out: null },
        { stage: 'Staging', duration: null, status: 'idle', rows_in: null, rows_out: null },
        { stage: 'Complete', duration: null, status: 'idle', rows_in: null, rows_out: null },
      ],
    },
    {
      id: 'JOB-D2C4E9',
      file_name: 'adhoc_parole_data.xlsx',
      system: 'AdHocExports',
      uploaded_at: new Date(now - 1000 * 60 * 3).toISOString(),
      stage: 'Bronze',
      stage_index: 1,
      status: 'running',
      rows: 8750,
      issues: 0,
      stage_timings: [
        { stage: 'Upload', duration: 8, status: 'complete', rows_in: 8750, rows_out: 8750 },
        { stage: 'Bronze', duration: null, status: 'running', rows_in: 8750, rows_out: null },
        { stage: 'Silver', duration: null, status: 'idle', rows_in: null, rows_out: null },
        { stage: 'Mapping', duration: null, status: 'idle', rows_in: null, rows_out: null },
        { stage: 'Gold', duration: null, status: 'idle', rows_in: null, rows_out: null },
        { stage: 'Staging', duration: null, status: 'idle', rows_in: null, rows_out: null },
        { stage: 'Complete', duration: null, status: 'idle', rows_in: null, rows_out: null },
      ],
    },
    {
      id: 'JOB-F5A3C2',
      file_name: 'oj_charges_batch7.csv',
      system: 'OpenJustice',
      uploaded_at: new Date(now - 1000 * 60 * 120).toISOString(),
      stage: 'Silver',
      stage_index: 2,
      status: 'failed',
      rows: 51200,
      issues: 7,
      stage_timings: [
        { stage: 'Upload', duration: 31, status: 'complete', rows_in: 51200, rows_out: 51200 },
        { stage: 'Bronze', duration: 88, status: 'complete', rows_in: 51200, rows_out: 51155 },
        { stage: 'Silver', duration: 180, status: 'failed', rows_in: 51155, rows_out: null },
        { stage: 'Mapping', duration: null, status: 'idle', rows_in: null, rows_out: null },
        { stage: 'Gold', duration: null, status: 'idle', rows_in: null, rows_out: null },
        { stage: 'Staging', duration: null, status: 'idle', rows_in: null, rows_out: null },
        { stage: 'Complete', duration: null, status: 'idle', rows_in: null, rows_out: null },
      ],
    },
    {
      id: 'JOB-E1D9B3',
      file_name: 'legacy_attorneys_2023.csv',
      system: 'LegacyCase',
      uploaded_at: new Date(now - 1000 * 60 * 220).toISOString(),
      stage: 'Complete',
      stage_index: 6,
      status: 'complete',
      rows: 4210,
      issues: 1,
      stage_timings: [
        { stage: 'Upload', duration: 9, status: 'complete', rows_in: 4210, rows_out: 4210 },
        { stage: 'Bronze', duration: 21, status: 'complete', rows_in: 4210, rows_out: 4205 },
        { stage: 'Silver', duration: 48, status: 'complete', rows_in: 4205, rows_out: 4200 },
        { stage: 'Mapping', duration: 35, status: 'complete', rows_in: 4200, rows_out: 4198 },
        { stage: 'Gold', duration: 65, status: 'complete', rows_in: 4198, rows_out: 4195 },
        { stage: 'Staging', duration: 55, status: 'complete', rows_in: 4195, rows_out: 4195 },
        { stage: 'Complete', duration: 2, status: 'complete', rows_in: 4195, rows_out: 4195 },
      ],
    },
  ];
}

// ─── Source system display name map (matches backend _SYSTEM_DISPLAY) ─────────
const SYSTEM_DISPLAY = {
  LEGACY_CASE: 'LegacyCase',
  OPEN_JUSTICE: 'OpenJustice',
  AD_HOC_EXPORTS: 'AdHocExports',
};

// Stage enum → display name (matches backend _STAGE_NAMES ordering)
const STAGE_DISPLAY = {
  UPLOAD: 'Upload',
  BRONZE: 'Bronze',
  SILVER: 'Silver',
  MAPPING: 'Mapping',
  GOLD: 'Gold',
  STAGING: 'Staging',
  COMPLETE: 'Complete',
  FAILED: 'Complete',
};

const STAGE_INDEX = {
  UPLOAD: 0, BRONZE: 1, SILVER: 2, MAPPING: 3, GOLD: 4, STAGING: 5, COMPLETE: 6, FAILED: 6,
};

const DEFAULT_TIMINGS = STAGES.map(s => ({
  stage: s, status: 'idle', duration: null, rows_in: null, rows_out: null,
}));

/**
 * Normalize a job object from the API so the frontend always sees consistent
 * field names, regardless of whether the backend has been updated.
 *
 * Old format: { job_id, source_system, current_stage, status (uppercase),
 *               rows_processed, issue_count }
 * New format: { id, system, stage, stage_index, status (lowercase),
 *               rows, issues, stage_timings }
 */
function normalizeJob(j) {
  // Status: convert uppercase enum to lowercase display value
  const rawStatus = (j.status || '').toUpperCase();
  const statusMap = {
    RUNNING: 'running', COMPLETE: 'complete', FAILED: 'failed',
    REVIEW_NEEDED: 'review', UPLOAD: 'upload',
  };
  const rawStage = (j.current_stage || j.stage || 'UPLOAD').toUpperCase();
  const fe_status = j.id
    ? (j.status || 'running')                         // new format: already lowercase
    : (rawStage === 'UPLOAD' ? 'upload' : (statusMap[rawStatus] || 'running')); // old format

  // System: convert enum to display name
  const systemRaw = (j.source_system || j.system || '').toUpperCase();
  const system = j.system || SYSTEM_DISPLAY[systemRaw] || j.source_system || '';

  // Stage index and name
  const stageKey = rawStage;
  const stage_index = j.stage_index !== undefined ? j.stage_index : (STAGE_INDEX[stageKey] ?? 0);
  const stage = j.stage || STAGE_DISPLAY[stageKey] || 'Upload';

  return {
    id:          j.id || j.job_id,
    file_name:   j.file_name,
    system,
    uploaded_at: j.uploaded_at,
    stage,
    stage_index,
    status:      fe_status,
    rows:        j.rows !== undefined ? j.rows : (j.rows_processed || 0),
    issues:      j.issues !== undefined ? j.issues : (j.issue_count || 0),
    stage_timings: j.stage_timings || DEFAULT_TIMINGS,
    error_message: j.error_message,
    databricks_run_id: j.databricks_run_id,
  };
}

// ─── Utilities ────────────────────────────────────────────────────────────────
function relativeTime(isoStr) {
  const diff = Date.now() - new Date(isoStr).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return 'just now';
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  return `${Math.floor(hrs / 24)}d ago`;
}

function formatRows(n) {
  if (n == null) return '—';
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(0)}K`;
  return n.toString();
}

function formatDuration(seconds) {
  if (seconds == null) return '—';
  if (seconds < 60) return `${seconds}s`;
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  return s > 0 ? `${m}m ${s}s` : `${m}m`;
}

// ─── Stage progress bar ───────────────────────────────────────────────────────
function StageProgressBar({ stageIndex, status, timings }) {
  const total = STAGES.length;
  return (
    <div className="flex items-center gap-0.5 w-full">
      {STAGES.map((stage, i) => {
        const timing = timings?.[i];
        const st = timing?.status || (i < stageIndex ? 'complete' : i === stageIndex ? status : 'idle');
        const color = STAGE_COLORS[st] || STAGE_COLORS.idle;
        return (
          <div
            key={stage}
            title={`${stage}: ${st}`}
            className={`h-1.5 flex-1 rounded-sm transition-all ${color.bg}
              ${st === 'running' ? 'animate-pulse' : ''}
              ${i === 0 ? 'rounded-l-full' : ''}
              ${i === total - 1 ? 'rounded-r-full' : ''}
            `}
            style={{ backgroundColor: st !== 'idle' ? color.fill + '60' : undefined }}
          />
        );
      })}
    </div>
  );
}

// ─── Expanded row detail ──────────────────────────────────────────────────────
function ExpandedRow({ job }) {
  return (
    <tr className="bg-doj-surface-2 border-b border-doj-border">
      <td colSpan={9} className="px-6 py-4">
        <div className="animate-fade-in">
          <div className="flex items-center gap-2 mb-3">
            <span className="text-xs font-semibold text-doj-muted uppercase tracking-wider">Stage Breakdown</span>
          </div>
          <div className="grid grid-cols-7 gap-2">
            {STAGES.map((stage, i) => {
              const timing = job.stage_timings?.[i];
              const st = timing?.status || 'idle';
              const color = STAGE_COLORS[st] || STAGE_COLORS.idle;
              return (
                <div
                  key={stage}
                  className={`p-3 rounded-lg border ${color.border} ${color.bg}`}
                >
                  <div className={`text-xs font-semibold ${color.text} mb-1`}>{stage}</div>
                  <div className="text-xs text-doj-muted">
                    <div className="flex justify-between">
                      <span>Time</span>
                      <span className="font-mono text-doj-text">{formatDuration(timing?.duration)}</span>
                    </div>
                    <div className="flex justify-between mt-0.5">
                      <span>In</span>
                      <span className="font-mono text-doj-text">{formatRows(timing?.rows_in)}</span>
                    </div>
                    <div className="flex justify-between mt-0.5">
                      <span>Out</span>
                      <span className="font-mono text-doj-text">{formatRows(timing?.rows_out)}</span>
                    </div>
                  </div>
                  {st === 'running' && (
                    <div className="mt-1.5 h-0.5 bg-doj-border rounded-full overflow-hidden">
                      <div className="h-full bg-doj-amber rounded-full animate-pulse" style={{ width: '60%' }} />
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      </td>
    </tr>
  );
}

// ─── Status badge ─────────────────────────────────────────────────────────────
function StatusBadge({ status }) {
  const color = STAGE_COLORS[status] || STAGE_COLORS.idle;
  const icons = {
    complete: (
      <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" strokeWidth={2.5} stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
      </svg>
    ),
    running: (
      <svg className="w-3 h-3 animate-spin" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0l3.181 3.183a8.25 8.25 0 0013.803-3.7M4.031 9.865a8.25 8.25 0 0113.803-3.7l3.181 3.182m0-4.991v4.99" />
      </svg>
    ),
    failed: (
      <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" strokeWidth={2.5} stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
      </svg>
    ),
    review: (
      <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m9-.75a9 9 0 11-18 0 9 9 0 0118 0zm-9 3.75h.008v.008H12v-.008z" />
      </svg>
    ),
    upload: (
      <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" d="M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5m-13.5-9L12 3m0 0l4.5 4.5M12 3v13.5" />
      </svg>
    ),
  };
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium border ${color.bg} ${color.text} ${color.border}`}>
      {icons[status]}
      {color.label}
    </span>
  );
}

// ─── System badge ─────────────────────────────────────────────────────────────
function SystemBadge({ system }) {
  const c = SYSTEM_COLORS[system];
  if (!c) return <span className="text-doj-muted text-xs">{system}</span>;
  return (
    <span className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded-md text-xs font-medium border ${c.bg} ${c.text} ${c.border}`}>
      <span className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: c.hex }} />
      {system}
    </span>
  );
}

// ─── Empty state ──────────────────────────────────────────────────────────────
function EmptyState() {
  return (
    <tr>
      <td colSpan={9} className="py-20 text-center">
        <div className="flex flex-col items-center gap-3">
          <svg className="w-12 h-12 text-doj-muted opacity-30" fill="none" viewBox="0 0 24 24" strokeWidth={1} stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" d="M20.25 7.5l-.625 10.632a2.25 2.25 0 01-2.247 2.118H6.622a2.25 2.25 0 01-2.247-2.118L3.75 7.5M10 11.25h4M3.375 7.5h17.25c.621 0 1.125-.504 1.125-1.125v-1.5c0-.621-.504-1.125-1.125-1.125H3.375c-.621 0-1.125.504-1.125 1.125v1.5c0 .621.504 1.125 1.125 1.125z" />
          </svg>
          <p className="text-doj-muted text-sm">No jobs found matching your filters</p>
          <a
            href="/upload"
            className="mt-2 inline-flex items-center gap-2 px-4 py-2 bg-doj-blue/15 border border-doj-blue/40 text-doj-blue rounded-lg text-sm font-medium hover:bg-doj-blue/25 transition-all"
          >
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" d="M12 4.5v15m7.5-7.5h-15" />
            </svg>
            Upload First File
          </a>
        </div>
      </td>
    </tr>
  );
}

// ─── Main Component ───────────────────────────────────────────────────────────
export default function IngestionStatusBoard() {
  const { statusFilter, systemFilters } = useFilterContext();
  const [jobs, setJobs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [expandedRows, setExpandedRows] = useState(new Set());
  const [search, setSearch] = useState('');
  const [sortBy, setSortBy] = useState('uploaded_at');
  const [sortDir, setSortDir] = useState('desc');

  const [isLiveData, setIsLiveData] = useState(false);
  const [lastRefreshed, setLastRefreshed] = useState(null);
  const [refreshCountdown, setRefreshCountdown] = useState(10);
  const hasLiveDataRef = useRef(false);

  const demoJobs = useMemo(() => generateDemoJobs(), []);

  const fetchJobs = useCallback(async () => {
    try {
      const res = await fetch('/api/jobs');
      if (res.ok) {
        const data = await res.json();
        const normalized = data.map(normalizeJob);
        setJobs(normalized);
        hasLiveDataRef.current = true;
        setIsLiveData(true);
        setLastRefreshed(Date.now());
      } else {
        // API error — only fall back to demo if we've never received live data
        if (!hasLiveDataRef.current) setJobs(demoJobs);
      }
    } catch {
      if (!hasLiveDataRef.current) setJobs(demoJobs);
    } finally {
      setLoading(false);
    }
  }, [demoJobs]);

  // Polling every 10 seconds
  useEffect(() => {
    fetchJobs();
    const interval = setInterval(fetchJobs, 10000);
    return () => clearInterval(interval);
  }, [fetchJobs]);

  // Countdown timer — resets to 10 whenever lastRefreshed updates
  useEffect(() => {
    setRefreshCountdown(10);
    const timer = setInterval(() => setRefreshCountdown(s => Math.max(0, s - 1)), 1000);
    return () => clearInterval(timer);
  }, [lastRefreshed]);

  const toggleRow = (id) => {
    setExpandedRows(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const handleSort = (col) => {
    if (sortBy === col) setSortDir(d => d === 'asc' ? 'desc' : 'asc');
    else { setSortBy(col); setSortDir('desc'); }
  };

  const filteredJobs = useMemo(() => {
    let result = [...jobs];
    if (!systemFilters.has('LegacyCase') || !systemFilters.has('OpenJustice') || !systemFilters.has('AdHocExports')) {
      result = result.filter(j => systemFilters.has(j.system));
    }
    if (statusFilter !== 'all') {
      if (statusFilter === 'active') result = result.filter(j => j.status === 'running' || j.status === 'upload');
      else if (statusFilter === 'review') result = result.filter(j => j.status === 'review');
      else if (statusFilter === 'failed') result = result.filter(j => j.status === 'failed');
    }
    if (search) {
      const q = search.toLowerCase();
      result = result.filter(j =>
        j.id.toLowerCase().includes(q) ||
        j.file_name.toLowerCase().includes(q) ||
        j.system.toLowerCase().includes(q)
      );
    }
    result.sort((a, b) => {
      let av, bv;
      if (sortBy === 'uploaded_at') { av = new Date(a.uploaded_at); bv = new Date(b.uploaded_at); }
      else if (sortBy === 'rows') { av = a.rows || 0; bv = b.rows || 0; }
      else if (sortBy === 'issues') { av = a.issues || 0; bv = b.issues || 0; }
      else return 0;
      return sortDir === 'asc' ? (av > bv ? 1 : -1) : (av < bv ? 1 : -1);
    });
    return result;
  }, [jobs, statusFilter, systemFilters, search, sortBy, sortDir]);

  const SortIcon = ({ col }) => {
    if (sortBy !== col) return <span className="text-doj-border ml-1">↕</span>;
    return <span className="text-doj-blue ml-1">{sortDir === 'asc' ? '↑' : '↓'}</span>;
  };

  const ThBtn = ({ col, children }) => (
    <button
      onClick={() => handleSort(col)}
      className="flex items-center gap-1 text-xs font-semibold text-doj-muted uppercase tracking-wider hover:text-doj-text transition-colors"
    >
      {children}
      <SortIcon col={col} />
    </button>
  );

  return (
    <div>
      <div className="flex items-center justify-between mb-5">
        <div>
          <h1 className="text-xl font-bold text-doj-text">Ingestion Status Board</h1>
          <div className="flex items-center gap-3 mt-0.5">
            <p className="text-sm text-doj-muted">
              {filteredJobs.length} job{filteredJobs.length !== 1 ? 's' : ''}
            </p>
            {/* Live / demo indicator */}
            {isLiveData ? (
              <span className="inline-flex items-center gap-1.5 text-xs text-doj-green">
                <span className="w-1.5 h-1.5 rounded-full bg-doj-green animate-pulse" />
                Live · refreshes in {refreshCountdown}s
              </span>
            ) : (
              <span className="inline-flex items-center gap-1.5 text-xs text-doj-amber">
                <span className="w-1.5 h-1.5 rounded-full bg-doj-amber" />
                Demo data
              </span>
            )}
          </div>
        </div>
        {/* Search bar */}
        <div className="relative">
          <svg className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-doj-muted" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z" />
          </svg>
          <input
            type="text"
            placeholder="Search jobs, files..."
            value={search}
            onChange={e => setSearch(e.target.value)}
            className="pl-9 pr-4 py-2 bg-doj-surface border border-doj-border rounded-lg text-sm text-doj-text placeholder-doj-muted focus:outline-none focus:border-doj-blue/50 w-64"
          />
        </div>
      </div>

      <div className="bg-doj-surface border border-doj-border rounded-xl overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="border-b border-doj-border bg-doj-surface-2">
                <th className="px-4 py-3 text-left">
                  <span className="text-xs font-semibold text-doj-muted uppercase tracking-wider">Job ID</span>
                </th>
                <th className="px-4 py-3 text-left">
                  <span className="text-xs font-semibold text-doj-muted uppercase tracking-wider">File Name</span>
                </th>
                <th className="px-4 py-3 text-left">
                  <span className="text-xs font-semibold text-doj-muted uppercase tracking-wider">System</span>
                </th>
                <th className="px-4 py-3 text-left">
                  <ThBtn col="uploaded_at">Uploaded</ThBtn>
                </th>
                <th className="px-4 py-3 text-left">
                  <span className="text-xs font-semibold text-doj-muted uppercase tracking-wider">Stage</span>
                </th>
                <th className="px-4 py-3 text-left">
                  <span className="text-xs font-semibold text-doj-muted uppercase tracking-wider">Status</span>
                </th>
                <th className="px-4 py-3 text-left">
                  <ThBtn col="rows">Rows</ThBtn>
                </th>
                <th className="px-4 py-3 text-left">
                  <ThBtn col="issues">Issues</ThBtn>
                </th>
                <th className="px-4 py-3 text-left">
                  <span className="text-xs font-semibold text-doj-muted uppercase tracking-wider">Progress</span>
                </th>
              </tr>
            </thead>
            <tbody>
              {loading ? (
                [...Array(4)].map((_, i) => (
                  <tr key={i} className="border-b border-doj-border/50">
                    {[...Array(9)].map((_, j) => (
                      <td key={j} className="px-4 py-3.5">
                        <div className="h-3 bg-doj-border/40 rounded animate-pulse" style={{ width: `${40 + Math.random() * 40}%` }} />
                      </td>
                    ))}
                  </tr>
                ))
              ) : filteredJobs.length === 0 ? (
                <EmptyState />
              ) : (
                filteredJobs.flatMap(job => {
                  const isExpanded = expandedRows.has(job.id);
                  const rows = [
                    <tr
                      key={job.id}
                      onClick={() => toggleRow(job.id)}
                      className={`border-b border-doj-border/50 cursor-pointer transition-colors hover:bg-white/2
                        ${isExpanded ? 'bg-doj-blue/5' : ''}
                        ${job.status === 'failed' ? 'hover:bg-doj-red/5' : ''}
                        ${job.status === 'review' ? 'hover:bg-doj-red/5' : ''}
                      `}
                    >
                      {/* Job ID */}
                      <td className="px-4 py-3.5">
                        <div className="flex items-center gap-2">
                          <span className={`transition-transform duration-200 text-doj-muted text-xs ${isExpanded ? 'rotate-90' : ''}`}>▶</span>
                          <span className="font-mono text-sm font-medium text-doj-blue">{job.id}</span>
                        </div>
                      </td>
                      {/* File name */}
                      <td className="px-4 py-3.5">
                        <span className="text-sm text-doj-text truncate max-w-[200px] block">{job.file_name}</span>
                      </td>
                      {/* System */}
                      <td className="px-4 py-3.5">
                        <SystemBadge system={job.system} />
                      </td>
                      {/* Uploaded */}
                      <td className="px-4 py-3.5">
                        <span className="text-sm text-doj-muted">{relativeTime(job.uploaded_at)}</span>
                      </td>
                      {/* Stage */}
                      <td className="px-4 py-3.5">
                        <span className="text-sm text-doj-text">{job.stage}</span>
                      </td>
                      {/* Status */}
                      <td className="px-4 py-3.5">
                        <StatusBadge status={job.status} />
                      </td>
                      {/* Rows */}
                      <td className="px-4 py-3.5">
                        <span className="font-mono text-sm text-doj-text">{formatRows(job.rows)}</span>
                      </td>
                      {/* Issues */}
                      <td className="px-4 py-3.5">
                        {job.issues > 0 ? (
                          <span className="inline-flex items-center justify-center min-w-[22px] h-5 px-1.5 rounded-full bg-doj-red/20 border border-doj-red/40 text-doj-red text-xs font-bold">
                            {job.issues}
                          </span>
                        ) : (
                          <span className="text-doj-muted text-xs">—</span>
                        )}
                      </td>
                      {/* Progress bar */}
                      <td className="px-4 py-3.5 min-w-[140px]">
                        <StageProgressBar
                          stageIndex={job.stage_index}
                          status={job.status}
                          timings={job.stage_timings}
                        />
                        <div className="text-[10px] text-doj-muted mt-1">
                          {job.stage_index + 1}/{STAGES.length} stages
                        </div>
                      </td>
                    </tr>,
                    isExpanded && <ExpandedRow key={`${job.id}-detail`} job={job} />,
                  ].filter(Boolean);
                  return rows;
                })
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
