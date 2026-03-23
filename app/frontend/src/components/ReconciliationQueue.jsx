import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { SYSTEM_COLORS } from '../App';

// ─── Data Transform Helpers ───────────────────────────────────────────────────
const TABLE_SYSTEM = {
  legacycase_tbl_defendant: 'LegacyCase',
  adhoc_client: 'AdHocExports',
  adhoc_lookup: 'AdHocExports',
  openjustice_arrests: 'OpenJustice',
  schema_mappings: 'LegacyCase',
};
const SYS_JOB = {
  LegacyCase: 'JOB-LEGACY',
  OpenJustice: 'JOB-OPENJUST',
  AdHocExports: 'JOB-ADHOC',
};

function transformMappings(rows) {
  return rows.map((r, i) => ({
    id: `LC-${String(i + 1).padStart(3, '0')}`,
    issue_key: `mapping::${r.source_system || 'unknown'}::${r.source_column}`,
    system: r.source_system || 'LegacyCase',
    job_id: SYS_JOB[r.source_system] || 'JOB-LEGACY',
    source_column: r.source_column,
    proposed_target: r.suggested_target,
    confidence: parseFloat(r.mapping_confidence) || 0,
    rationale: r.llm_rationale || 'Automated mapping suggestion',
    null_rate: 0,
    top_values: [],
    status: (r.review_status || 'pending').toLowerCase(),
    reviewer: null,
    note: r.reviewer_note || '',
  }));
}

function transformDuplicates(rows) {
  const SEV_SCORE = { CRITICAL: 0.96, HIGH: 0.87, MEDIUM: 0.75 };
  return rows.map((r, i) => {
    const groupId = r.duplicate_group_id || `DUP-${String(i + 1).padStart(3, '0')}`;
    const raw = r.all_defendant_ids;
    let defIds;
    if (Array.isArray(raw)) {
      defIds = raw;
    } else if (typeof raw === 'string' && raw.trim().startsWith('[')) {
      try { defIds = JSON.parse(raw); } catch { defIds = []; }
    } else {
      defIds = (raw || '').split(',').map(s => s.trim()).filter(Boolean);
    }
    return {
      id: groupId,
      issue_key: `duplicate::${groupId}`,
      similarity: SEV_SCORE[r.severity] || 0.80,
      system: 'LegacyCase',
      job_id: 'JOB-LEGACY',
      status: 'pending',
      records: defIds.map(defId => ({
        id: defId,
        name: `${r.first_name || ''} ${r.last_name || ''}`.trim(),
        dob: r.date_of_birth || 'Unknown',
        system: 'LegacyCase',
        source_id: defId,
      })),
    };
  });
}

function transformUnmapped(rows) {
  return rows.map((r, i) => {
    const sys = TABLE_SYSTEM[r.source_table] || 'LegacyCase';
    return {
      id: `UNM-${String(i + 1).padStart(3, '0')}`,
      issue_key: `unmapped::${r.source_table}::${r.field_name}::${r.code_value}`,
      source_value: r.code_value,
      field: r.field_name,
      system: sys,
      job_id: SYS_JOB[sys] || 'JOB-LEGACY',
      suggested: r.lookup_domain || '',
      status: 'pending',
    };
  });
}

function transformSchemaDrift(rows) {
  const DRIFT_TYPE = {
    WRONG_DATE_FORMAT: 'added',
    NULL_VALUE: 'added',
    TEXT_INSTEAD_OF_DATE: 'added',
    TYPE_MISMATCH: 'added',
    MISSING_COLUMN: 'missing',
    UNEXPECTED_COLUMN: 'added',
  };
  return rows.map((r, i) => {
    const sys = TABLE_SYSTEM[r.source_table] || 'LegacyCase';
    return {
      id: `SD-${String(i + 1).padStart(3, '0')}`,
      issue_key: `drift::${r.source_table}::${r.field_name}::${r.drift_type}`,
      column: r.field_name,
      type: DRIFT_TYPE[r.drift_type] || 'added',
      field_type: (r.drift_type || '').includes('DATE') ? 'DATE' : 'VARCHAR',
      system: sys,
      job_id: SYS_JOB[sys] || 'JOB-LEGACY',
      status: 'pending',
      description: r.drift_description || r.drift_type || '',
    };
  });
}

// ─── Utilities ────────────────────────────────────────────────────────────────
function SystemBadge({ system }) {
  const c = SYSTEM_COLORS[system];
  if (!c) return <span className="text-xs text-doj-muted">{system}</span>;
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-md text-xs font-medium border ${c.bg} ${c.text} ${c.border}`}>
      <span className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: c.hex }} />
      {system}
    </span>
  );
}

function ConfidenceMeter({ value }) {
  const pct = Math.round(value * 100);
  const color = value >= 0.85 ? '#22c55e' : value >= 0.75 ? '#f59e0b' : '#ef4444';
  const textColor = value >= 0.85 ? 'text-doj-green' : value >= 0.75 ? 'text-doj-amber' : 'text-doj-red';
  return (
    <div className="flex items-center gap-2">
      <div className="flex-1 h-1.5 bg-doj-border rounded-full overflow-hidden">
        <div
          className="h-full rounded-full transition-all"
          style={{ width: `${pct}%`, backgroundColor: color, boxShadow: `0 0 6px ${color}60` }}
        />
      </div>
      <span className={`font-mono text-xs font-bold ${textColor} w-8`}>{pct}%</span>
    </div>
  );
}

function TabBar({ tabs, active, onSelect }) {
  return (
    <div className="flex border-b border-doj-border">
      {tabs.map(tab => (
        <button
          key={tab.id}
          onClick={() => onSelect(tab.id)}
          className={`flex items-center gap-2 px-4 py-3 text-sm font-medium border-b-2 transition-all -mb-px
            ${active === tab.id
              ? 'border-doj-blue text-doj-blue'
              : 'border-transparent text-doj-muted hover:text-doj-text hover:border-doj-border'
            }`}
        >
          {tab.label}
          {tab.count > 0 && (
            <span className={`px-1.5 py-0.5 rounded-full text-xs font-bold
              ${active === tab.id ? 'bg-doj-blue/20 text-doj-blue' : 'bg-doj-border text-doj-muted'}`}
            >
              {tab.count}
            </span>
          )}
        </button>
      ))}
    </div>
  );
}

function LoadingState() {
  return (
    <div className="flex flex-col items-center justify-center py-16 gap-3">
      <div className="w-8 h-8 border-2 border-doj-blue/30 border-t-doj-blue rounded-full animate-spin" />
      <span className="text-sm text-doj-muted">Loading review queue from silver tables…</span>
    </div>
  );
}

function EmptyState({ label }) {
  return (
    <div className="flex flex-col items-center justify-center py-16 gap-2">
      <svg className="w-10 h-10 text-doj-border" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" d="M9 12.75L11.25 15 15 9.75M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
      </svg>
      <p className="text-sm text-doj-muted">No {label} issues found</p>
    </div>
  );
}

// ─── Low-Confidence Mappings Panel ───────────────────────────────────────────
function MappingsPanel({ items, setItems, filters }) {
  const [editingId, setEditingId] = useState(null);
  const [editTarget, setEditTarget] = useState('');
  const [notes, setNotes] = useState({});

  const filtered = useMemo(() => {
    return items.filter(item => {
      if (filters.system && item.system !== filters.system) return false;
      if (filters.jobId && !item.job_id.includes(filters.jobId)) return false;
      return true;
    });
  }, [items, filters]);

  const submit = async (id, decision, target, note) => {
    const item = items.find(it => it.id === id);
    // Optimistic update first so the UI responds immediately
    setItems(prev => prev.map(it => it.id === id
      ? { ...it, status: decision, reviewer: 'Current User', note: note || it.note }
      : it
    ));
    setEditingId(null);
    // Persist to silver.review_decisions
    if (item?.issue_key) {
      try {
        await fetch('/api/quality/decision', {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            issue_type: 'mapping',
            issue_key: item.issue_key,
            decision,
            reviewer: 'Current User',
            note: note || '',
          }),
        });
      } catch (e) {
        console.error('Failed to persist mapping decision:', e);
      }
    }
  };

  const bulkApprove = () => {
    const eligible = filtered.filter(it => it.confidence >= 0.85 && it.status === 'pending');
    eligible.forEach(it => submit(it.id, 'approved', it.proposed_target, 'Bulk approved'));
  };

  const eligibleBulk = filtered.filter(it => it.confidence >= 0.85 && it.status === 'pending').length;

  if (filtered.length === 0) return <EmptyState label="low-confidence mapping" />;

  return (
    <div className="space-y-4">
      <div className="flex justify-between items-center">
        <p className="text-sm text-doj-muted">{filtered.filter(i => i.status === 'pending').length} pending decisions</p>
        {eligibleBulk > 0 && (
          <button
            onClick={bulkApprove}
            className="flex items-center gap-2 px-3 py-1.5 bg-doj-green/15 border border-doj-green/40 text-doj-green rounded-lg text-xs font-medium hover:bg-doj-green/25 transition-all"
          >
            <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" strokeWidth={2.5} stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
            </svg>
            Bulk Approve {eligibleBulk} (≥85%)
          </button>
        )}
      </div>

      {filtered.map(item => (
        <div
          key={item.id}
          className={`bg-doj-surface-2 border rounded-xl p-4 transition-all
            ${item.status === 'approved' ? 'border-doj-green/30 opacity-70' : ''}
            ${item.status === 'rejected' ? 'border-doj-red/30 opacity-50' : ''}
            ${item.status === 'pending' ? 'border-doj-border' : ''}
          `}
        >
          <div className="flex items-start justify-between gap-4">
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-3 mb-3">
                <SystemBadge system={item.system} />
                <span className="text-xs text-doj-muted font-mono">{item.job_id}</span>
                {item.status !== 'pending' && (
                  <span className={`text-xs font-medium ${item.status === 'approved' ? 'text-doj-green' : 'text-doj-red'}`}>
                    {item.status === 'approved' ? '✓ Approved' : '✗ Rejected'}
                  </span>
                )}
              </div>

              <div className="flex items-center gap-3 mb-3 flex-wrap">
                {/* Source column */}
                <div className="flex flex-col gap-1">
                  <span className="text-[10px] text-doj-muted uppercase tracking-wider">Source</span>
                  <span className="font-mono text-sm text-doj-text bg-doj-surface px-2 py-1 rounded border border-doj-border">
                    {item.source_column}
                  </span>
                </div>

                <svg className="w-5 h-5 text-doj-muted mt-4" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" d="M13.5 4.5L21 12m0 0l-7.5 7.5M21 12H3" />
                </svg>

                {/* Proposed target */}
                <div className="flex flex-col gap-1">
                  <span className="text-[10px] text-doj-muted uppercase tracking-wider">Proposed Target</span>
                  {editingId === item.id ? (
                    <input
                      type="text"
                      value={editTarget}
                      onChange={e => setEditTarget(e.target.value)}
                      className="font-mono text-sm text-doj-text bg-doj-surface px-2 py-1 rounded border border-doj-blue/50 focus:outline-none focus:border-doj-blue w-48"
                      autoFocus
                    />
                  ) : (
                    <span className="font-mono text-sm text-doj-blue bg-doj-blue/10 px-2 py-1 rounded border border-doj-blue/20">
                      {item.proposed_target}
                    </span>
                  )}
                </div>
              </div>

              {/* Profiling stats */}
              <div className="flex gap-6 mb-3">
                <div>
                  <span className="text-[10px] text-doj-muted">Null Rate</span>
                  <div className="font-mono text-xs text-doj-text mt-0.5">{(item.null_rate * 100).toFixed(1)}%</div>
                </div>
                {item.top_values.length > 0 && (
                  <div>
                    <span className="text-[10px] text-doj-muted">Top Values</span>
                    <div className="flex gap-1 mt-0.5 flex-wrap">
                      {item.top_values.slice(0, 5).map(v => (
                        <span key={v} className="px-1.5 py-0.5 bg-doj-surface border border-doj-border rounded text-[10px] font-mono text-doj-muted">{v}</span>
                      ))}
                    </div>
                  </div>
                )}
              </div>

              {/* Confidence */}
              <div className="mb-3">
                <span className="text-[10px] text-doj-muted uppercase tracking-wider block mb-1">Confidence</span>
                <ConfidenceMeter value={item.confidence} />
              </div>

              {/* Rationale */}
              <p className="text-xs text-doj-muted italic mb-3">{item.rationale}</p>

              {/* Notes field */}
              <textarea
                placeholder="Add reviewer note..."
                value={notes[item.id] || ''}
                onChange={e => setNotes(prev => ({ ...prev, [item.id]: e.target.value }))}
                className="w-full bg-doj-surface border border-doj-border rounded-lg px-3 py-2 text-xs text-doj-text placeholder-doj-muted focus:outline-none focus:border-doj-blue/50 resize-none h-16"
                disabled={item.status !== 'pending'}
              />

              {/* Audit trail */}
              {item.status !== 'pending' && item.reviewer && (
                <p className="text-[10px] text-doj-muted mt-2">
                  Reviewed by <span className="text-doj-text">{item.reviewer}</span>
                  {item.note && <> — "{item.note}"</>}
                </p>
              )}
            </div>

            {/* Action buttons */}
            {item.status === 'pending' && (
              <div className="flex flex-col gap-2 flex-shrink-0">
                <button
                  onClick={() => submit(item.id, 'approved', item.proposed_target, notes[item.id])}
                  className="flex items-center gap-1.5 px-3 py-1.5 bg-doj-green/15 border border-doj-green/40 text-doj-green rounded-lg text-xs font-medium hover:bg-doj-green/25 transition-all"
                >
                  <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" strokeWidth={2.5} stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
                  </svg>
                  Approve
                </button>
                <button
                  onClick={() => submit(item.id, 'rejected', item.proposed_target, notes[item.id])}
                  className="flex items-center gap-1.5 px-3 py-1.5 bg-doj-red/15 border border-doj-red/40 text-doj-red rounded-lg text-xs font-medium hover:bg-doj-red/25 transition-all"
                >
                  <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" strokeWidth={2.5} stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
                  </svg>
                  Reject
                </button>
                {editingId === item.id ? (
                  <button
                    onClick={() => submit(item.id, 'approved', editTarget, notes[item.id])}
                    className="px-3 py-1.5 bg-doj-blue/15 border border-doj-blue/40 text-doj-blue rounded-lg text-xs font-medium hover:bg-doj-blue/25 transition-all"
                  >
                    Save
                  </button>
                ) : (
                  <button
                    onClick={() => { setEditingId(item.id); setEditTarget(item.proposed_target); }}
                    className="px-3 py-1.5 bg-doj-surface border border-doj-border text-doj-muted rounded-lg text-xs font-medium hover:text-doj-text transition-all"
                  >
                    Edit
                  </button>
                )}
              </div>
            )}
          </div>
        </div>
      ))}
    </div>
  );
}

// ─── Record Detail Drawer ────────────────────────────────────────────────────
function RecordDetailDrawer({ sourceId, recordInfo, onClose }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!sourceId) return;
    setLoading(true);
    setData(null);
    setError(null);
    fetch(`/api/cases/${encodeURIComponent(sourceId)}/profile`)
      .then(r => { if (!r.ok) throw new Error(`API ${r.status}`); return r.json(); })
      .then(d => { setData(d); setLoading(false); })
      .catch(e => { setError(e.message); setLoading(false); });
  }, [sourceId]);

  return (
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 bg-black/40 z-40 animate-fade-in"
        onClick={onClose}
      />
      {/* Drawer */}
      <div className="fixed right-0 top-0 h-full w-[480px] max-w-full bg-doj-surface border-l border-doj-border z-50 flex flex-col shadow-2xl animate-slide-in-right">
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-doj-border flex-shrink-0">
          <div>
            <div className="text-xs text-doj-muted uppercase tracking-wider mb-0.5">Full Record</div>
            <div className="font-mono text-sm text-doj-blue">{sourceId}</div>
          </div>
          <button
            onClick={onClose}
            className="p-1.5 rounded-md text-doj-muted hover:text-doj-text hover:bg-doj-surface-2 transition-colors"
          >
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto p-5 space-y-5">
          {loading && (
            <div className="flex items-center gap-3 text-sm text-doj-muted justify-center py-12">
              <div className="w-5 h-5 border-2 border-doj-blue/30 border-t-doj-blue rounded-full animate-spin" />
              Loading record…
            </div>
          )}

          {error && (
            <div className="bg-doj-red/10 border border-doj-red/30 rounded-lg p-4 text-sm text-doj-red">
              Failed to load: {error}
            </div>
          )}

          {data && (
            <>
              {/* Name / identity header */}
              {data.defendant ? (
                <div className="bg-doj-surface-2 border border-doj-border rounded-xl p-4">
                  <div className="text-base font-bold text-doj-text mb-1">
                    {[data.defendant.first_name, data.defendant.middle_name, data.defendant.last_name]
                      .filter(Boolean).join(' ') || 'Unknown Name'}
                  </div>
                  {data.defendant.date_of_birth && (
                    <div className="text-xs text-doj-muted">DOB: {data.defendant.date_of_birth}</div>
                  )}
                  {data.defendant.ssn_last4 && (
                    <div className="text-xs text-doj-muted">SSN (last 4): ***-**-{data.defendant.ssn_last4}</div>
                  )}
                </div>
              ) : recordInfo && (
                <div className="bg-doj-surface-2 border border-doj-border rounded-xl p-4">
                  <div className="text-base font-bold text-doj-text mb-1">{recordInfo.name || 'Unknown Name'}</div>
                  {recordInfo.dob && <div className="text-xs text-doj-muted">DOB: {recordInfo.dob}</div>}
                  <div className="text-xs text-doj-muted mt-1 font-mono">{recordInfo.source_id}</div>
                  <div className="mt-2 text-[10px] px-2 py-1 rounded bg-doj-amber/10 border border-doj-amber/30 text-doj-amber inline-block">
                    Duplicate Detection Record — full profile not yet in target system
                  </div>
                </div>
              )}

              {/* Defendant fields */}
              {data.defendant && (
                <div>
                  <div className="text-[10px] font-semibold uppercase tracking-widest text-doj-muted mb-2">Defendant Details</div>
                  <div className="bg-doj-surface-2 border border-doj-border rounded-xl overflow-hidden">
                    {Object.entries(data.defendant).map(([k, v], i, arr) => (
                      <div
                        key={k}
                        className={`flex gap-3 px-4 py-2.5 ${i < arr.length - 1 ? 'border-b border-doj-border/50' : ''}`}
                      >
                        <span className="text-xs text-doj-muted capitalize w-36 flex-shrink-0">
                          {k.replace(/_/g, ' ')}
                        </span>
                        <span className="text-xs text-doj-text font-mono break-all">
                          {v == null || v === '' ? <span className="text-doj-border italic">—</span> : String(v)}
                        </span>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Cases */}
              {Array.isArray(data.cases) && data.cases.length > 0 && (
                <div>
                  <div className="text-[10px] font-semibold uppercase tracking-widest text-doj-muted mb-2">
                    Cases ({data.cases.length})
                  </div>
                  <div className="space-y-2">
                    {data.cases.map((c, i) => (
                      <div key={i} className="bg-doj-surface-2 border border-doj-border rounded-lg px-4 py-3">
                        <div className="flex items-center justify-between mb-1">
                          <span className="font-mono text-xs text-doj-blue">{c.case_number || c.case_id || `Case #${i + 1}`}</span>
                          {c.case_status && (
                            <span className="text-[10px] text-doj-muted">{c.case_status}</span>
                          )}
                        </div>
                        {c.offense_description && (
                          <p className="text-xs text-doj-text">{c.offense_description}</p>
                        )}
                        {c.filing_date && (
                          <p className="text-[10px] text-doj-muted mt-0.5">Filed: {c.filing_date}</p>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Duplicate group info */}
              {Array.isArray(data.quality_flags) && data.quality_flags.length > 0 && (
                <div>
                  <div className="text-[10px] font-semibold uppercase tracking-widest text-doj-muted mb-2">Duplicate Group Info</div>
                  <div className="bg-doj-surface-2 border border-doj-border rounded-xl overflow-hidden">
                    {data.quality_flags.map((f, i, arr) => {
                      let siblingIds = [];
                      try { siblingIds = JSON.parse(f.all_defendant_ids || '[]'); } catch {}
                      return (
                        <React.Fragment key={i}>
                          {[
                            ['Group ID', f.duplicate_group_id],
                            ['Severity', f.severity],
                            ['Total Records', f.total_records],
                            ['All IDs in Group', siblingIds.join(', ')],
                          ].map(([label, val], j, la) => (
                            <div key={label} className={`flex gap-3 px-4 py-2.5 ${j < la.length - 1 || i < arr.length - 1 ? 'border-b border-doj-border/50' : ''}`}>
                              <span className="text-xs text-doj-muted w-36 flex-shrink-0">{label}</span>
                              <span className="text-xs text-doj-text font-mono break-all">{val || '—'}</span>
                            </div>
                          ))}
                        </React.Fragment>
                      );
                    })}
                  </div>
                </div>
              )}

              {/* Programs */}
              {Array.isArray(data.programs) && data.programs.length > 0 && (
                <div>
                  <div className="text-[10px] font-semibold uppercase tracking-widest text-doj-muted mb-2">
                    Programs ({data.programs.length})
                  </div>
                  <div className="space-y-2">
                    {data.programs.map((p, i) => (
                      <div key={i} className="bg-doj-surface-2 border border-doj-border rounded-lg px-4 py-3">
                        <div className="flex items-center justify-between">
                          <span className="text-xs text-doj-text">{p.program_name || p.program_type || `Program #${i + 1}`}</span>
                          {p.status && (
                            <span className={`text-[10px] px-1.5 py-0.5 rounded border font-medium
                              ${p.status === 'Active' ? 'bg-doj-green/15 border-doj-green/30 text-doj-green' : 'bg-doj-border/20 border-doj-border text-doj-muted'}`}>
                              {p.status}
                            </span>
                          )}
                        </div>
                        {p.enrollment_date && (
                          <p className="text-[10px] text-doj-muted mt-0.5">Enrolled: {p.enrollment_date}</p>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </>
  );
}

// ─── Duplicate Contacts Panel ─────────────────────────────────────────────────
function DuplicatesPanel({ items, setItems, filters }) {
  const [drawerSourceId, setDrawerSourceId] = useState(null);
  const [drawerRecord, setDrawerRecord] = useState(null);

  const submitDecision = async (id, decision) => {
    const item = items.find(it => it.id === id);
    setItems(prev => prev.map(it => it.id === id ? { ...it, status: decision } : it));
    if (item?.issue_key) {
      try {
        await fetch('/api/quality/decision', {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            issue_type: 'duplicate',
            issue_key: item.issue_key,
            decision,
            reviewer: 'Current User',
            note: '',
          }),
        });
      } catch (e) {
        console.error('Failed to persist duplicate decision:', e);
      }
    }
  };

  const filtered = items.filter(item => {
    if (filters.system && item.system !== filters.system) return false;
    if (filters.jobId && !item.job_id.includes(filters.jobId)) return false;
    return true;
  });

  if (filtered.length === 0) return <EmptyState label="duplicate contact" />;

  return (
    <>
      {drawerSourceId && (
        <RecordDetailDrawer sourceId={drawerSourceId} recordInfo={drawerRecord} onClose={() => { setDrawerSourceId(null); setDrawerRecord(null); }} />
      )}
      <div className="space-y-4">
      {filtered.map(group => {
        const scoreColor = group.similarity >= 0.9 ? 'text-doj-red' : group.similarity >= 0.8 ? 'text-doj-amber' : 'text-doj-muted';
        return (
          <div key={group.id} className={`bg-doj-surface-2 border rounded-xl p-4 ${group.status !== 'pending' ? 'border-doj-border/30 opacity-60' : 'border-doj-border'}`}>
            <div className="flex items-center justify-between mb-3">
              <div className="flex items-center gap-3">
                <span className="text-xs font-mono text-doj-muted">{group.id}</span>
                <SystemBadge system={group.system} />
                <span className={`text-xs font-bold font-mono ${scoreColor}`}>
                  {Math.round(group.similarity * 100)}% similar
                </span>
                {group.status !== 'pending' && (
                  <span className="text-xs text-doj-green">{group.status}</span>
                )}
              </div>
            </div>

            {/* Records table */}
            <div className="overflow-x-auto rounded-lg border border-doj-border mb-4">
              <table className="w-full text-xs">
                <thead>
                  <tr className="bg-doj-surface border-b border-doj-border">
                    {['Name', 'Date of Birth', 'System', 'Source ID', ''].map(h => (
                      <th key={h} className="px-3 py-2 text-left font-medium text-doj-muted">{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {group.records.map((rec, i) => (
                    <tr key={rec.id} className={`${i < group.records.length - 1 ? 'border-b border-doj-border/50' : ''} hover:bg-doj-surface/50`}>
                      <td className="px-3 py-2 font-medium text-doj-text">{rec.name}</td>
                      <td className="px-3 py-2 font-mono text-doj-muted">{rec.dob}</td>
                      <td className="px-3 py-2"><SystemBadge system={rec.system} /></td>
                      <td className="px-3 py-2 font-mono text-doj-muted">{rec.source_id}</td>
                      <td className="px-3 py-2">
                        <button
                          onClick={() => { setDrawerSourceId(rec.source_id); setDrawerRecord(rec); }}
                          className="flex items-center gap-1 px-2 py-1 bg-doj-blue/15 border border-doj-blue/40 text-doj-blue rounded text-[11px] font-medium hover:bg-doj-blue/25 transition-all whitespace-nowrap"
                        >
                          <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
                            <path strokeLinecap="round" strokeLinejoin="round" d="M2.036 12.322a1.012 1.012 0 010-.639C3.423 7.51 7.36 4.5 12 4.5c4.638 0 8.573 3.007 9.963 7.178.07.207.07.431 0 .639C20.577 16.49 16.64 19.5 12 19.5c-4.638 0-8.573-3.007-9.963-7.178z" />
                            <path strokeLinecap="round" strokeLinejoin="round" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
                          </svg>
                          View Record
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {/* Actions */}
            {group.status === 'pending' && (
              <div className="flex items-center gap-2">
                <button
                  onClick={() => submitDecision(group.id, 'merged')}
                  className="flex items-center gap-1.5 px-3 py-1.5 bg-doj-green/15 border border-doj-green/40 text-doj-green rounded-lg text-xs font-medium hover:bg-doj-green/25 transition-all"
                >
                  Merge All
                </button>
                <button
                  onClick={() => submitDecision(group.id, 'kept_separate')}
                  className="flex items-center gap-1.5 px-3 py-1.5 bg-doj-surface border border-doj-border text-doj-muted rounded-lg text-xs font-medium hover:text-doj-text transition-all"
                >
                  Keep Separate
                </button>
                <button
                  onClick={() => submitDecision(group.id, 'flagged')}
                  className="flex items-center gap-1.5 px-3 py-1.5 bg-doj-amber/15 border border-doj-amber/40 text-doj-amber rounded-lg text-xs font-medium hover:bg-doj-amber/25 transition-all"
                >
                  Flag for Supervisor
                </button>
              </div>
            )}
          </div>
        );
      })}
      </div>
    </>
  );
}

// ─── Unmapped Codes Panel ─────────────────────────────────────────────────────
function UnmappedCodesPanel({ items, setItems, filters }) {
  const [newValues, setNewValues] = useState({});

  const submitDecision = async (id, decision, newValue) => {
    const item = items.find(it => it.id === id);
    setItems(prev => prev.map(it => it.id === id ? { ...it, status: decision } : it));
    if (item?.issue_key) {
      try {
        await fetch('/api/quality/decision', {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            issue_type: 'unmapped',
            issue_key: item.issue_key,
            decision,
            reviewer: 'Current User',
            note: newValue || '',
          }),
        });
      } catch (e) {
        console.error('Failed to persist unmapped decision:', e);
      }
    }
  };

  const filtered = items.filter(item => {
    if (filters.system && item.system !== filters.system) return false;
    return true;
  });

  if (filtered.length === 0) return <EmptyState label="unmapped code" />;

  return (
    <div className="bg-doj-surface-2 border border-doj-border rounded-xl overflow-hidden">
      <table className="w-full">
        <thead>
          <tr className="bg-doj-surface border-b border-doj-border">
            {['Source Value', 'Field', 'System', 'Lookup Domain', 'Action'].map(h => (
              <th key={h} className="px-4 py-3 text-left text-xs font-semibold text-doj-muted uppercase tracking-wider">{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {filtered.map((item, i) => (
            <tr key={item.id} className={`${i < filtered.length - 1 ? 'border-b border-doj-border/50' : ''} hover:bg-white/2 ${item.status !== 'pending' ? 'opacity-50' : ''}`}>
              <td className="px-4 py-3">
                <span className="font-mono text-sm text-doj-amber bg-doj-amber/10 border border-doj-amber/20 px-2 py-0.5 rounded">
                  {item.source_value}
                </span>
              </td>
              <td className="px-4 py-3">
                <span className="text-sm text-doj-muted font-mono">{item.field}</span>
              </td>
              <td className="px-4 py-3">
                <SystemBadge system={item.system} />
              </td>
              <td className="px-4 py-3">
                <span className="font-mono text-sm text-doj-blue">{item.suggested}</span>
              </td>
              <td className="px-4 py-3">
                {item.status === 'pending' ? (
                  <div className="flex items-center gap-2">
                    <button
                      onClick={() => submitDecision(item.id, 'accepted', item.suggested)}
                      className="px-2 py-1 bg-doj-green/15 border border-doj-green/40 text-doj-green rounded text-xs hover:bg-doj-green/25 transition-all"
                    >
                      Accept
                    </button>
                    <input
                      type="text"
                      placeholder="Map to..."
                      value={newValues[item.id] || ''}
                      onChange={e => setNewValues(prev => ({ ...prev, [item.id]: e.target.value }))}
                      className="bg-doj-surface border border-doj-border rounded px-2 py-1 text-xs text-doj-text font-mono w-32 focus:outline-none focus:border-doj-blue/50"
                    />
                    <button
                      onClick={() => { if (newValues[item.id]) submitDecision(item.id, 'mapped', newValues[item.id]); }}
                      disabled={!newValues[item.id]}
                      className="px-2 py-1 bg-doj-blue/15 border border-doj-blue/40 text-doj-blue rounded text-xs hover:bg-doj-blue/25 transition-all disabled:opacity-40"
                    >
                      Map
                    </button>
                  </div>
                ) : (
                  <span className="text-xs text-doj-green">{item.status}</span>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ─── Schema Drift Panel ───────────────────────────────────────────────────────
function SchemaDriftPanel({ items, setItems, filters }) {
  const submitDecision = async (id, decision) => {
    const item = items.find(it => it.id === id);
    setItems(prev => prev.map(it => it.id === id ? { ...it, status: decision } : it));
    if (item?.issue_key) {
      try {
        await fetch('/api/quality/decision', {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            issue_type: 'drift',
            issue_key: item.issue_key,
            decision,
            reviewer: 'Current User',
            note: '',
          }),
        });
      } catch (e) {
        console.error('Failed to persist drift decision:', e);
      }
    }
  };

  const filtered = items.filter(item => {
    if (filters.system && item.system !== filters.system) return false;
    return true;
  });

  if (filtered.length === 0) return <EmptyState label="schema drift" />;

  return (
    <div className="space-y-3">
      {filtered.map(item => (
        <div
          key={item.id}
          className={`bg-doj-surface-2 border rounded-xl p-4 flex items-center justify-between gap-4
            ${item.status !== 'pending' ? 'border-doj-border/30 opacity-60' : 'border-doj-border'}
          `}
        >
          <div className="flex items-center gap-4 flex-1 min-w-0">
            <div className={`px-2 py-0.5 rounded text-xs font-bold border
              ${item.type === 'added' ? 'bg-doj-blue/15 border-doj-blue/40 text-doj-blue' : 'bg-doj-amber/15 border-doj-amber/40 text-doj-amber'}
            `}>
              {item.type === 'added' ? '+ NEW' : '- MISSING'}
            </div>
            <div>
              <div className="flex items-center gap-2">
                <span className="font-mono text-sm text-doj-text">{item.column}</span>
                <span className="text-xs text-doj-muted font-mono">({item.field_type})</span>
                <SystemBadge system={item.system} />
                <span className="text-xs text-doj-muted font-mono">{item.job_id}</span>
              </div>
              <p className="text-xs text-doj-muted mt-0.5">{item.description}</p>
            </div>
          </div>
          {item.status === 'pending' ? (
            <div className="flex items-center gap-2 flex-shrink-0">
              <button
                onClick={() => submitDecision(item.id, 'mapped')}
                className="px-3 py-1.5 bg-doj-blue/15 border border-doj-blue/40 text-doj-blue rounded-lg text-xs hover:bg-doj-blue/25 transition-all"
              >
                Map Column
              </button>
              <button
                onClick={() => submitDecision(item.id, 'ignored')}
                className="px-3 py-1.5 bg-doj-surface border border-doj-border text-doj-muted rounded-lg text-xs hover:text-doj-text transition-all"
              >
                Ignore
              </button>
              <button
                onClick={() => submitDecision(item.id, 'schema_updated')}
                className="px-3 py-1.5 bg-doj-amber/15 border border-doj-amber/40 text-doj-amber rounded-lg text-xs hover:bg-doj-amber/25 transition-all"
              >
                Update Schema
              </button>
            </div>
          ) : (
            <span className="text-xs text-doj-green flex-shrink-0">{item.status}</span>
          )}
        </div>
      ))}
    </div>
  );
}

// ─── Main Component ───────────────────────────────────────────────────────────
export default function ReconciliationQueue() {
  const [activeTab, setActiveTab] = useState('mappings');
  const [filterSystem, setFilterSystem] = useState('');
  const [filterJobId, setFilterJobId] = useState('');
  const [loading, setLoading] = useState(true);

  const [mappings, setMappings] = useState([]);
  const [duplicates, setDuplicates] = useState([]);
  const [unmapped, setUnmapped] = useState([]);
  const [schemaDrift, setSchemaDrift] = useState([]);

  const fetchIssues = useCallback(async () => {
    setLoading(true);
    try {
      const [lcRes, ucRes, dupRes, sdRes] = await Promise.all([
        fetch('/api/quality/low-confidence'),
        fetch('/api/quality/unmapped-codes'),
        fetch('/api/quality/duplicates'),
        fetch('/api/quality/schema-drift'),
      ]);
      const [lcData, ucData, dupData, sdData] = await Promise.all([
        lcRes.ok ? lcRes.json() : Promise.resolve([]),
        ucRes.ok ? ucRes.json() : Promise.resolve([]),
        dupRes.ok ? dupRes.json() : Promise.resolve([]),
        sdRes.ok ? sdRes.json() : Promise.resolve([]),
      ]);
      if (Array.isArray(lcData) && lcData.length > 0) setMappings(transformMappings(lcData));
      if (Array.isArray(ucData) && ucData.length > 0) setUnmapped(transformUnmapped(ucData));
      if (Array.isArray(dupData) && dupData.length > 0) setDuplicates(transformDuplicates(dupData));
      if (Array.isArray(sdData) && sdData.length > 0) setSchemaDrift(transformSchemaDrift(sdData));
    } catch (e) {
      console.error('Failed to load quality review data:', e);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetchIssues(); }, [fetchIssues]);

  const pendingMappings = mappings.filter(m => m.status === 'pending').length;
  const pendingDuplicates = duplicates.filter(d => d.status === 'pending').length;
  const pendingUnmapped = unmapped.filter(u => u.status === 'pending').length;
  const pendingDrift = schemaDrift.filter(s => s.status === 'pending').length;
  const totalPending = pendingMappings + pendingDuplicates + pendingUnmapped + pendingDrift;

  const TABS = [
    { id: 'mappings', label: 'Low-Confidence Mappings', count: pendingMappings },
    { id: 'duplicates', label: 'Duplicate Contacts', count: pendingDuplicates },
    { id: 'unmapped', label: 'Unmapped Codes', count: pendingUnmapped },
    { id: 'schema', label: 'Schema Drift', count: pendingDrift },
  ];

  const filters = { system: filterSystem, jobId: filterJobId };

  return (
    <div>
      <div className="mb-5 flex items-start justify-between">
        <div>
          <h1 className="text-xl font-bold text-doj-text">Reconciliation Queue</h1>
          <p className="text-sm text-doj-muted mt-0.5">
            {loading
              ? 'Loading issues from silver quarantine tables…'
              : `SME review required — ${totalPending} items pending`}
          </p>
        </div>
        <button
          onClick={fetchIssues}
          disabled={loading}
          className="flex items-center gap-1.5 px-3 py-1.5 bg-doj-surface border border-doj-border text-doj-muted rounded-lg text-xs hover:text-doj-text transition-all disabled:opacity-40"
        >
          <svg className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0l3.181 3.183a8.25 8.25 0 0013.803-3.7M4.031 9.865a8.25 8.25 0 0113.803-3.7l3.181 3.182m0-4.991v4.99" />
          </svg>
          Refresh
        </button>
      </div>

      <div className="bg-doj-surface border border-doj-border rounded-xl overflow-hidden">
        <TabBar tabs={TABS} active={activeTab} onSelect={setActiveTab} />

        {/* Filter bar */}
        <div className="flex items-center gap-3 px-4 py-3 border-b border-doj-border bg-doj-surface-2">
          <select
            value={filterSystem}
            onChange={e => setFilterSystem(e.target.value)}
            className="bg-doj-surface border border-doj-border rounded-lg px-3 py-1.5 text-sm text-doj-text focus:outline-none focus:border-doj-blue/50"
          >
            <option value="">All Systems</option>
            <option value="LegacyCase">LegacyCase</option>
            <option value="OpenJustice">OpenJustice</option>
            <option value="AdHocExports">AdHocExports</option>
          </select>
          <input
            type="text"
            placeholder="Filter by Job ID..."
            value={filterJobId}
            onChange={e => setFilterJobId(e.target.value)}
            className="bg-doj-surface border border-doj-border rounded-lg px-3 py-1.5 text-sm text-doj-text placeholder-doj-muted focus:outline-none focus:border-doj-blue/50 w-44"
          />
          {(filterSystem || filterJobId) && (
            <button
              onClick={() => { setFilterSystem(''); setFilterJobId(''); }}
              className="text-xs text-doj-muted hover:text-doj-red transition-colors"
            >
              Clear filters
            </button>
          )}
        </div>

        <div className="p-4">
          {loading ? (
            <LoadingState />
          ) : (
            <>
              {activeTab === 'mappings' && (
                <MappingsPanel items={mappings} setItems={setMappings} filters={filters} />
              )}
              {activeTab === 'duplicates' && (
                <DuplicatesPanel items={duplicates} setItems={setDuplicates} filters={filters} />
              )}
              {activeTab === 'unmapped' && (
                <UnmappedCodesPanel items={unmapped} setItems={setUnmapped} filters={filters} />
              )}
              {activeTab === 'schema' && (
                <SchemaDriftPanel items={schemaDrift} setItems={setSchemaDrift} filters={filters} />
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
