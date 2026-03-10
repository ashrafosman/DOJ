import React, { useState, useEffect, useCallback } from 'react';

// ─── Severity badge ───────────────────────────────────────────────────────────
function SeverityBadge({ severity }) {
  const map = {
    CRITICAL: 'bg-doj-red/20 text-red-300 border-red-500/40',
    HIGH:     'bg-doj-amber/20 text-amber-300 border-amber-500/40',
    MEDIUM:   'bg-doj-blue/20 text-blue-300 border-blue-500/40',
    LOW:      'bg-doj-surface-2 text-doj-muted border-doj-border',
  };
  const cls = map[severity] || map.LOW;
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-semibold border ${cls}`}>
      {severity}
    </span>
  );
}

// ─── KPI tile ─────────────────────────────────────────────────────────────────
function KpiTile({ label, value, sub, colorClass = 'text-doj-text', icon }) {
  return (
    <div className="bg-doj-surface border border-doj-border rounded-lg p-4 flex flex-col gap-1">
      <div className="flex items-center justify-between mb-1">
        <span className="text-xs text-doj-muted uppercase tracking-widest">{label}</span>
        {icon && <span className="text-doj-muted opacity-60">{icon}</span>}
      </div>
      <div className={`text-2xl font-bold font-mono ${colorClass}`}>{value ?? '—'}</div>
      {sub && <div className="text-xs text-doj-muted">{sub}</div>}
    </div>
  );
}

// ─── Section header ───────────────────────────────────────────────────────────
function SectionHeader({ title, count, description }) {
  return (
    <div className="flex items-start justify-between mb-3">
      <div>
        <h3 className="text-sm font-semibold text-doj-text">{title}</h3>
        {description && <p className="text-xs text-doj-muted mt-0.5">{description}</p>}
      </div>
      {count !== undefined && (
        <span className="text-xs font-mono bg-doj-surface-2 border border-doj-border text-doj-muted px-2 py-0.5 rounded">
          {count} record{count !== 1 ? 's' : ''}
        </span>
      )}
    </div>
  );
}

// ─── Low Confidence Mappings table ───────────────────────────────────────────
function LowConfidenceTable({ rows }) {
  if (!rows.length) return <div className="text-xs text-doj-muted py-4 text-center">No low-confidence mappings found.</div>;
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-doj-border text-doj-muted text-left">
            <th className="pb-2 pr-3 font-semibold">Source</th>
            <th className="pb-2 pr-3 font-semibold">Column</th>
            <th className="pb-2 pr-3 font-semibold">Suggested Target</th>
            <th className="pb-2 pr-3 font-semibold">Confidence</th>
            <th className="pb-2 pr-3 font-semibold">Priority</th>
            <th className="pb-2 font-semibold">Status</th>
          </tr>
        </thead>
        <tbody>
          {rows.slice(0, 20).map((r, i) => {
            const conf = parseFloat(r.mapping_confidence ?? 0);
            const confColor = conf < 0.40 ? 'text-red-400' : conf < 0.55 ? 'text-amber-400' : 'text-doj-text';
            return (
              <tr key={i} className="border-b border-doj-border/40 hover:bg-white/5 transition-colors">
                <td className="py-1.5 pr-3 font-mono text-doj-muted truncate max-w-[120px]" title={r.source_table}>
                  {r.source_system}
                </td>
                <td className="py-1.5 pr-3 font-mono text-doj-text">{r.source_column}</td>
                <td className="py-1.5 pr-3 font-mono text-doj-blue">{r.suggested_target}</td>
                <td className={`py-1.5 pr-3 font-mono font-bold ${confColor}`}>
                  {(conf * 100).toFixed(0)}%
                </td>
                <td className="py-1.5 pr-3"><SeverityBadge severity={r.priority} /></td>
                <td className="py-1.5">
                  <span className={`text-xs px-1.5 py-0.5 rounded border ${
                    r.review_status === 'FLAGGED'
                      ? 'bg-red-900/20 text-red-300 border-red-500/30'
                      : 'bg-doj-surface-2 text-doj-muted border-doj-border'
                  }`}>{r.review_status || 'PENDING'}</span>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
      {rows.length > 20 && (
        <div className="text-xs text-doj-muted text-center pt-2">
          Showing 20 of {rows.length} — filter by source system to narrow results
        </div>
      )}
    </div>
  );
}

// ─── Unmapped Codes table ─────────────────────────────────────────────────────
function UnmappedCodesTable({ rows }) {
  if (!rows.length) return <div className="text-xs text-doj-muted py-4 text-center">No unmapped codes found.</div>;
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-doj-border text-doj-muted text-left">
            <th className="pb-2 pr-3 font-semibold">Table</th>
            <th className="pb-2 pr-3 font-semibold">Field</th>
            <th className="pb-2 pr-3 font-semibold">Unrecognized Value</th>
            <th className="pb-2 pr-3 font-semibold">Lookup Domain</th>
            <th className="pb-2 font-semibold text-right"># Records</th>
          </tr>
        </thead>
        <tbody>
          {rows.slice(0, 25).map((r, i) => (
            <tr key={i} className="border-b border-doj-border/40 hover:bg-white/5">
              <td className="py-1.5 pr-3 font-mono text-doj-muted text-[10px]">{r.source_table}</td>
              <td className="py-1.5 pr-3 font-mono text-doj-blue">{r.field_name}</td>
              <td className="py-1.5 pr-3">
                <code className="bg-doj-surface-2 border border-doj-border px-1.5 py-0.5 rounded text-amber-300">
                  {r.code_value}
                </code>
              </td>
              <td className="py-1.5 pr-3 text-doj-muted">{r.lookup_domain}</td>
              <td className="py-1.5 text-right font-mono text-doj-text font-semibold">
                {parseInt(r.record_count || 0).toLocaleString()}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ─── Duplicates table ─────────────────────────────────────────────────────────
function DuplicatesTable({ rows }) {
  if (!rows.length) return <div className="text-xs text-doj-muted py-4 text-center">No duplicate contacts detected.</div>;
  return (
    <div className="space-y-2">
      {rows.map((r, i) => (
        <div key={i} className="bg-doj-surface-2 border border-doj-border rounded-lg p-3">
          <div className="flex items-start justify-between mb-1">
            <div className="flex items-center gap-2">
              <span className="text-sm font-semibold text-doj-text">
                {r.last_name === 'CLIENT_RECORD'
                  ? `Client Ref: ${r.first_name}`
                  : `${r.last_name}, ${r.first_name}`}
              </span>
              <SeverityBadge severity={r.severity} />
            </div>
            <span className="text-xs text-doj-muted font-mono">{r.total_records} records</span>
          </div>
          <div className="text-xs text-doj-muted">
            DOB: <span className="font-mono text-doj-text">{r.date_of_birth}</span>
            {' · '}
            {r.distinct_defendant_ids} distinct IDs
            {' · '}
            IDs: <span className="font-mono text-doj-blue text-[10px]">
              {Array.isArray(r.all_defendant_ids)
                ? r.all_defendant_ids.slice(0, 4).join(', ')
                : String(r.all_defendant_ids || '').slice(0, 60)}
              {r.total_records > 4 ? '...' : ''}
            </span>
          </div>
        </div>
      ))}
    </div>
  );
}

// ─── Schema Drift table ───────────────────────────────────────────────────────
function SchemaDriftTable({ rows }) {
  if (!rows.length) return <div className="text-xs text-doj-muted py-4 text-center">No schema drift detected.</div>;
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-doj-border text-doj-muted text-left">
            <th className="pb-2 pr-3 font-semibold">Table</th>
            <th className="pb-2 pr-3 font-semibold">Field</th>
            <th className="pb-2 pr-3 font-semibold">Drift Type</th>
            <th className="pb-2 pr-3 font-semibold">Example Value</th>
            <th className="pb-2 font-semibold text-right"># Affected</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i} className="border-b border-doj-border/40 hover:bg-white/5">
              <td className="py-1.5 pr-3 font-mono text-doj-muted text-[10px]">{r.source_table}</td>
              <td className="py-1.5 pr-3 font-mono text-doj-blue">{r.field_name}</td>
              <td className="py-1.5 pr-3">
                <span className="bg-amber-900/20 text-amber-300 border border-amber-500/30 px-1.5 py-0.5 rounded text-[10px]">
                  {r.drift_type}
                </span>
              </td>
              <td className="py-1.5 pr-3">
                <code className="text-doj-muted bg-doj-surface-2 border border-doj-border px-1 py-0.5 rounded text-[10px]">
                  {String(r.example_value || '').slice(0, 30)}
                </code>
              </td>
              <td className="py-1.5 text-right font-mono font-semibold text-amber-300">
                {parseInt(r.record_count || 0).toLocaleString()}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ─── Issue type summary bar ───────────────────────────────────────────────────
function IssueSummaryBar({ byType }) {
  const types = [
    { key: 'LOW_CONFIDENCE_MAPPING', label: 'Low Confidence Mappings', color: '#ef4444' },
    { key: 'UNMAPPED_CODE',          label: 'Unmapped Codes',          color: '#f97316' },
    { key: 'SCHEMA_DRIFT',           label: 'Schema Drift',            color: '#eab308' },
    { key: 'DUPLICATE_CONTACT',      label: 'Duplicate Contacts',      color: '#3b82f6' },
  ];
  const grandTotal = Object.values(byType).reduce((s, v) => s + (v.total || 0), 0) || 1;
  return (
    <div className="space-y-2">
      {types.map(t => {
        const data = byType[t.key] || { total: 0 };
        const pct = ((data.total / grandTotal) * 100).toFixed(1);
        return (
          <div key={t.key} className="flex items-center gap-3">
            <div className="w-36 text-xs text-doj-muted truncate">{t.label}</div>
            <div className="flex-1 h-2 bg-doj-surface-2 rounded-full overflow-hidden">
              <div
                className="h-full rounded-full transition-all duration-500"
                style={{ width: `${pct}%`, backgroundColor: t.color }}
              />
            </div>
            <div className="w-16 text-right font-mono text-xs text-doj-text">
              {data.total?.toLocaleString() || 0}
            </div>
          </div>
        );
      })}
    </div>
  );
}

// ─── Active tab ───────────────────────────────────────────────────────────────
const TABS = [
  { id: 'overview',      label: 'Overview' },
  { id: 'low-conf',      label: 'Low Confidence' },
  { id: 'unmapped',      label: 'Unmapped Codes' },
  { id: 'duplicates',    label: 'Duplicates' },
  { id: 'drift',         label: 'Schema Drift' },
];

// ─── Main component ───────────────────────────────────────────────────────────
export default function DataQualityBoard() {
  const [activeTab, setActiveTab] = useState('overview');
  const [loading, setLoading] = useState(true);
  const [summary, setSummary] = useState(null);
  const [lowConf, setLowConf] = useState([]);
  const [unmapped, setUnmapped] = useState([]);
  const [duplicates, setDuplicates] = useState([]);
  const [drift, setDrift] = useState([]);

  const fetchAll = useCallback(async () => {
    setLoading(true);
    try {
      const [sum, lc, um, dup, dr] = await Promise.all([
        fetch('/api/quality/summary').then(r => r.ok ? r.json() : null),
        fetch('/api/quality/low-confidence?limit=100').then(r => r.ok ? r.json() : []),
        fetch('/api/quality/unmapped-codes?limit=200').then(r => r.ok ? r.json() : []),
        fetch('/api/quality/duplicates?limit=50').then(r => r.ok ? r.json() : []),
        fetch('/api/quality/schema-drift?limit=100').then(r => r.ok ? r.json() : []),
      ]);
      setSummary(sum);
      setLowConf(Array.isArray(lc) ? lc : []);
      setUnmapped(Array.isArray(um) ? um : []);
      setDuplicates(Array.isArray(dup) ? dup : []);
      setDrift(Array.isArray(dr) ? dr : []);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetchAll(); }, [fetchAll]);

  const totals = summary?.totals || {};
  const byType = summary?.by_type || {};

  const totalIssues = totals.total_issues || 0;
  const critical   = totals.critical || 0;
  const high       = totals.high || 0;
  const medium     = totals.medium || 0;

  return (
    <div className="space-y-5">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-bold text-doj-text tracking-tight">Data Quality Board</h2>
          <p className="text-xs text-doj-muted mt-0.5">
            Silver-layer quarantine analysis — low-confidence mappings, unmapped codes, duplicates, schema drift
          </p>
        </div>
        <button
          onClick={fetchAll}
          disabled={loading}
          className="flex items-center gap-1.5 px-3 py-1.5 bg-doj-blue/15 border border-doj-blue/40 text-doj-blue rounded-lg text-xs font-medium hover:bg-doj-blue/25 transition-all disabled:opacity-50"
        >
          <svg className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0l3.181 3.183a8.25 8.25 0 0013.803-3.7M4.031 9.865a8.25 8.25 0 0113.803-3.7l3.181 3.182m0-4.991v4.99" />
          </svg>
          Refresh
        </button>
      </div>

      {/* KPI row */}
      <div className="grid grid-cols-4 gap-3">
        <KpiTile
          label="Total Issues"
          value={loading ? '...' : totalIssues.toLocaleString()}
          sub="across all issue types"
          colorClass={totalIssues > 0 ? 'text-doj-red' : 'text-doj-green'}
        />
        <KpiTile
          label="Critical"
          value={loading ? '...' : critical.toLocaleString()}
          sub="require immediate review"
          colorClass={critical > 0 ? 'text-red-400' : 'text-doj-muted'}
        />
        <KpiTile
          label="High Priority"
          value={loading ? '...' : high.toLocaleString()}
          sub="blocking silver promotion"
          colorClass={high > 0 ? 'text-amber-400' : 'text-doj-muted'}
        />
        <KpiTile
          label="Medium"
          value={loading ? '...' : medium.toLocaleString()}
          sub="flagged for review"
          colorClass="text-doj-blue"
        />
      </div>

      {/* Tab nav */}
      <div className="flex gap-1 border-b border-doj-border">
        {TABS.map(t => (
          <button
            key={t.id}
            onClick={() => setActiveTab(t.id)}
            className={`px-4 py-2 text-xs font-medium border-b-2 transition-all duration-150 -mb-px ${
              activeTab === t.id
                ? 'border-doj-blue text-doj-blue'
                : 'border-transparent text-doj-muted hover:text-doj-text'
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* Tab content */}
      <div className="bg-doj-surface border border-doj-border rounded-lg p-4">

        {/* Overview */}
        {activeTab === 'overview' && (
          <div className="space-y-5">
            <SectionHeader
              title="Issue Distribution by Type"
              description="Proportion of quality issues surfaced from bronze → silver transition"
            />
            <IssueSummaryBar byType={byType} />

            <div className="grid grid-cols-2 gap-4 mt-4">
              <div>
                <div className="text-xs font-semibold text-doj-muted uppercase tracking-wider mb-3">Issue Categories</div>
                <div className="space-y-2">
                  {[
                    { key: 'LOW_CONFIDENCE_MAPPING', label: 'Low Confidence Mappings', icon: '🔍', desc: 'LLM confidence < 65%' },
                    { key: 'UNMAPPED_CODE',          label: 'Unmapped Code Values',    icon: '❓', desc: 'Code not in lookup table' },
                    { key: 'SCHEMA_DRIFT',           label: 'Schema Drift Records',    icon: '⚠️', desc: 'Field format violations' },
                    { key: 'DUPLICATE_CONTACT',      label: 'Duplicate Contacts',      icon: '👥', desc: 'Same name+DOB, different ID' },
                  ].map(item => {
                    const d = byType[item.key] || {};
                    return (
                      <div key={item.key} className="flex items-start gap-3 p-2 rounded bg-doj-surface-2 border border-doj-border">
                        <span className="text-sm">{item.icon}</span>
                        <div className="flex-1 min-w-0">
                          <div className="text-xs font-medium text-doj-text">{item.label}</div>
                          <div className="text-[10px] text-doj-muted">{item.desc}</div>
                        </div>
                        <div className="text-xs font-mono font-bold text-doj-text">
                          {(d.total || 0).toLocaleString()}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
              <div>
                <div className="text-xs font-semibold text-doj-muted uppercase tracking-wider mb-3">What Happens Next</div>
                <div className="space-y-2 text-xs text-doj-muted">
                  {[
                    { step: '1', title: 'Bronze Ingestion', desc: 'Raw data ingested as-is from all source systems — no transformation' },
                    { step: '2', title: 'LLM Schema Mapping', desc: 'GPT-4 maps source columns to canonical target schema with confidence scores' },
                    { step: '3', title: 'Silver Quality Gate', desc: 'Records failing quality checks are quarantined here for SME review' },
                    { step: '4', title: 'SME Resolution', desc: 'Analysts approve/reject/override each issue before gold promotion' },
                    { step: '5', title: 'Gold Promotion', desc: 'Clean, validated records promoted to gold for downstream analytics' },
                  ].map(s => (
                    <div key={s.step} className="flex gap-2">
                      <div className="w-5 h-5 rounded-full bg-doj-blue/20 border border-doj-blue/40 text-doj-blue text-[10px] flex items-center justify-center flex-shrink-0 font-bold mt-0.5">
                        {s.step}
                      </div>
                      <div>
                        <div className="text-doj-text text-xs font-medium">{s.title}</div>
                        <div className="text-[10px] text-doj-muted">{s.desc}</div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Low Confidence */}
        {activeTab === 'low-conf' && (
          <div>
            <SectionHeader
              title="Low-Confidence Schema Mappings"
              count={lowConf.length}
              description="LLM-generated column mappings where confidence score is below 65% — require human validation before silver promotion"
            />
            <LowConfidenceTable rows={lowConf} />
          </div>
        )}

        {/* Unmapped Codes */}
        {activeTab === 'unmapped' && (
          <div>
            <SectionHeader
              title="Unmapped Code Values"
              count={unmapped.length}
              description="Records referencing code values not present in the adhoc_lookup canonical table — may indicate source schema changes or data entry errors"
            />
            <UnmappedCodesTable rows={unmapped} />
          </div>
        )}

        {/* Duplicates */}
        {activeTab === 'duplicates' && (
          <div>
            <SectionHeader
              title="Duplicate Contact Records"
              count={duplicates.length}
              description="Defendants and clients with matching name + date-of-birth fingerprints across different record IDs — likely the same person in multiple source systems"
            />
            <DuplicatesTable rows={duplicates} />
          </div>
        )}

        {/* Schema Drift */}
        {activeTab === 'drift' && (
          <div>
            <SectionHeader
              title="Schema Drift Records"
              count={drift.length}
              description="Records where field values deviate from the expected format — date format changes, type mismatches, or structural anomalies indicating upstream schema evolution"
            />
            <SchemaDriftTable rows={drift} />
          </div>
        )}
      </div>
    </div>
  );
}
