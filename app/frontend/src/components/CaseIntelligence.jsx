import React, { useState, useCallback, useRef, useEffect } from 'react';

// ─── Helpers ─────────────────────────────────────────────────────────────────

function fmt(val) {
  if (val == null || val === '') return '—';
  return String(val);
}

function fmtDate(val) {
  if (!val) return '—';
  const d = new Date(val);
  if (isNaN(d.getTime())) return fmt(val);
  return d.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
}

// Status badge colors
const STATUS_COLORS = {
  ACTIVE:    { bg: 'bg-emerald-500/20', text: 'text-emerald-300', border: 'border-emerald-500/30' },
  CLOSED:    { bg: 'bg-slate-500/20',   text: 'text-slate-300',   border: 'border-slate-500/30' },
  PENDING:   { bg: 'bg-amber-500/20',   text: 'text-amber-300',   border: 'border-amber-500/30' },
  OPEN:      { bg: 'bg-blue-500/20',    text: 'text-blue-300',    border: 'border-blue-500/30' },
  DUPLICATE: { bg: 'bg-orange-500/20',  text: 'text-orange-300',  border: 'border-orange-500/30' },
};

function StatusBadge({ value }) {
  const upper = (value || 'unknown').toUpperCase();
  const c = STATUS_COLORS[upper] || { bg: 'bg-white/10', text: 'text-doj-muted', border: 'border-white/10' };
  return (
    <span className={`inline-flex px-2 py-0.5 rounded text-[10px] font-semibold uppercase tracking-wide border ${c.bg} ${c.text} ${c.border}`}>
      {value || 'Unknown'}
    </span>
  );
}

// Section card wrapper
function Card({ title, accent = '#3b82f6', badge, children }) {
  return (
    <div className="rounded-lg border border-doj-border bg-doj-surface overflow-hidden">
      <div className="px-4 py-3 border-b border-doj-border flex items-center justify-between"
           style={{ borderLeft: `3px solid ${accent}` }}>
        <span className="text-xs font-semibold uppercase tracking-wider text-doj-muted">{title}</span>
        {badge != null && (
          <span className="text-[10px] font-bold text-doj-muted bg-white/5 px-1.5 py-0.5 rounded">{badge}</span>
        )}
      </div>
      <div className="p-4">{children}</div>
    </div>
  );
}

function KeyValue({ label, value }) {
  return (
    <div className="flex justify-between items-start py-1 border-b border-white/5 last:border-0">
      <span className="text-[11px] text-doj-muted w-2/5 flex-shrink-0">{label}</span>
      <span className="text-[12px] text-doj-text text-right break-words">{fmt(value)}</span>
    </div>
  );
}

// ─── Search Bar ───────────────────────────────────────────────────────────────

function SearchBar({ onSelect }) {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [open, setOpen] = useState(false);
  const debounceRef = useRef(null);
  const containerRef = useRef(null);

  useEffect(() => {
    function onClickOutside(e) {
      if (containerRef.current && !containerRef.current.contains(e.target)) {
        setOpen(false);
      }
    }
    document.addEventListener('mousedown', onClickOutside);
    return () => document.removeEventListener('mousedown', onClickOutside);
  }, []);

  const search = useCallback((q) => {
    if (q.trim().length < 2) { setResults([]); setOpen(false); return; }
    setLoading(true);
    fetch(`/api/cases/search?q=${encodeURIComponent(q)}&limit=10`)
      .then(r => r.ok ? r.json() : [])
      .then(rows => { setResults(rows); setOpen(true); })
      .catch(() => setResults([]))
      .finally(() => setLoading(false));
  }, []);

  function handleChange(e) {
    const v = e.target.value;
    setQuery(v);
    clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => search(v), 300);
  }

  function pick(row) {
    const name = [row.FIRST_NAME, row.MIDDLE_INIT, row.LAST_NAME].filter(Boolean).join(' ');
    setQuery(name);
    setOpen(false);
    onSelect(row.DEFENDANT_ID, row);
  }

  return (
    <div ref={containerRef} className="relative w-full max-w-xl">
      <div className="flex items-center gap-2 bg-doj-surface border border-doj-border rounded-lg px-3 py-2 focus-within:border-doj-blue/60 transition-colors">
        <svg className="w-4 h-4 text-doj-muted flex-shrink-0" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z" />
        </svg>
        <input
          className="flex-1 bg-transparent text-sm text-doj-text placeholder-doj-muted outline-none min-w-0"
          placeholder="Search by name or defendant ID…"
          value={query}
          onChange={handleChange}
          onFocus={() => results.length > 0 && setOpen(true)}
          autoComplete="off"
        />
        {loading && (
          <svg className="w-4 h-4 text-doj-muted animate-spin flex-shrink-0" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
          </svg>
        )}
      </div>

      {open && results.length > 0 && (
        <ul className="absolute z-50 w-full mt-1 bg-doj-surface border border-doj-border rounded-lg shadow-2xl overflow-hidden">
          {results.map((row) => {
            const name = [row.FIRST_NAME, row.MIDDLE_INIT, row.LAST_NAME].filter(Boolean).join(' ');
            return (
              <li key={row.DEFENDANT_ID}>
                <button
                  className="w-full px-4 py-2.5 text-left hover:bg-white/5 transition-colors flex items-center justify-between gap-3"
                  onClick={() => pick(row)}
                >
                  <div>
                    <div className="text-sm font-medium text-doj-text">{name || '—'}</div>
                    <div className="text-[11px] text-doj-muted mt-0.5">
                      {row.SOURCE === 'duplicate_contacts'
                        ? `Duplicate group · ${row.CHARGE_DESC}`
                        : `ID: ${row.DEFENDANT_ID} · DOB: ${fmtDate(row.DOB)} · County: ${fmt(row.COUNTY_CD)}`
                      }
                    </div>
                  </div>
                  <StatusBadge value={row.SOURCE === 'duplicate_contacts' ? 'DUPLICATE' : row.CASE_STATUS_CD} />
                </button>
              </li>
            );
          })}
          {results.length === 0 && (
            <li className="px-4 py-3 text-sm text-doj-muted">No results found.</li>
          )}
        </ul>
      )}
    </div>
  );
}

// ─── Profile Header ───────────────────────────────────────────────────────────

function ProfileHeader({ defendant }) {
  if (!defendant) return null;
  const name = [defendant.FIRST_NAME, defendant.MIDDLE_INIT, defendant.LAST_NAME].filter(Boolean).join(' ');
  const initials = [defendant.FIRST_NAME?.[0], defendant.LAST_NAME?.[0]].filter(Boolean).join('');

  return (
    <div className="bg-doj-surface border border-doj-border rounded-lg p-5 flex items-start gap-5">
      {/* Avatar */}
      <div className="w-14 h-14 rounded-full bg-doj-blue/20 border border-doj-blue/40 flex items-center justify-center flex-shrink-0">
        <span className="text-lg font-bold text-doj-blue">{initials || '?'}</span>
      </div>

      {/* Core info */}
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-3 flex-wrap">
          <h2 className="text-lg font-semibold text-doj-text">{name || 'Unknown'}</h2>
          <StatusBadge value={defendant.CASE_STATUS_CD} />
        </div>
        <div className="flex flex-wrap gap-x-5 gap-y-1 mt-1.5 text-xs text-doj-muted">
          <span>ID: <span className="font-mono text-doj-text">{defendant.DEFENDANT_ID}</span></span>
          {defendant.DOB && <span>DOB: {fmtDate(defendant.DOB)}</span>}
          {defendant.GENDER_CD && <span>Gender: {defendant.GENDER_CD}</span>}
          {defendant.RACE_CD && <span>Race: {defendant.RACE_CD}</span>}
          {defendant.COUNTY_CD && <span>County: {defendant.COUNTY_CD}</span>}
        </div>
        {defendant.CHARGE_DESC && (
          <div className="mt-2 text-xs text-amber-300/90">
            Current Charge: <span className="font-medium">{defendant.CHARGE_DESC}</span>
          </div>
        )}
      </div>

      {/* Source badge */}
      {defendant.SOURCE === 'duplicate_contacts' ? (
        <div className="text-[10px] font-semibold uppercase tracking-wider text-orange-300 bg-orange-500/10 border border-orange-500/20 px-2 py-1 rounded flex-shrink-0">
          Duplicate Group
        </div>
      ) : (
        <div className="text-[10px] font-semibold uppercase tracking-wider text-purple-300 bg-purple-500/10 border border-purple-500/20 px-2 py-1 rounded flex-shrink-0">
          LegacyCase
        </div>
      )}
    </div>
  );
}

// ─── Cases Timeline ───────────────────────────────────────────────────────────

function CasesSection({ cases }) {
  if (!cases.length) {
    return <p className="text-sm text-doj-muted">No case records found.</p>;
  }
  return (
    <div className="space-y-2">
      {cases.map((c, i) => (
        <div key={c.CaseID || i} className="bg-white/3 rounded p-3 border border-doj-border/60">
          <div className="flex items-center justify-between mb-1">
            <span className="text-xs font-semibold text-doj-text font-mono">{c.CaseID || `Case ${i + 1}`}</span>
            <StatusBadge value={c.STATUS} />
          </div>
          <div className="grid grid-cols-2 gap-x-4 text-[11px] text-doj-muted">
            {c.CASE_TYPE  && <span>Type: {c.CASE_TYPE}</span>}
            {c.FILING_DATE && <span>Filed: {fmtDate(c.FILING_DATE)}</span>}
            {c.COURT_ID   && <span>Court: {c.COURT_ID}</span>}
            {c.JUDGE_ID   && <span>Judge: {c.JUDGE_ID}</span>}
          </div>
        </div>
      ))}
    </div>
  );
}

// ─── Events Timeline ─────────────────────────────────────────────────────────

function EventsSection({ events }) {
  if (!events.length) {
    return <p className="text-sm text-doj-muted">No event records found.</p>;
  }
  return (
    <div className="relative">
      <div className="absolute left-3 top-2 bottom-2 w-px bg-doj-border" />
      <div className="space-y-3">
        {events.map((ev, i) => {
          // Extract date / type from whatever columns exist
          const dateVal = ev.EVENT_DATE || ev.event_date || ev.EVENT_DT || ev.CREATED_AT;
          const typeVal = ev.EVENT_TYPE || ev.event_type || ev.DESCRIPTION || ev.TYPE;
          const noteVal = ev.NOTES || ev.DESCRIPTION || ev.notes || '';
          return (
            <div key={i} className="flex gap-4 pl-2">
              <div className="w-3 h-3 rounded-full bg-doj-blue/80 border-2 border-doj-bg flex-shrink-0 mt-0.5 relative z-10" />
              <div className="flex-1">
                <div className="text-xs font-medium text-doj-text">{fmt(typeVal)}</div>
                <div className="text-[11px] text-doj-muted">{fmtDate(dateVal)}</div>
                {noteVal && <div className="text-[11px] text-doj-muted/80 mt-0.5">{noteVal}</div>}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ─── Programs / AdHocExports ──────────────────────────────────────────────────

function ProgramsSection({ programs }) {
  if (!programs.length) {
    return <p className="text-sm text-doj-muted">No program enrollment records found.</p>;
  }
  return (
    <div className="space-y-2">
      {programs.map((p, i) => (
        <div key={p.ClientID || i} className="bg-white/3 rounded p-3 border border-doj-border/60">
          <div className="flex items-center justify-between mb-1">
            <span className="text-xs font-semibold text-doj-text">{fmt(p.PROGRAM)}</span>
            <StatusBadge value={p.STATUS} />
          </div>
          <div className="grid grid-cols-2 gap-x-4 text-[11px] text-doj-muted">
            {p.COUNTY          && <span>County: {p.COUNTY}</span>}
            {p.RISK_LEVEL      && <span>Risk: {p.RISK_LEVEL}</span>}
            {p.ENROLLMENT_DATE && <span>Enrolled: {fmtDate(p.ENROLLMENT_DATE)}</span>}
            {p.EXIT_DATE       && <span>Exit: {fmtDate(p.EXIT_DATE)}</span>}
          </div>
        </div>
      ))}
    </div>
  );
}

// ─── OpenJustice Context ──────────────────────────────────────────────────────

function OJContextSection({ ojContext }) {
  if (!ojContext.length) {
    return <p className="text-sm text-doj-muted">No statewide arrest data available for this charge category.</p>;
  }
  // Group by year for a simple summary table
  const byYear = ojContext.slice(0, 5);
  return (
    <div>
      <p className="text-[11px] text-doj-muted mb-3">
        Statewide arrest statistics matching this charge category (OpenJustice aggregate data).
      </p>
      <table className="w-full text-xs border-collapse">
        <thead>
          <tr className="text-[10px] text-doj-muted uppercase tracking-wider border-b border-doj-border">
            <th className="text-left py-1 font-semibold">Year</th>
            <th className="text-left py-1 font-semibold">Category</th>
            <th className="text-right py-1 font-semibold">Total Arrests</th>
            <th className="text-right py-1 font-semibold">Felony</th>
          </tr>
        </thead>
        <tbody>
          {byYear.map((row, i) => (
            <tr key={i} className="border-b border-doj-border/40 hover:bg-white/3">
              <td className="py-1.5 text-doj-text">{fmt(row.YEAR)}</td>
              <td className="py-1.5 text-doj-muted truncate max-w-[140px]">{fmt(row.CHARGE_CATEGORY)}</td>
              <td className="py-1.5 text-right text-doj-text">{Number(row.TOTAL_ARRESTS || 0).toLocaleString()}</td>
              <td className="py-1.5 text-right text-doj-muted">{Number(row.FELONY_ARRESTS || 0).toLocaleString()}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ─── Quality Flags ────────────────────────────────────────────────────────────

function QualityFlagsSection({ flags, onSelectDefendant }) {
  const [expanded, setExpanded] = useState({});

  if (!flags.length) {
    return (
      <div className="flex items-center gap-2 text-sm text-emerald-400">
        <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" d="M9 12.75L11.25 15 15 9.75M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
        No duplicate or data quality issues detected.
      </div>
    );
  }

  const SEV_COLORS = {
    CRITICAL: { card: 'border-red-500/30 bg-red-500/10',    header: 'text-red-400',    badge: 'bg-red-500/20 text-red-300' },
    HIGH:     { card: 'border-orange-500/30 bg-orange-500/10', header: 'text-orange-400', badge: 'bg-orange-500/20 text-orange-300' },
    MEDIUM:   { card: 'border-amber-500/30 bg-amber-500/10', header: 'text-amber-400',  badge: 'bg-amber-500/20 text-amber-300' },
  };

  return (
    <div className="space-y-3">
      {flags.map((f, i) => {
        const c = SEV_COLORS[(f.severity || '').toUpperCase()] || { card: 'border-doj-border bg-white/5', header: 'text-doj-muted', badge: 'bg-white/10 text-doj-muted' };
        const ids = Array.isArray(f.all_defendant_ids) ? f.all_defendant_ids : [];
        const isOpen = !!expanded[i];

        return (
          <div key={i} className={`rounded-lg border text-xs ${c.card}`}>
            {/* Header row */}
            <div className={`flex items-center justify-between px-3 py-2.5 ${c.header}`}>
              <div className="flex items-center gap-2">
                <svg className="w-3.5 h-3.5 flex-shrink-0" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 17.25v3.375c0 .621-.504 1.125-1.125 1.125h-9.75a1.125 1.125 0 01-1.125-1.125V7.875c0-.621.504-1.125 1.125-1.125H6.75a9.06 9.06 0 011.5.124m7.5 10.376h3.375c.621 0 1.125-.504 1.125-1.125V11.25c0-4.46-3.243-8.161-7.5-8.876a9.06 9.06 0 00-1.5-.124H9.375c-.621 0-1.125.504-1.125 1.125v3.5m7.5 10.375H9.375a1.125 1.125 0 01-1.125-1.125v-9.25m12 6.625v-1.875a3.375 3.375 0 00-3.375-3.375h-1.5a1.125 1.125 0 01-1.125-1.125v-1.5a3.375 3.375 0 00-3.375-3.375H9.75" />
                </svg>
                <span className="font-semibold">Duplicate Group: <span className="font-mono">{f.duplicate_group_id}</span></span>
              </div>
              <div className="flex items-center gap-2">
                <span className={`text-[10px] font-bold uppercase tracking-wider px-1.5 py-0.5 rounded ${c.badge}`}>{f.severity}</span>
                <span className="text-doj-muted">{f.total_records ?? ids.length} records</span>
                {ids.length > 0 && (
                  <button
                    onClick={() => setExpanded(e => ({ ...e, [i]: !e[i] }))}
                    className="flex items-center gap-1 text-[11px] text-doj-muted hover:text-doj-text transition-colors ml-1"
                  >
                    <svg className={`w-3 h-3 transition-transform ${isOpen ? 'rotate-90' : ''}`} fill="none" viewBox="0 0 24 24" strokeWidth={2.5} stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
                    </svg>
                    {isOpen ? 'Hide' : 'Show'} records
                  </button>
                )}
              </div>
            </div>

            {/* Expanded record list */}
            {isOpen && ids.length > 0 && (
              <div className="border-t border-inherit px-3 py-2 space-y-1">
                <div className="text-[10px] text-doj-muted uppercase tracking-wider mb-2 font-semibold">
                  Duplicate Records in Group
                </div>
                {ids.map((defId, j) => (
                  <button
                    key={j}
                    onClick={() => onSelectDefendant && onSelectDefendant(defId)}
                    className="w-full flex items-center justify-between px-2.5 py-1.5 rounded bg-black/20 hover:bg-black/30 transition-colors group text-left"
                  >
                    <div className="flex items-center gap-2">
                      <span className="text-[10px] text-doj-muted w-4 text-right">{j + 1}.</span>
                      <span className="font-mono text-[11px] text-doj-text">{defId}</span>
                    </div>
                    <span className="text-[10px] text-doj-muted group-hover:text-doj-blue transition-colors flex items-center gap-1">
                      View profile
                      <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
                        <path strokeLinecap="round" strokeLinejoin="round" d="M13.5 4.5L21 12m0 0l-7.5 7.5M21 12H3" />
                      </svg>
                    </span>
                  </button>
                ))}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

// ─── Raw Details expander ─────────────────────────────────────────────────────

function RawDetails({ title, data }) {
  const [open, setOpen] = useState(false);
  if (!data || (Array.isArray(data) ? data.length === 0 : Object.keys(data).length === 0)) return null;
  return (
    <div className="mt-2">
      <button
        className="text-[11px] text-doj-muted hover:text-doj-text flex items-center gap-1 transition-colors"
        onClick={() => setOpen(o => !o)}
      >
        <svg className={`w-3 h-3 transition-transform ${open ? 'rotate-90' : ''}`} fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
        </svg>
        {open ? 'Hide' : 'Show'} {title}
      </button>
      {open && (
        <pre className="mt-2 bg-black/30 rounded p-3 text-[10px] text-doj-muted overflow-auto max-h-48 font-mono whitespace-pre-wrap">
          {JSON.stringify(data, null, 2)}
        </pre>
      )}
    </div>
  );
}

// ─── Main Component ───────────────────────────────────────────────────────────

export default function CaseIntelligence() {
  const [profile, setProfile] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [selectedId, setSelectedId] = useState(null);

  async function loadProfile(defendantId) {
    setSelectedId(defendantId);
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`/api/cases/${encodeURIComponent(defendantId)}/profile`);
      if (!res.ok) throw new Error(`API returned ${res.status}`);
      const data = await res.json();
      setProfile(data);
    } catch (e) {
      setError(e.message);
      setProfile(null);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="space-y-6">
      {/* Page title */}
      <div>
        <h1 className="text-xl font-bold text-doj-text">Case Intelligence</h1>
        <p className="text-sm text-doj-muted mt-1">
          Search defendants to view a 360° profile across LegacyCase, AdHocExports, and statewide OpenJustice data.
        </p>
      </div>

      {/* Search bar */}
      <SearchBar onSelect={(id) => loadProfile(id)} />

      {/* Loading */}
      {loading && (
        <div className="flex items-center gap-3 text-sm text-doj-muted py-8 justify-center">
          <svg className="w-5 h-5 animate-spin" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
          </svg>
          Loading profile for <code className="font-mono text-doj-text">{selectedId}</code>…
        </div>
      )}

      {/* Error */}
      {error && (
        <div className="bg-red-500/10 border border-red-500/30 rounded-lg p-4 text-sm text-red-400">
          Failed to load profile: {error}
        </div>
      )}

      {/* Profile */}
      {!loading && profile && (
        <div className="space-y-5">
          {/* Header */}
          <ProfileHeader defendant={profile.defendant} />

          {/* 3-column grid: LegacyCase | Programs | Quality */}
          <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
            {/* LegacyCase Defendant Details */}
            <Card title="Defendant Details" accent="#8b5cf6">
              {profile.defendant ? (
                <div>
                  {Object.entries(profile.defendant).map(([k, v]) => (
                    <KeyValue key={k} label={k.replace(/_/g, ' ')} value={v} />
                  ))}
                </div>
              ) : (
                <p className="text-sm text-doj-muted">No defendant record found.</p>
              )}
            </Card>

            {/* AdHocExports – Programs */}
            <Card title="Programs & Diversion" accent="#f97316" badge={`${profile.programs.length} records`}>
              <ProgramsSection programs={profile.programs} />
              <RawDetails title="raw fields" data={profile.programs} />
            </Card>

            {/* Data Quality */}
            <Card title="Data Quality Flags" accent="#ef4444">
              <QualityFlagsSection flags={profile.quality_flags} onSelectDefendant={(id) => loadProfile(id)} />
            </Card>
          </div>

          {/* Cases */}
          <Card title="Cases" accent="#06b6d4" badge={`${profile.cases.length} case${profile.cases.length !== 1 ? 's' : ''}`}>
            <CasesSection cases={profile.cases} />
          </Card>

          {/* Events timeline */}
          <Card title="Event Timeline" accent="#8b5cf6" badge={`${profile.events.length} events`}>
            <EventsSection events={profile.events} />
          </Card>

          {/* OpenJustice context */}
          <Card title="OpenJustice – Statewide Context" accent="#06b6d4">
            <OJContextSection ojContext={profile.oj_context} />
          </Card>
        </div>
      )}

      {/* Empty state */}
      {!loading && !profile && !error && (
        <div className="flex flex-col items-center justify-center py-20 text-center gap-3">
          <svg className="w-12 h-12 text-doj-muted/40" fill="none" viewBox="0 0 24 24" strokeWidth={1} stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 6a3.75 3.75 0 11-7.5 0 3.75 3.75 0 017.5 0zM4.501 20.118a7.5 7.5 0 0114.998 0A17.933 17.933 0 0112 21.75c-2.676 0-5.216-.584-7.499-1.632z" />
          </svg>
          <div className="text-sm text-doj-muted">Search for a defendant above to view their 360° profile</div>
          <div className="text-[11px] text-doj-muted/60">Searches across LegacyCase, AdHocExports, and OpenJustice data sources</div>
        </div>
      )}
    </div>
  );
}
