import React, { useEffect, useRef, useState } from 'react';

// ─── Scroll-reveal hook ───────────────────────────────────────────────────────
function useReveal(threshold = 0.15) {
  const ref = useRef(null);
  const [visible, setVisible] = useState(false);
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const obs = new IntersectionObserver(([e]) => { if (e.isIntersecting) { setVisible(true); obs.disconnect(); } }, { threshold });
    obs.observe(el);
    return () => obs.disconnect();
  }, [threshold]);
  return [ref, visible];
}

// ─── Animated number counter ─────────────────────────────────────────────────
function Counter({ to, suffix = '', duration = 1200 }) {
  const [val, setVal] = useState(0);
  const [ref, visible] = useReveal(0.5);
  useEffect(() => {
    if (!visible) return;
    let start = null;
    const step = (ts) => {
      if (!start) start = ts;
      const prog = Math.min((ts - start) / duration, 1);
      setVal(Math.floor(prog * to));
      if (prog < 1) requestAnimationFrame(step);
    };
    requestAnimationFrame(step);
  }, [visible, to, duration]);
  return <span ref={ref}>{val.toLocaleString()}{suffix}</span>;
}

// ─── Reveal wrapper ───────────────────────────────────────────────────────────
function Reveal({ children, delay = 0, className = '' }) {
  const [ref, visible] = useReveal();
  return (
    <div
      ref={ref}
      className={className}
      style={{
        transition: `opacity 0.6s ease ${delay}ms, transform 0.6s ease ${delay}ms`,
        opacity: visible ? 1 : 0,
        transform: visible ? 'translateY(0)' : 'translateY(24px)',
      }}
    >
      {children}
    </div>
  );
}

// ─── Pipeline flow animation ──────────────────────────────────────────────────
const PIPELINE_STAGES = [
  { label: 'Sources',  sub: '3 systems',        color: '#f97316', icon: '⬡' },
  { label: 'Bronze',   sub: 'Raw ingest',        color: '#cd7f32', icon: '◈' },
  { label: 'Silver',   sub: 'Cleansed',          color: '#94a3b8', icon: '◈' },
  { label: 'Gold',     sub: 'Analytical',        color: '#eab308', icon: '◈' },
  { label: 'Staging',  sub: 'Target schema',     color: '#22c55e', icon: '◈' },
  { label: 'Complete', sub: 'Migrated',          color: '#3b82f6', icon: '✓' },
];

function PipelineFlow() {
  const [ref, visible] = useReveal(0.2);
  const [active, setActive] = useState(-1);

  useEffect(() => {
    if (!visible) return;
    let i = 0;
    const t = setInterval(() => {
      setActive(prev => {
        if (prev >= PIPELINE_STAGES.length - 1) { clearInterval(t); return prev; }
        return prev + 1;
      });
      i++;
      if (i >= PIPELINE_STAGES.length) clearInterval(t);
    }, 220);
    return () => clearInterval(t);
  }, [visible]);

  return (
    <div ref={ref} className="flex items-center gap-0 flex-wrap justify-center">
      {PIPELINE_STAGES.map((s, i) => (
        <React.Fragment key={s.label}>
          <div
            className="flex flex-col items-center gap-1"
            style={{
              transition: `opacity 0.4s ease ${i * 120}ms, transform 0.4s ease ${i * 120}ms`,
              opacity: active >= i ? 1 : 0.15,
              transform: active >= i ? 'scale(1)' : 'scale(0.85)',
            }}
          >
            <div
              className="w-16 h-16 rounded-xl flex flex-col items-center justify-center border-2 relative overflow-hidden"
              style={{ borderColor: s.color, background: `${s.color}18` }}
            >
              {active >= i && (
                <div
                  className="absolute inset-0 rounded-xl"
                  style={{
                    boxShadow: `0 0 18px ${s.color}60`,
                    animation: 'pulse-glow 2s ease-in-out infinite',
                  }}
                />
              )}
              <span className="text-xl relative z-10" style={{ color: s.color }}>{s.icon}</span>
            </div>
            <span className="text-xs font-semibold text-doj-text">{s.label}</span>
            <span className="text-[10px] text-doj-muted">{s.sub}</span>
          </div>
          {i < PIPELINE_STAGES.length - 1 && (
            <div className="flex items-center mx-1 mb-6">
              <div
                className="h-0.5 w-8 transition-all duration-500"
                style={{
                  background: active > i ? `linear-gradient(90deg, ${s.color}, ${PIPELINE_STAGES[i+1].color})` : '#2d3748',
                  transitionDelay: `${i * 120 + 100}ms`,
                }}
              />
              <svg width="8" height="8" viewBox="0 0 8 8" style={{ color: active > i ? PIPELINE_STAGES[i+1].color : '#2d3748', transition: 'color 0.5s ease', transitionDelay: `${i * 120 + 150}ms` }}>
                <path d="M0 4 L8 4 M4 0 L8 4 L4 8" stroke="currentColor" strokeWidth="1.5" fill="none"/>
              </svg>
            </div>
          )}
        </React.Fragment>
      ))}
    </div>
  );
}

// ─── Data source card ─────────────────────────────────────────────────────────
function SourceCard({ name, color, hex, icon, tables, description, delay }) {
  return (
    <Reveal delay={delay}>
      <div
        className="rounded-xl border p-5 h-full"
        style={{ borderColor: `${hex}40`, background: `${hex}0d` }}
      >
        <div className="flex items-center gap-3 mb-3">
          <div className="w-9 h-9 rounded-lg flex items-center justify-center text-lg" style={{ background: `${hex}20`, border: `1px solid ${hex}40` }}>
            {icon}
          </div>
          <div>
            <div className="font-semibold text-sm" style={{ color: hex }}>{name}</div>
            <div className="text-[10px] text-doj-muted">{description}</div>
          </div>
        </div>
        <div className="space-y-1.5">
          {tables.map(t => (
            <div key={t.name} className="flex items-start gap-2">
              <span className="text-[10px] font-mono mt-0.5 px-1.5 py-0.5 rounded" style={{ background: `${hex}20`, color: hex }}>{t.name}</span>
              <span className="text-[11px] text-doj-muted leading-tight">{t.desc}</span>
            </div>
          ))}
        </div>
      </div>
    </Reveal>
  );
}

// ─── Process step ─────────────────────────────────────────────────────────────
function ProcessStep({ number, title, color, children, delay }) {
  return (
    <Reveal delay={delay}>
      <div className="flex gap-4">
        <div className="flex-shrink-0 flex flex-col items-center">
          <div
            className="w-8 h-8 rounded-full flex items-center justify-center text-sm font-bold border-2"
            style={{ borderColor: color, color, background: `${color}15` }}
          >
            {number}
          </div>
          <div className="w-px flex-1 mt-2" style={{ background: `${color}30` }} />
        </div>
        <div className="pb-6">
          <div className="font-semibold text-sm mb-2" style={{ color }}>{title}</div>
          <div className="text-[13px] text-doj-muted leading-relaxed">{children}</div>
        </div>
      </div>
    </Reveal>
  );
}

// ─── Page guide card ──────────────────────────────────────────────────────────
function PageCard({ icon, label, path, description, color, delay }) {
  return (
    <Reveal delay={delay}>
      <div className="rounded-xl border border-doj-border bg-doj-surface p-4 hover:border-doj-blue/40 transition-colors group">
        <div className="flex items-center gap-2 mb-2">
          <div
            className="w-8 h-8 rounded-lg flex items-center justify-center text-base flex-shrink-0"
            style={{ background: `${color}20`, border: `1px solid ${color}40` }}
          >
            {icon}
          </div>
          <div>
            <div className="text-sm font-semibold text-doj-text">{label}</div>
            <div className="text-[10px] font-mono text-doj-muted">{path}</div>
          </div>
        </div>
        <p className="text-[12px] text-doj-muted leading-relaxed">{description}</p>
      </div>
    </Reveal>
  );
}

// ─── Tag pill ─────────────────────────────────────────────────────────────────
function Tag({ label, color }) {
  return (
    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[11px] font-mono border" style={{ color, borderColor: `${color}50`, background: `${color}12` }}>
      <span>🏷</span> {label}
    </span>
  );
}

// ─── Section heading ──────────────────────────────────────────────────────────
function SectionHeading({ icon, title, subtitle }) {
  return (
    <div className="mb-6">
      <div className="flex items-center gap-2 mb-1">
        <span className="text-xl">{icon}</span>
        <h2 className="text-lg font-bold text-doj-text">{title}</h2>
      </div>
      {subtitle && <p className="text-[13px] text-doj-muted ml-8">{subtitle}</p>}
      <div className="ml-8 mt-2 h-px bg-gradient-to-r from-doj-blue/40 to-transparent" />
    </div>
  );
}

// ─── Main About component ─────────────────────────────────────────────────────
export default function About() {
  return (
    <div className="max-w-5xl mx-auto space-y-16 pb-16">

      {/* ── Hero ── */}
      <Reveal>
        <div className="text-center pt-4 pb-2">
          <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full border border-doj-blue/30 bg-doj-blue/10 text-doj-blue text-xs font-semibold mb-4">
            Oregon Department of Justice
          </div>
          <h1 className="text-3xl font-bold text-doj-text mb-3">
            Data Migration Monitor
          </h1>
          <p className="text-doj-muted text-sm max-w-2xl mx-auto leading-relaxed">
            A unified platform for orchestrating, observing, and validating the migration of criminal justice records
            from three legacy source systems into a consolidated Databricks Lakehouse.
          </p>
          <div className="flex justify-center gap-8 mt-6">
            {[
              { label: 'Source Tables', value: 9 },
              { label: 'Pipeline Stages', value: 6 },
              { label: 'PII Columns Tagged', value: 5 },
              { label: 'Quality Checks', value: 14 },
            ].map(s => (
              <div key={s.label} className="text-center">
                <div className="text-2xl font-bold text-doj-blue">
                  <Counter to={s.value} />
                </div>
                <div className="text-[11px] text-doj-muted">{s.label}</div>
              </div>
            ))}
          </div>
        </div>
      </Reveal>

      {/* ── Architecture flow ── */}
      <section>
        <Reveal>
          <SectionHeading
            icon="⚙️"
            title="Solution Architecture"
            subtitle="A Databricks Medallion (Bronze → Silver → Gold) pipeline ingesting three source systems into a unified staging schema."
          />
        </Reveal>

        <div className="bg-doj-surface border border-doj-border rounded-xl p-6 mb-6">
          <PipelineFlow />
        </div>

        <div className="grid grid-cols-3 gap-4 text-[12px]">
          {[
            { layer: 'Bronze', color: '#cd7f32', desc: 'Raw data landed exactly as received. No transformations. Metadata columns (_ingest_timestamp, _source_system, _source_file) added. PII columns tagged via Unity Catalog.' },
            { layer: 'Silver', color: '#94a3b8', desc: 'Column names standardised to snake_case. Type casting, null handling, deduplication. Entity resolution links records across systems. Quality flags computed.' },
            { layer: 'Gold', color: '#eab308', desc: 'Business-ready aggregates: resident 360 view, case timelines, disposition summaries, defendant cross-reference. Feeds the Case Intelligence search.' },
          ].map((l, i) => (
            <Reveal key={l.layer} delay={i * 100}>
              <div className="rounded-lg border p-4" style={{ borderColor: `${l.color}40`, background: `${l.color}0a` }}>
                <div className="font-semibold mb-1" style={{ color: l.color }}>{l.layer} Layer</div>
                <p className="text-doj-muted leading-relaxed">{l.desc}</p>
              </div>
            </Reveal>
          ))}
        </div>
      </section>

      {/* ── Data sources ── */}
      <section>
        <Reveal>
          <SectionHeading
            icon="🗄️"
            title="Data Sources"
            subtitle="Three source systems with different formats, schemas, and update cadences unified into one catalog."
          />
        </Reveal>

        <div className="grid grid-cols-3 gap-4">
          <SourceCard
            name="LegacyCase"
            hex="#8b5cf6"
            icon="🏛️"
            description="SQL Server · JDBC ingest"
            delay={0}
            tables={[
              { name: 'tbl_defendant', desc: 'Defendant demographics, charges, disposition' },
              { name: 'tbl_case',      desc: 'Case filings, court assignments, status' },
              { name: 'tbl_event',     desc: 'Hearing events, continuances, notes' },
            ]}
          />
          <SourceCard
            name="OpenJustice"
            hex="#06b6d4"
            icon="📊"
            description="Oregon DOJ open data · HTTP CSV"
            delay={100}
            tables={[
              { name: 'arrests',            desc: 'Statewide arrest statistics by charge category' },
              { name: 'arrest_dispositions',desc: 'Disposition outcomes by agency and year' },
              { name: 'crimes_clearances',  desc: 'Crime clearance rates by county and type' },
            ]}
          />
          <SourceCard
            name="AdHocExports"
            hex="#f97316"
            icon="📁"
            description="ADLS XLSX uploads · Manual drops"
            delay={200}
            tables={[
              { name: 'adhoc_client',   desc: 'Program enrollment records and risk scores' },
              { name: 'adhoc_incident', desc: 'Incident reports with victim metadata' },
              { name: 'adhoc_lookup',   desc: 'Reference / code table values' },
            ]}
          />
        </div>
      </section>

      {/* ── Mapping process ── */}
      <section>
        <Reveal>
          <SectionHeading
            icon="🔀"
            title="Schema Mapping Process"
            subtitle="Notebook 02b uses an LLM to propose source-to-target column mappings, reviewed and approved by SMEs before promotion."
          />
        </Reveal>

        <div className="grid grid-cols-2 gap-8">
          <div className="space-y-0">
            <ProcessStep number="1" title="Metadata Profiling (02a)" color="#3b82f6" delay={0}>
              Every bronze column is profiled: null rate, cardinality, uniqueness ratio, top-10 values, min/max,
              and a regex-based pattern classifier (SSN, DATE_ISO, PHONE, EMAIL, etc.). Results are written
              to <code className="text-doj-blue text-[11px]">bronze.column_profiles</code>.
            </ProcessStep>
            <ProcessStep number="2" title="LLM Schema Mapping (02b)" color="#8b5cf6" delay={100}>
              Column profiles are fed as context into a Databricks Foundation Model API prompt.
              The LLM proposes the best-matching target staging column (e.g. <em>DOB → Stg_Contact.DateOfBirth</em>)
              with a confidence score (0–1) and a reasoning note. Results are stored as a mapping table.
            </ProcessStep>
            <ProcessStep number="3" title="SME Review (03)" color="#f97316" delay={200}>
              Data stewards review every low-confidence or flagged mapping in the SME Review Widget.
              They can approve, override, or reject each proposed mapping before the pipeline advances.
            </ProcessStep>
            <ProcessStep number="4" title="Silver Transform (04)" color="#22c55e" delay={300}>
              Approved mappings are applied to produce silver tables with canonical column names and types.
              Rejected columns are nulled; overrides are applied verbatim.
            </ProcessStep>
          </div>

          <Reveal delay={150}>
            <div className="bg-doj-surface border border-doj-border rounded-xl p-5 h-full">
              <div className="text-[11px] font-semibold text-doj-muted uppercase tracking-widest mb-3">Example Mapping Output</div>
              <div className="space-y-2">
                {[
                  { src: 'tbl_defendant.DOB',        tgt: 'Stg_Contact.DateOfBirth',  conf: 0.97, status: 'approved' },
                  { src: 'tbl_defendant.FIRST_NAME',  tgt: 'Stg_Contact.FirstName',    conf: 0.99, status: 'approved' },
                  { src: 'tbl_case.STATUS',           tgt: 'Stg_Case.StatusCode',      conf: 0.95, status: 'approved' },
                  { src: 'tbl_defendant.CHARGE_DESC', tgt: 'NO_MATCH',                 conf: 0.45, status: 'review' },
                  { src: 'adhoc_client.RISK_LEVEL',   tgt: 'NO_MATCH',                 conf: 0.38, status: 'review' },
                ].map(m => (
                  <div key={m.src} className="flex items-center gap-2 text-[11px]">
                    <span className="font-mono text-purple-300 w-44 truncate">{m.src}</span>
                    <span className="text-doj-muted">→</span>
                    <span className={`font-mono flex-1 truncate ${m.tgt === 'NO_MATCH' ? 'text-doj-muted' : 'text-cyan-300'}`}>{m.tgt}</span>
                    <span className={`px-1.5 py-0.5 rounded text-[10px] font-semibold ${
                      m.status === 'approved' ? 'bg-green-900/40 text-green-400' : 'bg-amber-900/40 text-amber-400'
                    }`}>{(m.conf * 100).toFixed(0)}%</span>
                  </div>
                ))}
              </div>
              <div className="mt-4 pt-3 border-t border-doj-border text-[11px] text-doj-muted">
                Mappings keyed by <code className="text-doj-blue">(system, table_name, column_name)</code> — idempotent MERGE on each run.
              </div>
            </div>
          </Reveal>
        </div>
      </section>

      {/* ── Dedup & PII ── */}
      <section>
        <Reveal>
          <SectionHeading
            icon="🔒"
            title="Deduplication & PII Governance"
            subtitle="Entity resolution identifies duplicate defendant records across systems. Unity Catalog column tags enforce downstream access controls."
          />
        </Reveal>

        <div className="grid grid-cols-2 gap-6">
          <Reveal delay={0}>
            <div className="bg-doj-surface border border-doj-border rounded-xl p-5">
              <div className="font-semibold text-sm text-doj-text mb-3">Entity Resolution (Notebook 05)</div>
              <p className="text-[12px] text-doj-muted leading-relaxed mb-4">
                Blocked candidate pairs are compared using a composite similarity score across
                first name, last name, and date of birth. Pairs above the match threshold are
                grouped into duplicate clusters and written to <code className="text-doj-blue">silver.duplicate_contacts</code>.
              </p>
              <div className="space-y-2 text-[11px]">
                {[
                  { field: 'Name similarity',  method: 'Jaro-Winkler',  weight: '40%' },
                  { field: 'Date of birth',    method: 'Exact + year ±1', weight: '40%' },
                  { field: 'County / charge',  method: 'Exact match',   weight: '20%' },
                ].map(r => (
                  <div key={r.field} className="flex items-center gap-2">
                    <span className="text-doj-muted w-32">{r.field}</span>
                    <span className="font-mono text-cyan-300 flex-1">{r.method}</span>
                    <span className="text-amber-400 font-semibold">{r.weight}</span>
                  </div>
                ))}
              </div>
              <div className="mt-4 px-3 py-2 rounded-lg bg-amber-900/20 border border-amber-500/30 text-[11px] text-amber-300">
                Duplicate groups surface in <strong>Data Quality Board</strong> and are searchable by name in <strong>Case Intelligence</strong>.
              </div>
            </div>
          </Reveal>

          <Reveal delay={100}>
            <div className="bg-doj-surface border border-doj-border rounded-xl p-5">
              <div className="font-semibold text-sm text-doj-text mb-3">PII Column Tagging</div>
              <p className="text-[12px] text-doj-muted leading-relaxed mb-4">
                Notebook <code className="text-doj-blue">01_ingest_bronze</code> applies Unity Catalog column-level
                tags immediately after each table is written to bronze. Tags enable column masking policies
                and downstream lineage tracking.
              </p>
              <div className="space-y-3">
                {[
                  { table: 'legacycase_tbl_defendant', cols: ['DOB', 'FIRST_NAME', 'LAST_NAME'] },
                  { table: 'openjustice_arrests',      cols: ['RACE', 'SEX'] },
                ].map(t => (
                  <div key={t.table}>
                    <div className="font-mono text-[10px] text-doj-muted mb-1.5">{t.table}</div>
                    <div className="flex flex-wrap gap-1.5">
                      {t.cols.map(c => (
                        <Tag key={c} label={c} color="#3b82f6" />
                      ))}
                    </div>
                  </div>
                ))}
              </div>
              <div className="mt-4 pt-3 border-t border-doj-border text-[11px] text-doj-muted">
                Tags: <code className="text-cyan-400">doj_pii = 'true'</code> · <code className="text-cyan-400">doj_pii_category = 'DOJ_SENSITIVE'</code>
              </div>
            </div>
          </Reveal>
        </div>
      </section>

      {/* ── Data quality & cleaning ── */}
      <section>
        <Reveal>
          <SectionHeading
            icon="🧹"
            title="Data Quality & Cleaning"
            subtitle="Notebook 02a profiles every bronze column. Failures surface in the Data Quality Board for SME triage."
          />
        </Reveal>

        <div className="grid grid-cols-3 gap-4">
          {[
            {
              title: 'Null Rate Analysis',
              color: '#ef4444',
              icon: '⚠️',
              desc: 'Every column\'s null/empty rate is computed and stored. Columns with null rate >30% are flagged for SME review.',
              delay: 0,
            },
            {
              title: 'Pattern Classification',
              color: '#f59e0b',
              icon: '🔍',
              desc: 'Regex ladder classifies values as SSN, DATE_ISO, PHONE, EMAIL, ZIPCODE, CODE, ID, etc. Mismatches flag potential format corruption.',
              delay: 100,
            },
            {
              title: 'FK Candidate Detection',
              color: '#22c55e',
              icon: '🔗',
              desc: 'Column value sets are cross-checked against all other tables\' PK universes. ≥80% overlap flags a probable foreign key relationship.',
              delay: 200,
            },
            {
              title: 'Duplicate Contacts',
              color: '#8b5cf6',
              icon: '👥',
              desc: 'Entity resolution groups similar defendants into clusters. Cluster size, severity, and member IDs are tracked in silver.duplicate_contacts.',
              delay: 300,
            },
            {
              title: 'Reconciliation Checks',
              color: '#06b6d4',
              icon: '⚖️',
              desc: 'Row counts and hash fingerprints are compared between source and bronze on every pipeline run. Discrepancies enter the Review Queue.',
              delay: 400,
            },
            {
              title: 'Schema Drift Detection',
              color: '#f97316',
              icon: '📐',
              desc: 'Column profiles from successive runs are compared. New columns, type changes, or cardinality spikes generate alerts in the Status Board.',
              delay: 500,
            },
          ].map(c => (
            <Reveal key={c.title} delay={c.delay}>
              <div className="rounded-xl border border-doj-border bg-doj-surface p-4 h-full">
                <div className="flex items-center gap-2 mb-2">
                  <span>{c.icon}</span>
                  <span className="text-sm font-semibold" style={{ color: c.color }}>{c.title}</span>
                </div>
                <p className="text-[12px] text-doj-muted leading-relaxed">{c.desc}</p>
              </div>
            </Reveal>
          ))}
        </div>
      </section>

      {/* ── Page guide ── */}
      <section>
        <Reveal>
          <SectionHeading
            icon="🗺️"
            title="Application Pages"
            subtitle="Eight views covering every phase of the migration lifecycle — from ingest monitoring to post-migration case lookup."
          />
        </Reveal>

        <div className="grid grid-cols-2 gap-4">
          <PageCard
            icon="📋"
            label="Status Board"
            path="/"
            color="#3b82f6"
            delay={0}
            description="Real-time ingestion status for every source table across all three systems. Filterable by source system and pipeline stage. Shows row counts, last-run timestamps, and stage progress badges."
          />
          <PageCard
            icon="📈"
            label="Health Dashboard"
            path="/dashboard"
            color="#22c55e"
            delay={50}
            description="Aggregate pipeline health metrics: success/failure rates, average stage durations, row throughput over time, and top error sources. Backed by the pipeline execution log."
          />
          <PageCard
            icon="🔄"
            label="Pipeline Flow"
            path="/pipeline"
            color="#8b5cf6"
            delay={100}
            description="Animated node-graph showing the live pipeline topology — source systems through each medallion layer to staging. Click any node to see stage detail, job run ID, and last run logs."
          />
          <PageCard
            icon="✅"
            label="Data Quality Board"
            path="/quality"
            color="#f59e0b"
            delay={150}
            description="Surfaces all quality flags: high null-rate columns, pattern mismatches, duplicate contact groups, and reconciliation failures. Each flag links to the affected table and column."
          />
          <PageCard
            icon="⬆️"
            label="Upload Files"
            path="/upload"
            color="#f97316"
            delay={200}
            description="Drop zone for AdHocExports XLSX files. Validates file format, previews parsed rows, and triggers the ingestion pipeline for the uploaded file. Supports multi-file batch upload."
          />
          <PageCard
            icon="📝"
            label="Review Queue"
            path="/review"
            color="#06b6d4"
            delay={250}
            description="SME workflow for reviewing LLM-proposed schema mappings, reconciliation discrepancies, and entity resolution conflicts. Approve, override, or reject each item. Badge count shows pending items."
          />
          <PageCard
            icon="👤"
            label="Case Intelligence"
            path="/cases"
            color="#ec4899"
            delay={300}
            description="Search defendants by name or ID across both LegacyCase (DEF-xxxxxx) and the duplicate contacts index (DEF-DUP-xxx). View full case timeline, charge history, SDOH events, and statewide OpenJustice charge-category benchmarks."
          />
          <PageCard
            icon="ℹ️"
            label="About"
            path="/about"
            color="#94a3b8"
            delay={350}
            description="This page. Documents the solution architecture, data sources, mapping process, deduplication approach, PII governance, and the purpose of each application view."
          />
        </div>
      </section>

      {/* ── Footer ── */}
      <Reveal>
        <div className="text-center pt-4 border-t border-doj-border">
          <div className="text-[11px] text-doj-muted space-x-4">
            <span>Oregon Department of Justice · Data Migration Platform</span>
            <span>·</span>
            <span className="font-mono">v1.0.0-prod</span>
            <span>·</span>
            <span>Powered by Databricks Lakehouse</span>
          </div>
        </div>
      </Reveal>

      <style>{`
        @keyframes pulse-glow {
          0%, 100% { opacity: 0.4; }
          50% { opacity: 0.9; }
        }
      `}</style>
    </div>
  );
}
