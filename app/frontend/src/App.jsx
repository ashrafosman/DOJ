import React, { createContext, useContext, useState, useCallback } from 'react';
import { BrowserRouter as Router, Routes, Route, NavLink, useLocation } from 'react-router-dom';
import GlobalStatusHeader from './components/GlobalStatusHeader';
import IngestionStatusBoard from './components/IngestionStatusBoard';
import FileUploader from './components/FileUploader';
import ReconciliationQueue from './components/ReconciliationQueue';
import PipelineFlowCanvas from './components/PipelineFlowCanvas';
import PipelineDashboard from './components/PipelineDashboard';
import DataQualityBoard from './components/DataQualityBoard';
import CaseIntelligence from './components/CaseIntelligence';
import About from './components/About';

// ─── Named Error Boundary (identifies which component failed) ────────────────
class NamedErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { error: null };
  }
  static getDerivedStateFromError(error) {
    return { error };
  }
  componentDidCatch(error, info) {
    console.error(`[NamedErrorBoundary:${this.props.name}]`, error, info);
  }
  render() {
    if (this.state.error) {
      return (
        <div style={{ padding: '1rem', background: '#1a0000', border: '1px solid #ef4444', borderRadius: '6px', margin: '0.5rem' }}>
          <div style={{ color: '#ef4444', fontWeight: 'bold', marginBottom: '0.5rem', fontFamily: 'monospace' }}>
            React Error in: <code style={{ background: '#2a0000', padding: '2px 6px', borderRadius: '4px' }}>{this.props.name}</code>
          </div>
          <pre style={{ color: '#fca5a5', fontSize: '11px', whiteSpace: 'pre-wrap', fontFamily: 'monospace' }}>
            {this.state.error.toString()}
            {'\n'}
            {this.state.error.stack}
          </pre>
        </div>
      );
    }
    return this.props.children;
  }
}

// ─── App-wide filter context ────────────────────────────────────────────────
export const FilterContext = createContext(null);

export function useFilterContext() {
  const ctx = useContext(FilterContext);
  if (!ctx) throw new Error('useFilterContext must be inside FilterProvider');
  return ctx;
}

const SYSTEM_COLORS = {
  LegacyCase: { bg: 'bg-doj-legacy/20', text: 'text-purple-300', border: 'border-doj-legacy/40', hex: '#8b5cf6' },
  OpenJustice: { bg: 'bg-doj-open/20', text: 'text-cyan-300', border: 'border-doj-open/40', hex: '#06b6d4' },
  AdHocExports: { bg: 'bg-doj-adhoc/20', text: 'text-orange-300', border: 'border-doj-adhoc/40', hex: '#f97316' },
};
export { SYSTEM_COLORS };

// ─── SVG Icons ───────────────────────────────────────────────────────────────
function IconDashboard({ className = 'w-5 h-5' }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" d="M3.75 6A2.25 2.25 0 016 3.75h2.25A2.25 2.25 0 0110.5 6v2.25a2.25 2.25 0 01-2.25 2.25H6a2.25 2.25 0 01-2.25-2.25V6zM3.75 15.75A2.25 2.25 0 016 13.5h2.25a2.25 2.25 0 012.25 2.25V18a2.25 2.25 0 01-2.25 2.25H6A2.25 2.25 0 013.75 18v-2.25zM13.5 6a2.25 2.25 0 012.25-2.25H18A2.25 2.25 0 0120.25 6v2.25A2.25 2.25 0 0118 10.5h-2.25a2.25 2.25 0 01-2.25-2.25V6zM13.5 15.75a2.25 2.25 0 012.25-2.25H18a2.25 2.25 0 012.25 2.25V18A2.25 2.25 0 0118 20.25h-2.25A2.25 2.25 0 0113.5 18v-2.25z" />
    </svg>
  );
}

function IconUpload({ className = 'w-5 h-5' }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" d="M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5m-13.5-9L12 3m0 0l4.5 4.5M12 3v13.5" />
    </svg>
  );
}

function IconReview({ className = 'w-5 h-5' }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" d="M9 12h3.75M9 15h3.75M9 18h3.75m3 .75H18a2.25 2.25 0 002.25-2.25V6.108c0-1.135-.845-2.098-1.976-2.192a48.424 48.424 0 00-1.123-.08m-5.801 0c-.065.21-.1.433-.1.664 0 .414.336.75.75.75h4.5a.75.75 0 00.75-.75 2.25 2.25 0 00-.1-.664m-5.8 0A2.251 2.251 0 0113.5 2.25H15c1.012 0 1.867.668 2.15 1.586m-5.8 0c-.376.023-.75.05-1.124.08C9.095 4.01 8.25 4.973 8.25 6.108V8.25m0 0H4.875c-.621 0-1.125.504-1.125 1.125v11.25c0 .621.504 1.125 1.125 1.125h9.75c.621 0 1.125-.504 1.125-1.125V9.375c0-.621-.504-1.125-1.125-1.125H8.25zM6.75 12h.008v.008H6.75V12zm0 3h.008v.008H6.75V15zm0 3h.008v.008H6.75V18z" />
    </svg>
  );
}

function IconPipeline({ className = 'w-5 h-5' }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" d="M7.5 21L3 16.5m0 0L7.5 12M3 16.5h13.5m0-13.5L21 7.5m0 0L16.5 12M21 7.5H7.5" />
    </svg>
  );
}

function IconQuality({ className = 'w-5 h-5' }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" d="M9 12.75L11.25 15 15 9.75M21 12c0 1.268-.63 2.39-1.593 3.068a3.745 3.745 0 01-1.043 3.296 3.745 3.745 0 01-3.296 1.043A3.745 3.745 0 0112 21c-1.268 0-2.39-.63-3.068-1.593a3.746 3.746 0 01-3.296-1.043 3.745 3.745 0 01-1.043-3.296A3.745 3.745 0 013 12c0-1.268.63-2.39 1.593-3.068a3.745 3.745 0 011.043-3.296 3.746 3.746 0 013.296-1.043A3.746 3.746 0 0112 3c1.268 0 2.39.63 3.068 1.593a3.746 3.746 0 013.296 1.043 3.746 3.746 0 011.043 3.296A3.745 3.745 0 0121 12z" />
    </svg>
  );
}

function IconChart({ className = 'w-5 h-5' }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" d="M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 013 19.875v-6.75zM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V8.625zM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V4.125z" />
    </svg>
  );
}

function IconCases({ className = 'w-5 h-5' }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 6a3.75 3.75 0 11-7.5 0 3.75 3.75 0 017.5 0zM4.501 20.118a7.5 7.5 0 0114.998 0A17.933 17.933 0 0112 21.75c-2.676 0-5.216-.584-7.499-1.632z" />
    </svg>
  );
}

function IconInfo({ className = 'w-5 h-5' }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" d="M11.25 11.25l.041-.02a.75.75 0 011.063.852l-.708 2.836a.75.75 0 001.063.853l.041-.021M21 12a9 9 0 11-18 0 9 9 0 0118 0zm-9-3.75h.008v.008H12V8.25z" />
    </svg>
  );
}

function IconShield({ className = 'w-6 h-6' }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" d="M9 12.75L11.25 15 15 9.75m-3-7.036A11.959 11.959 0 013.598 6 11.99 11.99 0 003 9.749c0 5.592 3.824 10.29 9 11.623 5.176-1.332 9-6.03 9-11.622 0-1.31-.21-2.571-.598-3.751h-.152c-3.196 0-6.1-1.248-8.25-3.285z" />
    </svg>
  );
}

// ─── Sidebar NavLink ─────────────────────────────────────────────────────────
function SideNavLink({ to, icon: Icon, label, badge }) {
  return (
    <NavLink
      to={to}
      className={({ isActive }) =>
        `flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-all duration-150 group relative
        ${isActive
          ? 'bg-doj-blue/15 text-doj-text border border-doj-blue/30 shadow-[0_0_12px_#3b82f620]'
          : 'text-doj-muted hover:text-doj-text hover:bg-white/5 border border-transparent'
        }`
      }
    >
      <Icon className="w-5 h-5 flex-shrink-0" />
      <span className="flex-1">{label}</span>
      {badge > 0 && (
        <span className="ml-auto bg-doj-red text-white text-xs font-bold rounded-full min-w-[18px] h-[18px] flex items-center justify-center px-1 animate-badge-pop">
          {badge > 99 ? '99+' : badge}
        </span>
      )}
    </NavLink>
  );
}

// ─── Page header utility ─────────────────────────────────────────────────────
function PageBreadcrumb() {
  const location = useLocation();
  const crumbs = {
    '/': 'Ingestion Status Board',
    '/upload': 'File Upload',
    '/review': 'Reconciliation Queue',
    '/pipeline': 'Pipeline Flow',
    '/dashboard': 'Pipeline Health Dashboard',
    '/quality': 'Data Quality Board',
    '/cases': 'Case Intelligence',
    '/about': 'About',
  };
  return (
    <div className="flex items-center gap-2 text-xs text-doj-muted mb-1">
      <span>DOJ Migration Monitor</span>
      <span>/</span>
      <span className="text-doj-text">{crumbs[location.pathname] || 'Unknown'}</span>
    </div>
  );
}

// ─── Main App ────────────────────────────────────────────────────────────────
export default function App() {
  const [statusFilter, setStatusFilter] = useState('all');
  const [systemFilters, setSystemFilters] = useState(new Set(['LegacyCase', 'OpenJustice', 'AdHocExports']));
  const [reviewBadge, setReviewBadge] = useState(0);
  const [globalAlerts, setGlobalAlerts] = useState(0);

  const toggleSystem = useCallback((system) => {
    setSystemFilters(prev => {
      const next = new Set(prev);
      if (next.has(system)) {
        if (next.size > 1) next.delete(system);
      } else {
        next.add(system);
      }
      return next;
    });
  }, []);

  const contextValue = {
    statusFilter,
    setStatusFilter,
    systemFilters,
    toggleSystem,
    reviewBadge,
    setReviewBadge,
    globalAlerts,
    setGlobalAlerts,
  };

  return (
    <FilterContext.Provider value={contextValue}>
      <Router>
        <div className="min-h-screen bg-doj-bg text-doj-text flex flex-col font-sans antialiased">
          {/* Global status header */}
          <NamedErrorBoundary name="GlobalStatusHeader">
            <GlobalStatusHeader />
          </NamedErrorBoundary>

          <div className="flex flex-1 overflow-hidden">
            {/* ── Sidebar ── */}
            <aside className="w-56 flex-shrink-0 bg-doj-surface border-r border-doj-border flex flex-col overflow-y-auto">
              {/* Brand */}
              <div className="px-4 py-4 border-b border-doj-border">
                <div className="flex items-center gap-2">
                  <div className="w-8 h-8 rounded bg-doj-blue/20 border border-doj-blue/40 flex items-center justify-center">
                    <IconShield className="w-4 h-4 text-doj-blue" />
                  </div>
                  <div>
                    <div className="text-xs font-bold text-doj-text tracking-wider uppercase">DOJ</div>
                    <div className="text-[10px] text-doj-muted leading-tight">Data Migration</div>
                  </div>
                </div>
              </div>

              {/* Navigation */}
              <nav className="flex-1 px-3 py-4 space-y-1">
                <div className="text-[10px] font-semibold uppercase tracking-widest text-doj-muted px-3 mb-2">Monitor</div>
                <SideNavLink to="/" icon={IconDashboard} label="Status Board" />
                <SideNavLink to="/dashboard" icon={IconChart} label="Health Dashboard" />
                <SideNavLink to="/pipeline" icon={IconPipeline} label="Pipeline Flow" />
                <SideNavLink to="/quality" icon={IconQuality} label="Data Quality" />

                <div className="text-[10px] font-semibold uppercase tracking-widest text-doj-muted px-3 mt-4 mb-2">Operations</div>
                <SideNavLink to="/upload" icon={IconUpload} label="Upload Files" />
                <SideNavLink to="/review" icon={IconReview} label="Review Queue" badge={reviewBadge} />
                <SideNavLink to="/cases" icon={IconCases} label="Case Intelligence" />

                <div className="text-[10px] font-semibold uppercase tracking-widest text-doj-muted px-3 mt-4 mb-2">Info</div>
                <SideNavLink to="/about" icon={IconInfo} label="About" />
              </nav>

              {/* System legend */}
              <div className="px-4 py-4 border-t border-doj-border">
                <div className="text-[10px] font-semibold uppercase tracking-widest text-doj-muted mb-3">Systems</div>
                {[
                  { id: 'LegacyCase', label: 'LegacyCase', color: '#8b5cf6' },
                  { id: 'OpenJustice', label: 'OpenJustice', color: '#06b6d4' },
                  { id: 'AdHocExports', label: 'AdHocExports', color: '#f97316' },
                ].map(sys => (
                  <button
                    key={sys.id}
                    onClick={() => toggleSystem(sys.id)}
                    className={`flex items-center gap-2 w-full px-2 py-1.5 rounded text-xs mb-1 transition-all
                      ${systemFilters.has(sys.id) ? 'opacity-100' : 'opacity-30'}`}
                  >
                    <span
                      className="w-2.5 h-2.5 rounded-full flex-shrink-0"
                      style={{ backgroundColor: sys.color, boxShadow: systemFilters.has(sys.id) ? `0 0 6px ${sys.color}` : 'none' }}
                    />
                    <span className="text-doj-muted">{sys.label}</span>
                  </button>
                ))}
              </div>

              {/* Footer */}
              <div className="px-4 py-3 border-t border-doj-border">
                <div className="text-[10px] text-doj-muted">
                  <div className="font-mono">v1.0.0-prod</div>
                  <div>Migration Monitor</div>
                </div>
              </div>
            </aside>

            {/* ── Main content ── */}
            <main className="flex-1 overflow-auto bg-doj-bg">
              <div className="p-6">
                <PageBreadcrumb />
                <Routes>
                  <Route path="/" element={<NamedErrorBoundary name="IngestionStatusBoard"><IngestionStatusBoard /></NamedErrorBoundary>} />
                  <Route path="/upload" element={<NamedErrorBoundary name="FileUploader"><FileUploader /></NamedErrorBoundary>} />
                  <Route path="/review" element={<NamedErrorBoundary name="ReconciliationQueue"><ReconciliationQueue /></NamedErrorBoundary>} />
                  <Route path="/pipeline" element={<NamedErrorBoundary name="PipelineFlowCanvas"><PipelineFlowCanvas /></NamedErrorBoundary>} />
                  <Route path="/dashboard" element={<NamedErrorBoundary name="PipelineDashboard"><PipelineDashboard /></NamedErrorBoundary>} />
                  <Route path="/quality" element={<NamedErrorBoundary name="DataQualityBoard"><DataQualityBoard /></NamedErrorBoundary>} />
                  <Route path="/cases" element={<NamedErrorBoundary name="CaseIntelligence"><CaseIntelligence /></NamedErrorBoundary>} />
                  <Route path="/about" element={<NamedErrorBoundary name="About"><About /></NamedErrorBoundary>} />
                </Routes>
              </div>
            </main>
          </div>
        </div>
      </Router>
    </FilterContext.Provider>
  );
}
