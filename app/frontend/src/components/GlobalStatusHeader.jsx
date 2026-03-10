import React, { useState, useEffect, useCallback, useRef } from 'react';
import { useFilterContext } from '../App';

const REFRESH_INTERVAL = 10; // seconds

function AlertBadge({ count }) {
  if (count === 0) return null;
  return (
    <span className="inline-flex items-center justify-center min-w-[20px] h-5 px-1.5 rounded-full bg-doj-red text-white text-xs font-bold animate-pulse-red shadow-[0_0_8px_#ef444480]">
      {count > 99 ? '99+' : count}
    </span>
  );
}

function StatChip({ label, value, colorClass = 'text-doj-text' }) {
  return (
    <div className="flex items-center gap-1.5 px-3 py-1 bg-doj-surface-2 border border-doj-border rounded-lg">
      <span className="text-doj-muted text-xs">{label}</span>
      <span className={`font-mono font-semibold text-sm ${colorClass}`}>{value}</span>
    </div>
  );
}

export default function GlobalStatusHeader() {
  const { statusFilter, setStatusFilter, systemFilters, toggleSystem, setGlobalAlerts } = useFilterContext();

  const [summary, setSummary] = useState({
    active_jobs: 0,
    rows_in_flight: 0,
    alert_count: 0,
    last_refresh: null,
  });
  const [countdown, setCountdown] = useState(REFRESH_INTERVAL);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const countdownRef = useRef(null);
  const fetchRef = useRef(null);

  const fetchSummary = useCallback(async () => {
    setIsRefreshing(true);
    try {
      const res = await fetch('/api/stages/summary');
      if (res.ok) {
        const data = await res.json();
        const jobs = Array.isArray(data.active_jobs) ? data.active_jobs : [];
        const activeCount = jobs.filter(j => j.status === 'running').length;
        const rowsInFlight = jobs
          .filter(j => j.status === 'running')
          .reduce((sum, j) => sum + (j.rows ?? 0), 0);
        const today = new Date().toDateString();
        const alertCount = jobs.filter(j =>
          (j.status === 'review' || j.status === 'failed') &&
          new Date(j.uploaded_at).toDateString() === today
        ).length;
        setSummary({
          active_jobs: activeCount,
          rows_in_flight: rowsInFlight,
          alert_count: alertCount,
          last_refresh: new Date(),
        });
        setGlobalAlerts(alertCount);
      }
    } catch {
      // silently keep stale data on network error
    } finally {
      setIsRefreshing(false);
      setCountdown(REFRESH_INTERVAL);
    }
  }, [setGlobalAlerts]);

  // Initial fetch
  useEffect(() => {
    fetchSummary();
  }, [fetchSummary]);

  // Countdown ticker
  useEffect(() => {
    countdownRef.current = setInterval(() => {
      setCountdown(prev => {
        if (prev <= 1) {
          fetchSummary();
          return REFRESH_INTERVAL;
        }
        return prev - 1;
      });
    }, 1000);
    return () => clearInterval(countdownRef.current);
  }, [fetchSummary]);

  const handleManualRefresh = () => {
    clearInterval(countdownRef.current);
    fetchSummary();
    countdownRef.current = setInterval(() => {
      setCountdown(prev => {
        if (prev <= 1) {
          fetchSummary();
          return REFRESH_INTERVAL;
        }
        return prev - 1;
      });
    }, 1000);
  };

  const formatLastRefresh = (date) => {
    if (!date) return '—';
    return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  };

  const formatRowsInFlight = (n) => {
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
    if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
    return n.toString();
  };

  const FILTER_OPTIONS = [
    { id: 'all', label: 'All Jobs' },
    { id: 'active', label: 'Active Only' },
    { id: 'review', label: 'Needs Review' },
    { id: 'failed', label: 'Failed' },
  ];

  const SYSTEM_CHIPS = [
    { id: 'LegacyCase', label: 'LegacyCase', color: '#8b5cf6', activeClass: 'border-purple-500/60 bg-purple-500/15 text-purple-300' },
    { id: 'OpenJustice', label: 'OpenJustice', color: '#06b6d4', activeClass: 'border-cyan-500/60 bg-cyan-500/15 text-cyan-300' },
    { id: 'AdHocExports', label: 'AdHocExports', color: '#f97316', activeClass: 'border-orange-500/60 bg-orange-500/15 text-orange-300' },
  ];

  return (
    <header className="bg-doj-surface border-b border-doj-border sticky top-0 z-40 shadow-[0_2px_12px_#00000040]">
      {/* Top strip — stats + controls */}
      <div className="flex items-center justify-between px-4 py-2 border-b border-doj-border/50">
        {/* Left: stats */}
        <div className="flex items-center gap-2">
          <StatChip
            label="Active Jobs"
            value={summary.active_jobs}
            colorClass={summary.active_jobs > 0 ? 'text-doj-amber' : 'text-doj-muted'}
          />
          <StatChip
            label="Rows in Flight"
            value={formatRowsInFlight(summary.rows_in_flight)}
            colorClass="text-doj-blue"
          />
          <div className="flex items-center gap-1.5 px-3 py-1 bg-doj-surface-2 border border-doj-border rounded-lg">
            <span className="text-doj-muted text-xs">Alerts</span>
            {summary.alert_count > 0 ? (
              <AlertBadge count={summary.alert_count} />
            ) : (
              <span className="font-mono font-semibold text-sm text-doj-green">0</span>
            )}
          </div>
        </div>

        {/* Right: refresh controls */}
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-1.5 text-xs text-doj-muted">
            <span>Last:</span>
            <span className="font-mono text-doj-text">{formatLastRefresh(summary.last_refresh)}</span>
          </div>
          {/* Countdown ring */}
          <div className="flex items-center gap-1.5">
            <svg className="w-5 h-5 -rotate-90" viewBox="0 0 20 20">
              <circle cx="10" cy="10" r="8" fill="none" stroke="#2d3748" strokeWidth="2" />
              <circle
                cx="10" cy="10" r="8"
                fill="none"
                stroke={countdown <= 3 ? '#ef4444' : '#3b82f6'}
                strokeWidth="2"
                strokeDasharray={`${(countdown / REFRESH_INTERVAL) * 50.27} 50.27`}
                className="transition-all duration-1000"
              />
            </svg>
            <span className="font-mono text-xs text-doj-muted w-4 text-right">{countdown}s</span>
          </div>
          <button
            onClick={handleManualRefresh}
            disabled={isRefreshing}
            className="flex items-center gap-1.5 px-3 py-1 bg-doj-blue/15 border border-doj-blue/40 text-doj-blue rounded-lg text-xs font-medium hover:bg-doj-blue/25 transition-all disabled:opacity-50"
          >
            <svg
              className={`w-3.5 h-3.5 ${isRefreshing ? 'animate-spin' : ''}`}
              fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor"
            >
              <path strokeLinecap="round" strokeLinejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0l3.181 3.183a8.25 8.25 0 0013.803-3.7M4.031 9.865a8.25 8.25 0 0113.803-3.7l3.181 3.182m0-4.991v4.99" />
            </svg>
            Refresh
          </button>
        </div>
      </div>

      {/* Bottom strip — filters + system chips */}
      <div className="flex items-center justify-between px-4 py-1.5">
        {/* Status quick-filter buttons */}
        <div className="flex items-center gap-1">
          {FILTER_OPTIONS.map(opt => (
            <button
              key={opt.id}
              onClick={() => setStatusFilter(opt.id)}
              className={`px-3 py-1 rounded text-xs font-medium transition-all duration-150
                ${statusFilter === opt.id
                  ? 'bg-doj-blue/20 border border-doj-blue/50 text-doj-blue shadow-[0_0_8px_#3b82f620]'
                  : 'text-doj-muted hover:text-doj-text hover:bg-white/5 border border-transparent'
                }`}
            >
              {opt.label}
            </button>
          ))}
        </div>

        {/* System filter chips */}
        <div className="flex items-center gap-2">
          <span className="text-[10px] text-doj-muted uppercase tracking-widest mr-1">Filter:</span>
          {SYSTEM_CHIPS.map(sys => (
            <button
              key={sys.id}
              onClick={() => toggleSystem(sys.id)}
              className={`flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-xs font-medium border transition-all duration-150
                ${systemFilters.has(sys.id)
                  ? sys.activeClass
                  : 'border-doj-border text-doj-muted bg-transparent opacity-50'
                }`}
            >
              <span
                className="w-1.5 h-1.5 rounded-full"
                style={{ backgroundColor: sys.color }}
              />
              {sys.label}
            </button>
          ))}
        </div>
      </div>
    </header>
  );
}
