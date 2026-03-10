import React, { useState, useRef, useCallback } from 'react';

// ─── Constants ────────────────────────────────────────────────────────────────
const SYSTEMS = [
  {
    id: 'LegacyCase',
    label: 'LegacyCase',
    color: '#8b5cf6',
    bgClass: 'bg-purple-500/15 border-purple-500/40 text-purple-300',
    expectedColumns: ['CaseID', 'DefendantID', 'ChargeCode', 'FilingDate', 'CourtID', 'StatusCode', 'AssignedATY', 'DispositionCode', 'SentenceDate'],
  },
  {
    id: 'OpenJustice',
    label: 'OpenJustice',
    color: '#06b6d4',
    bgClass: 'bg-cyan-500/15 border-cyan-500/40 text-cyan-300',
    expectedColumns: ['case_number', 'party_id', 'attorney_bar', 'filing_dt', 'court_code', 'charge_desc', 'disposition', 'judge_id'],
  },
  {
    id: 'AdHocExports',
    label: 'AdHocExports',
    color: '#f97316',
    bgClass: 'bg-orange-500/15 border-orange-500/40 text-orange-300',
    expectedColumns: ['ID', 'Name', 'Date', 'Type', 'Value', 'Source'],
  },
];

const ACCEPT_TYPES = ['.xlsx', '.csv', 'text/csv', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'];

// ─── CSV parser (client-side, first 5 rows) ──────────────────────────────────
function parseCSVPreview(text) {
  const lines = text.split(/\r?\n/).filter(l => l.trim().length > 0);
  if (lines.length === 0) return { headers: [], rows: [] };
  const parse = (line) => {
    const result = [];
    let cur = '', inQ = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (ch === '"') { inQ = !inQ; continue; }
      if (ch === ',' && !inQ) { result.push(cur.trim()); cur = ''; continue; }
      cur += ch;
    }
    result.push(cur.trim());
    return result;
  };
  const headers = parse(lines[0]);
  const rows = lines.slice(1, 6).map(parse);
  return { headers, rows };
}

function detectMissingColumns(headers, expectedColumns) {
  const lowerHeaders = headers.map(h => h.toLowerCase());
  return expectedColumns.filter(col => !lowerHeaders.includes(col.toLowerCase()));
}

// ─── Sub-components ───────────────────────────────────────────────────────────
function SystemSelector({ selected, onSelect }) {
  return (
    <div className="flex flex-col gap-2">
      <label className="text-xs font-semibold text-doj-muted uppercase tracking-wider">Source System</label>
      <div className="flex gap-3">
        {SYSTEMS.map(sys => (
          <button
            key={sys.id}
            onClick={() => onSelect(sys.id)}
            className={`flex items-center gap-2 px-4 py-2.5 rounded-lg border text-sm font-medium transition-all duration-150
              ${selected === sys.id
                ? `${sys.bgClass} shadow-lg`
                : 'border-doj-border text-doj-muted hover:border-doj-border/80 hover:text-doj-text'
              }`}
          >
            <span className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: sys.color }} />
            {sys.label}
          </button>
        ))}
      </div>
    </div>
  );
}

function DropZone({ onFiles, isDragging, setIsDragging, disabled }) {
  const inputRef = useRef(null);

  const handleDrop = useCallback((e) => {
    e.preventDefault();
    setIsDragging(false);
    if (disabled) return;
    const files = Array.from(e.dataTransfer.files).filter(f =>
      f.name.endsWith('.csv') || f.name.endsWith('.xlsx')
    );
    if (files.length > 0) onFiles(files);
  }, [onFiles, setIsDragging, disabled]);

  const handleDragOver = useCallback((e) => {
    e.preventDefault();
    if (!disabled) setIsDragging(true);
  }, [setIsDragging, disabled]);

  const handleDragLeave = useCallback(() => setIsDragging(false), [setIsDragging]);

  const handleClick = () => {
    if (!disabled) inputRef.current?.click();
  };

  const handleInput = (e) => {
    const files = Array.from(e.target.files || []);
    if (files.length > 0) onFiles(files);
    e.target.value = '';
  };

  return (
    <div
      onDrop={handleDrop}
      onDragOver={handleDragOver}
      onDragLeave={handleDragLeave}
      onClick={handleClick}
      className={`relative border-2 border-dashed rounded-xl p-10 text-center transition-all duration-200 cursor-pointer
        ${isDragging
          ? 'border-doj-blue bg-doj-blue/10 scale-[1.01] shadow-[0_0_20px_#3b82f630]'
          : disabled
            ? 'border-doj-border/30 opacity-40 cursor-not-allowed'
            : 'border-doj-border hover:border-doj-blue/50 hover:bg-doj-blue/5'
        }`}
    >
      <input
        ref={inputRef}
        type="file"
        accept={ACCEPT_TYPES.join(',')}
        multiple
        onChange={handleInput}
        className="hidden"
        disabled={disabled}
      />
      <svg className={`w-10 h-10 mx-auto mb-3 ${isDragging ? 'text-doj-blue' : 'text-doj-muted'}`} fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" d="M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5m-13.5-9L12 3m0 0l4.5 4.5M12 3v13.5" />
      </svg>
      <p className={`text-sm font-medium mb-1 ${isDragging ? 'text-doj-blue' : 'text-doj-text'}`}>
        {isDragging ? 'Drop files here' : 'Drag & drop files here'}
      </p>
      <p className="text-xs text-doj-muted">or click to browse — accepts .csv and .xlsx</p>
    </div>
  );
}

function FilePreviewTable({ file, preview, missingColumns, system }) {
  const sys = SYSTEMS.find(s => s.id === system);
  const isExcel = file.name.endsWith('.xlsx');

  return (
    <div className="bg-doj-surface-2 border border-doj-border rounded-xl p-4 animate-fade-in">
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <svg className="w-4 h-4 text-doj-green" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m2.25 0H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z" />
          </svg>
          <span className="text-sm font-medium text-doj-text">{file.name}</span>
          <span className="text-xs text-doj-muted">({(file.size / 1024).toFixed(1)} KB)</span>
        </div>
        {sys && (
          <span className={`px-2 py-0.5 rounded-full text-xs font-medium border ${sys.bgClass}`}>
            {sys.label}
          </span>
        )}
      </div>

      {missingColumns.length > 0 && (
        <div className="flex items-start gap-2 p-3 bg-doj-amber/10 border border-doj-amber/30 rounded-lg mb-3">
          <svg className="w-4 h-4 text-doj-amber flex-shrink-0 mt-0.5" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126zM12 15.75h.007v.008H12v-.008z" />
          </svg>
          <div>
            <p className="text-xs font-medium text-doj-amber">Missing expected columns:</p>
            <p className="text-xs text-doj-muted mt-0.5">{missingColumns.join(', ')}</p>
          </div>
        </div>
      )}

      {isExcel ? (
        <div className="flex items-center gap-2 p-3 bg-doj-surface border border-doj-border rounded-lg">
          <svg className="w-4 h-4 text-doj-muted" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" d="M11.25 11.25l.041-.02a.75.75 0 011.063.852l-.708 2.836a.75.75 0 001.063.853l.041-.021M21 12a9 9 0 11-18 0 9 9 0 0118 0zm-9-3.75h.008v.008H12V8.25z" />
          </svg>
          <span className="text-xs text-doj-muted">Excel file — headers and rows will be detected on upload</span>
        </div>
      ) : preview && (
        <div className="overflow-x-auto rounded-lg border border-doj-border">
          <table className="w-full text-xs">
            <thead>
              <tr className="bg-doj-surface border-b border-doj-border">
                {preview.headers.map((h, i) => (
                  <th key={i} className={`px-3 py-2 text-left font-medium text-doj-muted whitespace-nowrap
                    ${sys && missingColumns.includes(h) ? 'text-doj-amber' : ''}
                    ${sys && sys.expectedColumns.map(c => c.toLowerCase()).includes(h.toLowerCase()) ? 'text-doj-green' : ''}`}
                  >
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {preview.rows.map((row, ri) => (
                <tr key={ri} className="border-b border-doj-border/50 hover:bg-white/2">
                  {row.map((cell, ci) => (
                    <td key={ci} className="px-3 py-1.5 text-doj-muted font-mono whitespace-nowrap max-w-[160px] truncate">
                      {cell || <span className="text-doj-border italic">null</span>}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
          <div className="px-3 py-1.5 text-[10px] text-doj-muted bg-doj-surface">Showing preview of first 5 rows</div>
        </div>
      )}
    </div>
  );
}

function ProgressBar({ progress }) {
  return (
    <div className="w-full bg-doj-border rounded-full h-2 overflow-hidden">
      <div
        className="h-full bg-doj-blue rounded-full transition-all duration-300 shadow-[0_0_8px_#3b82f680]"
        style={{ width: `${progress}%` }}
      />
    </div>
  );
}

// ─── Main component ───────────────────────────────────────────────────────────
export default function FileUploader() {
  const [selectedSystem, setSelectedSystem] = useState('LegacyCase');
  const [isDragging, setIsDragging] = useState(false);
  const [files, setFiles] = useState([]);
  const [previews, setPreviews] = useState({});
  const [missingCols, setMissingCols] = useState({});
  const [uploadState, setUploadState] = useState('idle'); // idle | uploading | success | error
  const [uploadProgress, setUploadProgress] = useState(0);
  const [uploadResult, setUploadResult] = useState(null);
  const [errorMsg, setErrorMsg] = useState('');

  const sys = SYSTEMS.find(s => s.id === selectedSystem);

  const handleFiles = useCallback(async (newFiles) => {
    setFiles(prev => {
      const existing = new Set(prev.map(f => f.name));
      return [...prev, ...newFiles.filter(f => !existing.has(f.name))];
    });

    for (const file of newFiles) {
      if (file.name.endsWith('.csv')) {
        const text = await file.text();
        const { headers, rows } = parseCSVPreview(text);
        setPreviews(prev => ({ ...prev, [file.name]: { headers, rows } }));
        const missing = detectMissingColumns(headers, sys?.expectedColumns || []);
        setMissingCols(prev => ({ ...prev, [file.name]: missing }));
      } else {
        setPreviews(prev => ({ ...prev, [file.name]: null }));
        setMissingCols(prev => ({ ...prev, [file.name]: [] }));
      }
    }
  }, [sys]);

  const removeFile = (name) => {
    setFiles(prev => prev.filter(f => f.name !== name));
    setPreviews(prev => { const n = { ...prev }; delete n[name]; return n; });
    setMissingCols(prev => { const n = { ...prev }; delete n[name]; return n; });
  };

  const handleUpload = async () => {
    if (files.length === 0) return;
    setUploadState('uploading');
    setUploadProgress(0);
    setErrorMsg('');

    // Map friendly IDs to backend SourceSystem enum values
    const systemMap = {
      LegacyCase: 'LEGACY_CASE',
      OpenJustice: 'OPEN_JUSTICE',
      AdHocExports: 'AD_HOC_EXPORTS',
    };
    const backendSystem = systemMap[selectedSystem] || selectedSystem;

    const formData = new FormData();
    files.forEach(f => formData.append('file', f));  // backend expects 'file' (singular)

    try {
      // Simulate progressive upload with XHR for progress tracking
      const result = await new Promise((resolve, reject) => {
        const xhr = new XMLHttpRequest();
        xhr.open('POST', `/api/upload?source_system=${encodeURIComponent(backendSystem)}`);
        xhr.upload.onprogress = (e) => {
          if (e.lengthComputable) {
            setUploadProgress(Math.round((e.loaded / e.total) * 90));
          }
        };
        xhr.onload = () => {
          if (xhr.status >= 200 && xhr.status < 300) {
            setUploadProgress(100);
            try {
              resolve(JSON.parse(xhr.responseText));
            } catch {
              resolve({ job_id: 'JOB-' + Math.random().toString(36).substr(2, 8).toUpperCase() });
            }
          } else {
            // Extract the detail message from the JSON error body if available.
            let detail = `Upload failed: ${xhr.status}`;
            try {
              const errBody = JSON.parse(xhr.responseText);
              if (errBody.detail) detail = errBody.detail;
            } catch { /* fall through to status-only message */ }
            reject(new Error(detail));
          }
        };
        xhr.onerror = () => reject(new Error('Network error during upload'));
        xhr.send(formData);
      });

      setUploadResult(result);
      setUploadState('success');
    } catch (err) {
      setErrorMsg(err.message || 'Upload failed. Please try again.');
      setUploadState('error');
    }
  };

  const handleReset = () => {
    setFiles([]);
    setPreviews({});
    setMissingCols({});
    setUploadState('idle');
    setUploadProgress(0);
    setUploadResult(null);
    setErrorMsg('');
  };

  const totalMissingWarn = Object.values(missingCols).some(v => v.length > 0);

  return (
    <div className="max-w-4xl">
      <div className="mb-6">
        <h1 className="text-xl font-bold text-doj-text">Upload Migration Files</h1>
        <p className="text-sm text-doj-muted mt-1">Ingest source data files into the migration pipeline</p>
      </div>

      {uploadState === 'success' ? (
        <div className="bg-doj-surface border border-doj-green/30 rounded-xl p-8 text-center animate-fade-in">
          <div className="w-16 h-16 rounded-full bg-doj-green/15 border border-doj-green/30 flex items-center justify-center mx-auto mb-4">
            <svg className="w-8 h-8 text-doj-green" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
            </svg>
          </div>
          <h2 className="text-lg font-bold text-doj-green mb-2">Upload Successful</h2>
          <p className="text-sm text-doj-muted mb-4">
            {files.length} file{files.length > 1 ? 's' : ''} submitted to the <span className={`font-medium ${sys?.bgClass.split(' ').find(c => c.startsWith('text-'))}`}>{selectedSystem}</span> pipeline
          </p>
          {uploadResult?.job_id && (
            <div className="inline-flex items-center gap-2 px-4 py-2 bg-doj-surface-2 border border-doj-border rounded-lg mb-6">
              <span className="text-xs text-doj-muted">Job ID</span>
              <span className="font-mono text-sm font-bold text-doj-text">{uploadResult.job_id}</span>
            </div>
          )}
          <div className="flex justify-center gap-3">
            <a
              href="/"
              className="px-4 py-2 bg-doj-blue/15 border border-doj-blue/40 text-doj-blue rounded-lg text-sm font-medium hover:bg-doj-blue/25 transition-all"
            >
              View Status Board
            </a>
            <button
              onClick={handleReset}
              className="px-4 py-2 bg-doj-surface-2 border border-doj-border text-doj-muted rounded-lg text-sm font-medium hover:text-doj-text hover:border-doj-border/80 transition-all"
            >
              Upload More Files
            </button>
          </div>
        </div>
      ) : (
        <div className="space-y-6">
          {/* System selector */}
          <div className="bg-doj-surface border border-doj-border rounded-xl p-5">
            <SystemSelector selected={selectedSystem} onSelect={setSelectedSystem} />
          </div>

          {/* Drop zone */}
          <div className="bg-doj-surface border border-doj-border rounded-xl p-5">
            <DropZone
              onFiles={handleFiles}
              isDragging={isDragging}
              setIsDragging={setIsDragging}
              disabled={uploadState === 'uploading'}
            />
          </div>

          {/* File list + previews */}
          {files.length > 0 && (
            <div className="space-y-4">
              <div className="flex items-center justify-between">
                <h3 className="text-sm font-semibold text-doj-text">{files.length} file{files.length > 1 ? 's' : ''} queued</h3>
                <button onClick={handleReset} className="text-xs text-doj-muted hover:text-doj-red transition-colors">Clear all</button>
              </div>
              {files.map(file => (
                <div key={file.name} className="relative group">
                  <button
                    onClick={() => removeFile(file.name)}
                    className="absolute top-3 right-3 z-10 w-6 h-6 rounded-full bg-doj-surface-2 border border-doj-border text-doj-muted hover:text-doj-red hover:border-doj-red/50 flex items-center justify-center opacity-0 group-hover:opacity-100 transition-all"
                  >
                    <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" strokeWidth={2.5} stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
                    </svg>
                  </button>
                  <FilePreviewTable
                    file={file}
                    preview={previews[file.name]}
                    missingColumns={missingCols[file.name] || []}
                    system={selectedSystem}
                  />
                </div>
              ))}
            </div>
          )}

          {/* Upload progress */}
          {uploadState === 'uploading' && (
            <div className="bg-doj-surface border border-doj-border rounded-xl p-5 animate-fade-in">
              <div className="flex items-center justify-between mb-3">
                <span className="text-sm font-medium text-doj-text">Uploading...</span>
                <span className="font-mono text-sm text-doj-blue">{uploadProgress}%</span>
              </div>
              <ProgressBar progress={uploadProgress} />
              <p className="text-xs text-doj-muted mt-2">Do not close this window while uploading</p>
            </div>
          )}

          {/* Error state */}
          {uploadState === 'error' && (
            <div className="flex items-start gap-3 p-4 bg-doj-red/10 border border-doj-red/30 rounded-xl animate-fade-in">
              <svg className="w-5 h-5 text-doj-red flex-shrink-0 mt-0.5" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m9-.75a9 9 0 11-18 0 9 9 0 0118 0zm-9 3.75h.008v.008H12v-.008z" />
              </svg>
              <div className="flex-1">
                <p className="text-sm font-medium text-doj-red">Upload Failed</p>
                <p className="text-xs text-doj-muted mt-0.5">{errorMsg}</p>
              </div>
              <button onClick={() => setUploadState('idle')} className="text-xs text-doj-muted hover:text-doj-text">Dismiss</button>
            </div>
          )}

          {/* Expected columns reference */}
          {sys && (
            <div className="bg-doj-surface border border-doj-border rounded-xl p-4">
              <h4 className="text-xs font-semibold text-doj-muted uppercase tracking-wider mb-2">Expected columns for {sys.label}</h4>
              <div className="flex flex-wrap gap-1.5">
                {sys.expectedColumns.map(col => (
                  <span key={col} className="px-2 py-0.5 bg-doj-surface-2 border border-doj-border rounded text-xs font-mono text-doj-muted">
                    {col}
                  </span>
                ))}
              </div>
            </div>
          )}

          {/* Upload CTA */}
          <div className="flex items-center justify-between">
            {totalMissingWarn && (
              <div className="flex items-center gap-2 text-xs text-doj-amber">
                <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126zM12 15.75h.007v.008H12v-.008z" />
                </svg>
                Some expected columns are missing — upload will proceed with warnings
              </div>
            )}
            <div className="ml-auto flex gap-3">
              <button
                onClick={handleReset}
                disabled={uploadState === 'uploading'}
                className="px-4 py-2 bg-doj-surface-2 border border-doj-border text-doj-muted rounded-lg text-sm font-medium hover:text-doj-text transition-all disabled:opacity-50"
              >
                Cancel
              </button>
              <button
                onClick={handleUpload}
                disabled={files.length === 0 || uploadState === 'uploading'}
                className="flex items-center gap-2 px-5 py-2 bg-doj-blue/20 border border-doj-blue/50 text-doj-blue rounded-lg text-sm font-semibold hover:bg-doj-blue/30 transition-all disabled:opacity-40 disabled:cursor-not-allowed shadow-[0_0_12px_#3b82f620]"
              >
                <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" d="M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5m-13.5-9L12 3m0 0l4.5 4.5M12 3v13.5" />
                </svg>
                Upload {files.length > 0 ? `${files.length} file${files.length > 1 ? 's' : ''}` : 'Files'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
