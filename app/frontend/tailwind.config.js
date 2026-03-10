/** @type {import('tailwindcss').Config} */
export default {
  content: [
    './index.html',
    './src/**/*.{js,ts,jsx,tsx}',
  ],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        'doj-bg': '#0f1117',
        'doj-surface': '#1a1f2e',
        'doj-surface-2': '#232838',
        'doj-border': '#2d3748',
        'doj-green': '#22c55e',
        'doj-amber': '#f59e0b',
        'doj-red': '#ef4444',
        'doj-blue': '#3b82f6',
        'doj-text': '#f1f5f9',
        'doj-muted': '#64748b',
        'doj-legacy': '#8b5cf6',
        'doj-open': '#06b6d4',
        'doj-adhoc': '#f97316',
      },
      animation: {
        'particle-flow': 'particleFlow 2s linear infinite',
        'particle-flow-slow': 'particleFlow 3s linear infinite',
        'particle-flow-fast': 'particleFlow 1.2s linear infinite',
        'pulse-glow': 'pulseGlow 2s ease-in-out infinite',
        'pulse-amber': 'pulseAmber 1.5s ease-in-out infinite',
        'pulse-red': 'pulseRed 1s ease-in-out infinite',
        'slide-in-right': 'slideInRight 0.3s ease-out forwards',
        'fade-in': 'fadeIn 0.2s ease-out forwards',
        'badge-pop': 'badgePop 0.4s cubic-bezier(0.68,-0.55,0.27,1.55) forwards',
        'flow-dash': 'flowDash 1.5s linear infinite',
        'scan-line': 'scanLine 4s linear infinite',
      },
      keyframes: {
        particleFlow: {
          '0%': { strokeDashoffset: '100' },
          '100%': { strokeDashoffset: '0' },
        },
        pulseGlow: {
          '0%, 100%': { boxShadow: '0 0 4px #22c55e40', borderColor: '#22c55e' },
          '50%': { boxShadow: '0 0 16px #22c55e80', borderColor: '#4ade80' },
        },
        pulseAmber: {
          '0%, 100%': { boxShadow: '0 0 4px #f59e0b40', borderColor: '#f59e0b' },
          '50%': { boxShadow: '0 0 20px #f59e0b80', borderColor: '#fbbf24' },
        },
        pulseRed: {
          '0%, 100%': { boxShadow: '0 0 4px #ef444440', borderColor: '#ef4444' },
          '50%': { boxShadow: '0 0 20px #ef444480', borderColor: '#f87171' },
        },
        slideInRight: {
          '0%': { transform: 'translateX(100%)', opacity: '0' },
          '100%': { transform: 'translateX(0)', opacity: '1' },
        },
        fadeIn: {
          '0%': { opacity: '0', transform: 'translateY(-4px)' },
          '100%': { opacity: '1', transform: 'translateY(0)' },
        },
        badgePop: {
          '0%': { transform: 'scale(0.5)', opacity: '0' },
          '100%': { transform: 'scale(1)', opacity: '1' },
        },
        flowDash: {
          '0%': { strokeDashoffset: '24' },
          '100%': { strokeDashoffset: '0' },
        },
        scanLine: {
          '0%': { transform: 'translateY(-100%)' },
          '100%': { transform: 'translateY(100%)' },
        },
      },
      fontFamily: {
        mono: ['JetBrains Mono', 'Fira Code', 'Consolas', 'monospace'],
      },
      backdropBlur: {
        xs: '2px',
      },
    },
  },
  plugins: [],
};
