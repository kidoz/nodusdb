export const brandColors = [
  {
    name: 'Signal',
    value: '#0F766E',
    usage: 'Primary actions, live state, active navigation',
  },
  {
    name: 'Ink',
    value: '#111827',
    usage: 'Primary text and high-emphasis UI',
  },
  {
    name: 'Steel',
    value: '#475569',
    usage: 'Secondary text, helper labels, quiet controls',
  },
  {
    name: 'Canvas',
    value: '#F8FAFC',
    usage: 'Application background and section bands',
  },
  {
    name: 'Warning',
    value: '#D97706',
    usage: 'Degraded state and delayed operations',
  },
  {
    name: 'Critical',
    value: '#DC2626',
    usage: 'Unavailable state, alerts, failed checks',
  },
]

export const brandPrinciples = [
  'Operational over ornamental',
  'Dense enough for repeated use',
  'Status should be readable before decoration',
  'Every risky action needs clear state and audit context',
]

export type StatusTone = 'healthy' | 'degraded' | 'critical' | 'neutral'
