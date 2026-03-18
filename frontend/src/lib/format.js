export function formatDateLabel(value) {
  if (!value) return "--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("vi-VN", {
    day: "2-digit",
    month: "2-digit",
  }).format(date);
}

export function formatDateTime(value) {
  if (!value) return "--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("vi-VN", {
    hour: "2-digit",
    minute: "2-digit",
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
  }).format(date);
}

export function formatRank(value) {
  if (value === null || value === undefined) return "—";
  if (value >= 101) return "101+";
  return Number.isInteger(value) ? String(value) : value.toFixed(1);
}

export function formatDelta(value) {
  if (value === null || value === undefined) return "—";
  if (value < 0) return `▲${Math.abs(value)}`;
  if (value > 0) return `▼${value}`;
  return "—";
}

export function deltaTone(value) {
  if (value === null || value === undefined || value === 0) return "text-slate-400";
  return value < 0 ? "text-neon-green" : "text-neon-red";
}

export function rankTone(value) {
  if (value === null || value === undefined) return "bg-slate-900/70 text-slate-400";
  if (value <= 5) return "bg-neon-green/15 text-neon-green border-neon-green/30";
  if (value <= 10) return "bg-neon-blue/15 text-neon-blue border-neon-blue/30";
  if (value <= 20) return "bg-neon-yellow/15 text-neon-yellow border-neon-yellow/30";
  if (value <= 50) return "bg-neon-orange/15 text-neon-orange border-neon-orange/30";
  return "bg-neon-red/15 text-neon-red border-neon-red/30";
}

