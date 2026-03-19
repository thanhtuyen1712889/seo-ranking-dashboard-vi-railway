import { useEffect, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Line,
  LineChart,
  Pie,
  PieChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { formatDateLabel, formatDateTime } from "../lib/format";

const PALETTE = ["#38bdf8", "#2dd4bf", "#7ee787", "#f59e0b", "#fb7185", "#f97316"];

export default function OverviewTab({
  overview,
  mode,
  generatingInsight,
  onGenerateInsight,
  onSaveWeeklyNote,
  savingWeeklyNote,
  manualEvent,
  setManualEvent,
  onAddEvent,
  addingEvent,
}) {
  const [editingNote, setEditingNote] = useState(false);
  const [weeklyDraft, setWeeklyDraft] = useState("");

  useEffect(() => {
    setWeeklyDraft(overview?.latest_insight?.content_vi || "");
    setEditingNote(false);
  }, [overview?.latest_insight?.generated_at, overview?.latest_insight?.content_vi]);

  if (!overview?.dates?.length) {
    return (
      <div className="panel-grid text-center">
        <p className="text-lg font-semibold text-white">Chưa có dữ liệu ranking</p>
        <p className="mt-2 text-sm text-slate-400">
          Hãy tải file Excel/CSV hoặc kết nối Google Sheets ở phần Cài đặt để bắt đầu.
        </p>
      </div>
    );
  }

  const groupNames = overview.group_names || [];
  const chartData = overview.timeline.map((item) => ({
    label: item.label,
    date: item.date,
    ...item.groups,
  }));
  const avgTrendData = overview.avg_trend || [];

  return (
    <div className="space-y-6">
      <div className="grid gap-4 lg:grid-cols-4">
        {overview.summary_cards.map((card, index) => (
          <div key={`${card.name}-${index}`} className="panel-grid">
            <p className="text-xs uppercase tracking-[0.25em] text-slate-500">{card.name}</p>
            <div className="mt-4 flex items-end justify-between gap-3">
              <div>
                <p className="text-3xl font-bold text-white">{card.value}</p>
                <p className="mt-2 text-sm text-slate-400">
                  {card.subtitle || `KPI ${card.achieved}/${card.kpi_target}`}
                </p>
              </div>
              <div className="rounded-2xl border border-white/10 bg-white/5 px-3 py-2 text-right">
                <p className="text-xs uppercase tracking-wider text-slate-500">% đạt</p>
                <p className="text-lg font-bold text-neon-cyan">{card.percent || 0}%</p>
              </div>
            </div>
          </div>
        ))}
      </div>

      <div className={`rounded-[32px] border bg-gradient-to-br p-6 ${mode === "client" ? "border-neon-yellow/45 from-neon-yellow/18 to-neon-orange/8 shadow-warning" : "border-neon-yellow/30 from-neon-yellow/12 to-transparent shadow-warning"}`}>
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div className="min-w-0 flex-1 lg:pr-4">
            <p className="text-xs uppercase tracking-[0.3em] text-neon-yellow">⚡ Nhận xét hàng tuần</p>
            <textarea
              className={`mt-3 min-h-[220px] w-full resize-y rounded-[24px] border border-white/10 bg-black/20 px-5 py-4 text-slate-100 outline-none transition ${mode === "client" ? "text-lg leading-8 lg:min-h-[260px]" : "text-base leading-8 lg:min-h-[240px]"} ${editingNote ? "ring-2 ring-neon-yellow/30" : ""}`}
              value={weeklyDraft}
              readOnly={!editingNote}
              onChange={(event) => setWeeklyDraft(event.target.value)}
              placeholder="Chưa có nhận xét cho mốc dữ liệu hiện tại."
            />
            <p className="mt-3 text-xs text-slate-400">
              {overview.latest_insight?.generated_at
                ? `Cập nhật ghi chú: ${formatDateTime(overview.latest_insight.generated_at)}`
                : "Bạn có thể tự tạo nhận xét mới hoặc nhập ghi chú tay cho tuần này."}
            </p>
          </div>
          <div className="flex shrink-0 flex-wrap gap-3 lg:w-[260px] lg:justify-end">
            <button className="button-secondary" type="button" onClick={onGenerateInsight} disabled={generatingInsight}>
              {generatingInsight ? "Đang tạo..." : "Tạo nhận xét mới"}
            </button>
            {editingNote ? (
              <>
                <button className="button-primary" type="button" onClick={() => onSaveWeeklyNote(weeklyDraft)} disabled={savingWeeklyNote || !weeklyDraft.trim()}>
                  {savingWeeklyNote ? "Đang lưu..." : "Lưu nhận xét"}
                </button>
                <button className="button-secondary" type="button" onClick={() => {
                  setWeeklyDraft(overview.latest_insight?.content_vi || "");
                  setEditingNote(false);
                }}>
                  Hủy sửa
                </button>
              </>
            ) : (
              <button className="button-secondary" type="button" onClick={() => setEditingNote(true)}>
                Chỉnh sửa tay
              </button>
            )}
          </div>
        </div>
      </div>

      <div className="grid gap-6 xl:grid-cols-[1.3fr,0.9fr]">
        <div className="panel-grid">
          <div className="mb-5 flex items-center justify-between gap-4">
            <div>
              <p className="text-xs uppercase tracking-[0.3em] text-slate-500">Các biến động đáng chú ý</p>
              <h2 className="mt-2 text-xl font-bold text-white">Timeline KPI theo bộ từ khóa</h2>
            </div>
            <p className="text-xs text-slate-400">Trục Y = số keyword đạt KPI</p>
          </div>
          <div className="h-[360px]">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={chartData}>
                <CartesianGrid stroke="rgba(255,255,255,0.06)" vertical={false} />
                <XAxis dataKey="label" stroke="#8b949e" tick={{ fontSize: 12 }} />
                <YAxis stroke="#8b949e" tick={{ fontSize: 12 }} />
                <Tooltip
                  contentStyle={{
                    background: "#11161f",
                    border: "1px solid rgba(255,255,255,0.08)",
                    borderRadius: 18,
                  }}
                />
                <Legend />
                {groupNames.map((groupName, index) => (
                  <Line
                    key={groupName}
                    type="monotone"
                    dataKey={groupName}
                    stroke={PALETTE[index % PALETTE.length]}
                    strokeWidth={3}
                    dot={false}
                    activeDot={{ r: 6 }}
                  />
                ))}
                {(overview.events || []).slice(0, 20).map((event) => (
                  <ReferenceLine
                    key={`${event.id || event.title}-${event.event_date}`}
                    x={formatDateLabel(event.event_date)}
                    stroke="rgba(251,113,133,0.5)"
                    strokeDasharray="4 4"
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>

        <div className="panel-grid">
          <div className="mb-4">
            <p className="text-xs uppercase tracking-[0.3em] text-slate-500">Sự kiện & cảnh báo</p>
            <h2 className="mt-2 text-xl font-bold text-white">Các biến động ranking đáng chú ý</h2>
          </div>
          <div className="space-y-3">
            {(overview.events || []).slice(0, 8).map((event) => (
              <div key={`${event.id || event.title}-${event.event_date}`} className="rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-4">
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <p className="font-semibold text-white">{event.title}</p>
                    <p className="mt-1 text-sm text-slate-400">{event.description}</p>
                  </div>
                  <span className="chip">{formatDateLabel(event.event_date)}</span>
                </div>
              </div>
            ))}
          </div>

          <form className="mt-5 space-y-3 rounded-[28px] border border-white/10 bg-black/10 p-4" onSubmit={onAddEvent}>
            <p className="text-sm font-semibold text-white">Thêm sự kiện thủ công</p>
            <div className="grid gap-3 md:grid-cols-2">
              <input
                className="input-dark"
                type="date"
                value={manualEvent.event_date}
                onChange={(event) => setManualEvent({ ...manualEvent, event_date: event.target.value })}
              />
              <input
                className="input-dark"
                value={manualEvent.title}
                onChange={(event) => setManualEvent({ ...manualEvent, title: event.target.value })}
                placeholder="Ví dụ: Triển khai on-page 5 bài SEO"
              />
            </div>
            <textarea
              className="input-dark min-h-24"
              value={manualEvent.description}
              onChange={(event) => setManualEvent({ ...manualEvent, description: event.target.value })}
              placeholder="Mô tả ngắn tác động hoặc phạm vi thay đổi"
            />
            <button className="button-secondary" type="submit" disabled={addingEvent || !manualEvent.title || !manualEvent.event_date}>
              {addingEvent ? "Đang lưu..." : "Lưu sự kiện"}
            </button>
          </form>
        </div>
      </div>

      <div className="grid gap-6 xl:grid-cols-2">
        <div className="panel-grid">
          <div className="mb-4">
            <p className="text-xs uppercase tracking-[0.3em] text-slate-500">% đạt KPI hiện tại</p>
            <h2 className="mt-2 text-xl font-bold text-white">Tỷ lệ đạt KPI theo bộ</h2>
          </div>
          <div className="h-[320px]">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie data={overview.donut} dataKey="value" nameKey="name" innerRadius={72} outerRadius={108} paddingAngle={4}>
                  {overview.donut.map((item, index) => (
                    <Cell key={item.name} fill={PALETTE[index % PALETTE.length]} />
                  ))}
                </Pie>
                <Tooltip
                  formatter={(value, _, payload) => [`${value}%`, `${payload.payload.name}`]}
                  contentStyle={{
                    background: "#11161f",
                    border: "1px solid rgba(255,255,255,0.08)",
                    borderRadius: 18,
                  }}
                />
                <Legend />
              </PieChart>
            </ResponsiveContainer>
          </div>
        </div>

        <div className="panel-grid">
          <div className="mb-4">
            <p className="text-xs uppercase tracking-[0.3em] text-slate-500">Phân bổ thứ hạng</p>
            <h2 className="mt-2 text-xl font-bold text-white">Rank distribution hiện tại</h2>
          </div>
          <div className="h-[320px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={overview.distribution}>
                <CartesianGrid stroke="rgba(255,255,255,0.06)" vertical={false} />
                <XAxis dataKey="name" stroke="#8b949e" tick={{ fontSize: 12 }} />
                <YAxis stroke="#8b949e" tick={{ fontSize: 12 }} />
                <Tooltip
                  contentStyle={{
                    background: "#11161f",
                    border: "1px solid rgba(255,255,255,0.08)",
                    borderRadius: 18,
                  }}
                />
                <Bar dataKey="value" radius={[10, 10, 0, 0]}>
                  {overview.distribution.map((item, index) => (
                    <Cell key={item.name} fill={PALETTE[index % PALETTE.length]} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      <div className="panel-grid">
        <div className="mb-5 flex flex-col gap-2 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <p className="text-xs uppercase tracking-[0.3em] text-slate-500">AVG Ranking Trend</p>
            <h2 className="mt-2 text-xl font-bold text-white">Xu hướng thứ hạng trung bình</h2>
          </div>
          <p className="text-xs text-slate-400">Lưu ý: Rank càng thấp càng tốt</p>
        </div>
        <div className="h-[360px]">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={avgTrendData}>
              <CartesianGrid stroke="rgba(255,255,255,0.06)" vertical={false} />
              <XAxis dataKey="label" stroke="#8b949e" tick={{ fontSize: 12 }} />
              <YAxis stroke="#8b949e" tick={{ fontSize: 12 }} reversed />
              <Tooltip
                labelFormatter={(value, payload) => {
                  const date = payload?.[0]?.payload?.date;
                  return date ? `${value} • ${formatDateTime(date)}` : value;
                }}
                contentStyle={{
                  background: "#11161f",
                  border: "1px solid rgba(255,255,255,0.08)",
                  borderRadius: 18,
                }}
              />
              <Legend />
              {groupNames.map((groupName, index) => (
                <Line
                  key={groupName}
                  type="monotone"
                  dataKey={groupName}
                  stroke={PALETTE[index % PALETTE.length]}
                  strokeWidth={3}
                  dot={false}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
}
