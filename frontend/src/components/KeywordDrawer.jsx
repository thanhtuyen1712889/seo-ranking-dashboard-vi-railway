import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { formatDateLabel, formatRank } from "../lib/format";

export default function KeywordDrawer({
  open,
  detail,
  mode,
  insightLoading,
  noteDraft,
  setNoteDraft,
  onClose,
  onSaveNotes,
  onGenerateInsight,
  savingNotes,
  readOnly = false,
}) {
  if (!open || !detail) return null;

  const chartData = (detail.history || []).map((item) => ({
    ...item,
    label: formatDateLabel(item.rank_date),
  }));

  return (
    <div className="fixed inset-0 z-40 flex justify-end bg-black/45 backdrop-blur-sm">
      <div className="h-full w-full max-w-2xl overflow-y-auto border-l border-white/10 bg-[#0b121b]/95 px-6 py-6 shadow-2xl">
        <div className="flex items-start justify-between gap-4">
          <div>
            <p className="text-xs uppercase tracking-[0.3em] text-neon-cyan">
              Chi tiết keyword
            </p>
            <h2 className="mt-2 text-2xl font-bold text-white">{detail.keyword.keyword}</h2>
            <p className="mt-2 text-sm text-slate-400">
              {detail.keyword.group_name} · {detail.keyword.cluster_name} · {detail.client_badge}
            </p>
          </div>
          <button className="button-secondary" type="button" onClick={onClose}>
            Đóng
          </button>
        </div>

        <div className="mt-6 grid gap-4 md:grid-cols-3">
          <div className="rounded-[28px] border border-white/10 bg-white/[0.03] p-4">
            <p className="text-xs uppercase tracking-[0.2em] text-slate-500">Best Rank</p>
            <p className="mt-2 text-2xl font-bold text-white">
              {mode === "client" ? "Ẩn ở chế độ khách hàng" : formatRank(detail.keyword.best_rank)}
            </p>
          </div>
          <div className="rounded-[28px] border border-white/10 bg-white/[0.03] p-4">
            <p className="text-xs uppercase tracking-[0.2em] text-slate-500">Vol</p>
            <p className="mt-2 text-2xl font-bold text-white">
              {mode === "client" ? "Ẩn" : detail.keyword.search_volume || "—"}
            </p>
          </div>
          <div className="rounded-[28px] border border-white/10 bg-white/[0.03] p-4">
            <p className="text-xs uppercase tracking-[0.2em] text-slate-500">KPI</p>
            <p className="mt-2 text-2xl font-bold text-white">Top {detail.keyword.kpi_target}</p>
          </div>
        </div>

        <div className="mt-6 rounded-[32px] border border-white/10 bg-white/[0.03] p-5">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-xs uppercase tracking-[0.3em] text-slate-500">Lịch sử ranking</p>
              <h3 className="mt-2 text-xl font-bold text-white">Biểu đồ keyword</h3>
            </div>
          </div>
          <div className="mt-4 h-[280px]">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={chartData}>
                <CartesianGrid stroke="rgba(255,255,255,0.06)" vertical={false} />
                <XAxis dataKey="label" stroke="#8b949e" tick={{ fontSize: 12 }} />
                <YAxis stroke="#8b949e" tick={{ fontSize: 12 }} reversed />
                <Tooltip
                  formatter={(value) => [`Top ${formatRank(value)}`, "Vị trí"]}
                  contentStyle={{
                    background: "#11161f",
                    border: "1px solid rgba(255,255,255,0.08)",
                    borderRadius: 18,
                  }}
                />
                <Line type="monotone" dataKey="position" stroke="#38bdf8" strokeWidth={3} dot={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>

        <div className="mt-6 rounded-[32px] border border-neon-yellow/25 bg-neon-yellow/10 p-5">
          <div className="flex items-center justify-between gap-3">
            <div>
              <p className="text-xs uppercase tracking-[0.3em] text-neon-yellow">AI Insight</p>
              <p className="mt-2 whitespace-pre-line text-sm leading-7 text-slate-100">
                {detail.latest_insight?.content_vi || "Chưa có insight cho keyword này."}
              </p>
            </div>
            {!readOnly ? (
              <button className="button-secondary shrink-0" type="button" onClick={onGenerateInsight} disabled={insightLoading}>
                {insightLoading ? "Đang tạo..." : "Tạo insight"}
              </button>
            ) : null}
          </div>
        </div>

        <div className="mt-6 rounded-[32px] border border-white/10 bg-white/[0.03] p-5">
          <p className="text-sm font-semibold text-white">{readOnly ? "Ghi chú hiện có" : "Ghi chú nội bộ"}</p>
          <textarea
            className="input-dark mt-3 min-h-28"
            value={noteDraft}
            onChange={(event) => setNoteDraft(event.target.value)}
            placeholder={readOnly ? "Chưa có ghi chú cho keyword này" : "Thêm ghi chú cho keyword này"}
            readOnly={readOnly}
          />
          {!readOnly ? (
            <div className="mt-3 flex justify-end">
              <button className="button-primary" type="button" onClick={onSaveNotes} disabled={savingNotes}>
                {savingNotes ? "Đang lưu..." : "Lưu ghi chú"}
              </button>
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}
