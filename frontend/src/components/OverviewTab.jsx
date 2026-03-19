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

function renderInlineEmphasis(text) {
  if (!text) return null;
  const tokens = [];
  const pattern = /(\*\*[^*]+\*\*|\*[^*]+\*)/g;
  let lastIndex = 0;
  let match = pattern.exec(text);
  while (match) {
    if (match.index > lastIndex) {
      tokens.push({ type: "text", value: text.slice(lastIndex, match.index) });
    }
    const value = match[0];
    if (value.startsWith("**")) {
      tokens.push({ type: "strong", value: value.slice(2, -2) });
    } else {
      tokens.push({ type: "em", value: value.slice(1, -1) });
    }
    lastIndex = match.index + value.length;
    match = pattern.exec(text);
  }
  if (lastIndex < text.length) {
    tokens.push({ type: "text", value: text.slice(lastIndex) });
  }
  return tokens.map((token, index) => {
    if (token.type === "strong") {
      return (
        <strong key={`${token.type}-${index}`} className="font-semibold text-white">
          {token.value}
        </strong>
      );
    }
    if (token.type === "em") {
      return (
        <em key={`${token.type}-${index}`} className="italic text-slate-100">
          {token.value}
        </em>
      );
    }
    return <span key={`${token.type}-${index}`}>{token.value}</span>;
  });
}

function parseWeeklyNote(content) {
  const sections = [];
  const lines = (content || "").split("\n");
  let currentSection = null;

  const pushSection = () => {
    if (currentSection) {
      sections.push(currentSection);
    }
  };

  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line) continue;
    const strongHeading = line.match(/^\*\*(.+?)\*\*$/);
    const plainHeading = line.match(/^(Tổng quan|Các điểm sáng|Các điểm cần chú ý)\s*:?\s*$/i);
    if (strongHeading || plainHeading) {
      pushSection();
      currentSection = {
        title: strongHeading ? strongHeading[1] : plainHeading[1],
        items: [],
        paragraphs: [],
      };
      continue;
    }
    const bulletMatch = line.match(/^[-•]\s+(.+)$/);
    if (!currentSection) {
      currentSection = { title: "Tổng quan", items: [], paragraphs: [] };
    }
    if (bulletMatch) {
      currentSection.items.push(bulletMatch[1]);
    } else {
      currentSection.paragraphs.push(line);
    }
  }

  pushSection();
  return sections;
}

function WeeklyNoteContent({ content }) {
  const sections = parseWeeklyNote(content);
  if (!sections.length) {
    return <p className="text-slate-300">Chưa có nhận xét cho khoảng ngày hiện tại.</p>;
  }
  return (
    <div className="space-y-5">
      {sections.map((section) => (
        <section key={section.title} className="space-y-3">
          <h3 className="text-sm font-semibold uppercase tracking-[0.24em] text-neon-yellow">{section.title}</h3>
          {section.paragraphs.map((paragraph, index) => (
            <p key={`${section.title}-paragraph-${index}`} className="text-base leading-8 text-slate-100">
              {renderInlineEmphasis(paragraph)}
            </p>
          ))}
          {section.items.length ? (
            <ul className="space-y-3">
              {section.items.map((item, index) => (
                <li key={`${section.title}-item-${index}`} className="flex items-start gap-3 text-base leading-8 text-slate-100">
                  <span className="mt-3 inline-block h-2 w-2 rounded-full bg-neon-cyan" />
                  <span>{renderInlineEmphasis(item)}</span>
                </li>
              ))}
            </ul>
          ) : null}
        </section>
      ))}
    </div>
  );
}

export default function OverviewTab({
  overview,
  mode,
  weeklyNote,
  weeklyNoteRange,
  setWeeklyNoteRange,
  weeklyNoteLoading,
  generatingInsight,
  onGenerateInsight,
  onSaveWeeklyNote,
  savingWeeklyNote,
  manualEvent,
  setManualEvent,
  onAddEvent,
  addingEvent,
  readOnly = false,
}) {
  const [editingNote, setEditingNote] = useState(false);
  const [weeklyDraft, setWeeklyDraft] = useState("");
  const activeNote = weeklyNote || overview?.latest_insight || null;
  const dates = overview?.dates || [];

  useEffect(() => {
    setWeeklyDraft(activeNote?.content_vi || "");
    setEditingNote(false);
  }, [activeNote?.generated_at, activeNote?.content_vi, activeNote?.saved_at]);

  if (!dates.length) {
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
  const draftChanged = weeklyDraft.trim() !== (activeNote?.content_vi || "").trim();
  const canEditRange = !readOnly && typeof setWeeklyNoteRange === "function";
  const noteTimestamp = activeNote?.saved_at || activeNote?.generated_at || null;

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
            <p className="text-xs uppercase tracking-[0.3em] text-neon-yellow">Nhận xét</p>
            {canEditRange ? (
              <div className="mt-4 grid gap-3 md:grid-cols-2">
                <label className="text-sm font-semibold text-white">
                  Từ ngày
                  <select
                    className="input-dark mt-2"
                    value={weeklyNoteRange?.from_date || ""}
                    onChange={(event) => setWeeklyNoteRange((previous) => ({ ...previous, from_date: event.target.value }))}
                  >
                    {dates.map((date) => (
                      <option key={`from-${date}`} value={date}>
                        {formatDateLabel(date)}
                      </option>
                    ))}
                  </select>
                </label>
                <label className="text-sm font-semibold text-white">
                  Đến ngày
                  <select
                    className="input-dark mt-2"
                    value={weeklyNoteRange?.to_date || ""}
                    onChange={(event) => setWeeklyNoteRange((previous) => ({ ...previous, to_date: event.target.value }))}
                  >
                    {dates.map((date) => (
                      <option key={`to-${date}`} value={date}>
                        {formatDateLabel(date)}
                      </option>
                    ))}
                  </select>
                </label>
              </div>
            ) : null}
            {editingNote ? (
              <textarea
                className={`mt-3 min-h-[220px] w-full resize-y rounded-[24px] border border-white/10 bg-black/20 px-5 py-4 text-slate-100 outline-none transition ${mode === "client" ? "text-lg leading-8 lg:min-h-[260px]" : "text-base leading-8 lg:min-h-[240px]"} ring-2 ring-neon-yellow/30`}
                value={weeklyDraft}
                readOnly={false}
                onChange={(event) => setWeeklyDraft(event.target.value)}
                placeholder={weeklyNoteLoading ? "Đang đọc dữ liệu dashboard để tạo nhận xét..." : "Chưa có nhận xét cho khoảng ngày hiện tại."}
              />
            ) : (
              <div className={`mt-3 min-h-[220px] w-full rounded-[24px] border border-white/10 bg-black/20 px-5 py-5 ${mode === "client" ? "text-lg lg:min-h-[260px]" : "lg:min-h-[240px]"}`}>
                {weeklyNoteLoading ? (
                  <p className="text-slate-400">Đang đọc dữ liệu dashboard để tạo nhận xét...</p>
                ) : (
                  <WeeklyNoteContent content={weeklyDraft} />
                )}
              </div>
            )}
            <p className="mt-3 text-xs text-slate-400">
              {weeklyNoteLoading
                ? "Đang cập nhật nhận xét theo dữ liệu mới nhất..."
                : noteTimestamp
                  ? `Cập nhật ghi chú lúc: ${formatDateTime(noteTimestamp)}`
                  : "Nếu chưa có ghi chú đã ghim cho khoảng này, hệ thống sẽ tự tạo một nhận xét mới từ dữ liệu live."}
            </p>
          </div>
          {!readOnly ? (
            <div className="flex shrink-0 flex-wrap gap-3 lg:w-[260px] lg:justify-end">
              <button className="button-secondary" type="button" onClick={onGenerateInsight} disabled={generatingInsight}>
                {generatingInsight ? "Đang tạo..." : "Tạo nhận xét mới"}
              </button>
              {editingNote ? (
                <>
                  <button
                    className="button-primary"
                    type="button"
                    onClick={() => onSaveWeeklyNote(weeklyDraft, draftChanged ? "User" : (activeNote?.author || "AI"))}
                    disabled={savingWeeklyNote || !weeklyDraft.trim()}
                  >
                    {savingWeeklyNote ? "Đang lưu..." : "Lưu & ghim"}
                  </button>
                  <button
                    className="button-secondary"
                    type="button"
                    onClick={() => {
                      setWeeklyDraft(activeNote?.content_vi || "");
                      setEditingNote(false);
                    }}
                  >
                    Hủy sửa
                  </button>
                </>
              ) : (
                <button className="button-secondary" type="button" onClick={() => setEditingNote(true)}>
                  Chỉnh sửa tay
                </button>
              )}
            </div>
          ) : null}
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

          {!readOnly ? (
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
          ) : null}
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
