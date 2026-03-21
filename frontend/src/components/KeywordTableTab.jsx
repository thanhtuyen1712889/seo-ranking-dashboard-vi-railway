import { deltaTone, formatDateLabel, formatDelta, formatRank, rankTone } from "../lib/format";

export default function KeywordTableTab({
  data,
  filters,
  setFilters,
  mode,
  onExport,
  exporting,
  onOpenKeyword,
  readOnly = false,
  showExportButton = true,
}) {
  if (!data) {
    return null;
  }

  const currentDate = data.current_date || data.dates[data.dates.length - 1];
  const orderedDates = [...data.dates].reverse();
  const currentSortBy = data.sort_by || filters.sort_by || "current_rank";
  const currentSortDir = data.sort_dir || filters.sort_dir || "asc";
  const stickyColumns =
    mode === "team"
      ? [
          { left: 0, width: 72 },
          { left: 72, width: 124 },
          { left: 196, width: 260 },
          { left: 456, width: 88 },
          { left: 544, width: 96 },
          { left: 640, width: 96 },
        ]
      : [
          { left: 0, width: 72 },
          { left: 72, width: 124 },
          { left: 196, width: 260 },
          { left: 456, width: 96 },
        ];

  function stickyProps(index, isHeader = false) {
    const config = stickyColumns[index];
    if (!config) return {};
    return {
      style: {
        left: `${config.left}px`,
        minWidth: `${config.width}px`,
        width: `${config.width}px`,
        ...(isHeader ? { top: "0px" } : {}),
      },
      className: `sticky ${isHeader ? "z-20 bg-[#0f1723]" : "z-10 bg-[#111723]"} ${index === stickyColumns.length - 1 ? "shadow-[10px_0_24px_rgba(3,7,18,0.55)]" : ""}`,
    };
  }

  function nextSortDirection(columnKey) {
    if (currentSortBy === columnKey) {
      return currentSortDir === "asc" ? "desc" : "asc";
    }
    return "asc";
  }

  function applySort(columnKey) {
    setFilters({
      ...filters,
      sort_by: columnKey,
      sort_dir: nextSortDirection(columnKey),
    });
  }

  function sortArrow(columnKey) {
    if (currentSortBy !== columnKey) return "↕";
    return currentSortDir === "asc" ? "▲" : "▼";
  }

  function sortArrowClass(columnKey) {
    return currentSortBy === columnKey ? "text-neon-cyan" : "text-slate-500";
  }

  function currentSortLabel() {
    if (currentSortBy.startsWith("date:")) {
      const sortDate = currentSortBy.split(":")[1];
      return `Ngày ${formatDateLabel(sortDate)}`;
    }
    const labels = {
      index: "#",
      group_name: "Bộ",
      keyword: "Keyword",
      search_volume: "Vol",
      best_rank: "Best Rank",
      current_rank: "Hạng hiện tại",
      delta_prev: "Thay Đổi",
      kpi_status: "KPI Status",
    };
    return labels[currentSortBy] || "Hạng hiện tại";
  }

  function SortableHeaderCell({ label, columnKey, className = "", stickyIndex = null }) {
    const sticky = stickyIndex === null ? null : stickyProps(stickyIndex, true);
    return (
      <th
        {...(sticky || {})}
        className={`${sticky?.className || ""} top-0 border-b border-white/10 px-4 py-3 text-left font-semibold ${className}`}
      >
        <button
          type="button"
          className="inline-flex items-center gap-2 text-inherit transition hover:text-white"
          onClick={() => applySort(columnKey)}
          aria-label={`Sắp xếp theo ${label}`}
          title={`Sắp xếp theo ${label}`}
        >
          <span>{label}</span>
          <span className={`text-[11px] ${sortArrowClass(columnKey)}`}>{sortArrow(columnKey)}</span>
        </button>
      </th>
    );
  }

  return (
    <div className="space-y-6">
      <div className="panel-grid">
        <div className="grid gap-3 xl:grid-cols-4">
          <input
            className="input-dark"
            value={filters.search}
            onChange={(event) => setFilters({ ...filters, search: event.target.value })}
            placeholder="Tìm keyword"
          />

          <select className="input-dark" value={filters.groups} onChange={(event) => setFilters({ ...filters, groups: event.target.value })}>
            <option value="">Tất cả bộ</option>
            {data.groups.map((group) => (
              <option key={group} value={group}>
                {group}
              </option>
            ))}
          </select>

          <select className="input-dark" value={filters.clusters} onChange={(event) => setFilters({ ...filters, clusters: event.target.value })}>
            <option value="">Tất cả cụm</option>
            {data.clusters.map((cluster) => (
              <option key={cluster} value={cluster}>
                {cluster}
              </option>
            ))}
          </select>

          <select className="input-dark" value={filters.status} onChange={(event) => setFilters({ ...filters, status: event.target.value })}>
            <option value="all">Tất cả trạng thái</option>
            <option value="kpi_met">Đạt KPI</option>
            <option value="up">Tăng</option>
            <option value="down">Giảm</option>
            <option value="stable">Ổn định</option>
            <option value="lost">Ngoài top</option>
          </select>
        </div>

        <div className="mt-4 grid gap-3 xl:grid-cols-5">
          <input className="input-dark" type="number" value={filters.vol_min} onChange={(event) => setFilters({ ...filters, vol_min: event.target.value })} placeholder="Vol từ" />
          <input className="input-dark" type="number" value={filters.vol_max} onChange={(event) => setFilters({ ...filters, vol_max: event.target.value })} placeholder="Vol đến" />
          <input className="input-dark" type="number" value={filters.rank_min} onChange={(event) => setFilters({ ...filters, rank_min: event.target.value })} placeholder="Rank từ" />
          <input className="input-dark" type="number" value={filters.rank_max} onChange={(event) => setFilters({ ...filters, rank_max: event.target.value })} placeholder="Rank đến" />
          <label className="flex items-center gap-3 rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3 text-sm font-semibold text-white">
            <input
              type="checkbox"
              checked={filters.movers_only}
              onChange={(event) => setFilters({ ...filters, movers_only: event.target.checked })}
            />
            Chỉ top movers tuần này
          </label>
        </div>

        {showExportButton ? (
          <div className="mt-4 flex flex-wrap gap-3">
            <button className="button-secondary" type="button" onClick={onExport} disabled={exporting}>
              {exporting ? "Đang xuất..." : "Tải view đã lọc xuống Excel"}
            </button>
          </div>
        ) : null}
      </div>

      <div className="panel-grid overflow-hidden p-0">
        <div className="flex flex-wrap items-center gap-3 border-b border-white/10 px-5 py-4 text-sm text-slate-400">
          <span className="chip border-neon-cyan/30 bg-neon-cyan/10 text-neon-cyan">Ngày hiện tại nằm bên trái: {formatDateLabel(currentDate)}</span>
          <span className="chip">Ngày quá khứ kéo dần về bên phải</span>
          <span className="chip">Sắp xếp: {currentSortLabel()} ({currentSortDir === "asc" ? "tăng dần" : "giảm dần"})</span>
          <span>6 cột đầu và hàng tiêu đề đã được cố định để kéo ngang hoặc kéo dọc vẫn bám được dữ liệu.</span>
        </div>
        <div className="max-h-[72vh] overflow-auto">
          <table className="min-w-full border-separate border-spacing-0 text-sm">
            <thead>
              <tr>
                <SortableHeaderCell label="#" columnKey="index" className="text-slate-400" stickyIndex={0} />
                <SortableHeaderCell label="Bộ" columnKey="group_name" className="text-slate-400" stickyIndex={1} />
                <SortableHeaderCell label="Keyword" columnKey="keyword" className="text-slate-400" stickyIndex={2} />
                {mode === "team" ? <SortableHeaderCell label="Vol" columnKey="search_volume" className="text-slate-400" stickyIndex={3} /> : null}
                {mode === "team" ? <SortableHeaderCell label="Best Rank" columnKey="best_rank" className="text-slate-400" stickyIndex={4} /> : null}
                <SortableHeaderCell label="Thay Đổi" columnKey="delta_prev" className="text-slate-400" stickyIndex={mode === "team" ? 5 : 3} />
                {orderedDates.map((date) => (
                  <th
                    key={date}
                    className={`sticky top-0 z-10 border-b px-3 py-3 text-center font-semibold ${date === currentDate ? "border-neon-cyan/35 bg-[#0f1723] text-neon-cyan" : "border-white/10 bg-[#0f1723] text-slate-400"}`}
                  >
                    <button
                      type="button"
                      className="inline-flex items-center gap-2 transition hover:text-white"
                      onClick={() => applySort(`date:${date}`)}
                      aria-label={`Sắp xếp theo ngày ${formatDateLabel(date)}`}
                      title={`Sắp xếp theo ngày ${formatDateLabel(date)}`}
                    >
                      <span>{formatDateLabel(date)}</span>
                      <span className={`text-[11px] ${sortArrowClass(`date:${date}`)}`}>{sortArrow(`date:${date}`)}</span>
                    </button>
                  </th>
                ))}
                <th className="sticky top-0 z-10 border-b border-white/10 bg-[#0f1723] px-4 py-3 text-left font-semibold text-slate-400">
                  <button
                    type="button"
                    className="inline-flex items-center gap-2 transition hover:text-white"
                    onClick={() => applySort("kpi_status")}
                    aria-label="Sắp xếp theo KPI Status"
                    title="Sắp xếp theo KPI Status"
                  >
                    <span>KPI Status</span>
                    <span className={`text-[11px] ${sortArrowClass("kpi_status")}`}>{sortArrow("kpi_status")}</span>
                  </button>
                </th>
              </tr>
            </thead>
            <tbody>
              {data.rows.map((row) => (
                <tr
                  key={row.id}
                  className={`${onOpenKeyword ? "cursor-pointer transition hover:bg-white/[0.03]" : ""}`}
                  onClick={onOpenKeyword ? () => onOpenKeyword(row.id) : undefined}
                >
                  <td {...stickyProps(0)} className={`${stickyProps(0).className} border-b border-white/5 px-4 py-4 text-slate-400`}>{row.index}</td>
                  <td {...stickyProps(1)} className={`${stickyProps(1).className} border-b border-white/5 px-4 py-4 text-slate-300`}>{row.group_name}</td>
                  <td {...stickyProps(2)} className={`${stickyProps(2).className} border-b border-white/5 px-4 py-4`}>
                    <div>
                      <p className="font-semibold text-white">{row.keyword}</p>
                      <p className="mt-1 text-xs text-slate-500">{row.cluster_name}</p>
                    </div>
                  </td>
                  {mode === "team" ? <td {...stickyProps(3)} className={`${stickyProps(3).className} border-b border-white/5 px-4 py-4 text-slate-300`}>{row.search_volume || "—"}</td> : null}
                  {mode === "team" ? <td {...stickyProps(4)} className={`${stickyProps(4).className} border-b border-white/5 px-4 py-4 text-slate-300`}>{formatRank(row.best_rank)}</td> : null}
                  <td {...stickyProps(mode === "team" ? 5 : 3)} className={`${stickyProps(mode === "team" ? 5 : 3).className} border-b border-white/5 px-4 py-4 font-bold ${deltaTone(row.delta_prev)}`}>{formatDelta(row.delta_prev)}</td>
                  {orderedDates.map((date) => {
                    const rank = row.positions[date];
                    return (
                      <td key={`${row.id}-${date}`} className={`border-b border-white/5 px-2 py-4 text-center ${date === currentDate ? "bg-neon-cyan/4" : ""}`}>
                        {mode === "client" ? (
                          <span className="text-xs text-slate-300">{row.client_badge}</span>
                        ) : (
                          <span className={`inline-flex min-w-14 items-center justify-center rounded-full border px-3 py-1 text-xs font-bold ${rankTone(rank)}`}>
                            {formatRank(rank)}
                          </span>
                        )}
                      </td>
                    );
                  })}
                  <td className="border-b border-white/5 px-4 py-4 text-white">{row.kpi_status}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
