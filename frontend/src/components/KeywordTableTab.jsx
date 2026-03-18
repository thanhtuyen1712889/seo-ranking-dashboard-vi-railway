import { deltaTone, formatDateLabel, formatDelta, formatRank, rankTone } from "../lib/format";

export default function KeywordTableTab({
  data,
  filters,
  setFilters,
  mode,
  onExport,
  exporting,
  onOpenKeyword,
}) {
  if (!data) {
    return null;
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

        <div className="mt-4 flex flex-wrap gap-3">
          <button className="button-secondary" type="button" onClick={onExport} disabled={exporting}>
            {exporting ? "Đang xuất..." : "Download filtered view as Excel"}
          </button>
        </div>
      </div>

      <div className="panel-grid overflow-hidden p-0">
        <div className="overflow-x-auto">
          <table className="min-w-full border-separate border-spacing-0 text-sm">
            <thead className="sticky top-0 z-10 bg-[#0f1723]">
              <tr>
                <th className="border-b border-white/10 px-4 py-3 text-left font-semibold text-slate-400">#</th>
                <th className="border-b border-white/10 px-4 py-3 text-left font-semibold text-slate-400">Bộ</th>
                <th className="border-b border-white/10 px-4 py-3 text-left font-semibold text-slate-400">Keyword</th>
                {mode === "team" ? <th className="border-b border-white/10 px-4 py-3 text-left font-semibold text-slate-400">Vol</th> : null}
                {mode === "team" ? <th className="border-b border-white/10 px-4 py-3 text-left font-semibold text-slate-400">Best Rank</th> : null}
                {data.dates.map((date) => (
                  <th key={date} className="border-b border-white/10 px-3 py-3 text-center font-semibold text-slate-400">
                    {formatDateLabel(date)}
                  </th>
                ))}
                <th className="border-b border-white/10 px-4 py-3 text-left font-semibold text-slate-400">Thay Đổi</th>
                <th className="border-b border-white/10 px-4 py-3 text-left font-semibold text-slate-400">KPI Status</th>
              </tr>
            </thead>
            <tbody>
              {data.rows.map((row) => (
                <tr key={row.id} className="cursor-pointer transition hover:bg-white/[0.03]" onClick={() => onOpenKeyword(row.id)}>
                  <td className="border-b border-white/5 px-4 py-4 text-slate-400">{row.index}</td>
                  <td className="border-b border-white/5 px-4 py-4 text-slate-300">{row.group_name}</td>
                  <td className="border-b border-white/5 px-4 py-4">
                    <div>
                      <p className="font-semibold text-white">{row.keyword}</p>
                      <p className="mt-1 text-xs text-slate-500">{row.cluster_name}</p>
                    </div>
                  </td>
                  {mode === "team" ? <td className="border-b border-white/5 px-4 py-4 text-slate-300">{row.search_volume || "—"}</td> : null}
                  {mode === "team" ? <td className="border-b border-white/5 px-4 py-4 text-slate-300">{formatRank(row.best_rank)}</td> : null}
                  {data.dates.map((date) => {
                    const rank = row.positions[date];
                    return (
                      <td key={`${row.id}-${date}`} className="border-b border-white/5 px-2 py-4 text-center">
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
                  <td className={`border-b border-white/5 px-4 py-4 font-bold ${deltaTone(row.delta_prev)}`}>{formatDelta(row.delta_prev)}</td>
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

