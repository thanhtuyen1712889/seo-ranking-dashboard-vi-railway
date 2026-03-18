import { createClusterInsight } from "../lib/api";
import { deltaTone, formatDelta, formatRank, rankTone } from "../lib/format";

export default function GroupTab({
  token,
  projectId,
  data,
  filters,
  setFilters,
  mode,
  onInsightCreated,
  setToast,
}) {
  if (!data?.groups?.length) {
    return (
      <div className="panel-grid text-center">
        <p className="text-lg font-semibold text-white">Chưa có dữ liệu theo bộ</p>
      </div>
    );
  }

  async function handleClusterInsight(clusterName) {
    try {
      const response = await createClusterInsight(token, projectId, clusterName);
      setToast({ type: "success", message: response.content_vi || "Đã tạo insight cụm." });
      onInsightCreated?.();
    } catch (error) {
      setToast({ type: "error", message: error.message });
    }
  }

  return (
    <div className="space-y-6">
      <div className="panel-grid">
        <div className="grid gap-3 xl:grid-cols-4">
          <label className="text-sm font-semibold text-white">
            Ngày xem hiện tại
            <select className="input-dark mt-2" value={filters.current_date || ""} onChange={(event) => setFilters({ ...filters, current_date: event.target.value })}>
              {data.dates.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
          </label>

          <label className="text-sm font-semibold text-white">
            So sánh với ngày
            <select className="input-dark mt-2" value={filters.baseline_date || ""} onChange={(event) => setFilters({ ...filters, baseline_date: event.target.value })}>
              {data.dates.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
          </label>

          <label className="text-sm font-semibold text-white">
            Chỉ hiển thị
            <select className="input-dark mt-2" value={filters.status || "all"} onChange={(event) => setFilters({ ...filters, status: event.target.value })}>
              <option value="all">Tất cả</option>
              <option value="down">🔴 Giảm</option>
              <option value="up">🟢 Tăng</option>
              <option value="stable">⚪ Ổn định</option>
              <option value="lost">⚠️ Ngoài top</option>
              <option value="rising">🚀 Đang tăng mạnh</option>
              <option value="declining">📉 Đang giảm</option>
            </select>
          </label>

          <div className="rounded-[28px] border border-neon-cyan/20 bg-neon-cyan/5 px-4 py-4 text-sm text-slate-300">
            <p className="font-semibold text-white">Dynamic tags</p>
            <p className="mt-1">
              Rising, Stable, Declining, Lost và milestone top 10/5/3 được tính tự động từ 3 mốc gần nhất.
            </p>
          </div>
        </div>
      </div>

      {data.groups.map((group) => (
        <section key={group.name} className="panel-grid">
          <div className="flex flex-col gap-4 border-b border-white/10 pb-5 lg:flex-row lg:items-center lg:justify-between">
            <div>
              <p className="text-xs uppercase tracking-[0.3em] text-neon-blue">{group.name}</p>
              <h2 className="mt-2 text-2xl font-bold text-white">
                {group.name} · KPI Top{group.kpi_target} · {group.keyword_count}kw
              </h2>
              <p className="mt-2 text-sm text-slate-400">
                Hiện {data.current_date}: {group.achieved}/{group.keyword_count}kw đạt KPI · {group.peak_info}
              </p>
            </div>
            <button className="button-secondary" type="button" onClick={() => handleClusterInsight(group.name)}>
              Tạo insight cụm
            </button>
          </div>

          <div className="mt-5 space-y-3">
            {group.keywords.map((item) => (
              <div key={item.id} className="rounded-[28px] border border-white/10 bg-white/[0.03] px-4 py-4">
                <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
                  <div className="min-w-0 flex-1">
                    <div className="flex flex-wrap items-center gap-2">
                      <p className="truncate text-base font-semibold text-white">{item.keyword}</p>
                      <span className="chip">{item.cluster_name}</span>
                      {mode === "team" && item.search_volume ? <span className="chip">Vol {item.search_volume}</span> : null}
                    </div>
                    <p className="mt-2 text-sm text-slate-400">{item.sub_cluster_name}</p>
                  </div>

                  <div className="flex w-full flex-col gap-3 lg:w-[420px]">
                    <div className="flex items-center justify-between gap-3">
                      {mode === "client" ? (
                        <span className="rounded-full border border-white/10 px-4 py-2 text-sm font-semibold text-white">
                          {item.client_badge}
                        </span>
                      ) : (
                        <span className={`rounded-full border px-4 py-2 text-sm font-bold ${rankTone(item.current_rank)}`}>
                          Top {formatRank(item.current_rank)}
                        </span>
                      )}
                      <span className={`text-sm font-bold ${deltaTone(item.delta_prev)}`}>{formatDelta(item.delta_prev)}</span>
                    </div>

                    <div>
                      <div className="mb-2 flex items-center justify-between text-xs text-slate-500">
                        <span>Tiến độ KPI Top {item.kpi_target}</span>
                        <span>{Math.round(item.progress)}%</span>
                      </div>
                      <div className="h-3 overflow-hidden rounded-full bg-white/5">
                        <div
                          className={`h-full rounded-full ${item.current_rank <= item.kpi_target ? "bg-gradient-to-r from-neon-green to-neon-cyan" : "bg-gradient-to-r from-neon-orange to-neon-red"}`}
                          style={{ width: `${item.progress}%` }}
                        />
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </section>
      ))}
    </div>
  );
}

