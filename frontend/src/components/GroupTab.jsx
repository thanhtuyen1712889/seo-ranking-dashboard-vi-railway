import { useEffect, useRef, useState } from "react";
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { formatDateLabel, formatDelta, formatRank } from "../lib/format";

const priorityTone = {
  "Rising & strong": "border-neon-green/30 bg-neon-green/12 text-neon-green",
  "Rising & opportunity": "border-neon-cyan/30 bg-neon-cyan/12 text-neon-cyan",
  Declining: "border-neon-red/30 bg-neon-red/12 text-neon-red",
  Stable: "border-white/10 bg-white/[0.04] text-slate-300",
};

const priorityDisplay = {
  "Rising & strong": "Tăng mạnh và đang khỏe",
  "Rising & opportunity": "Tăng và còn cơ hội",
  Declining: "Đang giảm",
  Stable: "Ổn định",
};

const trendTone = {
  rising: "text-neon-green",
  stable: "text-slate-300",
  declining: "text-neon-red",
};

const trendLabel = {
  rising: "Tăng",
  stable: "Ổn định",
  declining: "Giảm",
};

const trendArrow = {
  rising: "↗",
  stable: "→",
  declining: "↘",
};

function clusterRowTone(priority) {
  return priorityTone[priority] || priorityTone.Stable;
}

function currentRankLabel(value) {
  if (!value) return "Chưa có";
  if (value <= 5) return "Top 5";
  if (value <= 10) return "Top 10";
  if (value <= 20) return "Top 20";
  return `Top ${Math.round(value)}`;
}

export default function GroupTab({ data, filters, setFilters, mode, controlsMode = "full" }) {
  const [selectedClusterId, setSelectedClusterId] = useState("");
  const [expandedClusterId, setExpandedClusterId] = useState("");
  const [expandedSize, setExpandedSize] = useState({});
  const [scenarioMenuOpen, setScenarioMenuOpen] = useState(false);
  const scenarioMenuRef = useRef(null);

  useEffect(() => {
    if (!data) return;
    if (!filters.main_cluster && data.selected_main_cluster) {
      setFilters((previous) => ({ ...previous, main_cluster: data.selected_main_cluster }));
    }
    if (!filters.active_scenario_id && data.active_scenario_id) {
      setFilters((previous) => ({ ...previous, active_scenario_id: data.active_scenario_id }));
    }
  }, [data, filters.main_cluster, filters.active_scenario_id, setFilters]);

  useEffect(() => {
    const defaultClusterId = data?.trend_panel?.selected_cluster_id || data?.cluster_list?.[0]?.cluster_id || "";
    if (!defaultClusterId) {
      setSelectedClusterId("");
      setExpandedClusterId("");
      return;
    }
    setSelectedClusterId((previous) => (data?.cluster_list?.some((item) => item.cluster_id === previous) ? previous : defaultClusterId));
    setExpandedClusterId((previous) => (data?.cluster_list?.some((item) => item.cluster_id === previous) ? previous : defaultClusterId));
  }, [data]);

  useEffect(() => {
    if (!scenarioMenuOpen) return undefined;
    function handlePointerDown(event) {
      if (!scenarioMenuRef.current?.contains(event.target)) {
        setScenarioMenuOpen(false);
      }
    }
    window.addEventListener("pointerdown", handlePointerDown);
    return () => window.removeEventListener("pointerdown", handlePointerDown);
  }, [scenarioMenuOpen]);

  if (!data?.cluster_list?.length) {
    return (
      <div className="panel-grid text-center">
        <p className="text-lg font-semibold text-white">Chưa có dữ liệu cluster insight</p>
        <p className="mt-2 text-sm text-slate-400">
          Hãy chọn bộ dữ liệu có ranking theo ngày hoặc nới bộ lọc hiện tại để xem panel insight.
        </p>
      </div>
    );
  }

  const selectedCluster =
    data.cluster_list.find((item) => item.cluster_id === selectedClusterId) ||
    data.cluster_list[0];
  const selectedScenario =
    data.scenarios?.find((item) => item.scenario_id === data.active_scenario_id) ||
    data.scenarios?.[0] ||
    null;
  const selectedDrilldown =
    data.drilldown_tables.find((item) => item.cluster_id === selectedCluster?.cluster_id) || null;
  const visibleKeywords = selectedDrilldown
    ? selectedDrilldown.keywords.slice(0, expandedSize[selectedDrilldown.cluster_id] || 8)
    : [];

  function handleSelectCluster(clusterId) {
    setSelectedClusterId(clusterId);
    setExpandedClusterId((previous) => (previous === clusterId ? "" : clusterId));
  }

  return (
    <div className="space-y-6">
      <div className="panel-grid">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
          <div>
            <p className="text-sm font-semibold text-white">Đổi góc nhìn sub-cluster</p>
            <p className="mt-1 text-sm text-slate-400">
              Engine tự suy ra 2-3 kịch bản gom cụm từ chính dataset này. Bạn chỉ đổi view, còn tập keyword và dữ liệu gốc vẫn giữ nguyên.
            </p>
          </div>
          <div ref={scenarioMenuRef} className="relative">
            <button
              className="button-secondary min-w-[260px] justify-between gap-3"
              type="button"
              onClick={() => setScenarioMenuOpen((previous) => !previous)}
            >
              <span>Change sub-cluster view</span>
              <span className="truncate text-slate-300">
                {selectedScenario?.scenario_label || "Chọn góc nhìn"}
              </span>
            </button>

            {scenarioMenuOpen ? (
              <div className="absolute right-0 top-[calc(100%+12px)] z-20 w-[320px] rounded-[24px] border border-white/10 bg-[#0f1722] p-2 shadow-glow">
                {(data.scenarios || []).map((scenario) => {
                  const isActive = filters.active_scenario_id === scenario.scenario_id;
                  return (
                    <button
                      key={scenario.scenario_id}
                      className={`w-full rounded-[18px] px-4 py-3 text-left transition ${isActive ? "bg-neon-cyan/15 text-white" : "text-slate-300 hover:bg-white/[0.04]"}`}
                      type="button"
                      onClick={() => {
                        setFilters({ ...filters, active_scenario_id: scenario.scenario_id });
                        setScenarioMenuOpen(false);
                      }}
                    >
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <p className="font-semibold">{scenario.scenario_label}</p>
                          <p className="mt-1 text-xs leading-6 text-slate-400">{scenario.scenario_description}</p>
                        </div>
                        {isActive ? <span className="chip border-neon-cyan/30 bg-neon-cyan/10 text-neon-cyan">Đang xem</span> : null}
                      </div>
                    </button>
                  );
                })}
              </div>
            ) : null}
          </div>
        </div>
        <div className="mt-4 rounded-[24px] border border-neon-cyan/18 bg-neon-cyan/6 px-4 py-4 text-sm leading-7 text-slate-300">
          <p className="font-semibold text-white">Kịch bản đang xem</p>
          {selectedScenario ? (
            <p className="mt-2 font-medium text-white">{selectedScenario.scenario_label}</p>
          ) : null}
          <p className="mt-2">
            {data.insight_note_global || "Engine đang phân tích phân bố tag để gom cụm phù hợp nhất cho bộ dữ liệu này."}
          </p>
          {selectedScenario?.scenario_description ? (
            <p className="mt-2 text-xs uppercase tracking-[0.18em] text-slate-500">
              {selectedScenario.scenario_description}
            </p>
          ) : null}
        </div>
      </div>

      {controlsMode === "full" ? (
        <div className="panel-grid">
          <div className="grid gap-3 xl:grid-cols-6">
            <label className="text-sm font-semibold text-white">
              Ngày hiện tại
              <select
                className="input-dark mt-2"
                value={filters.current_date || data.current_date || ""}
                onChange={(event) => setFilters({ ...filters, current_date: event.target.value })}
              >
                {data.dates.map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </select>
            </label>

            <label className="text-sm font-semibold text-white">
              So sánh với ngày
              <select
                className="input-dark mt-2"
                value={filters.baseline_date || data.baseline_date || ""}
                onChange={(event) => setFilters({ ...filters, baseline_date: event.target.value })}
              >
                {data.dates.map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </select>
            </label>

            <label className="text-sm font-semibold text-white">
              Bộ chính
              <select
                className="input-dark mt-2"
                value={filters.main_cluster || data.selected_main_cluster || ""}
                onChange={(event) => setFilters({ ...filters, main_cluster: event.target.value })}
              >
                {data.main_clusters.map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </select>
            </label>

            <label className="text-sm font-semibold text-white">
              Lọc tag
              <select className="input-dark mt-2" value={filters.tag || "all"} onChange={(event) => setFilters({ ...filters, tag: event.target.value })}>
                <option value="all">Tất cả tag</option>
                {data.available_tags.map((tag) => (
                  <option key={tag} value={tag}>
                    {tag}
                  </option>
                ))}
              </select>
            </label>

            <label className="text-sm font-semibold text-white">
              Xu hướng
              <select className="input-dark mt-2" value={filters.status || "all"} onChange={(event) => setFilters({ ...filters, status: event.target.value })}>
                <option value="all">Tất cả</option>
                <option value="rising">Đang tăng</option>
                <option value="stable">Ổn định</option>
                <option value="declining">Đang giảm</option>
              </select>
            </label>

            <label className="text-sm font-semibold text-white">
              Sắp xếp
              <select className="input-dark mt-2" value={filters.sort_by || "health_score"} onChange={(event) => setFilters({ ...filters, sort_by: event.target.value })}>
                <option value="health_score">Điểm khỏe</option>
                <option value="trend_strength">Độ mạnh xu hướng</option>
                <option value="total_volume">Tổng volume</option>
                <option value="avg_rank">Hạng trung bình</option>
              </select>
            </label>
          </div>
        </div>
      ) : (
        <div className="panel-grid">
          <div className="flex flex-wrap gap-2">
            <span className="chip">Ngày hiện tại {formatDateLabel(data.current_date)}</span>
            <span className="chip">Mốc đối chiếu {formatDateLabel(data.baseline_date)}</span>
            <span className="chip">{data.selected_main_cluster || data.cluster_overview?.main_cluster}</span>
            <span className="chip">{data.cluster_overview?.total_keywords || 0} keyword</span>
          </div>
        </div>
      )}

      <div className="grid gap-6 xl:grid-cols-[1.65fr,0.95fr]">
        <section className="panel-grid">
          <div className="flex flex-col gap-3 border-b border-white/10 pb-5 lg:flex-row lg:items-end lg:justify-between">
            <div>
              <p className="text-xs uppercase tracking-[0.3em] text-neon-cyan">Danh sách cụm con</p>
              <h2 className="mt-2 text-3xl font-bold text-white">
                {data.cluster_overview?.main_cluster} · {data.cluster_overview?.total_keywords} keyword
              </h2>
              <p className="mt-2 text-sm text-slate-400">
                Tổng volume {data.cluster_overview?.total_volume?.toLocaleString("en-US") || 0} · Mặc định sắp theo cụm khỏe nhất để team sales và marketing nhìn thấy ưu tiên ngay.
              </p>
            </div>
            <div className="flex flex-wrap gap-2">
              <span className="chip border-neon-green/30 bg-neon-green/10 text-neon-green">Tăng mạnh và đang khỏe</span>
              <span className="chip border-neon-cyan/30 bg-neon-cyan/10 text-neon-cyan">Tăng và còn cơ hội</span>
              <span className="chip border-neon-red/30 bg-neon-red/10 text-neon-red">Đang giảm</span>
              <span className="chip">Ổn định</span>
            </div>
          </div>

          <div className="mt-5 space-y-3">
            {data.cluster_list.map((cluster) => {
              const isSelected = cluster.cluster_id === selectedCluster?.cluster_id;
              const isExpanded = cluster.cluster_id === expandedClusterId;
              const limit = expandedSize[cluster.cluster_id] || 8;
              const visibleRows = cluster.keywords.slice(0, limit);
              return (
                <div
                  key={cluster.cluster_id}
                  className={`rounded-[30px] border transition ${isSelected ? "border-neon-cyan/35 bg-neon-cyan/6 shadow-glow" : "border-white/10 bg-white/[0.03]"}`}
                >
                  <button
                    className="w-full px-5 py-5 text-left"
                    type="button"
                    onClick={() => handleSelectCluster(cluster.cluster_id)}
                  >
                    <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
                      <div className="min-w-0 flex-1">
                        <div className="flex flex-wrap items-center gap-3">
                          <h3 className="text-xl font-semibold text-white">{cluster.cluster_name}</h3>
                          <span className={`rounded-full border px-3 py-1 text-xs font-semibold ${clusterRowTone(cluster.priority_label)}`}>
                            {priorityDisplay[cluster.priority_label] || cluster.priority_label}
                          </span>
                        </div>
                        <div className="mt-3 flex flex-wrap gap-2">
                          {cluster.tags.map((tag) => (
                            <span key={`${cluster.cluster_id}-${tag}`} className="chip">
                              {tag}
                            </span>
                          ))}
                        </div>
                      </div>

                      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-5 xl:text-right">
                        <div>
                          <p className="text-xs uppercase tracking-[0.2em] text-slate-500">Số KW</p>
                          <p className="mt-2 text-2xl font-bold text-white">{cluster.keyword_count}</p>
                        </div>
                        <div>
                          <p className="text-xs uppercase tracking-[0.2em] text-slate-500">Volume</p>
                          <p className="mt-2 text-lg font-semibold text-white">{cluster.total_volume.toLocaleString("en-US")}</p>
                          <p className="mt-1 text-xs text-slate-500">TB {cluster.avg_volume}</p>
                        </div>
                        <div>
                          <p className="text-xs uppercase tracking-[0.2em] text-slate-500">Hạng TB</p>
                          <p className="mt-2 text-lg font-semibold text-white">{formatRank(cluster.avg_rank_current)}</p>
                        </div>
                        <div>
                          <p className="text-xs uppercase tracking-[0.2em] text-slate-500">Trend</p>
                          <p className={`mt-2 text-lg font-semibold ${trendTone[cluster.trend_status]}`}>
                            {trendArrow[cluster.trend_status]} {formatDelta(cluster.rank_delta)}
                          </p>
                        </div>
                        <div>
                          <p className="text-xs uppercase tracking-[0.2em] text-slate-500">Điểm khỏe</p>
                          <div className="mt-2">
                            <div className="flex items-center justify-between text-sm font-semibold text-white">
                              <span>{cluster.health_score}/100</span>
                              <span className={trendTone[cluster.trend_status]}>{trendLabel[cluster.trend_status]}</span>
                            </div>
                            <div className="mt-2 h-2.5 overflow-hidden rounded-full bg-white/5">
                              <div
                                className={`h-full rounded-full ${cluster.trend_status === "declining" ? "bg-gradient-to-r from-neon-orange to-neon-red" : cluster.trend_status === "rising" ? "bg-gradient-to-r from-neon-green to-neon-cyan" : "bg-gradient-to-r from-slate-500 to-slate-300"}`}
                                style={{ width: `${Math.max(cluster.health_score, 6)}%` }}
                              />
                            </div>
                          </div>
                        </div>
                      </div>
                    </div>
                  </button>

                  {isExpanded ? (
                    <div className="border-t border-white/10 bg-black/15 px-5 py-5">
                      <div className="mb-4 flex items-center justify-between gap-3">
                        <p className="text-sm font-semibold text-white">Drill-down keyword trong cụm</p>
                        {cluster.keywords.length > visibleRows.length ? (
                          <button
                            className="button-secondary px-4 py-2 text-xs"
                            type="button"
                            onClick={() =>
                              setExpandedSize((previous) => ({
                                ...previous,
                                [cluster.cluster_id]: previous[cluster.cluster_id] ? previous[cluster.cluster_id] + 6 : 14,
                              }))
                            }
                          >
                            Xem thêm
                          </button>
                        ) : null}
                      </div>
                      <div className="overflow-x-auto">
                        <table className="min-w-full text-sm">
                          <thead>
                            <tr className="text-left text-slate-400">
                              <th className="pb-3 pr-4 font-semibold">Keyword</th>
                              <th className="pb-3 pr-4 font-semibold">Tags</th>
                              <th className="pb-3 pr-4 font-semibold">Vol</th>
                              <th className="pb-3 pr-4 font-semibold">Hiện tại</th>
                              <th className="pb-3 pr-4 font-semibold">Thay đổi</th>
                              <th className="pb-3 font-semibold">Trend</th>
                            </tr>
                          </thead>
                          <tbody>
                            {visibleRows.map((item) => (
                              <tr key={`${cluster.cluster_id}-${item.keyword}`} className="border-t border-white/5 text-slate-200">
                                <td className="py-3 pr-4">
                                  <p className="font-medium text-white">{item.keyword}</p>
                                </td>
                                <td className="py-3 pr-4">
                                  <div className="flex flex-wrap gap-2">
                                    {item.tags.map((tag) => (
                                      <span key={`${item.keyword}-${tag}`} className="chip">
                                        {tag}
                                      </span>
                                    ))}
                                  </div>
                                </td>
                                <td className="py-3 pr-4">{item.volume ? item.volume.toLocaleString("en-US") : "—"}</td>
                                <td className="py-3 pr-4">{mode === "client" ? currentRankLabel(item.current_rank) : formatRank(item.current_rank)}</td>
                                <td className={`py-3 pr-4 ${trendTone[item.trend_status]}`}>{formatDelta(item.rank_delta)}</td>
                                <td className={`py-3 ${trendTone[item.trend_status]}`}>
                                  {trendArrow[item.trend_status]} {trendLabel[item.trend_status]}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  ) : null}
                </div>
              );
            })}
          </div>
        </section>

        <aside className="panel-grid">
          <div className="border-b border-white/10 pb-5">
            <p className="text-xs uppercase tracking-[0.3em] text-neon-blue">Bảng xu hướng</p>
            <div className="mt-3 flex flex-wrap items-center gap-3">
              <h2 className="text-2xl font-bold text-white">{selectedCluster.cluster_name}</h2>
              <span className={`rounded-full border px-3 py-1 text-xs font-semibold ${clusterRowTone(selectedCluster.priority_label)}`}>
                {priorityDisplay[selectedCluster.priority_label] || selectedCluster.priority_label}
              </span>
            </div>
            <div className="mt-4 flex flex-wrap gap-2">
              <span className="chip">Volume {selectedCluster.total_volume.toLocaleString("en-US")}</span>
              <span className="chip">Vol TB {selectedCluster.avg_volume}</span>
              <span className="chip">Hạng TB {formatRank(selectedCluster.avg_rank_current)}</span>
              <span className="chip">Điểm khỏe {selectedCluster.health_score}</span>
              <span className={`chip ${trendTone[selectedCluster.trend_status]}`}>{trendArrow[selectedCluster.trend_status]} {trendLabel[selectedCluster.trend_status]}</span>
            </div>
          </div>

          <div className="mt-5">
            <div className="flex items-center justify-between gap-3">
              <p className="text-sm font-semibold text-white">Sparkline 30 ngày</p>
              <span className={`text-sm font-semibold ${selectedCluster.sparkline.delta_vs_previous_period >= 0 ? "text-neon-green" : "text-neon-red"}`}>
                {selectedCluster.sparkline.delta_vs_previous_period >= 0 ? "+" : ""}
                {selectedCluster.sparkline.delta_vs_previous_period}% vs kỳ trước
              </span>
            </div>
            <div className="mt-3 h-[220px]">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={selectedCluster.sparkline.points}>
                  <CartesianGrid stroke="rgba(255,255,255,0.06)" vertical={false} />
                  <XAxis dataKey="date" tickFormatter={formatDateLabel} stroke="#8b949e" tick={{ fontSize: 11 }} />
                  <YAxis reversed stroke="#8b949e" tick={{ fontSize: 11 }} />
                  <Tooltip
                    labelFormatter={(value) => formatDateLabel(value)}
                    formatter={(value) => [`${value}`, "Hạng trung bình"]}
                    contentStyle={{
                      background: "#11161f",
                      border: "1px solid rgba(255,255,255,0.08)",
                      borderRadius: 18,
                    }}
                  />
                  <Line type="monotone" dataKey="value" stroke="#38bdf8" strokeWidth={3} dot={false} activeDot={{ r: 5 }} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className="mt-5 rounded-[28px] border border-white/10 bg-white/[0.03] p-4">
            <p className="text-sm font-semibold text-white">Insight ngắn cho cụm</p>
            <p className="mt-3 text-sm leading-7 text-slate-300">{selectedCluster.insight_note}</p>
          </div>

          <div className="mt-5">
            <div className="mb-3 flex items-center justify-between gap-3">
              <p className="text-sm font-semibold text-white">Top keyword đại diện</p>
              <span className="text-xs text-slate-500">Hiện tại so với mốc đối chiếu</span>
            </div>
            <div className="space-y-3">
              {selectedCluster.top_keywords.map((item) => (
                <div key={`${selectedCluster.cluster_id}-${item.keyword}`} className="rounded-[24px] border border-white/10 bg-white/[0.03] px-4 py-3">
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <p className="font-semibold text-white">{item.keyword}</p>
                      <p className="mt-1 text-sm text-slate-400">Vol {item.volume ? item.volume.toLocaleString("en-US") : "—"}</p>
                    </div>
                    <div className="text-right">
                      <p className="text-sm font-semibold text-white">
                        {mode === "client" ? currentRankLabel(item.current_rank) : `Top ${formatRank(item.current_rank)}`}
                      </p>
                      <p className={`mt-1 text-sm font-semibold ${trendTone[item.trend_status]}`}>
                        {trendArrow[item.trend_status]} {formatDelta(item.rank_delta)}
                      </p>
                    </div>
                  </div>
                </div>
              ))}
            </div>

            {selectedDrilldown && selectedDrilldown.keywords.length > visibleKeywords.length ? (
              <button
                className="button-secondary mt-4 w-full"
                type="button"
                onClick={() =>
                  setExpandedSize((previous) => ({
                    ...previous,
                    [selectedDrilldown.cluster_id]: previous[selectedDrilldown.cluster_id]
                      ? previous[selectedDrilldown.cluster_id] + 6
                      : 14,
                  }))
                }
              >
                Xem thêm keyword trong panel
              </button>
            ) : null}
          </div>
        </aside>
      </div>
    </div>
  );
}
