import { useDeferredValue, useEffect, useMemo, useState } from "react";
import GroupTab from "./GroupTab";
import KeywordDrawer from "./KeywordDrawer";
import KeywordTableTab from "./KeywordTableTab";
import OverviewTab from "./OverviewTab";
import { getPublicKeywordDetail, getPublicShare, loginPublicShare } from "../lib/api";
import { formatDateTime } from "../lib/format";

const TAB_LABELS = {
  overview: "Tổng Quan Timeline",
  groups: "Theo Bộ",
  keywords: "Chi Tiết Keyword",
};

function storageKeyForShare(shareToken) {
  return `seo-dashboard-public-${shareToken}`;
}

function shareHeading(shareType, viewMode) {
  if (shareType === "report_snapshot") return "Snapshot báo cáo";
  return viewMode === "team" ? "Cổng SEO riêng theo dự án" : "Cổng khách hàng riêng theo dự án";
}

function maskSensitiveProjectLabel(value, fallback = "Dự án đã chia sẻ") {
  const raw = `${value || ""}`.trim();
  if (!raw) return fallback;

  const looksLikeRawUrl = /^(https?:\/\/|docs\.google\.com|www\.)/i.test(raw);
  const cleaned = raw
    .replace(/https?:\/\/\S+/gi, "")
    .replace(/docs\.google\.com\/\S+/gi, "")
    .replace(/\s+/g, " ")
    .trim();

  if (looksLikeRawUrl || !cleaned) return fallback;
  if (cleaned.length > 90) return `${cleaned.slice(0, 87)}...`;
  return cleaned;
}

export default function PublicShareView({ shareToken, shareType }) {
  const initialPublicToken = useMemo(() => {
    try {
      return localStorage.getItem(storageKeyForShare(shareToken)) || "";
    } catch {
      return "";
    }
  }, [shareToken]);

  const [publicToken, setPublicToken] = useState(initialPublicToken);
  const [password, setPassword] = useState("");
  const [loggingIn, setLoggingIn] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [payload, setPayload] = useState(null);
  const [requiresPassword, setRequiresPassword] = useState(false);
  const [activeTab, setActiveTab] = useState("overview");
  const [groupFilters, setGroupFilters] = useState(null);
  const [keywordFilters, setKeywordFilters] = useState(null);
  const [manualEvent, setManualEvent] = useState({
    event_date: "",
    title: "",
    description: "",
  });
  const [detailOpen, setDetailOpen] = useState(false);
  const [keywordDetail, setKeywordDetail] = useState(null);
  const [noteDraft, setNoteDraft] = useState("");
  const [detailLoading, setDetailLoading] = useState(false);
  const deferredSearch = useDeferredValue(keywordFilters?.search || "");

  useEffect(() => {
    setPublicToken(initialPublicToken);
    setPassword("");
    setError("");
    setRequiresPassword(false);
    setPayload(null);
    setGroupFilters(null);
    setKeywordFilters(null);
    setActiveTab("overview");
    setDetailOpen(false);
    setKeywordDetail(null);
    setNoteDraft("");
  }, [shareToken, initialPublicToken]);

  useEffect(() => {
    let cancelled = false;
    async function loadPublicView() {
      setLoading(true);
      setError("");
      try {
        const response = await getPublicShare(
          shareToken,
          {
            group_current_date: groupFilters?.current_date || undefined,
            group_baseline_date: groupFilters?.baseline_date || undefined,
            group_status: groupFilters?.status || undefined,
            group_main_cluster: groupFilters?.main_cluster || undefined,
            group_tag: groupFilters?.tag || undefined,
            group_sort_by: groupFilters?.sort_by || undefined,
            active_scenario_id: groupFilters?.active_scenario_id || undefined,
            keyword_current_date: keywordFilters?.current_date || undefined,
            keyword_search: deferredSearch || undefined,
            keyword_groups: keywordFilters?.groups || undefined,
            keyword_clusters: keywordFilters?.clusters || undefined,
            keyword_status: keywordFilters?.status || undefined,
            keyword_vol_min: keywordFilters?.vol_min,
            keyword_vol_max: keywordFilters?.vol_max,
            keyword_rank_min: keywordFilters?.rank_min,
            keyword_rank_max: keywordFilters?.rank_max,
            keyword_movers_only: keywordFilters?.movers_only,
            keyword_sort_by: keywordFilters?.sort_by || undefined,
            keyword_sort_dir: keywordFilters?.sort_dir || undefined,
          },
          publicToken,
        );
        if (cancelled) return;
        setRequiresPassword(Boolean(response.requires_password));
        if (response.requires_password) {
          setPayload(response);
          return;
        }
        setPayload(response);
        const allowedTabs = (response.available_tabs || ["overview", "groups"]).filter(
          (tabId) => (response.view_mode || "client") === "team" || tabId !== "keywords",
        );
        setActiveTab((previous) => {
          const requestedTab = response.view_state?.active_tab || allowedTabs[0] || "overview";
          const fallback = allowedTabs.includes(requestedTab) ? requestedTab : (allowedTabs[0] || "overview");
          const nextValue = previous || fallback;
          return allowedTabs.includes(nextValue) ? nextValue : fallback;
        });
        setGroupFilters((previous) => previous || {
          current_date: response.group_view?.current_date || response.view_state?.group_filters?.current_date || "",
          baseline_date: response.group_view?.baseline_date || response.view_state?.group_filters?.baseline_date || "",
          status: response.view_state?.group_filters?.status || "all",
          main_cluster: response.group_view?.selected_main_cluster || response.view_state?.group_filters?.main_cluster || "",
          tag: response.view_state?.group_filters?.tag || "all",
          sort_by: response.view_state?.group_filters?.sort_by || "health_score",
          active_scenario_id: response.group_view?.active_scenario_id || response.view_state?.group_filters?.active_scenario_id || "",
        });
        setKeywordFilters((previous) => previous || {
          current_date: response.keyword_table?.current_date || response.view_state?.keyword_filters?.current_date || "",
          search: response.view_state?.keyword_filters?.search || "",
          groups: response.view_state?.keyword_filters?.groups || "",
          clusters: response.view_state?.keyword_filters?.clusters || "",
          status: response.view_state?.keyword_filters?.status || "all",
          vol_min: response.view_state?.keyword_filters?.vol_min ?? 0,
          vol_max: response.view_state?.keyword_filters?.vol_max ?? 1000000,
          rank_min: response.view_state?.keyword_filters?.rank_min ?? 0,
          rank_max: response.view_state?.keyword_filters?.rank_max ?? 101,
          movers_only: response.view_state?.keyword_filters?.movers_only ?? false,
          sort_by: response.keyword_table?.sort_by || response.view_state?.keyword_filters?.sort_by || "current_rank",
          sort_dir: response.keyword_table?.sort_dir || response.view_state?.keyword_filters?.sort_dir || "asc",
        });
      } catch (requestError) {
        if (cancelled) return;
        setError(requestError.message);
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    loadPublicView();
    return () => {
      cancelled = true;
    };
  }, [
    shareToken,
    publicToken,
    groupFilters?.current_date,
    groupFilters?.baseline_date,
    groupFilters?.status,
    groupFilters?.main_cluster,
    groupFilters?.tag,
    groupFilters?.sort_by,
    groupFilters?.active_scenario_id,
    keywordFilters?.current_date,
    keywordFilters?.groups,
    keywordFilters?.clusters,
    keywordFilters?.status,
    keywordFilters?.vol_min,
    keywordFilters?.vol_max,
    keywordFilters?.rank_min,
    keywordFilters?.rank_max,
    keywordFilters?.movers_only,
    keywordFilters?.sort_by,
    keywordFilters?.sort_dir,
    deferredSearch,
  ]);

  async function handlePublicLogin(event) {
    event.preventDefault();
    setLoggingIn(true);
    setError("");
    try {
      const response = await loginPublicShare(shareToken, password);
      try {
        localStorage.setItem(storageKeyForShare(shareToken), response.token);
      } catch {
        // Ignore storage errors for public share links.
      }
      setPublicToken(response.token);
      setPassword("");
      setRequiresPassword(false);
    } catch (loginError) {
      setError(loginError.message);
    } finally {
      setLoggingIn(false);
    }
  }

  async function handleOpenKeyword(keywordId) {
    if (payload?.view_mode !== "team") return;
    setDetailLoading(true);
    try {
      const detail = await getPublicKeywordDetail(shareToken, keywordId, publicToken);
      setKeywordDetail(detail);
      setNoteDraft(detail.keyword.notes || "");
      setDetailOpen(true);
    } catch (detailError) {
      setError(detailError.message);
    } finally {
      setDetailLoading(false);
    }
  }

  const mode = payload?.view_mode || "client";
  const visibleTabs = (payload?.available_tabs || ["overview", "groups"]).filter(
    (tabId) => mode === "team" || tabId !== "keywords",
  );
  const safeHeaderTitle = maskSensitiveProjectLabel(payload?.title || payload?.project_name, "Portal dự án");
  const safeProjectName = maskSensitiveProjectLabel(payload?.project_name, "Dự án đã chia sẻ");

  useEffect(() => {
    if (mode !== "team" && activeTab === "keywords") {
      setActiveTab("overview");
    }
  }, [mode, activeTab]);

  if (loading && !payload) {
    return <div className="flex min-h-screen items-center justify-center text-sm text-slate-400">Đang tải link chia sẻ...</div>;
  }

  if (requiresPassword) {
    return (
      <div className="flex min-h-screen items-center justify-center px-4">
        <form className="glass-panel w-full max-w-lg p-8" onSubmit={handlePublicLogin}>
          <p className="text-xs uppercase tracking-[0.3em] text-neon-cyan">
            {shareType === "report" ? "Report snapshot" : shareType === "seo" ? "SEO portal" : "Client portal"}
          </p>
          <h1 className="mt-3 text-3xl font-bold text-white">{payload?.title || "Link chia sẻ có mật khẩu"}</h1>
          <p className="mt-3 text-sm leading-7 text-slate-400">
            Link này được bảo vệ bằng mật khẩu để người xem chỉ thấy đúng project đã chia sẻ.
          </p>
          <input
            className="input-dark mt-5"
            type="password"
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            placeholder="Nhập mật khẩu"
          />
          {error ? <p className="mt-3 text-sm text-neon-red">{error}</p> : null}
          <button className="button-primary mt-5 w-full" type="submit" disabled={loggingIn || !password.trim()}>
            {loggingIn ? "Đang mở..." : "Mở dashboard"}
          </button>
        </form>
      </div>
    );
  }

  if (!payload?.group_view) {
    return (
      <div className="flex min-h-screen items-center justify-center px-4 text-center text-sm text-slate-400">
        {error || "Không thể tải nội dung chia sẻ ở thời điểm này."}
      </div>
    );
  }

  return (
    <div className="min-h-screen px-4 py-4 lg:px-6">
      {error ? (
        <div className="fixed left-1/2 top-5 z-50 -translate-x-1/2 rounded-full border border-neon-red/40 bg-neon-red/10 px-5 py-3 text-sm font-semibold text-neon-red shadow-glow">
          {error}
        </div>
      ) : null}

      <div className="mx-auto max-w-[1600px] space-y-6">
        <header className="glass-panel overflow-hidden shadow-glow">
          <div className="border-b border-white/10 bg-gradient-to-r from-neon-blue/15 via-transparent to-neon-cyan/10 px-5 py-5">
            <div className="flex flex-col gap-5 xl:flex-row xl:items-start xl:justify-between">
              <div className="min-w-0 flex-1">
                <p className="font-display text-xs uppercase tracking-[0.4em] text-neon-cyan">
                  ⚡ SEO RANKING DASHBOARD
                </p>
                <h1 className="mt-3 font-display text-3xl font-bold text-white lg:text-4xl">
                  {safeHeaderTitle}
                </h1>
                <p className="mt-3 max-w-4xl text-sm leading-7 text-slate-300">
                  {payload.overview?.subtitle || "Portal này chỉ hiển thị đúng một dự án đã được chia sẻ."}
                </p>
              </div>

              <div className="grid gap-3 sm:grid-cols-2 xl:w-[520px]">
                <div className="rounded-[24px] border border-white/10 bg-black/10 px-4 py-3">
                  <p className="text-xs uppercase tracking-[0.2em] text-slate-500">Dự án đang xem</p>
                  <p className="mt-2 text-lg font-semibold text-white">{safeProjectName}</p>
                </div>

                <div className="rounded-[24px] border border-white/10 bg-black/10 px-4 py-3">
                  <p className="text-xs uppercase tracking-[0.2em] text-slate-500">Quyền truy cập</p>
                  <p className="mt-2 text-lg font-semibold text-white">{shareHeading(payload.share_type, mode)}</p>
                </div>
              </div>
            </div>

            <div className="mt-5 flex flex-wrap gap-2">
              {(payload.overview?.kpi_chips || []).map((chip) => (
                <span
                  key={chip.name}
                  className={`chip ${chip.status === "đạt" ? "border-neon-green/30 bg-neon-green/10 text-neon-green" : "border-neon-red/25 bg-neon-red/10 text-neon-red"}`}
                >
                  {chip.label}
                </span>
              ))}
            </div>
          </div>

          <div className="flex flex-col gap-4 px-5 py-4 lg:flex-row lg:items-center lg:justify-between">
            <div className="flex flex-wrap items-center gap-3 text-sm text-slate-400">
              <span>Cập nhật link: {formatDateTime(payload.snapshot_created_at)}</span>
              <span>•</span>
              <span>{payload.group_view?.cluster_overview?.total_keywords || 0} keyword</span>
              <span>•</span>
              <span>{payload.overview?.dates?.length || 0} mốc ngày</span>
            </div>

            <div className="flex flex-wrap gap-2">
              <span className="chip border-neon-cyan/30 bg-neon-cyan/10 text-neon-cyan">
                {mode === "team" ? "Team SEO" : "Khách hàng"}
              </span>
              <span className="chip">Không hiển thị project khác</span>
            </div>
          </div>
        </header>

        <div className="flex flex-wrap gap-3">
          {visibleTabs.map((tabId) => (
            <button
              key={tabId}
              className={`rounded-full px-5 py-3 text-sm font-semibold transition ${activeTab === tabId ? "bg-gradient-to-r from-neon-blue to-neon-cyan text-slate-950" : "border border-white/10 bg-white/[0.03] text-white hover:border-neon-blue/40"}`}
              type="button"
              onClick={() => setActiveTab(tabId)}
            >
              {TAB_LABELS[tabId] || tabId}
            </button>
          ))}
        </div>

        {activeTab === "overview" ? (
          <OverviewTab
            overview={payload.overview}
            mode={mode}
            generatingInsight={false}
            onGenerateInsight={() => {}}
            onSaveWeeklyNote={() => {}}
            savingWeeklyNote={false}
            manualEvent={manualEvent}
            setManualEvent={setManualEvent}
            onAddEvent={(event) => event.preventDefault()}
            addingEvent={false}
            readOnly
          />
        ) : null}

        {activeTab === "groups" ? (
          <GroupTab
            data={payload.group_view}
            filters={groupFilters || {
              current_date: "",
              baseline_date: "",
              status: "all",
              main_cluster: "",
              tag: "all",
              sort_by: "health_score",
              active_scenario_id: "",
            }}
            setFilters={setGroupFilters}
            mode={mode}
            controlsMode={payload.share_type === "report_snapshot" ? "scenario_only" : "full"}
          />
        ) : null}

        {mode === "team" && activeTab === "keywords" && payload.keyword_table ? (
          <KeywordTableTab
            data={payload.keyword_table}
            filters={keywordFilters || {
              current_date: "",
              search: "",
              groups: "",
              clusters: "",
              status: "all",
              vol_min: 0,
              vol_max: 1000000,
              rank_min: 0,
              rank_max: 101,
              movers_only: false,
              sort_by: "current_rank",
              sort_dir: "asc",
            }}
            setFilters={setKeywordFilters}
            mode={mode}
            onExport={() => {}}
            exporting={false}
            onOpenKeyword={payload.share_type === "seo_view" ? handleOpenKeyword : undefined}
            readOnly
            showExportButton={false}
          />
        ) : null}
      </div>

      <KeywordDrawer
        open={detailOpen}
        detail={keywordDetail}
        mode={mode}
        insightLoading={detailLoading}
        noteDraft={noteDraft}
        setNoteDraft={setNoteDraft}
        onClose={() => setDetailOpen(false)}
        onSaveNotes={() => {}}
        onGenerateInsight={() => {}}
        savingNotes={false}
        readOnly
      />
    </div>
  );
}
