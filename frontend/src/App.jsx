import { useDeferredValue, useEffect, useState } from "react";
import GroupTab from "./components/GroupTab";
import KeywordDrawer from "./components/KeywordDrawer";
import KeywordTableTab from "./components/KeywordTableTab";
import LoginView from "./components/LoginView";
import OverviewTab from "./components/OverviewTab";
import SettingsPanel from "./components/SettingsPanel";
import {
  clearSession,
  createEvent,
  createProject,
  createWeeklyInsight,
  exportKeywords,
  generateKeywordInsight,
  getGroupView,
  getKeywordDetail,
  getKeywordTable,
  getOverview,
  getProjects,
  getSettings,
  getStoredSession,
  login,
  reclusterProject,
  refreshProject,
  saveKeywordNotes,
  saveWeeklyNote,
  storeSession,
  testGoogleSheet,
  updateSettings,
  uploadProjectFile,
  validateSession,
} from "./lib/api";
import { formatDateTime } from "./lib/format";

const TEAM_MODE = "team";
const CLIENT_MODE = "client";
const tabs = [
  { id: "overview", label: "Tổng Quan Timeline" },
  { id: "groups", label: "Theo Bộ" },
  { id: "keywords", label: "Chi Tiết Keyword" },
];

export default function App() {
  const [token, setToken] = useState("");
  const [authChecked, setAuthChecked] = useState(false);
  const [password, setPassword] = useState("");
  const [loginError, setLoginError] = useState("");
  const [loginLoading, setLoginLoading] = useState(false);

  const [projects, setProjects] = useState([]);
  const [selectedProjectId, setSelectedProjectId] = useState("");
  const [mode, setMode] = useState(TEAM_MODE);
  const [activeTab, setActiveTab] = useState("overview");
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [toast, setToast] = useState(null);

  const [overview, setOverview] = useState(null);
  const [settings, setSettings] = useState(null);
  const [groupView, setGroupView] = useState(null);
  const [keywordTable, setKeywordTable] = useState(null);
  const [detailOpen, setDetailOpen] = useState(false);
  const [keywordDetail, setKeywordDetail] = useState(null);
  const [noteDraft, setNoteDraft] = useState("");

  const [pageLoading, setPageLoading] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [testingSheet, setTestingSheet] = useState(false);
  const [savingSettings, setSavingSettings] = useState(false);
  const [creatingProject, setCreatingProject] = useState(false);
  const [generatingInsight, setGeneratingInsight] = useState(false);
  const [savingWeeklyNote, setSavingWeeklyNote] = useState(false);
  const [addingEvent, setAddingEvent] = useState(false);
  const [dragActive, setDragActive] = useState(false);
  const [savingNotes, setSavingNotes] = useState(false);
  const [insightLoading, setInsightLoading] = useState(false);
  const [exporting, setExporting] = useState(false);
  const [reclustering, setReclustering] = useState(false);

  const [manualEvent, setManualEvent] = useState({
    event_date: "",
    title: "",
    description: "",
  });
  const [groupFilters, setGroupFilters] = useState({
    current_date: "",
    baseline_date: "",
    status: "all",
    main_cluster: "",
    tag: "all",
    sort_by: "health_score",
    sub_cluster_mode: "auto",
  });
  const [keywordFilters, setKeywordFilters] = useState({
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
  });
  const deferredSearch = useDeferredValue(keywordFilters.search);

  useEffect(() => {
    let cancelled = false;
    const stored = getStoredSession();
    if (!stored) {
      setAuthChecked(true);
      return;
    }
    validateSession(stored.token)
      .then(() => {
        if (cancelled) return;
        setToken(stored.token);
      })
      .catch(() => {
        clearSession();
      })
      .finally(() => {
        if (!cancelled) setAuthChecked(true);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!token) return;
    loadProjectsList();
  }, [token]);

  useEffect(() => {
    if (!selectedProjectId || !token) return;
    loadOverviewAndSettings(selectedProjectId);
  }, [selectedProjectId, token]);

  useEffect(() => {
    if (!selectedProjectId || !token || !groupFilters.current_date) return;
    getGroupView(token, selectedProjectId, groupFilters)
      .then(setGroupView)
      .catch((error) => setToast({ type: "error", message: error.message }));
  }, [selectedProjectId, token, groupFilters.current_date, groupFilters.baseline_date, groupFilters.status, groupFilters.main_cluster, groupFilters.tag, groupFilters.sort_by, groupFilters.sub_cluster_mode]);

  useEffect(() => {
    if (!selectedProjectId || !token || !keywordFilters.current_date) return;
    getKeywordTable(token, selectedProjectId, {
      ...keywordFilters,
      search: deferredSearch,
    })
      .then(setKeywordTable)
      .catch((error) => setToast({ type: "error", message: error.message }));
  }, [
    selectedProjectId,
    token,
    keywordFilters.current_date,
    keywordFilters.groups,
    keywordFilters.clusters,
    keywordFilters.status,
    keywordFilters.vol_min,
    keywordFilters.vol_max,
    keywordFilters.rank_min,
    keywordFilters.rank_max,
    keywordFilters.movers_only,
    deferredSearch,
  ]);

  useEffect(() => {
    if (!toast) return;
    const timer = window.setTimeout(() => setToast(null), 3200);
    return () => window.clearTimeout(timer);
  }, [toast]);

  async function loadProjectsList(preferredProjectId) {
    try {
      const response = await getProjects(token);
      setProjects(response.projects || []);
      const nextProjectId =
        preferredProjectId ||
        localStorage.getItem("seo-dashboard-project-id") ||
        response.projects?.[0]?.id ||
        "";
      if (nextProjectId) {
        setSelectedProjectId(String(nextProjectId));
        localStorage.setItem("seo-dashboard-project-id", String(nextProjectId));
      } else {
        setSelectedProjectId("");
        setOverview(null);
        setGroupView(null);
        setKeywordTable(null);
        setSettings(null);
      }
    } catch (error) {
      setToast({ type: "error", message: error.message });
    }
  }

  async function loadOverviewAndSettings(projectId) {
    setPageLoading(true);
    try {
      const [overviewResponse, settingsResponse] = await Promise.all([
        getOverview(token, projectId),
        getSettings(token, projectId),
      ]);
      setOverview(overviewResponse);
      setSettings(settingsResponse);
      const dates = overviewResponse.dates || [];
      if (dates.length) {
        const current = dates[dates.length - 1];
        const baseline = dates[dates.length - 2] || dates[0];
        const nextGroupFilters = {
          ...groupFilters,
          current_date: current,
          baseline_date: baseline,
        };
        const nextKeywordFilters = {
          ...keywordFilters,
          current_date: current,
        };
        setGroupFilters(nextGroupFilters);
        setKeywordFilters(nextKeywordFilters);
        setManualEvent((previous) => ({
          ...previous,
          event_date: current,
        }));
        const [groupsResponse, keywordResponse] = await Promise.all([
          getGroupView(token, projectId, nextGroupFilters),
          getKeywordTable(token, projectId, {
            ...nextKeywordFilters,
            search: nextKeywordFilters.search,
          }),
        ]);
        setGroupView(groupsResponse);
        setKeywordTable(keywordResponse);
      } else {
        setGroupView(null);
        setKeywordTable(null);
      }
    } catch (error) {
      setToast({ type: "error", message: error.message });
    } finally {
      setPageLoading(false);
    }
  }

  async function handleLogin(event) {
    event.preventDefault();
    setLoginLoading(true);
    setLoginError("");
    try {
      const response = await login(password);
      const session = storeSession(response.token, response.expires_in_seconds);
      setToken(session.token);
      setPassword("");
    } catch (error) {
      setLoginError(error.message);
    } finally {
      setLoginLoading(false);
      setAuthChecked(true);
    }
  }

  function handleLogout() {
    clearSession();
    setToken("");
    setProjects([]);
    setSelectedProjectId("");
    setOverview(null);
    setGroupView(null);
    setKeywordTable(null);
    setKeywordDetail(null);
    setDetailOpen(false);
  }

  async function handleCreateProject(name) {
    setCreatingProject(true);
    try {
      const response = await createProject(token, { name });
      await loadProjectsList(response.project.id);
      setSettingsOpen(false);
      setToast({ type: "success", message: "Đã tạo project mới." });
    } catch (error) {
      setToast({ type: "error", message: error.message });
    } finally {
      setCreatingProject(false);
    }
  }

  async function handleUpload(file) {
    if (!selectedProjectId) {
      setToast({ type: "error", message: "Hãy tạo project trước khi tải file." });
      return;
    }
    setUploading(true);
    try {
      const response = await uploadProjectFile(token, selectedProjectId, file);
      setToast({
        type: "success",
        message: `Đã nhập ${response.imported_keywords} keyword và ${response.imported_rankings} bản ghi ranking.`,
      });
      await loadOverviewAndSettings(selectedProjectId);
      const [groupsResponse, keywordsResponse] = await Promise.all([
        getGroupView(token, selectedProjectId, groupFilters),
        getKeywordTable(token, selectedProjectId, keywordFilters),
      ]);
      setGroupView(groupsResponse);
      setKeywordTable(keywordsResponse);
    } catch (error) {
      setToast({ type: "error", message: error.message });
    } finally {
      setUploading(false);
      setSettingsOpen(false);
    }
  }

  async function handleRefresh() {
    if (!selectedProjectId) return;
    setPageLoading(true);
    try {
      await refreshProject(token, selectedProjectId);
      await loadOverviewAndSettings(selectedProjectId);
      setToast({ type: "success", message: "Đã refresh dữ liệu từ Google Sheets." });
    } catch (error) {
      setToast({ type: "error", message: error.message });
    } finally {
      setPageLoading(false);
    }
  }

  async function handleTestSheet(sheetUrl, sheetGid = "") {
    if (!sheetUrl?.trim()) {
      setToast({ type: "error", message: "Vui lòng nhập link dữ liệu public." });
      return;
    }
    setTestingSheet(true);
    try {
      const response = await testGoogleSheet(token, sheetUrl, sheetGid);
      setToast({
        type: "success",
        message: `Kết nối thành công. Tìm thấy ${response.row_count} keyword, ${response.dates.length} mốc ngày${response.selected_sheet_name ? ` · tab: ${response.selected_sheet_name}` : ""}.`,
      });
    } catch (error) {
      setToast({ type: "error", message: error.message });
    } finally {
      setTestingSheet(false);
    }
  }

  async function handleSaveSettings(form) {
    if (!selectedProjectId) return;
    setSavingSettings(true);
    try {
      await updateSettings(token, selectedProjectId, form);
      await loadOverviewAndSettings(selectedProjectId);
      setToast({ type: "success", message: "Đã lưu cài đặt." });
    } catch (error) {
      setToast({ type: "error", message: error.message });
    } finally {
      setSavingSettings(false);
    }
  }

  async function handleGenerateWeeklyInsight() {
    if (!selectedProjectId) return;
    setGeneratingInsight(true);
    try {
      await createWeeklyInsight(token, selectedProjectId);
      await loadOverviewAndSettings(selectedProjectId);
      setToast({ type: "success", message: "Đã tạo insight tuần mới." });
    } catch (error) {
      setToast({ type: "error", message: error.message });
    } finally {
      setGeneratingInsight(false);
    }
  }

  async function handleSaveWeeklyNote(content) {
    if (!selectedProjectId) return;
    setSavingWeeklyNote(true);
    try {
      await saveWeeklyNote(token, selectedProjectId, content);
      await loadOverviewAndSettings(selectedProjectId);
      setToast({ type: "success", message: "Đã lưu nhận xét tuần." });
    } catch (error) {
      setToast({ type: "error", message: error.message });
    } finally {
      setSavingWeeklyNote(false);
    }
  }

  async function handleAddEvent(event) {
    event.preventDefault();
    if (!selectedProjectId) return;
    setAddingEvent(true);
    try {
      await createEvent(token, selectedProjectId, {
        ...manualEvent,
        impact_type: "manual",
      });
      await loadOverviewAndSettings(selectedProjectId);
      setManualEvent((previous) => ({ ...previous, title: "", description: "" }));
      setToast({ type: "success", message: "Đã thêm sự kiện thủ công." });
    } catch (error) {
      setToast({ type: "error", message: error.message });
    } finally {
      setAddingEvent(false);
    }
  }

  async function handleOpenKeyword(keywordId) {
    if (!selectedProjectId) return;
    try {
      const response = await getKeywordDetail(token, selectedProjectId, keywordId);
      setKeywordDetail(response);
      setNoteDraft(response.keyword.notes || "");
      setDetailOpen(true);
    } catch (error) {
      setToast({ type: "error", message: error.message });
    }
  }

  async function handleSaveNotes() {
    if (!selectedProjectId || !keywordDetail) return;
    setSavingNotes(true);
    try {
      const response = await saveKeywordNotes(token, selectedProjectId, keywordDetail.keyword.id, noteDraft);
      setKeywordDetail(response);
      await getKeywordTable(token, selectedProjectId, { ...keywordFilters, search: deferredSearch }).then(setKeywordTable);
      setToast({ type: "success", message: "Đã lưu ghi chú keyword." });
    } catch (error) {
      setToast({ type: "error", message: error.message });
    } finally {
      setSavingNotes(false);
    }
  }

  async function handleKeywordInsight() {
    if (!selectedProjectId || !keywordDetail) return;
    setInsightLoading(true);
    try {
      await generateKeywordInsight(token, selectedProjectId, keywordDetail.keyword.id);
      const detail = await getKeywordDetail(token, selectedProjectId, keywordDetail.keyword.id);
      setKeywordDetail(detail);
      setToast({ type: "success", message: "Đã tạo insight cho keyword." });
    } catch (error) {
      setToast({ type: "error", message: error.message });
    } finally {
      setInsightLoading(false);
    }
  }

  async function handleExport() {
    if (!selectedProjectId) return;
    setExporting(true);
    try {
      await exportKeywords(token, selectedProjectId, { ...keywordFilters, search: deferredSearch });
    } catch (error) {
      setToast({ type: "error", message: error.message });
    } finally {
      setExporting(false);
    }
  }

  async function handleRecluster() {
    if (!selectedProjectId) return;
    setReclustering(true);
    try {
      await reclusterProject(token, selectedProjectId);
      await loadOverviewAndSettings(selectedProjectId);
      setToast({ type: "success", message: "Đã chạy lại sub-cluster." });
    } catch (error) {
      setToast({ type: "error", message: error.message });
    } finally {
      setReclustering(false);
    }
  }

  if (!authChecked) {
    return <div className="flex min-h-screen items-center justify-center text-sm text-slate-400">Đang kiểm tra phiên đăng nhập...</div>;
  }

  if (!token) {
    return (
      <LoginView
        password={password}
        setPassword={setPassword}
        submitting={loginLoading}
        error={loginError}
        onSubmit={handleLogin}
      />
    );
  }

  const project = projects.find((item) => String(item.id) === String(selectedProjectId)) || null;
  const visibleTabs = mode === CLIENT_MODE ? tabs.filter((tab) => tab.id !== "keywords") : tabs;

  return (
    <div className="min-h-screen px-4 py-4 lg:px-6">
      {toast ? (
        <div className={`fixed left-1/2 top-5 z-50 -translate-x-1/2 rounded-full border px-5 py-3 text-sm font-semibold shadow-glow ${toast.type === "error" ? "border-neon-red/40 bg-neon-red/10 text-neon-red" : "border-neon-cyan/30 bg-neon-cyan/10 text-neon-cyan"}`}>
          {toast.message}
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
                  Theo dõi thứ hạng SEO theo thời gian thực
                </h1>
                <p className="mt-3 max-w-4xl text-sm leading-7 text-slate-300">
                  {overview?.subtitle || "Chưa có dữ liệu. Kết nối Google Sheets hoặc tải file để dựng dashboard."}
                </p>
              </div>

              <div className="grid gap-3 sm:grid-cols-2 xl:w-[520px]">
                <label className="rounded-[24px] border border-white/10 bg-black/10 px-4 py-3 text-sm font-semibold text-white">
                  Dự án
                  <select
                    className="mt-2 w-full bg-transparent text-sm text-slate-200 outline-none"
                    value={selectedProjectId}
                    onChange={(event) => {
                      setSelectedProjectId(event.target.value);
                      localStorage.setItem("seo-dashboard-project-id", event.target.value);
                    }}
                  >
                    <option value="">Chọn project</option>
                    {projects.map((item) => (
                      <option key={item.id} value={item.id} className="bg-slate-900">
                        {item.name}
                      </option>
                    ))}
                  </select>
                </label>

                <div className="rounded-[24px] border border-white/10 bg-black/10 px-4 py-3">
                  <p className="text-xs uppercase tracking-[0.2em] text-slate-500">Chế độ</p>
                  <div className="mt-2 flex rounded-full border border-white/10 bg-white/[0.04] p-1">
                    <button
                      className={`flex-1 rounded-full px-4 py-2 text-sm font-semibold transition ${mode === TEAM_MODE ? "bg-neon-cyan text-slate-950" : "text-slate-300"}`}
                      type="button"
                      onClick={() => {
                        setMode(TEAM_MODE);
                        if (activeTab === "keywords") return;
                      }}
                    >
                      Team SEO 👩‍💻
                    </button>
                    <button
                      className={`flex-1 rounded-full px-4 py-2 text-sm font-semibold transition ${mode === CLIENT_MODE ? "bg-neon-blue text-slate-950" : "text-slate-300"}`}
                      type="button"
                      onClick={() => {
                        setMode(CLIENT_MODE);
                        if (activeTab === "keywords") setActiveTab("overview");
                      }}
                    >
                      Khách Hàng 👔
                    </button>
                  </div>
                </div>
              </div>
            </div>

            <div className="mt-5 flex flex-wrap gap-2">
              {(overview?.kpi_chips || []).map((chip) => (
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
              <span>Cập nhật lần cuối: {formatDateTime(project?.last_pulled_at)}</span>
              <span>•</span>
              <span>{project?.keyword_count || 0} keyword</span>
              <span>•</span>
              <span>{project?.date_count || 0} mốc ngày</span>
            </div>

            <div className="flex flex-wrap gap-3">
              <button className="button-secondary" type="button" onClick={handleRefresh} disabled={pageLoading || !selectedProjectId}>
                {pageLoading ? "Đang tải..." : "Refresh thủ công"}
              </button>
              <button className="button-secondary" type="button" onClick={() => setSettingsOpen(true)}>
                Cài đặt
              </button>
              <button className="button-secondary" type="button" onClick={handleLogout}>
                Đăng xuất
              </button>
            </div>
          </div>
        </header>

        <div className="flex flex-wrap gap-3">
          {visibleTabs.map((tab) => (
            <button
              key={tab.id}
              className={`rounded-full px-5 py-3 text-sm font-semibold transition ${activeTab === tab.id ? "bg-gradient-to-r from-neon-blue to-neon-cyan text-slate-950" : "border border-white/10 bg-white/[0.03] text-white hover:border-neon-blue/40"}`}
              type="button"
              onClick={() => setActiveTab(tab.id)}
            >
              {tab.label}
            </button>
          ))}
        </div>

        {!selectedProjectId ? (
          <div className="panel-grid text-center">
            <p className="text-2xl font-bold text-white">Chưa có project nào</p>
            <p className="mt-3 text-sm text-slate-400">
              Bấm <span className="text-neon-cyan">Cài đặt</span> để tạo project mới, kết nối Google Sheets hoặc tải file mẫu.
            </p>
          </div>
        ) : activeTab === "overview" ? (
          <OverviewTab
            overview={overview}
            mode={mode}
            generatingInsight={generatingInsight}
            onGenerateInsight={handleGenerateWeeklyInsight}
            onSaveWeeklyNote={handleSaveWeeklyNote}
            savingWeeklyNote={savingWeeklyNote}
            manualEvent={manualEvent}
            setManualEvent={setManualEvent}
            onAddEvent={handleAddEvent}
            addingEvent={addingEvent}
          />
        ) : activeTab === "groups" ? (
          <GroupTab
            token={token}
            projectId={selectedProjectId}
            data={groupView}
            filters={groupFilters}
            setFilters={setGroupFilters}
            mode={mode}
            onInsightCreated={() => loadOverviewAndSettings(selectedProjectId)}
            setToast={setToast}
          />
        ) : (
          <KeywordTableTab
            data={keywordTable}
            filters={keywordFilters}
            setFilters={setKeywordFilters}
            mode={mode}
            onExport={handleExport}
            exporting={exporting}
            onOpenKeyword={handleOpenKeyword}
          />
        )}
      </div>

      <SettingsPanel
        open={settingsOpen}
            project={project}
            settings={settings}
            uploading={uploading}
            testingSheet={testingSheet}
            saving={savingSettings}
        dragActive={dragActive}
        setDragActive={setDragActive}
            onClose={() => setSettingsOpen(false)}
            onSave={handleSaveSettings}
            onTestSheet={handleTestSheet}
        onUpload={handleUpload}
        onCreateProject={handleCreateProject}
        creatingProject={creatingProject}
        onRecluster={handleRecluster}
        reclustering={reclustering}
      />

      <KeywordDrawer
        open={detailOpen}
        detail={keywordDetail}
        mode={mode}
        insightLoading={insightLoading}
        noteDraft={noteDraft}
        setNoteDraft={setNoteDraft}
        onClose={() => setDetailOpen(false)}
        onSaveNotes={handleSaveNotes}
        onGenerateInsight={handleKeywordInsight}
        savingNotes={savingNotes}
      />
    </div>
  );
}
