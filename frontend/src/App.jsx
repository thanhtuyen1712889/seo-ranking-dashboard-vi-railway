import { useDeferredValue, useEffect, useState } from "react";
import GroupTab from "./components/GroupTab";
import KeywordDrawer from "./components/KeywordDrawer";
import KeywordTableTab from "./components/KeywordTableTab";
import LoginView from "./components/LoginView";
import OverviewTab from "./components/OverviewTab";
import PublicShareView from "./components/PublicShareView";
import SharePanel from "./components/SharePanel";
import SettingsPanel from "./components/SettingsPanel";
import {
  clearSession,
  createClientViewShare,
  createEvent,
  createProject,
  createReportSnapshotShare,
  createSeoViewShare,
  exportKeywords,
  generateWeeklyRangeNote,
  generateKeywordInsight,
  getGroupView,
  getKeywordDetail,
  getRefreshStatus,
  getKeywordTable,
  getOverview,
  getProjects,
  getSettings,
  getStoredSession,
  getWeeklyRangeNote,
  login,
  pinWeeklyRangeNote,
  reclusterProject,
  refreshProject,
  saveProjectViewState,
  saveKeywordNotes,
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
  const publicShareMatch = window.location.pathname.match(/^\/(client|seo|report)\/([^/]+)$/);
  if (publicShareMatch) {
    return <PublicShareView shareType={publicShareMatch[1]} shareToken={publicShareMatch[2]} />;
  }

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
  const [weeklyNote, setWeeklyNote] = useState(null);
  const [weeklyNoteRange, setWeeklyNoteRange] = useState({
    from_date: "",
    to_date: "",
  });
  const [detailOpen, setDetailOpen] = useState(false);
  const [keywordDetail, setKeywordDetail] = useState(null);
  const [noteDraft, setNoteDraft] = useState("");

  const [pageLoading, setPageLoading] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [testingSheet, setTestingSheet] = useState(false);
  const [sheetTestStatus, setSheetTestStatus] = useState(null);
  const [savingSettings, setSavingSettings] = useState(false);
  const [creatingProject, setCreatingProject] = useState(false);
  const [generatingInsight, setGeneratingInsight] = useState(false);
  const [savingWeeklyNote, setSavingWeeklyNote] = useState(false);
  const [weeklyNoteLoading, setWeeklyNoteLoading] = useState(false);
  const [addingEvent, setAddingEvent] = useState(false);
  const [dragActive, setDragActive] = useState(false);
  const [savingNotes, setSavingNotes] = useState(false);
  const [insightLoading, setInsightLoading] = useState(false);
  const [exporting, setExporting] = useState(false);
  const [reclustering, setReclustering] = useState(false);
  const [creatingClientView, setCreatingClientView] = useState(false);
  const [creatingSeoView, setCreatingSeoView] = useState(false);
  const [creatingReportSnapshot, setCreatingReportSnapshot] = useState(false);
  const [viewStateReady, setViewStateReady] = useState(false);
  const [shareResult, setShareResult] = useState({
    client_view_url: "",
    client_view_password: "",
    seo_view_url: "",
    seo_view_password: "",
    report_snapshot_url: "",
  });

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
    active_scenario_id: "",
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
    sort_by: "current_rank",
    sort_dir: "asc",
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
    setViewStateReady(false);
    setWeeklyNote(null);
    setWeeklyNoteRange({ from_date: "", to_date: "" });
    setShareResult({
      client_view_url: "",
      client_view_password: "",
      seo_view_url: "",
      seo_view_password: "",
      report_snapshot_url: "",
    });
    loadOverviewAndSettings(selectedProjectId, { preserveExisting: false });
  }, [selectedProjectId, token]);

  useEffect(() => {
    if (!selectedProjectId || !token || !groupFilters.current_date) return;
    getGroupView(token, selectedProjectId, groupFilters)
      .then(setGroupView)
      .catch((error) => setToast({ type: "error", message: error.message }));
  }, [selectedProjectId, token, groupFilters.current_date, groupFilters.baseline_date, groupFilters.status, groupFilters.main_cluster, groupFilters.tag, groupFilters.sort_by, groupFilters.active_scenario_id]);

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
    keywordFilters.sort_by,
    keywordFilters.sort_dir,
    deferredSearch,
  ]);

  useEffect(() => {
    if (!selectedProjectId || !token || !weeklyNoteRange.from_date || !weeklyNoteRange.to_date) return;
    let cancelled = false;
    setWeeklyNoteLoading(true);
    getWeeklyRangeNote(token, selectedProjectId, weeklyNoteRange)
      .then((response) => {
        if (cancelled) return;
        setWeeklyNote(response);
      })
      .catch((error) => {
        if (cancelled) return;
        setToast({ type: "error", message: error.message });
      })
      .finally(() => {
        if (!cancelled) setWeeklyNoteLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [
    selectedProjectId,
    token,
    weeklyNoteRange.from_date,
    weeklyNoteRange.to_date,
    overview?.project?.last_pulled_at,
    overview?.dates?.length,
  ]);

  useEffect(() => {
    if (!toast) return;
    const timer = window.setTimeout(() => setToast(null), 3200);
    return () => window.clearTimeout(timer);
  }, [toast]);

  useEffect(() => {
    if (!selectedProjectId || !token || !viewStateReady) return;
    const timer = window.setTimeout(() => {
      saveProjectViewState(token, selectedProjectId, {
        mode,
        active_tab: activeTab,
        weekly_note_range: weeklyNoteRange,
        group_filters: groupFilters,
        keyword_filters: keywordFilters,
      }).catch(() => {});
    }, 500);
    return () => window.clearTimeout(timer);
  }, [
    selectedProjectId,
    token,
    viewStateReady,
    mode,
    activeTab,
    weeklyNoteRange.from_date,
    weeklyNoteRange.to_date,
    groupFilters.current_date,
    groupFilters.baseline_date,
    groupFilters.status,
    groupFilters.main_cluster,
    groupFilters.tag,
    groupFilters.sort_by,
    groupFilters.active_scenario_id,
    keywordFilters.current_date,
    keywordFilters.search,
    keywordFilters.groups,
    keywordFilters.clusters,
    keywordFilters.status,
    keywordFilters.vol_min,
    keywordFilters.vol_max,
    keywordFilters.rank_min,
    keywordFilters.rank_max,
    keywordFilters.movers_only,
    keywordFilters.sort_by,
    keywordFilters.sort_dir,
  ]);

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
        setViewStateReady(false);
        setSelectedProjectId("");
        setOverview(null);
        setGroupView(null);
        setKeywordTable(null);
        setWeeklyNote(null);
        setWeeklyNoteRange({ from_date: "", to_date: "" });
        setSettings(null);
      }
    } catch (error) {
      setToast({ type: "error", message: error.message });
    }
  }

  async function loadOverviewAndSettings(projectId, { preserveExisting = true } = {}) {
    setPageLoading(true);
    try {
      const [overviewResponse, settingsResponse] = await Promise.all([
        getOverview(token, projectId),
        getSettings(token, projectId),
      ]);
      setOverview(overviewResponse);
      setSettings(settingsResponse);
      const savedState = settingsResponse?.project?.saved_view_state || overviewResponse?.project?.saved_view_state || {};
      const savedGroupFilters = savedState.group_filters || {};
      const savedKeywordFilters = savedState.keyword_filters || {};
      const savedWeeklyRange = savedState.weekly_note_range || {};
      if (!preserveExisting) {
        const nextMode = savedState.mode === CLIENT_MODE ? CLIENT_MODE : TEAM_MODE;
        setMode(nextMode);
        const nextTab = savedState.active_tab || "overview";
        setActiveTab(nextMode === CLIENT_MODE && nextTab === "keywords" ? "overview" : nextTab);
      }
      const dates = overviewResponse.dates || [];
      if (dates.length) {
        const current = dates[dates.length - 1];
        const baseline = dates[dates.length - 2] || dates[0];
        const defaultWeeklyFrom = dates[Math.max(0, dates.length - 7)];
        const nextWeeklyRange = {
          from_date:
            (preserveExisting && weeklyNoteRange.from_date && dates.includes(weeklyNoteRange.from_date) && weeklyNoteRange.from_date) ||
            (savedWeeklyRange.from_date && dates.includes(savedWeeklyRange.from_date) && savedWeeklyRange.from_date) ||
            defaultWeeklyFrom,
          to_date:
            (preserveExisting && weeklyNoteRange.to_date && dates.includes(weeklyNoteRange.to_date) && weeklyNoteRange.to_date) ||
            (savedWeeklyRange.to_date && dates.includes(savedWeeklyRange.to_date) && savedWeeklyRange.to_date) ||
            current,
        };
        const resolvedCurrentGroupDate =
          (preserveExisting && groupFilters.current_date && dates.includes(groupFilters.current_date) && groupFilters.current_date) ||
          (savedGroupFilters.current_date && dates.includes(savedGroupFilters.current_date) && savedGroupFilters.current_date) ||
          current;
        const resolvedBaselineGroupDate =
          (preserveExisting && groupFilters.baseline_date && dates.includes(groupFilters.baseline_date) && groupFilters.baseline_date) ||
          (savedGroupFilters.baseline_date && dates.includes(savedGroupFilters.baseline_date) && savedGroupFilters.baseline_date) ||
          baseline;
        const nextGroupFilters = {
          current_date: resolvedCurrentGroupDate,
          baseline_date: resolvedBaselineGroupDate,
          status: (preserveExisting ? groupFilters.status : "") || savedGroupFilters.status || "all",
          main_cluster: (preserveExisting ? groupFilters.main_cluster : "") || savedGroupFilters.main_cluster || "",
          tag: (preserveExisting ? groupFilters.tag : "") || savedGroupFilters.tag || "all",
          sort_by: (preserveExisting ? groupFilters.sort_by : "") || savedGroupFilters.sort_by || "health_score",
          active_scenario_id:
            (preserveExisting ? groupFilters.active_scenario_id : "") ||
            savedGroupFilters.active_scenario_id ||
            "",
        };
        const resolvedCurrentKeywordDate =
          (preserveExisting && keywordFilters.current_date && dates.includes(keywordFilters.current_date) && keywordFilters.current_date) ||
          (savedKeywordFilters.current_date && dates.includes(savedKeywordFilters.current_date) && savedKeywordFilters.current_date) ||
          current;
        const nextKeywordFilters = {
          current_date: resolvedCurrentKeywordDate,
          search: (preserveExisting ? keywordFilters.search : "") || savedKeywordFilters.search || "",
          groups: (preserveExisting ? keywordFilters.groups : "") || savedKeywordFilters.groups || "",
          clusters: (preserveExisting ? keywordFilters.clusters : "") || savedKeywordFilters.clusters || "",
          status: (preserveExisting ? keywordFilters.status : "") || savedKeywordFilters.status || "all",
          vol_min: (preserveExisting ? keywordFilters.vol_min : undefined) ?? savedKeywordFilters.vol_min ?? 0,
          vol_max: (preserveExisting ? keywordFilters.vol_max : undefined) ?? savedKeywordFilters.vol_max ?? 1000000,
          rank_min: (preserveExisting ? keywordFilters.rank_min : undefined) ?? savedKeywordFilters.rank_min ?? 0,
          rank_max: (preserveExisting ? keywordFilters.rank_max : undefined) ?? savedKeywordFilters.rank_max ?? 101,
          movers_only:
            (preserveExisting ? keywordFilters.movers_only : undefined) ??
            savedKeywordFilters.movers_only ??
            false,
          sort_by: (preserveExisting ? keywordFilters.sort_by : "") || savedKeywordFilters.sort_by || "current_rank",
          sort_dir:
            (preserveExisting ? keywordFilters.sort_dir : "") ||
            savedKeywordFilters.sort_dir ||
            "asc",
        };
        setWeeklyNoteRange(nextWeeklyRange);
        setGroupFilters(nextGroupFilters);
        setKeywordFilters(nextKeywordFilters);
        setManualEvent((previous) => ({
          ...previous,
          event_date: resolvedCurrentGroupDate,
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
        setWeeklyNote(null);
        setWeeklyNoteRange({ from_date: "", to_date: "" });
      }
    } catch (error) {
      setToast({ type: "error", message: error.message });
    } finally {
      setViewStateReady(true);
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
    setWeeklyNote(null);
    setWeeklyNoteRange({ from_date: "", to_date: "" });
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
    } catch (error) {
      setToast({ type: "error", message: error.message });
    } finally {
      setUploading(false);
    }
  }

  async function handleRefresh() {
    if (!selectedProjectId) return;
    setPageLoading(true);
    try {
      const trigger = await refreshProject(token, selectedProjectId);
      const startedAt = Date.now();
      let sawRunning = trigger?.status === "running";
      let completed = false;
      while (Date.now() - startedAt < 180000) {
        await new Promise((resolve) => {
          setTimeout(resolve, 2000);
        });
        const status = await getRefreshStatus(token, selectedProjectId);
        if (status.status === "running") {
          sawRunning = true;
        }
        if (status.status === "completed") {
          completed = true;
          break;
        }
        if (status.status === "failed") {
          throw new Error(status.error || "Refresh thất bại.");
        }
        if (status.status === "idle" && sawRunning) {
          completed = true;
          break;
        }
      }
      if (!completed) {
        await loadOverviewAndSettings(selectedProjectId);
        setToast({
          type: "success",
          message: "Đã gửi lệnh refresh. Server đang xử lý nền, dữ liệu sẽ tự cập nhật sau ít phút.",
        });
        return;
      }
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
      setSheetTestStatus({
        type: "error",
        message: "Thiếu link dữ liệu public.",
      });
      return;
    }
    setTestingSheet(true);
    setSheetTestStatus({
      type: "loading",
      message: "Đang kiểm tra kết nối. Với Google Sheets, bước này có thể mất 30-60 giây.",
    });
    try {
      const response = await testGoogleSheet(token, sheetUrl, sheetGid);
      const successMessage = `Kết nối thành công. Tìm thấy ${response.row_count} keyword, ${response.dates.length} mốc ngày${response.selected_sheet_name ? ` · tab: ${response.selected_sheet_name}` : ""}.`;
      setSheetTestStatus({
        type: "success",
        message: `${successMessage} Nhấn “Lưu cài đặt” để ghi vào project hiện tại.`,
      });
      setToast({
        type: "success",
        message: successMessage,
      });
    } catch (error) {
      setSheetTestStatus({
        type: "error",
        message: error.message || "Không kiểm tra được kết nối.",
      });
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
      setSheetTestStatus({
        type: "success",
        message: "Đã lưu cài đặt. Bấm “Refresh thủ công” ở header để kéo dữ liệu mới ngay.",
      });
      setToast({ type: "success", message: "Đã lưu cài đặt." });
    } catch (error) {
      setSheetTestStatus({
        type: "error",
        message: error.message || "Lưu cài đặt thất bại.",
      });
      setToast({ type: "error", message: error.message });
    } finally {
      setSavingSettings(false);
    }
  }

  async function handleGenerateWeeklyInsight() {
    if (!selectedProjectId) return;
    setGeneratingInsight(true);
    try {
      const response = await generateWeeklyRangeNote(token, selectedProjectId, weeklyNoteRange);
      setWeeklyNote(response);
      setToast({ type: "success", message: "Đã tạo nhận xét mới theo dữ liệu hiện tại." });
    } catch (error) {
      setToast({ type: "error", message: error.message });
    } finally {
      setGeneratingInsight(false);
    }
  }

  async function handleSaveWeeklyNote(content, author = "User") {
    if (!selectedProjectId) return;
    setSavingWeeklyNote(true);
    try {
      const response = await pinWeeklyRangeNote(token, selectedProjectId, {
        ...weeklyNoteRange,
        content,
        author,
      });
      setWeeklyNote(response);
      setToast({ type: "success", message: "Đã lưu và ghim nhận xét cho khoảng ngày này." });
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

  async function handleCreateClientViewShare(form) {
    if (!selectedProjectId) return;
    setCreatingClientView(true);
    try {
      const response = await createClientViewShare(token, selectedProjectId, {
        ...form,
        state: buildShareState(CLIENT_MODE),
      });
      setShareResult((previous) => ({
        ...previous,
        client_view_url: response.client_view_url || previous.client_view_url,
        client_view_password: response.client_view_password || "",
      }));
      setToast({ type: "success", message: "Đã tạo link khách hàng." });
      const refreshedGroupView = await getGroupView(token, selectedProjectId, groupFilters);
      setGroupView(refreshedGroupView);
    } catch (error) {
      setToast({ type: "error", message: error.message });
    } finally {
      setCreatingClientView(false);
    }
  }

  function buildShareState(targetMode) {
    return {
      mode: targetMode,
      active_tab: targetMode === CLIENT_MODE && activeTab === "keywords" ? "overview" : activeTab,
      weekly_note_range: weeklyNoteRange,
      group_filters: groupFilters,
      keyword_filters: keywordFilters,
    };
  }

  async function handleCreateSeoViewShare(form) {
    if (!selectedProjectId) return;
    setCreatingSeoView(true);
    try {
      const response = await createSeoViewShare(token, selectedProjectId, {
        ...form,
        state: buildShareState(TEAM_MODE),
      });
      setShareResult((previous) => ({
        ...previous,
        seo_view_url: response.seo_view_url || previous.seo_view_url,
        seo_view_password: response.seo_view_password || "",
      }));
      setToast({ type: "success", message: "Đã tạo link SEO riêng cho project này." });
      const refreshedGroupView = await getGroupView(token, selectedProjectId, groupFilters);
      setGroupView(refreshedGroupView);
    } catch (error) {
      setToast({ type: "error", message: error.message });
    } finally {
      setCreatingSeoView(false);
    }
  }

  async function handleCreateReportSnapshot(form) {
    if (!selectedProjectId) return;
    setCreatingReportSnapshot(true);
    try {
      const response = await createReportSnapshotShare(token, selectedProjectId, {
        ...form,
        state: buildShareState(mode),
      });
      setShareResult((previous) => ({
        ...previous,
        report_snapshot_url: response.report_snapshot_url || previous.report_snapshot_url,
      }));
      setToast({ type: "success", message: "Đã đóng băng report snapshot." });
      const refreshedGroupView = await getGroupView(token, selectedProjectId, groupFilters);
      setGroupView(refreshedGroupView);
    } catch (error) {
      setToast({ type: "error", message: error.message });
    } finally {
      setCreatingReportSnapshot(false);
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
        <div className={`fixed left-1/2 top-5 z-[70] -translate-x-1/2 rounded-full border px-5 py-3 text-sm font-semibold shadow-glow ${toast.type === "error" ? "border-neon-red/40 bg-neon-red/10 text-neon-red" : "border-neon-cyan/30 bg-neon-cyan/10 text-neon-cyan"}`}>
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
                  Theo dõi thứ hạng SEO
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
            weeklyNote={weeklyNote}
            weeklyNoteRange={weeklyNoteRange}
            setWeeklyNoteRange={setWeeklyNoteRange}
            weeklyNoteLoading={weeklyNoteLoading}
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
          <div className="space-y-6">
            {mode === TEAM_MODE ? (
              <SharePanel
                projectName={project?.name || overview?.project?.name || ""}
                clientViewUrl={shareResult.client_view_url || groupView?.client_view_url || settings?.client_view_url || ""}
                seoViewUrl={shareResult.seo_view_url || groupView?.seo_view_url || settings?.seo_view_url || ""}
                reportSnapshotUrl={shareResult.report_snapshot_url || groupView?.report_snapshot_url || settings?.report_snapshot_url || ""}
                latestClientPassword={shareResult.client_view_password || ""}
                latestSeoPassword={shareResult.seo_view_password || ""}
                onCreateClientView={handleCreateClientViewShare}
                onCreateSeoView={handleCreateSeoViewShare}
                onCreateReportSnapshot={handleCreateReportSnapshot}
                creatingClientView={creatingClientView}
                creatingSeoView={creatingSeoView}
                creatingReportSnapshot={creatingReportSnapshot}
              />
            ) : null}
            <GroupTab
              data={groupView}
              filters={groupFilters}
              setFilters={setGroupFilters}
              mode={mode}
            />
          </div>
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
        sheetTestStatus={sheetTestStatus}
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
