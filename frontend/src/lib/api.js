const SESSION_KEY = "seo-dashboard-session";

export function getStoredSession() {
  try {
    const raw = localStorage.getItem(SESSION_KEY);
    if (!raw) return null;
    const value = JSON.parse(raw);
    if (!value?.token || !value?.expiresAt) return null;
    if (Date.now() > value.expiresAt) {
      localStorage.removeItem(SESSION_KEY);
      return null;
    }
    return value;
  } catch {
    return null;
  }
}

export function storeSession(token, expiresInSeconds) {
  const payload = {
    token,
    expiresAt: Date.now() + expiresInSeconds * 1000,
  };
  localStorage.setItem(SESSION_KEY, JSON.stringify(payload));
  return payload;
}

export function clearSession() {
  localStorage.removeItem(SESSION_KEY);
}

async function request(path, { token, method = "GET", body, isForm = false } = {}) {
  const headers = {};
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }
  if (body && !isForm) {
    headers["Content-Type"] = "application/json";
  }
  const response = await fetch(path, {
    method,
    headers,
    body: body ? (isForm ? body : JSON.stringify(body)) : undefined,
  });
  if (!response.ok) {
    let detail = "Có lỗi xảy ra.";
    try {
      const payload = await response.json();
      detail = payload.detail || detail;
    } catch {
      detail = response.statusText || detail;
    }
    throw new Error(detail);
  }
  const contentType = response.headers.get("content-type") || "";
  if (contentType.includes("application/json")) {
    return response.json();
  }
  return response.blob();
}

export function login(password) {
  return request("/api/auth/login", {
    method: "POST",
    body: { password },
  });
}

export function validateSession(token) {
  return request("/api/auth/session", { token });
}

export function getProjects(token) {
  return request("/api/projects", { token });
}

export function createProject(token, payload) {
  return request("/api/projects", { token, method: "POST", body: payload });
}

export function deleteProject(token, projectId) {
  return request(`/api/projects/${projectId}`, { token, method: "DELETE" });
}

export function testGoogleSheet(token, sheetUrl, sheetGid = "") {
  return request("/api/projects/test-sheet", {
    token,
    method: "POST",
    body: { sheet_url: sheetUrl, sheet_gid: sheetGid || null },
  });
}

export function uploadProjectFile(token, projectId, file) {
  const form = new FormData();
  form.append("file", file);
  return request(`/api/projects/${projectId}/upload`, {
    token,
    method: "POST",
    body: form,
    isForm: true,
  });
}

export function refreshProject(token, projectId) {
  return request(`/api/projects/${projectId}/refresh`, { token, method: "POST" });
}

export function getOverview(token, projectId) {
  return request(`/api/projects/${projectId}/overview`, { token });
}

export function getGroupView(token, projectId, params) {
  const query = new URLSearchParams(params);
  return request(`/api/projects/${projectId}/groups?${query.toString()}`, { token });
}

export function getKeywordTable(token, projectId, params) {
  const query = new URLSearchParams(params);
  return request(`/api/projects/${projectId}/keywords?${query.toString()}`, { token });
}

export function getKeywordDetail(token, projectId, keywordId) {
  return request(`/api/projects/${projectId}/keywords/${keywordId}`, { token });
}

export function saveKeywordNotes(token, projectId, keywordId, notes) {
  return request(`/api/projects/${projectId}/keywords/${keywordId}/notes`, {
    token,
    method: "POST",
    body: { notes },
  });
}

export function generateKeywordInsight(token, projectId, keywordId) {
  return request(`/api/projects/${projectId}/keywords/${keywordId}/insight`, {
    token,
    method: "POST",
  });
}

export function getSettings(token, projectId) {
  return request(`/api/projects/${projectId}/settings`, { token });
}

export function updateSettings(token, projectId, payload) {
  return request(`/api/projects/${projectId}/settings`, {
    token,
    method: "POST",
    body: payload,
  });
}

export function reclusterProject(token, projectId) {
  return request(`/api/projects/${projectId}/recluster`, { token, method: "POST" });
}

export function createWeeklyInsight(token, projectId) {
  return request(`/api/projects/${projectId}/insights/weekly`, { token, method: "POST" });
}

export function saveWeeklyNote(token, projectId, content) {
  return request(`/api/projects/${projectId}/insights/weekly-note`, {
    token,
    method: "POST",
    body: { content },
  });
}

export function createClusterInsight(token, projectId, clusterName) {
  return request(`/api/projects/${projectId}/insights/cluster`, {
    token,
    method: "POST",
    body: { cluster_name: clusterName },
  });
}

export function getEvents(token, projectId) {
  return request(`/api/projects/${projectId}/events`, { token });
}

export function createEvent(token, projectId, payload) {
  return request(`/api/projects/${projectId}/events`, {
    token,
    method: "POST",
    body: payload,
  });
}

export async function exportKeywords(token, projectId, params) {
  const query = new URLSearchParams(params);
  const blob = await request(`/api/projects/${projectId}/export?${query.toString()}`, { token });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = "seo-keywords.xlsx";
  anchor.click();
  URL.revokeObjectURL(url);
}
