import { useEffect, useMemo, useState } from "react";
import GroupTab from "./GroupTab";
import { getPublicShare, loginPublicShare } from "../lib/api";
import { formatDateTime } from "../lib/format";

function storageKeyForShare(shareToken) {
  return `seo-dashboard-public-${shareToken}`;
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
  const [groupFilters, setGroupFilters] = useState({
    active_scenario_id: "",
  });

  useEffect(() => {
    let cancelled = false;
    async function loadPublicView() {
      setLoading(true);
      setError("");
      try {
        const response = await getPublicShare(
          shareToken,
          {
            active_scenario_id: groupFilters.active_scenario_id || undefined,
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
        setGroupFilters((previous) => ({
          ...previous,
          active_scenario_id:
            previous.active_scenario_id ||
            response.group_view?.active_scenario_id ||
            "",
        }));
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
  }, [shareToken, publicToken, groupFilters.active_scenario_id]);

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

  if (loading) {
    return <div className="flex min-h-screen items-center justify-center text-sm text-slate-400">Đang tải link chia sẻ...</div>;
  }

  if (requiresPassword) {
    return (
      <div className="flex min-h-screen items-center justify-center px-4">
        <form className="glass-panel w-full max-w-lg p-8" onSubmit={handlePublicLogin}>
          <p className="text-xs uppercase tracking-[0.3em] text-neon-cyan">
            {shareType === "report" ? "Report snapshot" : "Client view"}
          </p>
          <h1 className="mt-3 text-3xl font-bold text-white">{payload?.title || "Link chia sẻ có mật khẩu"}</h1>
          <p className="mt-3 text-sm leading-7 text-slate-400">
            Link này được bảo vệ bằng mật khẩu để khách chỉ xem đúng snapshot báo cáo đã chia sẻ.
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
            {loggingIn ? "Đang mở..." : "Mở báo cáo"}
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
    <div className="min-h-screen px-4 py-5 lg:px-6">
      <div className="mx-auto max-w-[1600px] space-y-6">
        <header className="glass-panel overflow-hidden shadow-glow">
          <div className="border-b border-white/10 bg-gradient-to-r from-neon-blue/15 via-transparent to-neon-cyan/10 px-5 py-5">
            <p className="font-display text-xs uppercase tracking-[0.4em] text-neon-cyan">
              {payload.share_type === "report_snapshot" ? "Snapshot báo cáo" : "Link khách hàng"}
            </p>
            <h1 className="mt-3 text-3xl font-bold text-white">{payload.title || payload.project_name}</h1>
            <p className="mt-3 max-w-4xl text-sm leading-7 text-slate-300">
              {payload.overview?.subtitle || "Link chia sẻ chỉ hiển thị snapshot và các scenario đã lưu cho khách hàng."}
            </p>
            <div className="mt-4 flex flex-wrap gap-2 text-sm text-slate-400">
              <span className="chip">{payload.project_name}</span>
              <span className="chip">{payload.group_view?.cluster_overview?.total_keywords || 0} keyword</span>
              <span className="chip">Cập nhật {formatDateTime(payload.snapshot_created_at)}</span>
            </div>
          </div>
        </header>

        {payload.overview?.latest_insight?.content_vi ? (
          <div className="rounded-[32px] border border-neon-yellow/30 bg-gradient-to-br from-neon-yellow/12 to-transparent p-6 shadow-warning">
            <p className="text-xs uppercase tracking-[0.3em] text-neon-yellow">Nhận xét hàng tuần</p>
            <div className="mt-3 rounded-[24px] border border-white/10 bg-black/20 px-5 py-4 text-base leading-8 text-slate-100">
              {payload.overview.latest_insight.content_vi}
            </div>
          </div>
        ) : null}

        <GroupTab
          data={payload.group_view}
          filters={groupFilters}
          setFilters={setGroupFilters}
          mode="client"
          controlsMode="scenario_only"
        />
      </div>
    </div>
  );
}
