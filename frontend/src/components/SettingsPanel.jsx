import { useEffect, useState } from "react";
import Dropzone from "./Dropzone";

export default function SettingsPanel({
  open,
  project,
  settings,
  uploading,
  testingSheet,
  saving,
  dragActive,
  setDragActive,
  onClose,
  onSave,
  onTestSheet,
  onUpload,
  onCreateProject,
  creatingProject,
  onRecluster,
  reclustering,
}) {
  const [form, setForm] = useState({
    name: "",
    source_name: "",
    sheet_url: "",
    refresh_interval_minutes: 30,
    anthropic_api_key: "",
    clusters: [],
    new_project_name: "",
  });

  useEffect(() => {
    setForm({
      name: settings?.project?.name || "",
      source_name: settings?.project?.source_name || "",
      sheet_url: settings?.project?.sheet_url || "",
      refresh_interval_minutes: settings?.project?.refresh_interval_minutes || 30,
      anthropic_api_key: settings?.project?.anthropic_api_key || "",
      clusters: (settings?.clusters || []).map((cluster) => ({ ...cluster })),
      new_project_name: "",
    });
  }, [settings]);

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 bg-black/55 backdrop-blur-sm">
      <div className="absolute right-0 top-0 h-full w-full max-w-3xl overflow-y-auto border-l border-white/10 bg-[#0b121b]/96 px-6 py-6">
        <div className="flex items-start justify-between gap-4">
          <div>
            <p className="text-xs uppercase tracking-[0.3em] text-neon-cyan">Settings</p>
            <h2 className="mt-2 text-3xl font-bold text-white">Cài đặt dữ liệu & KPI</h2>
            <p className="mt-2 text-sm text-slate-400">
              Kết nối Google Sheets, tải file Excel/CSV, cấu hình KPI theo cụm và bật AI insight.
            </p>
          </div>
          <button className="button-secondary" type="button" onClick={onClose}>
            Đóng
          </button>
        </div>

        <div className="mt-6 space-y-6">
          <section className="rounded-[32px] border border-white/10 bg-white/[0.03] p-5">
            <p className="text-sm font-semibold text-white">Tạo project mới</p>
            <div className="mt-3 flex flex-col gap-3 md:flex-row">
              <input
                className="input-dark"
                value={form.new_project_name}
                onChange={(event) => setForm({ ...form, new_project_name: event.target.value })}
                placeholder="Ví dụ: SEO Magento Q2"
              />
              <button
                className="button-primary md:w-52"
                type="button"
                disabled={creatingProject || !form.new_project_name.trim()}
                onClick={() => onCreateProject(form.new_project_name.trim())}
              >
                {creatingProject ? "Đang tạo..." : "Tạo project"}
              </button>
            </div>
          </section>

          {project ? (
            <>
              <section className="rounded-[32px] border border-white/10 bg-white/[0.03] p-5">
                <p className="text-sm font-semibold text-white">Google Sheets live pull</p>
                <div className="mt-4 grid gap-4">
                  <input
                    className="input-dark"
                    value={form.name}
                    onChange={(event) => setForm({ ...form, name: event.target.value })}
                    placeholder="Tên project"
                  />
                  <input
                    className="input-dark"
                    value={form.source_name}
                    onChange={(event) => setForm({ ...form, source_name: event.target.value })}
                    placeholder="Tên nguồn hiển thị ở header"
                  />
                  <input
                    className="input-dark"
                    value={form.sheet_url}
                    onChange={(event) => setForm({ ...form, sheet_url: event.target.value })}
                    placeholder="Dán Google Sheet URL public"
                  />
                  <div className="grid gap-3 md:grid-cols-[1fr,220px]">
                    <select
                      className="input-dark"
                      value={form.refresh_interval_minutes}
                      onChange={(event) => setForm({ ...form, refresh_interval_minutes: Number(event.target.value) })}
                    >
                      <option value={15}>15 phút</option>
                      <option value={30}>30 phút</option>
                      <option value={60}>1 giờ</option>
                      <option value={999999}>Chỉ cập nhật thủ công</option>
                    </select>
                    <button className="button-secondary" type="button" onClick={() => onTestSheet(form.sheet_url)} disabled={testingSheet || !form.sheet_url.trim()}>
                      {testingSheet ? "Đang kiểm tra..." : "Test connection"}
                    </button>
                  </div>
                </div>
              </section>

              <section className="rounded-[32px] border border-white/10 bg-white/[0.03] p-5">
                <p className="text-sm font-semibold text-white">Tải file Excel / CSV</p>
                <div className="mt-4">
                  <Dropzone
                    dragging={dragActive}
                    onDragState={setDragActive}
                    onFileSelect={onUpload}
                    label={uploading ? "Đang tải file..." : "Kéo file hoặc bấm để chọn file"}
                  />
                </div>
              </section>

              <section className="rounded-[32px] border border-white/10 bg-white/[0.03] p-5">
                <p className="text-sm font-semibold text-white">Claude API key (optional)</p>
                <input
                  className="input-dark mt-3"
                  value={form.anthropic_api_key}
                  onChange={(event) => setForm({ ...form, anthropic_api_key: event.target.value })}
                  placeholder="sk-ant-..."
                />
              </section>

              <section className="rounded-[32px] border border-white/10 bg-white/[0.03] p-5">
                <div className="flex items-center justify-between gap-3">
                  <p className="text-sm font-semibold text-white">KPI theo cụm</p>
                  <button className="button-secondary" type="button" onClick={onRecluster} disabled={reclustering}>
                    {reclustering ? "Đang phân cụm..." : "Chạy lại sub-cluster"}
                  </button>
                </div>
                <div className="mt-4 space-y-3">
                  {form.clusters.map((cluster, index) => (
                    <div key={`${cluster.group_name}-${cluster.name}-${index}`} className="grid gap-3 rounded-[24px] border border-white/10 bg-black/10 p-4 md:grid-cols-[1.2fr,1fr,120px]">
                      <div>
                        <p className="font-semibold text-white">{cluster.name}</p>
                        <p className="mt-1 text-sm text-slate-400">{cluster.group_name}</p>
                      </div>
                      <select
                        className="input-dark"
                        value={cluster.kpi_target}
                        onChange={(event) => {
                          const next = [...form.clusters];
                          next[index] = { ...cluster, kpi_target: Number(event.target.value) };
                          setForm({ ...form, clusters: next });
                        }}
                      >
                        <option value={3}>Top 3</option>
                        <option value={5}>Top 5</option>
                        <option value={10}>Top 10</option>
                      </select>
                      <input
                        className="input-dark"
                        type="number"
                        value={cluster.target_keywords}
                        onChange={(event) => {
                          const next = [...form.clusters];
                          next[index] = { ...cluster, target_keywords: Number(event.target.value) };
                          setForm({ ...form, clusters: next });
                        }}
                        placeholder="Target KW"
                      />
                    </div>
                  ))}
                </div>
              </section>

              <div className="sticky bottom-0 flex justify-end gap-3 border-t border-white/10 bg-[#0b121b]/95 py-4">
                <button className="button-secondary" type="button" onClick={onClose}>
                  Hủy
                </button>
                <button className="button-primary" type="button" onClick={() => onSave(form)} disabled={saving}>
                  {saving ? "Đang lưu..." : "Lưu cài đặt"}
                </button>
              </div>
            </>
          ) : (
            <section className="rounded-[32px] border border-white/10 bg-white/[0.03] p-5 text-sm text-slate-300">
              Hãy tạo project trước để tải dữ liệu hoặc cấu hình KPI.
            </section>
          )}
        </div>
      </div>
    </div>
  );
}

