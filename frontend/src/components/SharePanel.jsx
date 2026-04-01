import { useEffect, useState } from "react";

export default function SharePanel({
  projectName,
  clientViewUrl,
  seoViewUrl,
  reportSnapshotUrl,
  latestClientPassword,
  latestSeoPassword,
  onCreateClientView,
  onCreateSeoView,
  onCreateReportSnapshot,
  creatingClientView,
  creatingSeoView,
  creatingReportSnapshot,
}) {
  const [title, setTitle] = useState("");
  const [password, setPassword] = useState("");
  const [copiedField, setCopiedField] = useState("");

  useEffect(() => {
    setTitle(projectName ? `${projectName} · Portal dự án` : "");
  }, [projectName]);

  useEffect(() => {
    if (!copiedField) return undefined;
    const timer = window.setTimeout(() => setCopiedField(""), 1800);
    return () => window.clearTimeout(timer);
  }, [copiedField]);

  async function handleCopy(value, field) {
    if (!value) return;
    try {
      await navigator.clipboard.writeText(value);
      setCopiedField(field);
    } catch {
      setCopiedField("");
    }
  }

  return (
    <div className="panel-grid">
      <div className="flex flex-col gap-4 xl:flex-row xl:items-end xl:justify-between">
        <div className="max-w-3xl">
          <p className="text-sm font-semibold text-white">Chia sẻ snapshot cho khách</p>
          <p className="mt-1 text-sm leading-7 text-slate-400">
            Link Khách và SEO đều chỉ mở đúng project hiện tại, không thấy project khác. Link SEO được đóng băng dữ liệu tương tác và giữ ổn định 24 giờ; link report cũng đóng băng snapshot tại thời điểm bấm xuất.
          </p>
        </div>
        <div className="grid w-full gap-3 xl:max-w-[920px] xl:grid-cols-[1.2fr,1fr,auto,auto,auto]">
          <input
            className="input-dark"
            value={title}
            onChange={(event) => setTitle(event.target.value)}
            placeholder="Tiêu đề hiển thị cho link khách"
          />
          <input
            className="input-dark"
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            placeholder="Mật khẩu link khách (tuỳ chọn)"
          />
          <button
            className="button-secondary"
            type="button"
            disabled={creatingClientView}
            onClick={() => onCreateClientView({ title, password })}
          >
            {creatingClientView ? "Đang tạo..." : "Tạo link khách"}
          </button>
          <button
            className="button-secondary"
            type="button"
            disabled={creatingSeoView}
            onClick={() => onCreateSeoView({ title, password })}
          >
            {creatingSeoView ? "Đang tạo..." : "Tạo link SEO"}
          </button>
          <button
            className="button-primary"
            type="button"
            disabled={creatingReportSnapshot}
            onClick={() => onCreateReportSnapshot({ title, password })}
          >
            {creatingReportSnapshot ? "Đang xuất..." : "Export report"}
          </button>
        </div>
      </div>

      <div className="mt-5 grid gap-3 xl:grid-cols-3">
        <div className="rounded-[24px] border border-white/10 bg-black/10 p-4">
          <p className="text-xs uppercase tracking-[0.22em] text-slate-500">Client portal</p>
          <div className="mt-3 flex gap-3">
            <input className="input-dark flex-1" readOnly value={clientViewUrl || ""} placeholder="Chưa tạo link khách hàng" />
            <button className="button-secondary px-4" type="button" onClick={() => handleCopy(clientViewUrl, "client")} disabled={!clientViewUrl}>
              {copiedField === "client" ? "Đã copy" : "Copy"}
            </button>
          </div>
          <p className="mt-2 text-sm text-slate-400">
            {latestClientPassword ? `Mật khẩu vừa tạo: ${latestClientPassword}` : "Bạn có thể để trống mật khẩu nếu chỉ cần link đọc nhanh."}
          </p>
        </div>

        <div className="rounded-[24px] border border-white/10 bg-black/10 p-4">
          <p className="text-xs uppercase tracking-[0.22em] text-slate-500">SEO portal</p>
          <div className="mt-3 flex gap-3">
            <input className="input-dark flex-1" readOnly value={seoViewUrl || ""} placeholder="Chưa tạo link SEO" />
            <button className="button-secondary px-4" type="button" onClick={() => handleCopy(seoViewUrl, "seo")} disabled={!seoViewUrl}>
              {copiedField === "seo" ? "Đã copy" : "Copy"}
            </button>
          </div>
          <p className="mt-2 text-sm text-slate-400">
            {latestSeoPassword ? `Mật khẩu vừa tạo: ${latestSeoPassword}` : "Link Team SEO đóng băng dữ liệu, vẫn dùng được tab Chi Tiết Keyword và giữ ổn định trong 24 giờ."}
          </p>
        </div>

        <div className="rounded-[24px] border border-white/10 bg-black/10 p-4">
          <p className="text-xs uppercase tracking-[0.22em] text-slate-500">Report snapshot</p>
          <div className="mt-3 flex gap-3">
            <input className="input-dark flex-1" readOnly value={reportSnapshotUrl || ""} placeholder="Chưa tạo report snapshot" />
            <button className="button-secondary px-4" type="button" onClick={() => handleCopy(reportSnapshotUrl, "report")} disabled={!reportSnapshotUrl}>
              {copiedField === "report" ? "Đã copy" : "Copy"}
            </button>
          </div>
          <p className="mt-2 text-sm text-slate-400">
            Link này đọc-only và giữ nguyên snapshot hiện tại ngay cả khi project tiếp tục import thêm dữ liệu.
          </p>
        </div>
      </div>
    </div>
  );
}
