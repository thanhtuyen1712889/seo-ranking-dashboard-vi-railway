# SEO Ranking Dashboard

Ứng dụng dashboard SEO production-ready với `FastAPI + React + TailwindCSS + Recharts + SQLite/Turso`.

## Tính năng chính

- Kết nối Google Sheets public và tự pull dữ liệu định kỳ.
- Tải file `xlsx`, `xls`, `csv` bằng drag-and-drop.
- Tự phát hiện header row, cột ngày, KPI, volume và chuẩn hóa dữ liệu ranking.
- 3 view chính: `Tổng Quan Timeline`, `Theo Bộ`, `Chi Tiết Keyword`.
- Chế độ `Team SEO` và `Khách Hàng`.
- Insight AI bằng Claude API, có fallback rule-based nếu không có key.
- Auth đơn giản bằng `DASHBOARD_PASSWORD`.
- Deploy một service duy nhất: FastAPI phục vụ cả API lẫn frontend build tại `frontend/dist`.
- Hỗ trợ DB bền bằng Turso (`libsql`) + keepalive + snapshot backup tự động.

## Cấu trúc chính

```text
/Users/bssgroup/Codex test
├── main.py
├── seo_dashboard/
├── frontend/
│   ├── src/
│   └── dist/
├── requirements.txt
├── Dockerfile
├── railway.toml
├── render.yaml
└── .env.example
```

## Chạy local

```bash
cd "/Users/bssgroup/Codex test"
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cd frontend
npm install
npm run build
cd ..
export DASHBOARD_PASSWORD=demo123
export DATABASE_URL=sqlite:///./data/seo_dashboard.db
export USE_REMOTE_DATABASE=false
uvicorn main:app --reload
```

Mở tại:

```text
http://127.0.0.1:8000
```

## Deploy to Railway (5 phút)
1. Fork hoặc push code lên GitHub
2. Vào railway.app → New Project → Deploy from GitHub
3. Chọn repo này
4. Vào Variables tab → thêm:
   - DASHBOARD_PASSWORD = [mật khẩu bạn muốn]
   - ANTHROPIC_API_KEY = [optional]
5. Deploy → copy URL dạng https://your-app.up.railway.app
6. Gửi link này cho khách hàng

## Best Free Thực Tế: Turso + Keepalive + Snapshot Backup

Mục tiêu: giảm tối đa rủi ro mất dữ liệu khi deploy/restart host.

### 1. Tạo Turso database
1. Tạo database trên Turso (ví dụ `seo-dashboard-prod`)
2. Lấy:
   - `TURSO_DATABASE_URL` dạng `libsql://...`
   - `TURSO_AUTH_TOKEN`

### 2. Set env trên host (Render/Railway)
- `USE_REMOTE_DATABASE=true` (bật chế độ DB từ xa)
- `DATABASE_URL=libsql://your-db-your-org.turso.io`
- `TURSO_AUTH_TOKEN=...`
- `TURSO_REPLICA_PATH=./data/turso-replica.db`
- `TURSO_SYNC_INTERVAL_SECONDS=5`
- `AUTO_BACKUP_ENABLED=true`
- `AUTO_BACKUP_KEEP_DAYS=30`

### 3. Keepalive miễn phí
- Dùng cron miễn phí (ví dụ `cron-job.org`, UptimeRobot) ping endpoint `/health` mỗi 6 giờ.
- Mục tiêu: tránh lâu ngày không truy cập khiến dịch vụ ngủ quá lâu.
- URL ping ví dụ: `https://your-app.onrender.com/health`.

### 4. Snapshot backup tự động
- App tự chạy maintenance trong background:
  - ping DB keepalive
  - tạo `report_snapshot` tự động mỗi ngày cho từng project có dữ liệu
  - giữ lại số ngày backup theo `AUTO_BACKUP_KEEP_DAYS`

API mới:
- `GET /api/projects/{id}/backups` (liệt kê snapshot tự động)
- `POST /api/projects/{id}/backups/run` (tạo snapshot ngay)
- `POST /api/system/maintenance/run` (chạy maintenance thủ công)

## Lưu ý deploy

- Railway sẽ build từ `Dockerfile` và đọc `railway.toml`.
- SQLite nên dùng volume `/data` để dữ liệu không mất sau khi redeploy.
- Khi dùng Turso: không cần phụ thuộc volume local để giữ DB chính.
- Nếu không set `ANTHROPIC_API_KEY`, app vẫn chạy bình thường với insight rule-based.
- Sau khi deploy, vào `Cài đặt` để nhập Google Sheet URL hoặc upload file demo.

## API chính

- `POST /api/auth/login`
- `GET /api/projects`
- `POST /api/projects`
- `POST /api/projects/{id}/upload`
- `POST /api/projects/{id}/refresh`
- `GET /api/projects/{id}/overview`
- `GET /api/projects/{id}/groups`
- `GET /api/projects/{id}/keywords`
- `GET /api/projects/{id}/settings`
- `POST /api/projects/{id}/settings`
- `POST /api/projects/{id}/insights/weekly`
- `POST /api/projects/{id}/events`
- `GET /health`
