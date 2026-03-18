# SEO Ranking Dashboard

Ứng dụng dashboard SEO production-ready với `FastAPI + React + TailwindCSS + Recharts + SQLite`.

## Tính năng chính

- Kết nối Google Sheets public và tự pull dữ liệu định kỳ.
- Tải file `xlsx`, `xls`, `csv` bằng drag-and-drop.
- Tự phát hiện header row, cột ngày, KPI, volume và chuẩn hóa dữ liệu ranking.
- 3 view chính: `Tổng Quan Timeline`, `Theo Bộ`, `Chi Tiết Keyword`.
- Chế độ `Team SEO` và `Khách Hàng`.
- Insight AI bằng Claude API, có fallback rule-based nếu không có key.
- Auth đơn giản bằng `DASHBOARD_PASSWORD`.
- Deploy một service duy nhất: FastAPI phục vụ cả API lẫn frontend build tại `frontend/dist`.

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

## Lưu ý deploy

- Railway sẽ build từ `Dockerfile` và đọc `railway.toml`.
- SQLite nên dùng volume `/data` để dữ liệu không mất sau khi redeploy.
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

