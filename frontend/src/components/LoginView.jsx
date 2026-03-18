export default function LoginView({ password, setPassword, submitting, error, onSubmit }) {
  return (
    <div className="flex min-h-screen items-center justify-center px-4 py-10">
      <div className="glass-panel w-full max-w-md overflow-hidden shadow-glow">
        <div className="border-b border-white/10 bg-gradient-to-r from-neon-blue/15 to-neon-cyan/5 px-6 py-6">
          <p className="font-display text-xs uppercase tracking-[0.35em] text-neon-cyan">
            SEO Ranking Dashboard
          </p>
          <h1 className="mt-3 font-display text-3xl font-bold text-white">
            Đăng nhập xem báo cáo ranking
          </h1>
          <p className="mt-2 text-sm text-slate-300">
            Dùng một mật khẩu chia sẻ cho team SEO hoặc khách hàng. Phiên đăng nhập có hiệu lực 24 giờ.
          </p>
        </div>

        <form className="space-y-4 px-6 py-6" onSubmit={onSubmit}>
          <label className="block text-sm font-semibold text-white">
            Mật khẩu truy cập
            <input
              className="input-dark mt-2"
              type="password"
              value={password}
              onChange={(event) => setPassword(event.target.value)}
              placeholder="Nhập mật khẩu dashboard"
              autoFocus
            />
          </label>

          {error ? (
            <div className="rounded-2xl border border-neon-red/30 bg-neon-red/10 px-4 py-3 text-sm text-neon-red">
              {error}
            </div>
          ) : null}

          <button className="button-primary w-full" type="submit" disabled={submitting || !password.trim()}>
            {submitting ? "Đang kiểm tra..." : "Vào dashboard"}
          </button>
        </form>
      </div>
    </div>
  );
}

