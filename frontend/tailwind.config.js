/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,jsx}"],
  theme: {
    extend: {
      colors: {
        canvas: "#0D1117",
        panel: "#11161f",
        line: "#1f2a37",
        text: "#f0f6fc",
        muted: "#8b949e",
        neon: {
          cyan: "#2dd4bf",
          blue: "#38bdf8",
          green: "#7ee787",
          yellow: "#f59e0b",
          red: "#fb7185",
          orange: "#fb923c"
        }
      },
      boxShadow: {
        glow: "0 0 0 1px rgba(56,189,248,0.2), 0 10px 40px rgba(0,0,0,0.35)",
        warning: "0 0 0 1px rgba(245,158,11,0.35), 0 10px 40px rgba(0,0,0,0.35)"
      },
      fontFamily: {
        display: ["Space Grotesk", "sans-serif"],
        body: ["Manrope", "sans-serif"]
      }
    },
  },
  plugins: [],
};

