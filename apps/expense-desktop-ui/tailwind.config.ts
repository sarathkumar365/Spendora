import type { Config } from "tailwindcss";

const config: Config = {
  darkMode: ["class"],
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        background: "var(--bg)",
        surface: "var(--surface)",
        border: "var(--border)",
        foreground: "var(--text-primary)",
        muted: "var(--text-secondary)",
        accent: "var(--accent)"
      },
      borderRadius: {
        lg: "18px",
        md: "12px",
        sm: "10px"
      }
    }
  },
  plugins: []
};

export default config;
