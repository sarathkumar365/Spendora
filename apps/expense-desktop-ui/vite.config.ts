import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { resolve } from "node:path";

export default defineConfig({
  plugins: [react()],
  test: {
    environment: "jsdom",
    setupFiles: "./src/test/setup.ts",
    css: true
  },
  resolve: {
    alias: {
      "@": resolve(__dirname, "src")
    }
  },
  server: {
    port: 1420,
    strictPort: true,
    proxy: {
      // Prefer dev proxy for browser runs so local UI does not depend on cross-origin CORS.
      "/api": {
        target: "http://127.0.0.1:8081",
        changeOrigin: true
      }
    }
  }
});
