import { svelte } from "@sveltejs/vite-plugin-svelte";
import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "vite";

function parseInteger(value?: string, defaultValue?: number): number {
  try {
    return Number.parseInt(value, 10);
  } catch {
    return defaultValue;
  }
}

export default defineConfig({
  plugins: [tailwindcss(), svelte()],
  server: {
    port: parseInteger(process.env.PORT, 5174),
    strictPort: true,
  },
});
