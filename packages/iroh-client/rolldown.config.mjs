import { defineConfig } from "rolldown";
import { wasm } from "rolldown-plugin-wasm";

export default defineConfig({
  input: "src/index.ts",
  output: {
    file: "browser/index.js",
  },
  plugins: [
    wasm(),
    {
      name: "esm-import-to-url",
      resolveId(source) {
        const urlMap = {
          tslib: "https://unpkg.com/tslib@2/tslib.es6.js",
        };

        if (urlMap[source]) {
          return { id: urlMap[source], external: true };
        }

        return null;
      },
    },
  ],
});
