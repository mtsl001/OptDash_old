/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        brand:  { DEFAULT: "#1B3A6B", light: "#2E75B6", muted: "#D6E4F7" },
        accent: "#E8A020",
        bull:   { DEFAULT: "#1E7C44", light: "#D6F0E0" },
        bear:   { DEFAULT: "#C0392B", light: "#FAD7D2" },
        warn:   { DEFAULT: "#F0A000", light: "#FFF3CD" },
        ink:    { DEFAULT: "#404040", muted: "#808080" },
        panel:  "#0F1923",
        surface:"#162030",
        border: "#243040",
      },
      fontFamily: { mono: ["JetBrains Mono", "Courier New", "monospace"] },
    },
  },
  plugins: [],
};
