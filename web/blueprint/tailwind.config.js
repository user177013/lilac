/** @type {import('tailwindcss').Config} */
export default {
  content: ['./src/**/*.{html,js,svelte,ts}'],
  theme: {
    extend: {
      colors: {
        'osmanthus-floral': '#FAF7F0',
        'osmanthus-charcoal': '#2D3747',
        'osmanthus-teal': '#89BD9E',
        'osmanthus-apricot': '#FFDAB9',
        'osmanthus-gold': '#F59E0B'
      }
    }
  },
  plugins: []
};
