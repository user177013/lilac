# Design System: Osmanthus

## 1. Visual Theme & Atmosphere
An organic, warm, and highly functional workspace. Moving away from the sterile "AI Purple" software norms, **Osmanthus** leverages natural, earthy hexes to create a high-density "Cockpit Mode" that still feels airy and breathable. The atmosphere is like a modern botanical laboratory—precise, structured, but grounded in natural warmth.

## 2. Color Palette & Roles
- **Canvas Floral** (`#FAF7F0`) — The primary background surface replacing harsh pure whites or stark grays.
- **Pure Surface** (`#FFFFFF`) — Used strictly for nested cards or floating modals over the Canvas Floral to create elevation without shadow spam.
- **Charcoal Blue Ink** (`#2D3747`) — The primary structural dark. Used for H1 headings, precise metrics, and pure text. Never use pure black (`#000000`).
- **Muted Teal** (`#89BD9E`) — Secondary structural accent. Perfect for subtle success/status indicators and data graph fills.
- **Soft Apricot** (`#FFDAB9`) — Tertiary warmth. Used for subtle background highlights, active list items, or secondary hover boundaries.
- **Golden Orange Accent** (`#F59E0B`) — The singular, decisive interactive CTA color. Used for primary buttons, focus rings, and active state bounding boxes.
*(Constraint: No purple/neon. Only ONE high-contrast interactive accent (Golden Orange) active at a time.)*

## 3. Typography Rules
- **Display:** `Geist` or `Outfit` — Track-tight, controlled scale, weight-driven hierarchy. No oversized screaming headers.
- **Body:** `Geist` — Relaxed leading, 65ch max-width, neutral secondary color (tinted from Charcoal Blue).
- **Mono:** `Geist Mono` or `JetBrains Mono` — For code, metadata, timestamps, high-density numbers.
- **Banned:** `Inter`, generic system fonts for premium contexts. Serif fonts banned in dashboards.

## 4. Component Stylings
* **Buttons:** Flat, no outer glow. Tactile -1px translating scale on active. `Golden Orange` primary fill, `Muted Teal` or outline for secondary actions.
* **Cards:** Generously rounded corners (1.5rem). Diffused whisper shadow over the `Floral White` canvas. High-density: replace with 1px border-top dividers tinted with `Soft Apricot`.
* **Inputs:** Label above, error below. Focus ring in `Golden Orange`. No floating labels.
* **Loaders:** Skeletal shimmer matching exact layout dimensions. No generic circular spinners.
* **Empty States:** Composed, illustrated compositions utilizing the muted teal and apricot values — not just "No data" text.

## 5. Layout Principles
Grid-first responsive architecture. Asymmetric splits for Hero sections.
Strict single-column collapse below 768px. Max-width containment (`max-w-[1400px]`).
No flexbox percentage math. Generous internal padding (`p-6` or `p-8` on structural cards).
Full-height sections must use `min-h-[100dvh]` (never `h-screen`). 

## 6. Motion & Interaction
Spring physics for all interactive elements (`type: "spring", stiffness: 100, damping: 20`). 
Staggered cascade reveals for list data.
Perpetual micro-loops on active dashboard components. Hardware-accelerated transforms (`translate`, `scale`, `opacity`) exclusively. Isolated Client Components for CPU-heavy animations.

## 7. Anti-Patterns (Banned)
Explicit list of forbidden patterns: 
- No emojis
- No `Inter` font or basic Serif typography
- No pure black (`#000000`)
- No neon/purple glowing box-shadows
- No 3-column equal grids (force asymmetric or zig-zag layouts)
- No AI copywriting clichés (e.g., "Elevate", "Next-Gen")
- No generic placeholder names
- No overlapping content without explicit structural framing
