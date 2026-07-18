---
hide:
  - toc
---

# Layout and style directions

The prototype uses the exact logo-sampled tokens supplied for **FC Infinity**, then adds only four pale neutral documentation surfaces for a readable light scheme. The dark scheme maps directly to the supplied near-black, carbon, graphite, silver, flame, and aqua palette.

## Source palette

| Role | Token | Value | Prototype use |
| --- | --- | --- | --- |
| Canvas | `--fc-void` | `#08080A` | Dark reading canvas |
| Global chrome | `--fc-ink` | `#15171A` | Header and dark surface |
| Elevated surface | `--fc-graphite` | `#23252A` | Dark panels |
| Border | `--fc-surface` | `#2F3238` | Dark dividers |
| Body text | `--fc-silver` | `#D1D1D4` | Dark-mode prose |
| Primary warm accent | `--fc-flame` | `#FE7501` | Header stripe and dark active state |
| Warm depth | `--fc-crimson` | `#A30A06` | Accessible light active/focus state |
| Primary cool accent | `--fc-aqua` | `#04B7C4` | Information and the cool brand half |
| Light link | `--fc-ocean` | `#037493` | Accessible links on white |
| Dark link | `--fc-glacier` | `#6CE0E1` | Accessible links on void |
| Highlight | `--fc-gold` | `#FFDB4B` | Diagrams and large accents only |

The docs-only light extensions are `#FDFDFC` surface, `#F7F9FA` canvas, `#EEF2F4` muted surface, and `#D8E0E5` border. They exist to keep long-form reading calm; they do not replace the logo palette.

<div class="fc-direction-grid">
  <section class="fc-direction fc-direction--recommended">
    <p class="fc-direction__name">A. Graphite shell + split spectrum</p>
    <p><strong>Recommended.</strong> Dark ink header, light task navigation, flame active state, aqua information cues, and a white reading surface.</p>
    <div class="fc-direction__swatches" aria-label="Void, graphite, silver, flame, gold, and aqua swatches"><span></span><span></span><span></span><span></span><span></span><span></span></div>
    <div class="fc-mini-shell" aria-hidden="true"><div class="fc-mini-shell__header"></div><div class="fc-mini-shell__nav"></div><div class="fc-mini-shell__content"></div></div>
    <p>Best balance of GitLab-like clarity, Full Chaos recognition, long-form readability, and dense reference support.</p>
  </section>

  <section class="fc-direction fc-direction--ember">
    <p class="fc-direction__name">B. FC Infinity Ember</p>
    <p>Near-black canvas, carbon panels, flame navigation and focus, gold highlights, and aqua as the supporting reference color.</p>
    <div class="fc-direction__swatches" aria-label="Void, graphite, silver, flame, gold, and aqua swatches"><span></span><span></span><span></span><span></span><span></span><span></span></div>
    <div class="fc-mini-shell" aria-hidden="true"><div class="fc-mini-shell__header"></div><div class="fc-mini-shell__nav"></div><div class="fc-mini-shell__content"></div></div>
    <p>Strong operator and editor identity, retained as a dark-mode influence rather than the default documentation surface.</p>
  </section>

  <section class="fc-direction fc-direction--tide">
    <p class="fc-direction__name">C. FC Infinity Tide</p>
    <p>Deep-ocean surfaces, aqua navigation and links, glacier emphasis, and flame reserved for warnings or the warm brand half.</p>
    <div class="fc-direction__swatches" aria-label="Void, graphite, silver, flame, gold, and aqua swatches"><span></span><span></span><span></span><span></span><span></span><span></span></div>
    <div class="fc-mini-shell" aria-hidden="true"><div class="fc-mini-shell__header"></div><div class="fc-mini-shell__nav"></div><div class="fc-mini-shell__content"></div></div>
    <p>Useful as an alternative dark visual direction, but too cool to carry the entire public documentation identity by itself.</p>
  </section>
</div>

## Recommended implementation

Use **Graphite shell + split spectrum** for the default prototype. Use the supplied Ember/Tide palette to define the user-selectable dark scheme rather than publishing three separate documentation products.

### Layout

- Persistent left navigation with one active location.
- Readable content column and optional page-local table of contents.
- Breadcrumbs and maintained edit/source actions.
- Section landing pages use compact task groups, not a marketing card wall.
- Dense reference pages use available width for tables and code.
- No permanent evidence rail or owner/canonical metadata block before content.

### Typography

Use the local system sans stack for shell, prose, and headings, and the local system monospace stack for code. Hierarchy comes from size, weight, spacing, and navigation rather than an editorial serif.

### Color behavior

- Flame and aqua share the header rule so the mark's warm/cool relationship remains visible without saturating the page.
- Light-theme body links use Ocean because it meets text contrast on white; brighter cyan and flame are used for large accents and dark surfaces.
- Light-theme active navigation and focus use Crimson for sufficient contrast.
- Dark-theme links use Glacier and active/focus states use Flame.
- Gold and Sun remain diagram, code, or large-highlight colors, not body text on light surfaces.

## Rejected WIP patterns

- evidence-trail side rail on every page;
- prominent page-owner, reviewed-date, or canonical panel before the title;
- primitive showcase as public reader reference;
- oversized editorial display headings;
- warm paper treatment disconnected from the supplied logo;
- visual-regression coverage for ordinary prose composition;
- layout decisions that preserve current page boundaries.
