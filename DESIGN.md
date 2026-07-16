# Dev Health Documentation Design System

## 0. Research Log

- **Current-site audit:** real Chrome capture of the committed Material baseline at
  `375/768/1280` preparation began with the 1280 audit artifact at
  `.omo/evidence/task-8-unified-cloudflare-documentation/baseline/material-default-1280.png`.
  It established a generic indigo/Roboto shell, repository-index hierarchy, no
  documentation-specific tokens, and no evidence-first wayfinding.
- **Current showcase audit:** the evidence rail and article were re-measured in real
  Chrome at 1280px after the foundation CSS landed. The source-backed desktop shell
  needs a full, legible evidence specimen in the showcase—not a Material-style
  sidebar insert—and fresh `375/768/1280` captures live at
  `.omo/evidence/task-8-unified-cloudflare-documentation/showcase/`.
- **Structural reference:** Stripe documentation was studied for its precise editorial
  hierarchy, readable content measure, conservative rounding, tabular metadata, and
  chromatic elevation. Its purple CTA and luxury retail tone are deliberately not
  adopted: Full Chaos orange remains the one action accent.
- **Existing contracts:** `docs/design-system.md`, `web/docs/design-system.md`, and
  `web/DESIGN.md` were read as product-UI references only. They do not govern this
  public documentation surface.
- **Designpowers constraints:** primary readers include a first-time operator looking
  for an accountable next step, a contributor tracing a claim to source material, and
  a keyboard or reduced-motion reader. Their success criteria are a visible route into
  the manual, clear claim-to-evidence context, readable long-form copy, and no motion
  dependency.
- **Skipped lanes:** lazyweb and Imagen are not used because this is an existing
  Material documentation surface with a locked product identity and a concrete
  structural reference; a live current-site audit is the relevant evidence source.

## 1. Atmosphere & Identity

Dev Health documentation is a calm, evidence-led editorial operations manual: the fixed
charcoal/ivory/orange direction makes a warm ivory page read like a working field
notebook, gives charcoal navigation operational weight, and reserves Full Chaos orange
for the next accountable step.
The signature is the **evidence-trail rail**: a compact, persistent editorial prompt
that makes source, interpretation, and responsible action visible without pretending a
documentation page is a dashboard. The surface is deliberate rather than decorative;
it should feel useful under pressure and unhurried when read in depth.

## 2. Color

### Palette

| Role | Token | Value | Usage |
| --- | --- | --- | --- |
| Paper | `--fc-paper` | `#f7f1e7` | Main reading canvas |
| Paper raised | `--fc-paper-raised` | `#fffaf2` | Panels, code-frame surrounds |
| Paper quiet | `--fc-paper-quiet` | `#eee6d8` | Tinted sections, table headers |
| Charcoal | `--fc-charcoal` | `#262421` | Header, high-emphasis surfaces |
| Ink | `--fc-ink` | `#282622` | Long-form text and headings |
| Ink muted | `--fc-ink-muted` | `#665f55` | Metadata and supporting copy |
| Rule | `--fc-rule` | `#d8cbbb` | Dividers and low-emphasis borders |
| Orange | `--fc-orange` | `#bd431b` | Links, primary actions, focus |
| Orange dark | `--fc-orange-dark` | `#913316` | Hover and pressed action state |
| Evidence | `--fc-evidence` | `#23565c` | Source/provenance context |
| Evidence wash | `--fc-evidence-wash` | `#e2efee` | Evidence callouts and rail steps |
| Positive | `--fc-positive` | `#356948` | Confirmed/safe state |
| Caution | `--fc-caution` | `#8b5b18` | Caveat state |
| Negative | `--fc-negative` | `#9c3930` | Blocking/error state |

### Rules

- Orange is the only action accent. Evidence teal communicates provenance, never a
  competing call to action.
- Status color is always paired with a text label and icon/shape treatment; no state is
  communicated by color alone.
- `--fc-focus-ring` combines paper separation with the orange action role so keyboard
  focus is visible against both ivory and charcoal surfaces.
- Material variables map to these tokens. No component, template, or showcase uses an
  undeclared raw color.

## 3. Typography

| Role | Token | Family | Size / line-height | Weight / tracking | Usage |
| --- | --- | --- | --- | --- | --- |
| Display | `--fc-type-display` | Iowan Old Style, Palatino, Georgia, serif | `clamp(2.3rem, 4vw, 4.4rem) / 1.02` | 600 / `-0.03em` | Manual landing and page title |
| H1 | `--fc-type-h1` | Editorial serif | `2.5rem / 1.08` | 600 / `-0.025em` | Page headings |
| H2 | `--fc-type-h2` | Editorial serif | `1.6rem / 1.22` | 600 / `-0.015em` | Major reading breaks |
| H3 | `--fc-type-h3` | System sans | `1.05rem / 1.35` | 700 / normal | Primitive and utility headings |
| Body | `--fc-type-body` | System sans | `1rem / 1.72` | 400 / normal | Reading copy |
| Metadata | `--fc-type-meta` | System sans | `.8125rem / 1.4` | 700 / `.08em` | Eyebrows and labels |
| Code | `--fc-type-code` | ui-monospace, SFMono-Regular, Menlo, monospace | `.84rem / 1.65` | 500 / normal | Commands and API fragments |

- Body copy stays within `--fc-reading-measure` (`67ch`). The manual preserves natural
  wrapping rather than force-justifying or truncating text.
- Numeric metadata uses `font-variant-numeric: tabular-nums`.
- The type stack is local-first to protect static-site performance and offline use;
  no remote type dependency is required for hierarchy.

## 4. Spacing & Layout

All spacing is on a 4px scale.

| Token | Value | Usage |
| --- | --- | --- |
| `--fc-space-1` | `0.25rem` | Tight marker-to-label pairing |
| `--fc-space-2` | `0.5rem` | Compact inline groups |
| `--fc-space-3` | `0.75rem` | List and metadata rhythm |
| `--fc-space-4` | `1rem` | Default component padding |
| `--fc-space-6` | `1.5rem` | Panel padding |
| `--fc-space-8` | `2rem` | Component groups |
| `--fc-space-12` | `3rem` | Section separation |
| `--fc-space-16` | `4rem` | Major editorial cadence |
| `--fc-showcase-measure` | `52rem` | Wide specimen-only layout; prose stays narrower |
| `--fc-evidence-rail-measure` | `14rem` | Legible desktop evidence column |
| `--fc-evidence-rail-extent` | `20rem` | Sustained desktop evidence rail presence |

- **Reading layout:** main prose uses `--fc-reading-measure`; wide tables and code
  blocks may use the available content width with horizontal scrolling instead of
  shrinking text.
- **Showcase layout:** a primitive specimen may use `--fc-showcase-measure` to prove
  its composition. This never expands body paragraphs past `--fc-reading-measure`.
- **Desktop (>= 60rem):** the primary navigation, readable content column, and
  `--fc-evidence-rail-measure` evidence trail rail form the operational shell. The
  rail is a visible secondary column, not a compressed Material sidebar afterthought.
  Its `--fc-evidence-rail-extent` preserves a sustained trail from source to action.
- **Tablet (>= 48rem):** body measure is retained; panels may arrange into two columns
  only when their labels remain readable.
- **Mobile (< 48rem):** navigation uses Material's drawer, all primitive panels stack,
  and the rail becomes an in-flow evidence card. No horizontal overflow is accepted at
  320, 375, 768, or 1280 CSS pixels.
- **Zoom:** 200% zoom preserves a single logical reading order; no interaction relies
  on hover or a fixed-width rail.

## 5. Components

### Editorial navigation

- **Structure:** Material header/search, section navigation, optional in-page table of
  contents, and evidence trail rail.
- **Variants:** desktop rail; compact/in-flow mobile rail.
- **Spacing:** `--fc-space-3` through `--fc-space-8`.
- **States:** current, hover, active, focus-visible, search open, drawer open.
- **Accessibility:** semantic nav labels, one current-page indicator, visible focus,
  skip link preserved, search remains keyboard reachable.
- **Motion:** Material drawer/search transitions only; no added decorative motion.

### Evidence trail

- **Structure:** labelled `aside`, three numbered stages (source, interpretation,
  responsible action), and a real link to evidence standards.
- **Variants:** rail on desktop, card in the primitive showcase and narrow layouts.
- **Desktop composition:** the showcase contains the actual three-step rail component
  at `--fc-evidence-rail-measure`; the documentation shell repeats it as a sticky
  secondary column so the reader can inspect evidence without losing their place.
- **Spacing:** `--fc-space-3`, `--fc-space-4`, `--fc-space-6`.
- **States:** default, link hover, link focus-visible; no disabled or loading state.
- **Accessibility:** the label explains purpose; numbers are supplementary rather than
  the only sequence cue; link target is meaningful without visual context.
- **Motion:** none.

### Callout

- **Structure:** semantic Material admonition with a concise label, claim, and source
  or caveat link.
- **Variants:** evidence, interpretation, caution, and blocking.
- **States:** static content; links have hover and focus-visible states.
- **Accessibility:** color is paired with an explicit title and prose instruction.

### Code and table frame

- **Structure:** labelled command/code block with safe wrapping, or a native table
  inside a horizontally scrollable frame. On narrow layouts, a visible text cue
  names the horizontal-scroll interaction before the table.
- **Variants:** command, API fragment, data table, diagram caption.
- **States:** copy affordance comes from Material; native focus/selection remains
  usable; overflow is scrollable rather than clipped.
- **Accessibility:** semantic table headers, caption/context before dense data,
  readable table type without forced compression, and code language metadata where
  known.

### Primary action and text link

- **Structure:** real anchors only; no dead `#` controls.
- **Variants:** orange primary action, underlined evidence link, subdued utility link.
- **States:** default, hover, active, focus-visible. No loading state applies to static
  navigation.
- **Accessibility:** minimum 44px touch target when rendered as an action; link text
  names its destination.

## 6. Motion & Interaction

| Type | Duration | Easing | Meaning |
| --- | --- | --- | --- |
| Micro | 120ms | ease-out | Link/action color response |
| Standard | 180ms | ease-out | Material search and drawer feedback |

- Added transitions use color, opacity, or transform only. Layout properties never
  animate.
- `prefers-reduced-motion: reduce` removes the added transitions and scroll animation.
- The evidence trail is intentionally still: its meaning comes from ordered provenance,
  not visual spectacle.

## 7. Depth & Surface

The strategy is **mixed, editorial tonal shift plus a single restrained shadow**.
Paper and quiet-paper surfaces provide the default hierarchy; a soft warm shadow is
reserved for raised showcase panels and the evidence rail. Borders are rules, not card
outlines. Radius is intentionally conservative: `--fc-radius-sm` (`.25rem`) for code
and controls, `--fc-radius-md` (`.5rem`) for panels, `--fc-radius-lg` (`.75rem`) for
the showcase frame. `--fc-layer-skip` reserves a single explicit focus layer so the
skip link remains visible above the charcoal header.

## 8. Accessibility Constraints & Accepted Debt

### Constraints

- Target WCAG 2.2 AA: 4.5:1 contrast for body text and 3:1 for large text; keyboard
  access and visible focus are mandatory for every interaction.
- The route works with keyboard-only navigation, screen readers, text zoom, narrow
  viewports, and reduced motion. Search and documentation navigation never depend on
  pointer hover.
- Content follows plain-language evidence framing: claims identify evidence or caveats;
  documentation never turns a signal into a person ranking or an AI verdict.
- The visual QA harness runs real Chrome, axe serious/critical checks, focus checks, and
  responsive captures at 375, 768, and 1280px.

### Accepted Debt

| Item | Location | Why accepted | Owner / exit |
| --- | --- | --- | --- |
| Fixture-backed real product screenshots | User-guide content pages | The existing foundation branch has no safe, running product fixture surface or approved sanitized capture. A visual placeholder is not substituted. | Todo 7 captures real product screenshots with source metadata and alt text before user-guide acceptance. |
| Search query acceptance set | Search content/navigation | The 20-query canonical set belongs to Todo 5, after its audience-first vocabulary exists. | Todo 5 owns the set and browser rank assertions. |
