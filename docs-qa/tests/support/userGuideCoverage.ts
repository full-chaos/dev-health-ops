export const userGuideCoverage = [
  {
    id: "first-10-minutes",
    path: "/user-guide/first-10-minutes/",
    title: "Your first 10 minutes",
    action: { label: "Read the evidence standard", target: "/product/concepts/" },
  },
  {
    id: "how-to-read-dev-health",
    path: "/user-guide/how-to-read-dev-health/",
    title: "How to read Dev Health",
    action: { label: "Read the evidence standard", target: "/product/concepts/" },
  },
  {
    id: "glossary",
    path: "/user-guide/glossary/",
    title: "Glossary",
    action: { label: "Read the evidence standard", target: "/product/concepts/" },
  },
  {
    id: "investment-view",
    path: "/user-guide/journeys/investment-view/",
    title: "Investment: follow the evidence",
    action: { label: "Read the evidence standard", target: "/product/concepts/" },
  },
  {
    id: "quadrants",
    path: "/user-guide/views/quadrants/",
    title: "Quadrants",
    action: { label: "Read the evidence standard", target: "/product/concepts/" },
  },
  {
    id: "flame-diagrams",
    path: "/user-guide/views/flame-diagrams/",
    title: "Flame diagrams",
    action: { label: "Read the evidence standard", target: "/product/concepts/" },
  },
  {
    id: "code-hotspots",
    path: "/user-guide/views/code-hotspots/",
    title: "Code Hotspots",
    action: { label: "Read the evidence standard", target: "/product/concepts/" },
  },
  {
    id: "pr-flow",
    path: "/user-guide/views/pr-flow/",
    title: "PR Flow",
    action: { label: "Read the evidence standard", target: "/product/concepts/" },
  },
  {
    id: "capacity-planning",
    path: "/user-guide/views/capacity-planning/",
    title: "Capacity Planning View",
    action: { label: "Read the evidence standard", target: "/product/concepts/" },
  },
  {
    id: "work-graph",
    path: "/user-guide/views/work-graph/",
    title: "Work Graph: follow relationships",
    action: { label: "Read the evidence standard", target: "/product/concepts/" },
  },
  {
    id: "ai-impact",
    path: "/user-guide/views/ai-impact/",
    title: "AI Impact",
    action: { label: "Open the evidence model", target: "/user-guide/how-to-read-dev-health/" },
    desktopAction: { label: "Read the evidence standard", target: "/product/concepts/" },
  },
  {
    id: "ai-attribution",
    path: "/user-guide/views/ai-attribution/",
    title: "AI Attribution",
    action: { label: "Open the evidence model", target: "/user-guide/how-to-read-dev-health/" },
    desktopAction: { label: "Read the evidence standard", target: "/product/concepts/" },
  },
  {
    id: "ai-review-load",
    path: "/user-guide/views/ai-review-load/",
    title: "AI Review Load",
    action: { label: "Open the evidence model", target: "/user-guide/how-to-read-dev-health/" },
    desktopAction: { label: "Read the evidence standard", target: "/product/concepts/" },
  },
  {
    id: "ai-risk",
    path: "/user-guide/views/ai-risk/",
    title: "AI Risk",
    action: { label: "Open the evidence model", target: "/user-guide/how-to-read-dev-health/" },
    desktopAction: { label: "Read the evidence standard", target: "/product/concepts/" },
  },
  {
    id: "reports",
    path: "/user-guide/reports/",
    title: "Report Center",
    action: { label: "Read the evidence standard", target: "/product/concepts/" },
  },
  {
    id: "metrics-interpretation",
    path: "/user-guide/metrics-interpretation/",
    title: "Interpret shared metrics",
    action: { label: "Read the evidence standard", target: "/product/concepts/" },
  },
] as const;

export const userGuideViewports = [
  { name: "mobile", width: 375, height: 900, variant: "in-flow" },
  { name: "tablet", width: 768, height: 900, variant: "in-flow" },
  { name: "desktop", width: 1280, height: 900, variant: "rail" },
] as const;
