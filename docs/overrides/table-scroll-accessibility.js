const makeScrollableTablesKeyboardReachable = () => {
  document.querySelectorAll(".md-typeset__table").forEach((element) => {
    if (element instanceof HTMLElement && element.scrollWidth > element.clientWidth) {
      element.tabIndex = 0;
      element.setAttribute("aria-label", "Scrollable table");
    }
  });
};

document$.subscribe(() => requestAnimationFrame(makeScrollableTablesKeyboardReachable));
