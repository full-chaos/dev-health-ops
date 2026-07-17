const tableScrollStep = 40;

const makeScrollableTablesKeyboardReachable = () => {
  document.querySelectorAll(".md-typeset__table").forEach((element) => {
    if (element instanceof HTMLElement && element.scrollWidth > element.clientWidth) {
      element.tabIndex = 0;
      element.setAttribute("aria-label", "Scrollable table");

      if (element.dataset.fcKeyboardScrollBound === "true") {
        return;
      }

      element.addEventListener("keydown", (event) => {
        if (
          event.target !== element ||
          event.altKey ||
          event.ctrlKey ||
          event.metaKey ||
          event.shiftKey
        ) {
          return;
        }

        const maximumScrollLeft = element.scrollWidth - element.clientWidth;
        let nextScrollLeft = element.scrollLeft;

        switch (event.key) {
          case "ArrowLeft":
            nextScrollLeft -= tableScrollStep;
            break;
          case "ArrowRight":
            nextScrollLeft += tableScrollStep;
            break;
          case "Home":
            nextScrollLeft = 0;
            break;
          case "End":
            nextScrollLeft = maximumScrollLeft;
            break;
          default:
            return;
        }

        nextScrollLeft = Math.max(0, Math.min(nextScrollLeft, maximumScrollLeft));
        if (nextScrollLeft === element.scrollLeft) {
          return;
        }

        event.preventDefault();
        element.scrollLeft = nextScrollLeft;
      });
      element.dataset.fcKeyboardScrollBound = "true";
    }
  });
};

document$.subscribe(() => requestAnimationFrame(makeScrollableTablesKeyboardReachable));
