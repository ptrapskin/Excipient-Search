const QUICK_CHOICES = [
  {
    category: "Osmotic Agents",
    items: [
      { label: "Glycerin", aliases: ["glycerin", "glycerol", "anhydrous glycerin", "glycerin solution"] },
      { label: "Propylene Glycol", aliases: ["propylene glycol", "propylene glycol solution"] },
      { label: "Polyethylene Glycol", aliases: ["polyethylene glycol", "peg", "peg 3350", "peg 400", "peg 300", "peg 1450", "macrogol"] },
    ],
  },
  {
    category: "Osmotic Salts",
    items: [
      { label: "Magnesium Salts", aliases: ["magnesium citrate", "magnesium sulfate", "magnesium hydroxide", "magnesium chloride"] },
      { label: "Phosphate Salts", aliases: ["sodium phosphate", "dibasic sodium phosphate", "monobasic sodium phosphate", "phosphoric acid"] },
    ],
  },
  {
    category: "Osmotic Sugars",
    items: [
      { label: "High Sugar", aliases: ["fructose", "high fructose corn syrup", "hfcs", "corn syrup", "sucrose", "invert sugar", "dextrose", "glucose syrup"] },
    ],
  },
];

const quickContainer = document.querySelector("[data-quick-include]");
const includeInput = document.getElementById("include-filter");

if (quickContainer && includeInput) {
  const activeAliaseSets = new Map(); // label -> aliases[]

  const getCurrentTerms = () =>
    includeInput.value.split(",").map((t) => t.trim().toLowerCase()).filter(Boolean);

  const setIncludeValue = (terms) => {
    includeInput.value = terms.join(", ");
  };

  const isActive = (aliases) => {
    const current = getCurrentTerms();
    return aliases.some((a) => current.includes(a.toLowerCase()));
  };

  const toggleChoice = (label, aliases, btn) => {
    const current = getCurrentTerms();
    if (isActive(aliases)) {
      const aliasSet = new Set(aliases.map((a) => a.toLowerCase()));
      setIncludeValue(current.filter((t) => !aliasSet.has(t)));
      btn.classList.remove("is-active");
    } else {
      const merged = [...current, ...aliases.map((a) => a.toLowerCase())];
      const deduped = [...new Set(merged)];
      setIncludeValue(deduped);
      btn.classList.add("is-active");
    }
  };

  QUICK_CHOICES.forEach((group) => {
    const groupEl = document.createElement("div");
    groupEl.className = "quick-choice-group";

    const label = document.createElement("span");
    label.className = "quick-choice-label";
    label.textContent = group.category;
    groupEl.appendChild(label);

    const chips = document.createElement("div");
    chips.className = "quick-choice-chips";

    group.items.forEach((item) => {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "quick-chip";
      btn.textContent = item.label;
      if (isActive(item.aliases)) btn.classList.add("is-active");
      btn.addEventListener("click", () => toggleChoice(item.label, item.aliases, btn));
      chips.appendChild(btn);
    });

    groupEl.appendChild(chips);
    quickContainer.appendChild(groupEl);
  });
}

const matchingFormFilter = document.querySelector("[data-matching-form-filter]");
const matchingFormChips = document.querySelector("[data-matching-form-chips]");
const matchingFormSummary = document.querySelector("[data-matching-form-summary]");
const matchingTable = document.querySelector("[data-sort-table]");
const matchingTableBody = matchingTable ? matchingTable.querySelector("tbody") : null;
const matchingRows = matchingTableBody
  ? Array.from(matchingTableBody.querySelectorAll("tr[data-matching-form]"))
  : [];
const sortButtons = matchingTable
  ? Array.from(matchingTable.querySelectorAll("[data-sort-key]"))
  : [];

if (matchingRows.length) {
  const collator = new Intl.Collator(undefined, { numeric: true, sensitivity: "base" });
  let activeSort = { key: "product", direction: "asc" };

  const parseStrengthValue = (value) => {
    const text = value.trim();
    const match = text.match(/(\d+(?:\.\d+)?(?:\s*\/\s*\d+(?:\.\d+)?)?)/);
    if (!match) return Number.POSITIVE_INFINITY;
    const normalized = match[1].replace(/\s+/g, "");
    if (normalized.includes("/")) {
      const [left, right] = normalized.split("/");
      const numerator = Number.parseFloat(left);
      const denominator = Number.parseFloat(right);
      if (Number.isFinite(numerator) && Number.isFinite(denominator) && denominator !== 0) {
        return numerator / denominator;
      }
    }
    const parsed = Number.parseFloat(normalized);
    return Number.isFinite(parsed) ? parsed : Number.POSITIVE_INFINITY;
  };

  const getSortValue = (row, key) => row.dataset[`sort${key.charAt(0).toUpperCase()}${key.slice(1)}`] || "";

  const compareRows = (left, right, key, direction) => {
    const leftValue = getSortValue(left, key);
    const rightValue = getSortValue(right, key);
    let result = 0;

    if (key === "strength") {
      const leftStrength = parseStrengthValue(leftValue);
      const rightStrength = parseStrengthValue(rightValue);
      result = leftStrength - rightStrength;
      if (result === 0) {
        result = collator.compare(leftValue, rightValue);
      }
    } else {
      result = collator.compare(leftValue, rightValue);
    }

    return direction === "asc" ? result : -result;
  };

  const renderSortedRows = () => {
    matchingRows.sort((left, right) => compareRows(left, right, activeSort.key, activeSort.direction));
    matchingRows.forEach((row) => matchingTableBody.appendChild(row));
  };

  const updateSortButtons = () => {
    sortButtons.forEach((button) => {
      const direction = button.dataset.sortKey === activeSort.key ? activeSort.direction : "";
      button.dataset.sortDirection = direction;
      const ariaSort = direction === "asc" ? "ascending" : direction === "desc" ? "descending" : "none";
      button.setAttribute("aria-sort", ariaSort);
    });
  };

  if (sortButtons.length) {
    sortButtons.forEach((button) => {
      button.addEventListener("click", () => {
        const key = button.dataset.sortKey;
        if (!key) return;
        if (activeSort.key === key) {
          activeSort.direction = activeSort.direction === "asc" ? "desc" : "asc";
        } else {
          activeSort = { key, direction: "asc" };
        }
        renderSortedRows();
        updateSortButtons();
      });
    });

    renderSortedRows();
    updateSortButtons();
  }
}

if (matchingFormFilter && matchingFormChips && matchingRows.length) {
  const forms = [...new Set(
    matchingRows
      .map((row) => row.dataset.matchingForm.trim())
      .filter(Boolean),
  )].sort((a, b) => a.localeCompare(b));

  if (forms.length > 1) {
    const activeForms = new Set();

    const updateSummary = () => {
      if (!matchingFormSummary) return;
      const visibleCount = matchingRows.filter((row) => !row.hidden).length;
      matchingFormSummary.textContent =
        activeForms.size === 0 ? "" : `Showing ${visibleCount} of ${matchingRows.length} rows`;
    };

    const applyFilter = () => {
      matchingRows.forEach((row) => {
        const form = row.dataset.matchingForm.trim();
        row.hidden = activeForms.size > 0 && !activeForms.has(form);
      });
      updateSummary();
    };

    forms.forEach((form) => {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "drug-form-chip";
      btn.textContent = form;
      btn.addEventListener("click", () => {
        if (activeForms.has(form)) {
          activeForms.delete(form);
          btn.classList.remove("is-active");
        } else {
          activeForms.add(form);
          btn.classList.add("is-active");
        }
        applyFilter();
      });
      matchingFormChips.appendChild(btn);
    });

    matchingFormFilter.hidden = false;
    updateSummary();
  }
}
