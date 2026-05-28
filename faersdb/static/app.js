const statusEl = document.getElementById("status");
const cohortSummaryEl = document.getElementById("cohort-summary");
const caseSummaryEl = document.getElementById("case-summary");
const caseFilterSummaryEl = document.getElementById("case-filter-summary");
const caseResultsEl = document.getElementById("case-results");
const casePagerEl = document.getElementById("case-pager");
const caseDetailEl = document.getElementById("case-detail");
const searchForm = document.getElementById("search-form");
const conceptModeEl = document.getElementById("concept-mode");
const drugTermsEl = document.getElementById("drug-terms");
const reactionTermsEl = document.getElementById("reaction-terms");
const addDrugTermButton = document.getElementById("add-drug-term");
const addReactionTermButton = document.getElementById("add-reaction-term");
const clearFiltersButton = document.getElementById("clear-filters");
const exportCasesButton = document.getElementById("export-cases");
const exportCaseReportButton = document.getElementById("export-case-report");

const DEFAULT_LIMIT = 25;
const SEARCH_MODE = "cases";

const DRUG_TERM_FIELDS = [
  "drug_name",
  "prod_ai",
  "indication_pt",
  "role_cod",
  "route",
  "dose_min",
  "dose_max",
  "dose_unit",
  "therapy_start_from",
  "therapy_start_to",
  "therapy_end_from",
  "therapy_end_to",
  "dur_min",
  "dur_max",
  "dur_cod",
];
const REACTION_TERM_FIELDS = ["reaction_pt"];
const CASE_FILTER_FIELDS = [
  "quarter",
  "report_type",
  "initial_or_followup",
  "event_dt_from",
  "event_dt_to",
  "fda_dt_from",
  "fda_dt_to",
  "mfr_dt_from",
  "mfr_dt_to",
  "sex_std",
  "age_min",
  "age_max",
  "age_unit",
  "age_group",
  "weight_min",
  "weight_max",
  "reporter_country",
  "case_outcome",
  "reporter_type",
];

const DRUG_TERM_CONFIG = {
  drug_name: { label: "Drug name", placeholder: "aspirin" },
  prod_ai: { label: "Active ingredient", placeholder: "ibuprofen" },
  indication_pt: { label: "Indication", placeholder: "pain" },
  role_cod: { label: "Role code", metadataKey: "role_codes" },
  route: { label: "Route", metadataKey: "routes" },
  dose_min: { label: "Dose min", type: "number", step: "0.1" },
  dose_max: { label: "Dose max", type: "number", step: "0.1" },
  dose_unit: { label: "Dose unit", metadataKey: "dose_units" },
  therapy_start_from: { label: "Therapy start from", type: "date" },
  therapy_start_to: { label: "Therapy start to", type: "date" },
  therapy_end_from: { label: "Therapy end from", type: "date" },
  therapy_end_to: { label: "Therapy end to", type: "date" },
  dur_min: { label: "Duration min", type: "number", step: "1" },
  dur_max: { label: "Duration max", type: "number", step: "1" },
  dur_cod: { label: "Duration unit", metadataKey: "dur_codes" },
};
const REACTION_TERM_CONFIG = {
  reaction_pt: { label: "Reaction", placeholder: "headache" },
};

const SELECT_FIELDS = {
  quarter: "quarters",
  report_type: "report_types",
  initial_or_followup: "initial_or_followup_values",
  sex_std: "sex_values",
  age_unit: "age_units",
  age_group: "age_groups",
  case_outcome: "case_outcomes",
  reporter_type: "reporter_types",
};

const CASE_FILTER_LABELS = {
  quarter: "Quarter",
  report_type: "Report type",
  initial_or_followup: "I/F",
  event_dt_from: "Event from",
  event_dt_to: "Event to",
  fda_dt_from: "FDA from",
  fda_dt_to: "FDA to",
  mfr_dt_from: "Manufacturer from",
  mfr_dt_to: "Manufacturer to",
  sex_std: "Sex",
  age_min: "Age min years",
  age_max: "Age max years",
  age_unit: "Original age unit",
  age_group: "Age group",
  weight_min: "Weight min",
  weight_max: "Weight max",
  reporter_country: "Country",
  case_outcome: "Case outcome",
  reporter_type: "Reporter",
};

let latestCasePayload = null;
let filterMetadata = null;
let caseSearchState = null;

function setStatus(message, isError = false) {
  statusEl.textContent = message;
  statusEl.classList.toggle("error", isError);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function arrayText(values) {
  if (!values || values.length === 0) {
    return "None";
  }
  return values.join(", ");
}

function valueText(value, fallback = "n/a") {
  if (value === null || value === undefined || value === "") {
    return fallback;
  }
  return String(value);
}

function compactList(values, limit = 4) {
  const items = (values || []).filter((value) => value !== null && value !== undefined && value !== "");
  if (items.length === 0) {
    return "None";
  }
  const visible = items.slice(0, limit).map(String);
  const remaining = items.length - visible.length;
  return remaining > 0 ? `${visible.join(", ")} + ${remaining} more` : visible.join(", ");
}

function chipList(values, emptyLabel = "None") {
  const items = (values || []).filter((value) => value !== null && value !== undefined && value !== "");
  if (items.length === 0) {
    return `<span class="chip">${escapeHtml(emptyLabel)}</span>`;
  }
  return items.map((value) => `<span class="chip">${escapeHtml(value)}</span>`).join("");
}

function dateRangeText(start, end) {
  if (!start && !end) {
    return "n/a";
  }
  return `${valueText(start, "unknown")} to ${valueText(end, "unknown")}`;
}

function emptyDrugTerm() {
  return Object.fromEntries(DRUG_TERM_FIELDS.map((field) => [field, ""]));
}

function emptyReactionTerm() {
  return { reaction_pt: "" };
}

function cleanByFields(rawValue = {}, fields = []) {
  const cleaned = {};
  fields.forEach((field) => {
    const value = String(rawValue[field] ?? "").trim();
    if (value !== "") {
      cleaned[field] = value;
    }
  });
  return cleaned;
}

function cleanDrugTerm(rawTerm = {}) {
  return cleanByFields(rawTerm, DRUG_TERM_FIELDS);
}

function cleanReactionTerm(rawTerm = {}) {
  return cleanByFields(rawTerm, REACTION_TERM_FIELDS);
}

function cleanCaseFilters(rawFilters = {}) {
  return cleanByFields(rawFilters, CASE_FILTER_FIELDS);
}

function hasObjectFilters(value = {}) {
  return Object.values(value).some((item) => item !== "" && item !== null && item !== undefined);
}

function selectOptions(metadataKey, selectedValue = "", placeholder = "Any") {
  const values = filterMetadata?.[metadataKey] || [];
  return [
    `<option value="">${escapeHtml(placeholder)}</option>`,
    ...values.map((value) => {
      const selected = String(value) === String(selectedValue) ? " selected" : "";
      return `<option value="${escapeHtml(value)}"${selected}>${escapeHtml(value)}</option>`;
    }),
  ].join("");
}

function renderField(field, config, value, dataAttribute) {
  const attribute = `${dataAttribute}="${escapeHtml(field)}"`;
  const label = escapeHtml(config.label);
  const fieldValue = value ?? "";

  if (config.metadataKey) {
    return `
      <label>
        ${label}
        <select ${attribute}>
          ${selectOptions(config.metadataKey, fieldValue, `Any ${config.label.toLowerCase()}`)}
        </select>
      </label>
    `;
  }

  const type = config.type || "text";
  const step = config.step ? ` step="${escapeHtml(config.step)}"` : "";
  const min = type === "number" ? ' min="0"' : "";
  const placeholder = config.placeholder ? ` placeholder="${escapeHtml(config.placeholder)}"` : "";
  return `
    <label>
      ${label}
      <input ${attribute} type="${escapeHtml(type)}" value="${escapeHtml(fieldValue)}"${step}${min}${placeholder}>
    </label>
  `;
}

function updateRemoveButtons(container, selector) {
  const buttons = container.querySelectorAll(selector);
  buttons.forEach((button) => {
    button.disabled = buttons.length === 1;
  });
}

function renderDrugTerms(terms = [emptyDrugTerm()]) {
  const rows = terms.length > 0 ? terms : [emptyDrugTerm()];
  drugTermsEl.innerHTML = rows.map((term, index) => `
    <div class="term-row drug-term-row" data-drug-term-index="${index}">
      ${DRUG_TERM_FIELDS.map((field) =>
        renderField(field, DRUG_TERM_CONFIG[field], term[field] || "", "data-drug-term-field")
      ).join("")}
      <button type="button" class="ghost remove-drug-term" ${rows.length === 1 ? "disabled" : ""}>Remove</button>
    </div>
  `).join("");

  drugTermsEl.querySelectorAll(".remove-drug-term").forEach((button) => {
    button.addEventListener("click", () => {
      button.closest(".drug-term-row")?.remove();
      if (drugTermsEl.querySelectorAll(".drug-term-row").length === 0) {
        renderDrugTerms([emptyDrugTerm()]);
      } else {
        updateRemoveButtons(drugTermsEl, ".remove-drug-term");
      }
    });
  });
}

function renderReactionTerms(terms = [emptyReactionTerm()]) {
  const rows = terms.length > 0 ? terms : [emptyReactionTerm()];
  reactionTermsEl.innerHTML = rows.map((term, index) => `
    <div class="term-row reaction-term-row" data-reaction-term-index="${index}">
      ${REACTION_TERM_FIELDS.map((field) =>
        renderField(field, REACTION_TERM_CONFIG[field], term[field] || "", "data-reaction-term-field")
      ).join("")}
      <button type="button" class="ghost remove-reaction-term" ${rows.length === 1 ? "disabled" : ""}>Remove</button>
    </div>
  `).join("");

  reactionTermsEl.querySelectorAll(".remove-reaction-term").forEach((button) => {
    button.addEventListener("click", () => {
      button.closest(".reaction-term-row")?.remove();
      if (reactionTermsEl.querySelectorAll(".reaction-term-row").length === 0) {
        renderReactionTerms([emptyReactionTerm()]);
      } else {
        updateRemoveButtons(reactionTermsEl, ".remove-reaction-term");
      }
    });
  });
}

function addDrugTerm(term = emptyDrugTerm()) {
  const currentTerms = rowsToDrugTerms({ includeEmpty: true });
  currentTerms.push(term);
  renderDrugTerms(currentTerms);
}

function addReactionTerm(term = emptyReactionTerm()) {
  const currentTerms = rowsToReactionTerms({ includeEmpty: true });
  currentTerms.push(term);
  renderReactionTerms(currentTerms);
}

function rowsToDrugTerms({ includeEmpty = false } = {}) {
  return Array.from(drugTermsEl.querySelectorAll(".drug-term-row"))
    .map((row) => {
      const term = {};
      row.querySelectorAll("[data-drug-term-field]").forEach((input) => {
        term[input.dataset.drugTermField] = input.value;
      });
      return includeEmpty ? { ...emptyDrugTerm(), ...term } : cleanDrugTerm(term);
    })
    .filter((term) => includeEmpty || hasObjectFilters(term));
}

function rowsToReactionTerms({ includeEmpty = false } = {}) {
  return Array.from(reactionTermsEl.querySelectorAll(".reaction-term-row"))
    .map((row) => {
      const term = {};
      row.querySelectorAll("[data-reaction-term-field]").forEach((input) => {
        term[input.dataset.reactionTermField] = input.value;
      });
      return includeEmpty ? { ...emptyReactionTerm(), ...term } : cleanReactionTerm(term);
    })
    .filter((term) => includeEmpty || hasObjectFilters(term));
}

function currentCaseFilters() {
  const filters = {};
  CASE_FILTER_FIELDS.forEach((field) => {
    const input = searchForm.elements.namedItem(field);
    if (input) {
      const cleaned = String(input.value ?? "").trim();
      if (cleaned !== "") {
        filters[field] = cleaned;
      }
    }
  });
  return filters;
}

function normalizeRequest(rawRequest = {}) {
  const drugTerms = Array.isArray(rawRequest.drug_terms)
    ? rawRequest.drug_terms.map(cleanDrugTerm).filter(hasObjectFilters)
    : [];
  const reactionTerms = Array.isArray(rawRequest.reaction_terms)
    ? rawRequest.reaction_terms.map(cleanReactionTerm).filter(hasObjectFilters)
    : [];
  const caseFilters = cleanCaseFilters(rawRequest.case_filters || {});
  const request = {
    drug_terms: drugTerms,
    reaction_terms: reactionTerms,
    concept_mode: rawRequest.concept_mode === "all" ? "all" : "any",
    case_filters: caseFilters,
    limit: DEFAULT_LIMIT,
  };

  const offset = Number(rawRequest.offset || 0);
  if (Number.isFinite(offset) && offset > 0) {
    request.offset = offset;
  }

  return request;
}

function currentRequest() {
  return normalizeRequest({
    drug_terms: rowsToDrugTerms(),
    reaction_terms: rowsToReactionTerms(),
    concept_mode: conceptModeEl.value || "any",
    case_filters: currentCaseFilters(),
  });
}

function compactRequestForUrl(request, offset = 0) {
  const normalized = normalizeRequest({ ...request, offset });
  const compact = {};
  if (normalized.drug_terms.length > 0) {
    compact.drug_terms = normalized.drug_terms;
  }
  if (normalized.reaction_terms.length > 0) {
    compact.reaction_terms = normalized.reaction_terms;
  }
  if (normalized.drug_terms.length + normalized.reaction_terms.length > 1 || normalized.concept_mode === "all") {
    compact.concept_mode = normalized.concept_mode;
  }
  if (hasObjectFilters(normalized.case_filters)) {
    compact.case_filters = normalized.case_filters;
  }
  if (normalized.offset && normalized.offset > 0) {
    compact.offset = normalized.offset;
  }
  return compact;
}

function activeFilterCount(request) {
  const normalized = normalizeRequest(request);
  return (
    normalized.drug_terms.length
    + normalized.reaction_terms.length
    + Object.keys(normalized.case_filters).length
  );
}

function hasActiveFilters(request) {
  return activeFilterCount(request) > 0;
}

function drugTermSummary(term) {
  return DRUG_TERM_FIELDS
    .filter((field) => term[field])
    .map((field) => `${DRUG_TERM_CONFIG[field].label}: ${term[field]}`)
    .join(" | ");
}

function reactionTermSummary(term) {
  return REACTION_TERM_FIELDS
    .filter((field) => term[field])
    .map((field) => `${REACTION_TERM_CONFIG[field].label}: ${term[field]}`)
    .join(" | ");
}

function filterChips(request) {
  const normalized = normalizeRequest(request);
  const chips = [];
  const conceptTotal = normalized.drug_terms.length + normalized.reaction_terms.length;
  if (conceptTotal > 1 || normalized.concept_mode === "all") {
    chips.push(["Concept mode", normalized.concept_mode === "all" ? "All concepts" : "Any concept"]);
  }
  normalized.drug_terms.forEach((term, index) => {
    chips.push([`Drug concept ${index + 1}`, drugTermSummary(term)]);
  });
  normalized.reaction_terms.forEach((term, index) => {
    chips.push([`Reaction concept ${index + 1}`, reactionTermSummary(term)]);
  });
  Object.entries(normalized.case_filters)
    .filter(([key]) => CASE_FILTER_LABELS[key])
    .forEach(([key, value]) => chips.push([CASE_FILTER_LABELS[key], value]));

  if (chips.length === 0) {
    return "";
  }

  return chips
    .map(([label, value]) => `<span class="chip"><strong>${escapeHtml(label)}:</strong> ${escapeHtml(value)}</span>`)
    .join("");
}

function resetFormToDefaults() {
  searchForm.reset();
  conceptModeEl.value = "any";
  renderDrugTerms([emptyDrugTerm()]);
  renderReactionTerms([emptyReactionTerm()]);
}

function applyRequestToForm(request) {
  const normalized = normalizeRequest(request);
  resetFormToDefaults();
  conceptModeEl.value = normalized.concept_mode;
  renderDrugTerms(normalized.drug_terms.length > 0 ? normalized.drug_terms : [emptyDrugTerm()]);
  renderReactionTerms(normalized.reaction_terms.length > 0 ? normalized.reaction_terms : [emptyReactionTerm()]);

  Object.entries(normalized.case_filters).forEach(([key, value]) => {
    const field = searchForm.elements.namedItem(key);
    if (field) {
      field.value = String(value);
    }
  });
}

function readUrlState() {
  const query = new URLSearchParams(window.location.search);
  const encoded = query.get("q");
  if (!encoded) {
    return { request: normalizeRequest({}), offset: 0 };
  }

  try {
    const parsed = JSON.parse(encoded);
    const request = normalizeRequest(parsed);
    return { request, offset: Number(request.offset || 0) };
  } catch (error) {
    return { request: normalizeRequest({}), offset: 0 };
  }
}

function writeUrlState(request, offset = 0) {
  const query = new URLSearchParams();
  const normalized = normalizeRequest(request);
  if (hasActiveFilters(normalized)) {
    query.set("q", JSON.stringify(compactRequestForUrl(normalized, offset)));
  }
  const nextUrl = query.toString() ? `${window.location.pathname}?${query.toString()}` : window.location.pathname;
  history.replaceState({}, "", nextUrl);
}

function buildSearchUrl(request, offset = 0) {
  const query = new URLSearchParams();
  query.set("q", JSON.stringify(compactRequestForUrl(request, offset)));
  return `${window.location.origin}${window.location.pathname}?${query.toString()}`;
}

function formatTimestamp(value) {
  if (!value) {
    return "n/a";
  }
  const timestamp = new Date(value);
  if (Number.isNaN(timestamp.getTime())) {
    return value;
  }
  return timestamp.toLocaleString();
}

function renderCohortSummary() {
  if (!caseSearchState) {
    cohortSummaryEl.innerHTML = '<p class="empty-state">No active cohort yet. Run a search to capture a reproducible cohort summary.</p>';
    return;
  }

  const payload = latestCasePayload;
  const request = caseSearchState.request;
  const total = payload?.total ?? 0;
  const shown = payload?.items?.length ?? 0;

  cohortSummaryEl.innerHTML = `
    <div class="toolbar">
      <strong>Active cohort</strong>
      <span class="hint">Case cohort</span>
    </div>
    <div class="meta">
      <div><strong>Matching cases</strong>${escapeHtml(total)}</div>
      <div><strong>Rows in current view</strong>${escapeHtml(shown)}</div>
      <div><strong>Active filters</strong>${escapeHtml(activeFilterCount(request))}</div>
      <div><strong>Last run</strong>${escapeHtml(formatTimestamp(caseSearchState.executedAt))}</div>
    </div>
    <p class="hint">The browser URL now reflects this cohort, so the current search can be bookmarked or shared locally.</p>
    <div class="chips">${filterChips(request) || '<span class="chip">No active filters</span>'}</div>
  `;
}

function errorDetailFromPayload(payload, fallback) {
  if (!payload || !payload.detail) {
    return fallback;
  }
  if (Array.isArray(payload.detail)) {
    return payload.detail
      .map((item) => {
        if (typeof item === "string") {
          return item;
        }
        return item.msg || item.detail || JSON.stringify(item);
      })
      .join("; ");
  }
  return payload.detail;
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    let detail = response.statusText;
    try {
      detail = errorDetailFromPayload(await response.json(), detail);
    } catch (error) {
      // ignore JSON parse errors
    }
    throw new Error(detail);
  }
  return response.json();
}

async function postJson(url, payload) {
  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Accept": "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    let detail = response.statusText;
    try {
      detail = errorDetailFromPayload(await response.json(), detail);
    } catch (error) {
      // ignore JSON parse errors
    }
    throw new Error(detail);
  }
  return response.json();
}

function downloadBlob(filename, content, mimeType) {
  const blob = new Blob([content], { type: mimeType });
  const link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(link.href);
}

function downloadCsv(filename, rows) {
  const csvLines = rows.map((row) =>
    row
      .map((value) => `"${String(value ?? "").replaceAll('"', '""')}"`)
      .join(",")
  );
  downloadBlob(filename, csvLines.join("\n"), "text/csv;charset=utf-8");
}

function downloadJson(filename, payload) {
  downloadBlob(filename, JSON.stringify(payload, null, 2), "application/json;charset=utf-8");
}

function timestampSlug() {
  return new Date().toISOString().replaceAll(":", "-");
}

function populateSelect(id, values, placeholder) {
  const select = document.getElementById(id);
  if (!select) {
    return;
  }

  select.innerHTML = "";
  const emptyOption = document.createElement("option");
  emptyOption.value = "";
  emptyOption.textContent = placeholder;
  select.appendChild(emptyOption);

  values.forEach((value) => {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = value;
    select.appendChild(option);
  });
}

async function loadFilterMetadata() {
  setStatus("Loading filter metadata...");
  try {
    filterMetadata = await fetchJson("/filters/metadata");
    Object.entries(SELECT_FIELDS).forEach(([field, metadataKey]) => {
      const selectId = field.replaceAll("_", "-");
      const placeholder = `Any ${field.replaceAll("_", " ")}`;
      populateSelect(selectId, filterMetadata[metadataKey] || [], placeholder);
    });
    renderDrugTerms(rowsToDrugTerms({ includeEmpty: true }));
    setStatus("Filter metadata loaded.");
  } catch (error) {
    setStatus(`Metadata load failed: ${error.message}`, true);
  }
}

function renderPager(payload, request) {
  if (!payload || payload.total === 0) {
    casePagerEl.innerHTML = "";
    return;
  }

  const currentPage = Math.floor(payload.offset / payload.limit) + 1;
  const totalPages = Math.max(1, Math.ceil(payload.total / payload.limit));
  const hasPrev = payload.offset > 0;
  const hasNext = payload.offset + payload.items.length < payload.total;

  casePagerEl.innerHTML = `
    <button id="case-prev" type="button" class="secondary" ${hasPrev ? "" : "disabled"}>Previous</button>
    <span>Page ${currentPage} of ${totalPages}</span>
    <button id="case-next" type="button" class="secondary" ${hasNext ? "" : "disabled"}>Next</button>
  `;

  const prev = document.getElementById("case-prev");
  const next = document.getElementById("case-next");

  if (prev) {
    prev.addEventListener("click", () => runCaseSearchWithOffset(Math.max(0, payload.offset - payload.limit), request));
  }
  if (next) {
    next.addEventListener("click", () => runCaseSearchWithOffset(payload.offset + payload.limit, request));
  }
}

function renderCaseResults(payload, request) {
  latestCasePayload = payload;
  caseSummaryEl.textContent = `Showing ${payload.items.length} of ${payload.total} matching cases.`;
  caseFilterSummaryEl.innerHTML = filterChips(request);

  if (payload.items.length === 0) {
    caseResultsEl.innerHTML = '<p class="empty-state">No matching latest cases found.</p>';
    renderPager(payload, request);
    return;
  }

  const rows = payload.items.map((item) => {
    const patientText = `${valueText(item.sex_std)} / ${valueText(item.age_value)} ${valueText(item.age_unit, "")}`.trim();
    const drugText = compactList(item.drugs, 3);
    const activeIngredientText = compactList(item.active_ingredients, 3);
    const reactionText = compactList(item.reactions, 5);
    const outcomeText = compactList(item.outcomes, 4);

    return `
      <tr>
        <td><button class="ghost view-case" data-case-version-pk="${escapeHtml(item.case_version_pk)}">Open</button></td>
        <td class="mono wrap-cell">${escapeHtml(item.source_report_id)}</td>
        <td>${escapeHtml(item.source_quarter)}</td>
        <td>${escapeHtml(valueText(item.report_type))}</td>
        <td>${escapeHtml(valueText(item.reporter_country))}</td>
        <td class="wrap-cell">
          <div class="cell-primary">${escapeHtml(patientText)}</div>
          <span class="cell-muted">${escapeHtml(valueText(item.age_group, "No age group"))}</span>
        </td>
        <td class="list-cell" title="${escapeHtml(arrayText(item.drugs))}">
          <div class="cell-primary">${escapeHtml(drugText)}</div>
          <span class="cell-muted">Active ingredients: ${escapeHtml(activeIngredientText)}</span>
        </td>
        <td class="list-cell" title="${escapeHtml(arrayText(item.reactions))}">${escapeHtml(reactionText)}</td>
        <td class="list-cell" title="${escapeHtml(arrayText(item.outcomes))}">${escapeHtml(outcomeText)}</td>
      </tr>
    `;
  }).join("");

  caseResultsEl.innerHTML = `
    <div class="results-table-shell" tabindex="0" aria-label="Case results">
      <table class="results-table">
        <colgroup>
          <col style="width: 64px;">
          <col style="width: 104px;">
          <col style="width: 70px;">
          <col style="width: 58px;">
          <col style="width: 64px;">
          <col style="width: 116px;">
          <col style="width: 250px;">
          <col style="width: 200px;">
          <col style="width: 114px;">
        </colgroup>
        <thead>
          <tr>
            <th>Detail</th>
            <th>Report</th>
            <th>Quarter</th>
            <th>Type</th>
            <th>Country</th>
            <th>Patient</th>
            <th>Drug exposure</th>
            <th>Reactions</th>
            <th>Outcomes</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `;

  caseResultsEl.querySelectorAll(".view-case").forEach((button) => {
    button.addEventListener("click", () => {
      caseResultsEl.querySelectorAll("tbody tr").forEach((row) => row.classList.remove("selected-row"));
      button.closest("tr")?.classList.add("selected-row");
      loadCaseDetail(button.dataset.caseVersionPk);
    });
  });

  renderPager(payload, request);
}

function renderCaseDetail(payload) {
  const reactionValues = (payload.reactions || []).map((reaction) => reaction.reaction_pt);
  const drugs = payload.drugs || [];
  const drugBlocks = drugs.length === 0
    ? '<p class="empty-state">No drug rows linked to this case version.</p>'
    : drugs.map((drug) => {
      const doseParts = [drug.dose_amt, drug.dose_unit].filter((value) => value !== null && value !== undefined && value !== "");
      const doseText = drug.dose_vbm || (doseParts.length > 0 ? doseParts.join(" ") : "n/a");
      return `
        <article class="drug-card">
          <strong>${escapeHtml(valueText(drug.drugname, "Unknown drug"))}</strong>
          <div class="meta">
            <div><strong>Role</strong>${escapeHtml(valueText(drug.role_cod))}</div>
            <div><strong>Route</strong>${escapeHtml(valueText(drug.route))}</div>
            <div><strong>Active ingredient</strong>${escapeHtml(valueText(drug.prod_ai))}</div>
            <div><strong>Dose</strong>${escapeHtml(doseText)}</div>
          </div>
          <p>Indications: ${escapeHtml(arrayText(drug.indications))}</p>
          <p>Therapy window: ${escapeHtml(dateRangeText(drug.therapy_start_dt, drug.therapy_end_dt))}</p>
          <p>Drug dates: ${escapeHtml(dateRangeText(drug.start_dt, drug.end_dt))}</p>
        </article>
      `;
    }).join("");

  caseDetailEl.innerHTML = `
    <div class="case-overview">
      <p class="eyebrow">Source report</p>
      <h3 class="mono">${escapeHtml(payload.source_report_id)}</h3>
      <div class="meta">
        <div><strong>Case</strong>${escapeHtml(payload.canonical_case_id)}</div>
        <div><strong>Quarter</strong>${escapeHtml(payload.source_quarter)}</div>
        <div><strong>Version</strong>${escapeHtml(valueText(payload.case_version_num))}</div>
        <div><strong>Report type</strong>${escapeHtml(valueText(payload.report_type))}</div>
        <div><strong>I / F</strong>${escapeHtml(valueText(payload.initial_or_followup))}</div>
        <div><strong>Country</strong>${escapeHtml(valueText(payload.reporter_country))}</div>
        <div><strong>Sex / Age</strong>${escapeHtml(valueText(payload.sex_std))} / ${escapeHtml(valueText(payload.age_value))} ${escapeHtml(valueText(payload.age_unit, ""))}</div>
        <div><strong>Age group / Weight</strong>${escapeHtml(valueText(payload.age_group))} / ${escapeHtml(valueText(payload.weight_kg))}</div>
      </div>
    </div>
    <section class="inspector-section">
      <h3>Outcomes</h3>
      <div class="chips">${chipList(payload.outcomes, "No outcomes linked")}</div>
    </section>
    <section class="inspector-section">
      <h3>Reactions</h3>
      <div class="chips">${chipList(reactionValues, "No reactions linked")}</div>
    </section>
    <section class="inspector-section">
      <h3>Reporter</h3>
      <div class="chips">${chipList(payload.reporter_types, "No reporter types linked")}</div>
    </section>
    <section class="inspector-section">
      <h3>Drug Exposure</h3>
      <div class="stack">${drugBlocks}</div>
    </section>
  `;
}

async function loadCaseDetail(caseVersionPk) {
  setStatus(`Loading case detail for ${caseVersionPk}...`);
  try {
    const payload = await fetchJson(`/cases/${caseVersionPk}`);
    renderCaseDetail(payload);
    setStatus(`Loaded case ${payload.source_report_id}.`);
  } catch (error) {
    caseDetailEl.innerHTML = '<p class="empty-state">Unable to load case detail.</p>';
    setStatus(`Case detail failed: ${error.message}`, true);
  }
}

async function runCaseSearchWithOffset(offset, request) {
  const baseRequest = normalizeRequest(request);
  if (!hasActiveFilters(baseRequest)) {
    setStatus("Choose at least one filter before searching.", true);
    return;
  }

  const effectiveRequest = normalizeRequest({ ...baseRequest, offset });
  setStatus("Searching cases...");
  try {
    const payload = await postJson("/cases/search", effectiveRequest);
    caseSearchState = {
      mode: SEARCH_MODE,
      request: baseRequest,
      offset: payload.offset,
      executedAt: new Date().toISOString(),
    };
    renderCaseResults(payload, baseRequest);
    renderCohortSummary();
    writeUrlState(baseRequest, payload.offset);

    if (payload.items.length > 0) {
      caseDetailEl.innerHTML = '<p class="empty-state">Select a case from the table to inspect its linked drugs, reactions, outcomes, and metadata.</p>';
      setStatus("Search complete.");
    } else {
      caseDetailEl.innerHTML = '<p class="empty-state">No case selected.</p>';
      setStatus("Search complete.");
    }
  } catch (error) {
    caseResultsEl.innerHTML = '<p class="empty-state">Case search failed.</p>';
    setStatus(`Case search failed: ${error.message}`, true);
  }
}

async function runCaseSearch(event) {
  event.preventDefault();
  await runCaseSearchWithOffset(0, currentRequest());
}

function buildReportPayload(payload, searchState) {
  const request = searchState?.request || normalizeRequest({});
  return {
    search_type: SEARCH_MODE,
    exported_at: new Date().toISOString(),
    shareable_url: buildSearchUrl(request, searchState?.offset || 0),
    active_filter_count: activeFilterCount(request),
    filters: compactRequestForUrl(request, searchState?.offset || 0),
    pagination: {
      limit: payload.limit,
      offset: payload.offset || 0,
      exported_rows: payload.items.length,
    },
    totals: {
      total_matches: payload.total,
    },
    items: payload.items,
  };
}

async function fetchAllCaseResultsForExport() {
  if (!latestCasePayload || !caseSearchState) {
    setStatus("Run a case search before exporting.", true);
    return null;
  }

  const request = normalizeRequest(caseSearchState.request);
  setStatus(`Exporting all ${latestCasePayload.total} matching cases...`);
  try {
    return await postJson("/cases/export", request);
  } catch (error) {
    setStatus(`Export failed: ${error.message}`, true);
    return null;
  }
}

async function exportCasesCsv() {
  const exportPayload = await fetchAllCaseResultsForExport();
  if (!exportPayload || exportPayload.items.length === 0) {
    if (exportPayload) {
      setStatus("No matching cases to export.", true);
    }
    return;
  }

  downloadCsv("faers-case-results.csv", [
    ["source_report_id", "source_quarter", "report_type", "reporter_country", "sex_std", "age_value", "age_unit", "drugs", "active_ingredients", "reactions", "outcomes"],
    ...exportPayload.items.map((item) => [
      item.source_report_id,
      item.source_quarter,
      item.report_type,
      item.reporter_country,
      item.sex_std,
      item.age_value,
      item.age_unit,
      arrayText(item.drugs),
      arrayText(item.active_ingredients),
      arrayText(item.reactions),
      arrayText(item.outcomes),
    ]),
  ]);
  setStatus(`Exported ${exportPayload.items.length} matching cases to CSV.`);
}

async function exportCaseReport() {
  if (!latestCasePayload || !caseSearchState) {
    setStatus("Run a case search before exporting a report.", true);
    return;
  }

  const exportPayload = await fetchAllCaseResultsForExport();
  if (!exportPayload) {
    return;
  }

  downloadJson(`faers-case-report-${timestampSlug()}.json`, buildReportPayload(exportPayload, caseSearchState));
  setStatus(`Exported case report JSON with ${exportPayload.items.length} matching cases.`);
}

function clearFilters() {
  resetFormToDefaults();
  caseSummaryEl.textContent = "No case search has been run yet.";
  caseFilterSummaryEl.innerHTML = "";
  caseResultsEl.innerHTML = "";
  casePagerEl.innerHTML = "";
  caseDetailEl.innerHTML = '<p class="empty-state">Select a case from the table to inspect its linked drugs, reactions, outcomes, and metadata.</p>';
  latestCasePayload = null;
  caseSearchState = null;
  renderCohortSummary();
  writeUrlState({}, 0);
  setStatus("Filters cleared.");
}

async function hydrateFromUrl() {
  const urlState = readUrlState();
  applyRequestToForm(urlState.request);

  if (!hasActiveFilters(urlState.request)) {
    renderCohortSummary();
    return;
  }

  await runCaseSearchWithOffset(urlState.offset, urlState.request);
}

renderDrugTerms([emptyDrugTerm()]);
renderReactionTerms([emptyReactionTerm()]);

searchForm.addEventListener("submit", runCaseSearch);
addDrugTermButton.addEventListener("click", () => addDrugTerm());
addReactionTermButton.addEventListener("click", () => addReactionTerm());
clearFiltersButton.addEventListener("click", clearFilters);
exportCasesButton.addEventListener("click", exportCasesCsv);
exportCaseReportButton.addEventListener("click", exportCaseReport);

loadFilterMetadata().then(() => hydrateFromUrl());
