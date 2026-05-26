const statusEl = document.getElementById("status");
const cohortSummaryEl = document.getElementById("cohort-summary");
const caseSummaryEl = document.getElementById("case-summary");
const caseFilterSummaryEl = document.getElementById("case-filter-summary");
const caseResultsEl = document.getElementById("case-results");
const casePagerEl = document.getElementById("case-pager");
const caseDetailEl = document.getElementById("case-detail");
const savedSearchNameEl = document.getElementById("saved-search-name");
const savedSearchListEl = document.getElementById("saved-searches-list");
const searchForm = document.getElementById("search-form");
const primaryTermsEl = document.getElementById("primary-terms");
const primaryTermModeEl = document.getElementById("primary-term-mode");
const addPrimaryTermButton = document.getElementById("add-primary-term");
const clearFiltersButton = document.getElementById("clear-filters");
const saveSearchButton = document.getElementById("save-search");
const exportCasesButton = document.getElementById("export-cases");
const exportCaseReportButton = document.getElementById("export-case-report");

const DEFAULT_LIMIT = 25;
const SAVED_SEARCH_STORAGE_KEY = "faersdb.savedSearches.v1";
const SEARCH_MODE = "cases";
const TERM_FIELDS = ["drug_name", "prod_ai", "reaction_pt", "indication_pt"];
const TERM_FIELD_CONFIG = {
  drug_name: { label: "Drug name", placeholder: "aspirin" },
  prod_ai: { label: "Active ingredient", placeholder: "ibuprofen" },
  reaction_pt: { label: "Reaction", placeholder: "headache" },
  indication_pt: { label: "Indication", placeholder: "pain" },
};
const FORM_FIELD_NAMES = Array.from(searchForm.elements)
  .filter((element) => element.name)
  .map((element) => element.name);

let latestCasePayload = null;
let filterMetadata = null;
let caseSearchState = null;

const SELECT_FIELDS = {
  quarter: "quarters",
  report_type: "report_types",
  initial_or_followup: "initial_or_followup_values",
  sex_std: "sex_values",
  age_group: "age_groups",
  role_cod: "role_codes",
  route: "routes",
  case_outcome: "case_outcomes",
  reporter_type: "reporter_types",
};

const FILTER_LABELS = {
  primary_terms: "Primary term",
  quarter: "Quarter",
  report_type: "Report type",
  initial_or_followup: "I/F",
  event_dt_from: "Event from",
  event_dt_to: "Event to",
  fda_dt_from: "FDA from",
  fda_dt_to: "FDA to",
  sex_std: "Sex",
  age_min: "Age min",
  age_max: "Age max",
  age_group: "Age group",
  reporter_country: "Country",
  role_cod: "Role",
  route: "Route",
  case_outcome: "Case outcome",
  reporter_type: "Reporter",
  therapy_start_from: "Therapy start",
  therapy_end_to: "Therapy end",
};

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

function emptyTerm() {
  return {
    drug_name: "",
    prod_ai: "",
    reaction_pt: "",
    indication_pt: "",
  };
}

function cleanTerm(rawTerm = {}) {
  const term = {};
  TERM_FIELDS.forEach((field) => {
    const value = String(rawTerm[field] ?? "").trim();
    if (value !== "") {
      term[field] = value;
    }
  });
  return term;
}

function termHasFilters(term) {
  return TERM_FIELDS.some((field) => String(term[field] ?? "").trim() !== "");
}

function parsePrimaryTermsValue(value) {
  if (!value) {
    return [];
  }
  try {
    const parsed = typeof value === "string" ? JSON.parse(value) : value;
    if (!Array.isArray(parsed)) {
      return [];
    }
    return parsed.map(cleanTerm).filter(termHasFilters);
  } catch (error) {
    return [];
  }
}

function termsFromParams(params = {}) {
  const explicitTerms = parsePrimaryTermsValue(params.primary_terms);
  if (explicitTerms.length > 0 || params.primary_terms) {
    return explicitTerms;
  }

  const legacyTerm = cleanTerm(params);
  return termHasFilters(legacyTerm) ? [legacyTerm] : [];
}

function renderPrimaryTerms(terms = [emptyTerm()]) {
  const rows = terms.length > 0 ? terms : [emptyTerm()];
  primaryTermsEl.innerHTML = rows.map((term, index) => `
    <div class="term-row" data-term-index="${index}">
      ${TERM_FIELDS.map((field) => `
        <label>
          ${escapeHtml(TERM_FIELD_CONFIG[field].label)}
          <input data-term-field="${escapeHtml(field)}" value="${escapeHtml(term[field] || "")}" placeholder="${escapeHtml(TERM_FIELD_CONFIG[field].placeholder)}">
        </label>
      `).join("")}
      <button type="button" class="ghost remove-primary-term" ${rows.length === 1 ? "disabled" : ""}>Remove</button>
    </div>
  `).join("");

  primaryTermsEl.querySelectorAll(".remove-primary-term").forEach((button) => {
    button.addEventListener("click", () => {
      button.closest(".term-row")?.remove();
      if (primaryTermsEl.querySelectorAll(".term-row").length === 0) {
        renderPrimaryTerms([emptyTerm()]);
      } else {
        updateRemoveTermButtons();
      }
    });
  });
}

function updateRemoveTermButtons() {
  const buttons = primaryTermsEl.querySelectorAll(".remove-primary-term");
  buttons.forEach((button) => {
    button.disabled = buttons.length === 1;
  });
}

function addPrimaryTerm(term = emptyTerm()) {
  const currentTerms = rowsToTerms({ includeEmpty: true });
  currentTerms.push(term);
  renderPrimaryTerms(currentTerms);
}

function rowsToTerms({ includeEmpty = false } = {}) {
  return Array.from(primaryTermsEl.querySelectorAll(".term-row"))
    .map((row) => {
      const term = {};
      row.querySelectorAll("[data-term-field]").forEach((input) => {
        term[input.dataset.termField] = input.value;
      });
      return includeEmpty ? { ...emptyTerm(), ...term } : cleanTerm(term);
    })
    .filter((term) => includeEmpty || termHasFilters(term));
}

function currentParams() {
  const formData = new FormData(searchForm);
  const params = {};

  for (const [key, value] of formData.entries()) {
    const cleaned = value.toString().trim();
    params[key] = cleaned;
  }

  const terms = rowsToTerms();
  if (terms.length > 0) {
    params.primary_terms = JSON.stringify(terms);
    params.primary_term_mode = primaryTermModeEl.value || "any";
  }

  return params;
}

function normalizeParams(rawParams = currentParams()) {
  const params = {};
  const terms = termsFromParams(rawParams);

  if (terms.length > 0) {
    params.primary_terms = JSON.stringify(terms);
    params.primary_term_mode = rawParams.primary_term_mode === "all" ? "all" : "any";
  }

  Object.entries(rawParams).forEach(([key, value]) => {
    if (TERM_FIELDS.includes(key) || key === "primary_terms" || key === "primary_term_mode" || key === "limit") {
      return;
    }
    if (key === "offset") {
      const parsed = Number(value);
      if (Number.isFinite(parsed) && parsed > 0) {
        params.offset = parsed;
      }
      return;
    }

    const cleaned = String(value ?? "").trim();
    if (cleaned !== "") {
      params[key] = cleaned;
    }
  });

  params.limit = DEFAULT_LIMIT;

  return params;
}

function activeFilterEntries(params) {
  return Object.entries(params).filter(([key, value]) => {
    if (key === "limit" || key === "offset" || key === "primary_term_mode") {
      return false;
    }
    if (key === "primary_terms") {
      return parsePrimaryTermsValue(value).length > 0;
    }
    return value !== "" && value !== null && value !== undefined;
  });
}

function hasActiveFilters(params) {
  return activeFilterEntries(params).length > 0;
}

function termSummary(term) {
  return TERM_FIELDS
    .filter((field) => term[field])
    .map((field) => `${TERM_FIELD_CONFIG[field].label}: ${term[field]}`)
    .join(" | ");
}

function filterChips(params) {
  const chips = [];
  const terms = parsePrimaryTermsValue(params.primary_terms);
  const termMode = params.primary_term_mode === "all" ? "All terms" : "Any term";

  terms.forEach((term, index) => {
    chips.push([
      terms.length > 1 ? `${termMode} ${index + 1}` : "Primary term",
      termSummary(term),
    ]);
  });

  activeFilterEntries(params)
    .filter(([key]) => key !== "primary_terms" && FILTER_LABELS[key])
    .forEach(([key, value]) => chips.push([FILTER_LABELS[key], value]));

  if (chips.length === 0) {
    return "";
  }

  return chips
    .map(([label, value]) => `<span class="chip"><strong>${escapeHtml(label)}:</strong> ${escapeHtml(value)}</span>`)
    .join("");
}

function buildQuery(params) {
  const query = new URLSearchParams();
  Object.entries(normalizeParams(params)).forEach(([key, value]) => {
    if (key !== "offset" && value !== null && value !== undefined && value !== "") {
      query.set(key, String(value));
    }
  });
  if (params.offset && Number(params.offset) > 0) {
    query.set("offset", String(params.offset));
  }
  return query.toString();
}

function resetFormToDefaults() {
  searchForm.reset();
  primaryTermModeEl.value = "any";
  renderPrimaryTerms([emptyTerm()]);
}

function applyParamsToForm(params) {
  resetFormToDefaults();
  const terms = termsFromParams(params);
  renderPrimaryTerms(terms.length > 0 ? terms : [emptyTerm()]);
  primaryTermModeEl.value = params.primary_term_mode === "all" ? "all" : "any";

  Object.entries(params).forEach(([key, value]) => {
    if (TERM_FIELDS.includes(key) || key === "primary_terms" || key === "primary_term_mode") {
      return;
    }
    const field = searchForm.elements.namedItem(key);
    if (field) {
      field.value = String(value);
    }
  });
}

function readUrlState() {
  const query = new URLSearchParams(window.location.search);
  const params = {};

  query.forEach((value, key) => {
    if (key !== "mode") {
      params[key] = value || "";
    }
  });

  return {
    params: normalizeParams(params),
    offset: Number(query.get("offset") || 0),
  };
}

function writeUrlState(params, offset = 0) {
  const query = new URLSearchParams();
  const normalized = normalizeParams(params);
  const active = hasActiveFilters(normalized);

  if (active) {
    Object.entries(normalized).forEach(([key, value]) => {
      if (key === "limit" || key === "offset") {
        return;
      }
      query.set(key, String(value));
    });
    if (offset > 0) {
      query.set("offset", String(offset));
    }
  }

  const nextUrl = query.toString() ? `${window.location.pathname}?${query.toString()}` : window.location.pathname;
  history.replaceState({}, "", nextUrl);
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
  const params = caseSearchState.params;
  const filterCount = activeFilterEntries(params).length;
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
      <div><strong>Active filters</strong>${escapeHtml(filterCount)}</div>
      <div><strong>Last run</strong>${escapeHtml(formatTimestamp(caseSearchState.executedAt))}</div>
    </div>
    <p class="hint">The browser URL now reflects this cohort, so the current search can be bookmarked or shared locally.</p>
    <div class="chips">${filterChips(params) || '<span class="chip">No active filters</span>'}</div>
  `;
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    let detail = response.statusText;
    try {
      const payload = await response.json();
      if (payload.detail) {
        if (Array.isArray(payload.detail)) {
          detail = payload.detail.map((item) => item.msg || item.detail || JSON.stringify(item)).join("; ");
        } else {
          detail = payload.detail;
        }
      }
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

function buildSearchUrl(params, offset = 0) {
  const query = new URLSearchParams();
  const normalized = normalizeParams(params);
  Object.entries(normalized).forEach(([key, value]) => {
    if (key === "limit" || key === "offset") {
      return;
    }
    query.set(key, String(value));
  });
  if (offset > 0) {
    query.set("offset", String(offset));
  }
  return `${window.location.origin}${window.location.pathname}?${query.toString()}`;
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
    setStatus("Filter metadata loaded.");
  } catch (error) {
    setStatus(`Metadata load failed: ${error.message}`, true);
  }
}

function readSavedSearches() {
  try {
    const raw = localStorage.getItem(SAVED_SEARCH_STORAGE_KEY);
    if (!raw) {
      return [];
    }
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch (error) {
    return [];
  }
}

function writeSavedSearches(savedSearches) {
  localStorage.setItem(SAVED_SEARCH_STORAGE_KEY, JSON.stringify(savedSearches));
}

function renderSavedSearches() {
  const savedSearches = readSavedSearches()
    .sort((left, right) => String(right.saved_at || "").localeCompare(String(left.saved_at || "")));

  if (savedSearches.length === 0) {
    savedSearchListEl.innerHTML = '<p class="saved-search-empty">No saved searches yet.</p>';
    return;
  }

  savedSearchListEl.innerHTML = savedSearches.map((savedSearch) => {
    const params = normalizeParams(savedSearch.params || {});
    return `
      <div class="saved-search-item">
        <div>
          <strong>${escapeHtml(savedSearch.name)}</strong>
          <p class="hint">Case cohort | ${escapeHtml(activeFilterEntries(params).length)} filters | saved ${escapeHtml(formatTimestamp(savedSearch.saved_at))}</p>
        </div>
        <div class="chips">${filterChips(params) || '<span class="chip">No active filters</span>'}</div>
        <div class="actions">
          <button type="button" class="ghost load-saved-search" data-saved-search-id="${escapeHtml(savedSearch.id)}">Load</button>
          <button type="button" class="ghost delete-saved-search" data-saved-search-id="${escapeHtml(savedSearch.id)}">Delete</button>
        </div>
      </div>
    `;
  }).join("");

  savedSearchListEl.querySelectorAll(".load-saved-search").forEach((button) => {
    button.addEventListener("click", async () => {
      await loadSavedSearch(button.dataset.savedSearchId || "");
    });
  });

  savedSearchListEl.querySelectorAll(".delete-saved-search").forEach((button) => {
    button.addEventListener("click", () => {
      deleteSavedSearch(button.dataset.savedSearchId || "");
    });
  });
}

function buildSavedSearchId() {
  if (window.crypto && typeof window.crypto.randomUUID === "function") {
    return window.crypto.randomUUID();
  }
  return `saved-${Date.now()}`;
}

function saveCurrentSearch() {
  const name = savedSearchNameEl.value.trim();
  const params = normalizeParams(currentParams());

  if (!name) {
    setStatus("Choose a saved-search name before saving.", true);
    return;
  }
  if (!hasActiveFilters(params)) {
    setStatus("Apply at least one filter before saving a search.", true);
    return;
  }

  const savedSearches = readSavedSearches();
  const existing = savedSearches.find((savedSearch) => savedSearch.name.toLowerCase() === name.toLowerCase());
  const savedAt = new Date().toISOString();
  const entry = {
    id: existing?.id || buildSavedSearchId(),
    name,
    mode: SEARCH_MODE,
    saved_at: savedAt,
    params,
  };

  const nextSavedSearches = existing
    ? savedSearches.map((savedSearch) => (savedSearch.id === existing.id ? entry : savedSearch))
    : [entry, ...savedSearches];

  writeSavedSearches(nextSavedSearches);
  renderSavedSearches();
  setStatus(existing ? `Overwrote saved search "${name}".` : `Saved search "${name}".`);
}

async function loadSavedSearch(savedSearchId) {
  const savedSearch = readSavedSearches().find((entry) => entry.id === savedSearchId);
  if (!savedSearch) {
    setStatus("Saved search could not be found.", true);
    return;
  }

  savedSearchNameEl.value = savedSearch.name;
  const params = normalizeParams(savedSearch.params || {});
  applyParamsToForm(params);
  await runCaseSearchWithOffset(0, params);
}

function deleteSavedSearch(savedSearchId) {
  const savedSearches = readSavedSearches();
  const savedSearch = savedSearches.find((entry) => entry.id === savedSearchId);
  if (!savedSearch) {
    return;
  }

  writeSavedSearches(savedSearches.filter((entry) => entry.id !== savedSearchId));
  renderSavedSearches();
  setStatus(`Deleted saved search "${savedSearch.name}".`);
}

function renderPager(payload, params) {
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
    prev.addEventListener("click", () => runCaseSearchWithOffset(Math.max(0, payload.offset - payload.limit), params));
  }
  if (next) {
    next.addEventListener("click", () => runCaseSearchWithOffset(payload.offset + payload.limit, params));
  }
}

function renderCaseResults(payload, params) {
  latestCasePayload = payload;
  caseSummaryEl.textContent = `Showing ${payload.items.length} of ${payload.total} matching cases.`;
  caseFilterSummaryEl.innerHTML = filterChips({ ...params, limit: payload.limit });

  if (payload.items.length === 0) {
    caseResultsEl.innerHTML = '<p class="empty-state">No matching latest cases found.</p>';
    renderPager(payload, params);
    return;
  }

  const rows = payload.items.map((item) => `
    <tr>
      <td><button class="ghost view-case" data-case-version-pk="${escapeHtml(item.case_version_pk)}">Open</button></td>
      <td class="mono">${escapeHtml(item.source_report_id)}</td>
      <td>${escapeHtml(item.source_quarter)}</td>
      <td>${escapeHtml(item.report_type || "n/a")}</td>
      <td>${escapeHtml(item.reporter_country || "n/a")}</td>
      <td>${escapeHtml(item.sex_std || "n/a")} / ${escapeHtml(item.age_value ?? "n/a")} ${escapeHtml(item.age_unit || "")}</td>
      <td>${escapeHtml(arrayText(item.drugs))}</td>
      <td>${escapeHtml(arrayText(item.active_ingredients))}</td>
      <td>${escapeHtml(arrayText(item.reactions))}</td>
      <td>${escapeHtml(arrayText(item.outcomes))}</td>
    </tr>
  `).join("");

  caseResultsEl.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Detail</th>
          <th>Report</th>
          <th>Quarter</th>
          <th>Type</th>
          <th>Country</th>
          <th>Demographics</th>
          <th>Drugs</th>
          <th>Active ingredients</th>
          <th>Reactions</th>
          <th>Outcomes</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;

  caseResultsEl.querySelectorAll(".view-case").forEach((button) => {
    button.addEventListener("click", () => {
      loadCaseDetail(button.dataset.caseVersionPk);
    });
  });

  renderPager(payload, params);
}

function renderCaseDetail(payload) {
  const drugBlocks = payload.drugs.length === 0
    ? '<p class="empty-state">No drug rows linked to this case version.</p>'
    : payload.drugs.map((drug) => `
        <div class="detail-block">
          <strong>${escapeHtml(drug.drugname || "Unknown drug")}</strong>
          <p class="hint">Active ingredient: ${escapeHtml(drug.prod_ai || "n/a")}</p>
          <p class="hint">Role: ${escapeHtml(drug.role_cod || "n/a")} | Route: ${escapeHtml(drug.route || "n/a")}</p>
          <p>Dose: ${escapeHtml(drug.dose_amt ?? "n/a")} ${escapeHtml(drug.dose_unit || "")}</p>
          <p>Indications: ${escapeHtml(arrayText(drug.indications))}</p>
          <p>Therapy window: ${escapeHtml(drug.therapy_start_dt || "n/a")} to ${escapeHtml(drug.therapy_end_dt || "n/a")}</p>
        </div>
      `).join("");

  const reactionBlocks = payload.reactions.length === 0
    ? '<p class="empty-state">No reactions linked to this case version.</p>'
    : payload.reactions.map((reaction) => `
        <div class="chip">${escapeHtml(reaction.reaction_pt)}</div>
      `).join("");

  caseDetailEl.innerHTML = `
    <div class="detail-block">
      <h3 class="mono">${escapeHtml(payload.source_report_id)}</h3>
      <div class="meta">
        <div><strong>Case</strong>${escapeHtml(payload.canonical_case_id)}</div>
        <div><strong>Quarter</strong>${escapeHtml(payload.source_quarter)}</div>
        <div><strong>Version</strong>${escapeHtml(payload.case_version_num ?? "n/a")}</div>
        <div><strong>Report type</strong>${escapeHtml(payload.report_type || "n/a")}</div>
        <div><strong>I / F</strong>${escapeHtml(payload.initial_or_followup || "n/a")}</div>
        <div><strong>Country</strong>${escapeHtml(payload.reporter_country || "n/a")}</div>
        <div><strong>Sex / Age</strong>${escapeHtml(payload.sex_std || "n/a")} / ${escapeHtml(payload.age_value ?? "n/a")} ${escapeHtml(payload.age_unit || "")}</div>
        <div><strong>Age group / Weight</strong>${escapeHtml(payload.age_group || "n/a")} / ${escapeHtml(payload.weight_kg ?? "n/a")}</div>
      </div>
      <p class="hint">Outcomes: ${escapeHtml(arrayText(payload.outcomes))}</p>
      <p class="hint">Reporter types: ${escapeHtml(arrayText(payload.reporter_types))}</p>
    </div>
    <section>
      <h3>Drugs</h3>
      <div class="stack">${drugBlocks}</div>
    </section>
    <section>
      <h3>Reactions</h3>
      <div class="chips">${reactionBlocks}</div>
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

async function runCaseSearchWithOffset(offset, params) {
  const baseParams = normalizeParams(params);
  if (!hasActiveFilters(baseParams)) {
    setStatus("Choose at least one filter before searching.", true);
    return;
  }

  const effectiveParams = { ...baseParams, offset };
  setStatus("Searching cases...");
  try {
    const payload = await fetchJson(`/cases/search?${buildQuery(effectiveParams)}`);
    caseSearchState = {
      mode: SEARCH_MODE,
      params: baseParams,
      offset: payload.offset,
      executedAt: new Date().toISOString(),
    };
    renderCaseResults(payload, baseParams);
    renderCohortSummary();
    writeUrlState(baseParams, payload.offset);
    savedSearchNameEl.value = savedSearchNameEl.value.trim();

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
  await runCaseSearchWithOffset(0, currentParams());
}

function buildReportPayload(payload, searchState) {
  const params = searchState?.params || {};
  return {
    search_type: SEARCH_MODE,
    exported_at: new Date().toISOString(),
    shareable_url: buildSearchUrl(params, searchState?.offset || 0),
    active_filter_count: activeFilterEntries(params).length,
    filters: Object.fromEntries(activeFilterEntries(params)),
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

function exportCasesCsv() {
  if (!latestCasePayload || latestCasePayload.items.length === 0) {
    setStatus("Run a case search before exporting.", true);
    return;
  }

  downloadCsv("faers-case-results.csv", [
    ["source_report_id", "source_quarter", "report_type", "reporter_country", "sex_std", "age_value", "age_unit", "drugs", "active_ingredients", "reactions", "outcomes"],
    ...latestCasePayload.items.map((item) => [
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
  setStatus("Exported current case results to CSV.");
}

function exportCaseReport() {
  if (!latestCasePayload || !caseSearchState) {
    setStatus("Run a case search before exporting a report.", true);
    return;
  }

  downloadJson(`faers-case-report-${timestampSlug()}.json`, buildReportPayload(latestCasePayload, caseSearchState));
  setStatus("Exported case report JSON with filters and totals.");
}

function clearFilters() {
  resetFormToDefaults();
  savedSearchNameEl.value = "";
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
  applyParamsToForm(urlState.params);

  if (!hasActiveFilters(urlState.params)) {
    renderCohortSummary();
    return;
  }

  savedSearchNameEl.value = "";
  await runCaseSearchWithOffset(urlState.offset, urlState.params);
}

renderPrimaryTerms([emptyTerm()]);

searchForm.addEventListener("submit", runCaseSearch);
addPrimaryTermButton.addEventListener("click", () => addPrimaryTerm());
clearFiltersButton.addEventListener("click", clearFilters);
saveSearchButton.addEventListener("click", saveCurrentSearch);
exportCasesButton.addEventListener("click", exportCasesCsv);
exportCaseReportButton.addEventListener("click", exportCaseReport);

renderSavedSearches();
loadFilterMetadata().then(() => hydrateFromUrl());
