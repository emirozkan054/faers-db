const statusEl = document.getElementById("status");
const cohortSummaryEl = document.getElementById("cohort-summary");
const caseSummaryEl = document.getElementById("case-summary");
const caseFilterSummaryEl = document.getElementById("case-filter-summary");
const caseResultsEl = document.getElementById("case-results");
const casePagerEl = document.getElementById("case-pager");
const aggregateSummaryEl = document.getElementById("aggregate-summary");
const aggregateFilterSummaryEl = document.getElementById("aggregate-filter-summary");
const aggregateResultsEl = document.getElementById("aggregate-results");
const caseDetailEl = document.getElementById("case-detail");
const savedSearchNameEl = document.getElementById("saved-search-name");
const savedSearchListEl = document.getElementById("saved-searches-list");
const searchForm = document.getElementById("search-form");
const aggregateButton = document.getElementById("search-aggregates");
const clearFiltersButton = document.getElementById("clear-filters");
const saveSearchButton = document.getElementById("save-search");
const exportCasesButton = document.getElementById("export-cases");
const exportCaseReportButton = document.getElementById("export-case-report");
const exportAggregatesButton = document.getElementById("export-aggregates");
const exportAggregateReportButton = document.getElementById("export-aggregate-report");

const DEFAULT_LIMIT = 25;
const SAVED_SEARCH_STORAGE_KEY = "faersdb.savedSearches.v1";
const SEARCH_MODES = {
  CASES: "cases",
  AGGREGATES: "aggregates",
};
const FORM_FIELD_NAMES = Array.from(searchForm.elements)
  .filter((element) => element.name)
  .map((element) => element.name);

let latestCasePayload = null;
let latestAggregatePayload = null;
let filterMetadata = null;
let caseSearchState = null;
let aggregateSearchState = null;
let activeWorkflowState = null;

const SELECT_FIELDS = {
  quarter: "quarters",
  report_type: "report_types",
  initial_or_followup: "initial_or_followup_values",
  sex_std: "sex_values",
  age_group: "age_groups",
  role_cod: "role_codes",
  route: "routes",
  reaction_outcome: "reaction_outcomes",
  case_outcome: "case_outcomes",
  reporter_type: "reporter_types",
};

const FILTER_LABELS = {
  drug_name: "Drug",
  prod_ai: "Ingredient",
  reaction_pt: "Reaction",
  indication_pt: "Indication",
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
  reaction_outcome: "Reaction outcome",
  case_outcome: "Case outcome",
  reporter_type: "Reporter",
  therapy_start_from: "Therapy start",
  therapy_end_to: "Therapy end",
  limit: "Limit",
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

function currentParams() {
  const formData = new FormData(searchForm);
  const params = {};

  for (const [key, value] of formData.entries()) {
    const cleaned = value.toString().trim();
    params[key] = key === "limit" ? Number(cleaned || DEFAULT_LIMIT) : cleaned;
  }

  return params;
}

function normalizeParams(rawParams = currentParams()) {
  const params = {};

  Object.entries(rawParams).forEach(([key, value]) => {
    if (key === "limit") {
      const parsed = Number(value);
      params.limit = Number.isFinite(parsed) && parsed > 0 ? Math.min(100, Math.max(1, parsed)) : DEFAULT_LIMIT;
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

  if (!("limit" in params)) {
    params.limit = DEFAULT_LIMIT;
  }

  return params;
}

function activeFilterEntries(params) {
  return Object.entries(params).filter(([key, value]) => {
    if (key === "limit" || key === "offset") {
      return false;
    }
    return value !== "" && value !== null && value !== undefined;
  });
}

function hasActiveFilters(params) {
  return activeFilterEntries(params).length > 0;
}

function filterChips(params) {
  const chips = activeFilterEntries(params)
    .filter(([key]) => FILTER_LABELS[key])
    .map(([key, value]) => [FILTER_LABELS[key], value]);

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
    if (value !== null && value !== undefined && value !== "") {
      query.set(key, String(value));
    }
  });
  return query.toString();
}

function resetFormToDefaults() {
  searchForm.reset();
  const limitField = searchForm.elements.namedItem("limit");
  if (limitField) {
    limitField.value = String(DEFAULT_LIMIT);
  }
}

function applyParamsToForm(params) {
  resetFormToDefaults();
  Object.entries(params).forEach(([key, value]) => {
    const field = searchForm.elements.namedItem(key);
    if (field) {
      field.value = String(value);
    }
  });
}

function readUrlState() {
  const query = new URLSearchParams(window.location.search);
  const params = {};

  FORM_FIELD_NAMES.forEach((name) => {
    if (query.has(name)) {
      params[name] = query.get(name) || "";
    }
  });

  if (query.has("offset")) {
    params.offset = query.get("offset") || "0";
  }

  const requestedMode = query.get("mode");
  const mode = requestedMode === SEARCH_MODES.AGGREGATES ? SEARCH_MODES.AGGREGATES : SEARCH_MODES.CASES;
  return {
    mode,
    params: normalizeParams(params),
    offset: Number(query.get("offset") || 0),
  };
}

function writeUrlState(params, mode, offset = 0) {
  const query = new URLSearchParams();
  const normalized = normalizeParams(params);
  const active = hasActiveFilters(normalized);

  if (active) {
    Object.entries(normalized).forEach(([key, value]) => {
      query.set(key, String(value));
    });
    query.set("mode", mode);
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

function modeLabel(mode) {
  return mode === SEARCH_MODES.AGGREGATES ? "Drug-reaction aggregates" : "Case cohort";
}

function activeSearchPayload() {
  if (!activeWorkflowState) {
    return null;
  }
  return activeWorkflowState.mode === SEARCH_MODES.AGGREGATES ? latestAggregatePayload : latestCasePayload;
}

function renderCohortSummary() {
  if (!activeWorkflowState) {
    cohortSummaryEl.innerHTML = '<p class="empty-state">No active cohort yet. Run a search to capture a reproducible cohort summary.</p>';
    return;
  }

  const payload = activeSearchPayload();
  const params = activeWorkflowState.params;
  const filterCount = activeFilterEntries(params).length;
  const total = payload?.total ?? 0;
  const shown = payload?.items?.length ?? 0;
  const totalLabel = activeWorkflowState.mode === SEARCH_MODES.AGGREGATES ? "Aggregate rows" : "Matching cases";

  cohortSummaryEl.innerHTML = `
    <div class="toolbar">
      <strong>Active cohort</strong>
      <span class="hint">${escapeHtml(modeLabel(activeWorkflowState.mode))}</span>
    </div>
    <div class="meta">
      <div><strong>${escapeHtml(totalLabel)}</strong>${escapeHtml(total)}</div>
      <div><strong>Rows in current view</strong>${escapeHtml(shown)}</div>
      <div><strong>Active filters</strong>${escapeHtml(filterCount)}</div>
      <div><strong>Last run</strong>${escapeHtml(formatTimestamp(activeWorkflowState.executedAt))}</div>
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

function buildSearchUrl(params, mode, offset = 0) {
  const query = new URLSearchParams();
  const normalized = normalizeParams(params);
  Object.entries(normalized).forEach(([key, value]) => {
    query.set(key, String(value));
  });
  query.set("mode", mode);
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

  savedSearchListEl.innerHTML = savedSearches.map((savedSearch) => `
    <div class="saved-search-item">
      <div>
        <strong>${escapeHtml(savedSearch.name)}</strong>
        <p class="hint">${escapeHtml(modeLabel(savedSearch.mode || SEARCH_MODES.CASES))} | ${escapeHtml(activeFilterEntries(savedSearch.params || {}).length)} filters | saved ${escapeHtml(formatTimestamp(savedSearch.saved_at))}</p>
      </div>
      <div class="chips">${filterChips(savedSearch.params || {}) || '<span class="chip">No active filters</span>'}</div>
      <div class="actions">
        <button type="button" class="ghost load-saved-search" data-saved-search-id="${escapeHtml(savedSearch.id)}">Load</button>
        <button type="button" class="ghost delete-saved-search" data-saved-search-id="${escapeHtml(savedSearch.id)}">Delete</button>
      </div>
    </div>
  `).join("");

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
  const mode = activeWorkflowState?.mode || SEARCH_MODES.CASES;

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
    mode,
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
  applyParamsToForm(savedSearch.params || {});

  if ((savedSearch.mode || SEARCH_MODES.CASES) === SEARCH_MODES.AGGREGATES) {
    await runAggregateSearch(savedSearch.params || {});
  } else {
    await runCaseSearchWithOffset(0, savedSearch.params || {});
  }
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
      <td><button class="ghost view-case" data-case-version-pk="${item.case_version_pk}">Open</button></td>
      <td class="mono">${escapeHtml(item.source_report_id)}</td>
      <td>${escapeHtml(item.source_quarter)}</td>
      <td>${escapeHtml(item.report_type || "n/a")}</td>
      <td>${escapeHtml(item.reporter_country || "n/a")}</td>
      <td>${escapeHtml(item.sex_std || "n/a")} / ${escapeHtml(item.age_value ?? "n/a")} ${escapeHtml(item.age_unit || "")}</td>
      <td>${escapeHtml(arrayText(item.drugs))}</td>
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
          <th>Reactions</th>
          <th>Outcomes</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;

  caseResultsEl.querySelectorAll(".view-case").forEach((button) => {
    button.addEventListener("click", () => {
      loadCaseDetail(Number(button.dataset.caseVersionPk));
    });
  });

  renderPager(payload, params);
}

function renderAggregateResults(payload, params) {
  latestAggregatePayload = payload;
  aggregateSummaryEl.textContent = `Showing ${payload.items.length} of ${payload.total} drug-reaction combinations.`;
  aggregateFilterSummaryEl.innerHTML = filterChips({ ...params, limit: payload.limit });

  if (payload.items.length === 0) {
    aggregateResultsEl.innerHTML = '<p class="empty-state">No aggregate matches found.</p>';
    return;
  }

  const rows = payload.items.map((item) => `
    <tr>
      <td>${escapeHtml(item.drugname)}</td>
      <td>
        ${escapeHtml(item.reaction_pt)}
        <div><button class="ghost use-reaction" data-reaction="${escapeHtml(item.reaction_pt)}">Find matching cases</button></div>
      </td>
      <td>${escapeHtml(item.case_count)}</td>
    </tr>
  `).join("");

  aggregateResultsEl.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Drug</th>
          <th>Reaction</th>
          <th>Latest Cases</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;

  aggregateResultsEl.querySelectorAll(".use-reaction").forEach((button) => {
    button.addEventListener("click", async () => {
      document.getElementById("reaction-pt").value = button.dataset.reaction || "";
      await runCaseSearchWithOffset(0, currentParams());
    });
  });
}

function renderCaseDetail(payload) {
  const drugBlocks = payload.drugs.length === 0
    ? '<p class="empty-state">No drug rows linked to this case version.</p>'
    : payload.drugs.map((drug) => `
        <div class="detail-block">
          <strong>${escapeHtml(drug.drugname || "Unknown drug")}</strong>
          <p class="hint">Role: ${escapeHtml(drug.role_cod || "n/a")} | Route: ${escapeHtml(drug.route || "n/a")}</p>
          <p>Dose: ${escapeHtml(drug.dose_amt ?? "n/a")} ${escapeHtml(drug.dose_unit || "")}</p>
          <p>Indications: ${escapeHtml(arrayText(drug.indications))}</p>
          <p>Therapy window: ${escapeHtml(drug.therapy_start_dt || "n/a")} to ${escapeHtml(drug.therapy_end_dt || "n/a")}</p>
        </div>
      `).join("");

  const reactionBlocks = payload.reactions.length === 0
    ? '<p class="empty-state">No reactions linked to this case version.</p>'
    : payload.reactions.map((reaction) => `
        <div class="chip">${escapeHtml(reaction.reaction_pt)}${reaction.outcome ? ` (${escapeHtml(reaction.outcome)})` : ""}</div>
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
      mode: SEARCH_MODES.CASES,
      params: baseParams,
      offset: payload.offset,
      executedAt: new Date().toISOString(),
    };
    activeWorkflowState = caseSearchState;
    renderCaseResults(payload, baseParams);
    renderCohortSummary();
    writeUrlState(baseParams, SEARCH_MODES.CASES, payload.offset);
    savedSearchNameEl.value = savedSearchNameEl.value.trim();

    if (payload.items.length > 0) {
      await loadCaseDetail(payload.items[0].case_version_pk);
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

async function runAggregateSearch(params = currentParams()) {
  const normalized = normalizeParams(params);
  if (!hasActiveFilters(normalized)) {
    setStatus("Choose at least one filter before searching.", true);
    return;
  }

  setStatus("Loading aggregate counts...");
  try {
    const payload = await fetchJson(`/aggregates/drug-reactions?${buildQuery(normalized)}`);
    aggregateSearchState = {
      mode: SEARCH_MODES.AGGREGATES,
      params: normalized,
      offset: 0,
      executedAt: new Date().toISOString(),
    };
    activeWorkflowState = aggregateSearchState;
    renderAggregateResults(payload, normalized);
    renderCohortSummary();
    writeUrlState(normalized, SEARCH_MODES.AGGREGATES, 0);
    setStatus("Aggregate search complete.");
  } catch (error) {
    aggregateResultsEl.innerHTML = '<p class="empty-state">Aggregate search failed.</p>';
    setStatus(`Aggregate search failed: ${error.message}`, true);
  }
}

function buildReportPayload(mode, payload, searchState) {
  const params = searchState?.params || {};
  return {
    search_type: mode,
    exported_at: new Date().toISOString(),
    shareable_url: buildSearchUrl(params, mode, searchState?.offset || 0),
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
    ["source_report_id", "source_quarter", "report_type", "reporter_country", "sex_std", "age_value", "age_unit", "drugs", "reactions", "outcomes"],
    ...latestCasePayload.items.map((item) => [
      item.source_report_id,
      item.source_quarter,
      item.report_type,
      item.reporter_country,
      item.sex_std,
      item.age_value,
      item.age_unit,
      arrayText(item.drugs),
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

  downloadJson(`faers-case-report-${timestampSlug()}.json`, buildReportPayload(SEARCH_MODES.CASES, latestCasePayload, caseSearchState));
  setStatus("Exported case report JSON with filters and totals.");
}

function exportAggregateCsv() {
  if (!latestAggregatePayload || latestAggregatePayload.items.length === 0) {
    setStatus("Run an aggregate search before exporting.", true);
    return;
  }

  downloadCsv("faers-drug-reaction-aggregates.csv", [
    ["drugname", "reaction_pt", "case_count"],
    ...latestAggregatePayload.items.map((item) => [
      item.drugname,
      item.reaction_pt,
      item.case_count,
    ]),
  ]);
  setStatus("Exported current aggregate results to CSV.");
}

function exportAggregateReport() {
  if (!latestAggregatePayload || !aggregateSearchState) {
    setStatus("Run an aggregate search before exporting a report.", true);
    return;
  }

  downloadJson(`faers-aggregate-report-${timestampSlug()}.json`, buildReportPayload(SEARCH_MODES.AGGREGATES, latestAggregatePayload, aggregateSearchState));
  setStatus("Exported aggregate report JSON with filters and totals.");
}

function clearFilters() {
  resetFormToDefaults();
  savedSearchNameEl.value = "";
  caseSummaryEl.textContent = "No case search has been run yet.";
  aggregateSummaryEl.textContent = "No aggregate search has been run yet.";
  caseFilterSummaryEl.innerHTML = "";
  aggregateFilterSummaryEl.innerHTML = "";
  caseResultsEl.innerHTML = "";
  aggregateResultsEl.innerHTML = "";
  casePagerEl.innerHTML = "";
  caseDetailEl.innerHTML = '<p class="empty-state">Select a case from the table to inspect its linked drugs, reactions, outcomes, and metadata.</p>';
  latestCasePayload = null;
  latestAggregatePayload = null;
  caseSearchState = null;
  aggregateSearchState = null;
  activeWorkflowState = null;
  renderCohortSummary();
  writeUrlState({}, SEARCH_MODES.CASES, 0);
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
  if (urlState.mode === SEARCH_MODES.AGGREGATES) {
    await runAggregateSearch(urlState.params);
  } else {
    await runCaseSearchWithOffset(urlState.offset, urlState.params);
  }
}

searchForm.addEventListener("submit", runCaseSearch);
aggregateButton.addEventListener("click", () => runAggregateSearch());
clearFiltersButton.addEventListener("click", clearFilters);
saveSearchButton.addEventListener("click", saveCurrentSearch);
exportCasesButton.addEventListener("click", exportCasesCsv);
exportCaseReportButton.addEventListener("click", exportCaseReport);
exportAggregatesButton.addEventListener("click", exportAggregateCsv);
exportAggregateReportButton.addEventListener("click", exportAggregateReport);

renderSavedSearches();
loadFilterMetadata().then(() => hydrateFromUrl());
