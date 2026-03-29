const statusEl = document.getElementById("status");
const caseSummaryEl = document.getElementById("case-summary");
const caseFilterSummaryEl = document.getElementById("case-filter-summary");
const caseResultsEl = document.getElementById("case-results");
const casePagerEl = document.getElementById("case-pager");
const aggregateSummaryEl = document.getElementById("aggregate-summary");
const aggregateFilterSummaryEl = document.getElementById("aggregate-filter-summary");
const aggregateResultsEl = document.getElementById("aggregate-results");
const caseDetailEl = document.getElementById("case-detail");
const searchForm = document.getElementById("search-form");
const aggregateButton = document.getElementById("search-aggregates");
const clearFiltersButton = document.getElementById("clear-filters");
const exportCasesButton = document.getElementById("export-cases");
const exportAggregatesButton = document.getElementById("export-aggregates");

let latestCasePayload = null;
let latestAggregatePayload = null;

function setStatus(message, isError = false) {
  statusEl.textContent = message;
  statusEl.classList.toggle("error", isError);
}

function buildQuery(params) {
  const query = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value !== null && value !== undefined && value !== "") {
      query.set(key, String(value));
    }
  });
  return query.toString();
}

function currentParams() {
  const formData = new FormData(searchForm);
  return {
    drug_name: (formData.get("drug_name") || "").toString().trim(),
    reaction_pt: (formData.get("reaction_pt") || "").toString().trim(),
    quarter: (formData.get("quarter") || "").toString().trim(),
    limit: Number(formData.get("limit") || 25),
  };
}

function filterChips(params) {
  const chips = [
    ["Drug", params.drug_name],
    ["Reaction", params.reaction_pt],
    ["Quarter", params.quarter],
    ["Limit", params.limit],
  ].filter(([, value]) => value !== "" && value !== null && value !== undefined);

  if (chips.length === 0) {
    return "";
  }

  return chips
    .map(([label, value]) => `<span class="chip"><strong>${escapeHtml(label)}:</strong> ${escapeHtml(value)}</span>`)
    .join("");
}

function arrayText(values) {
  if (!values || values.length === 0) {
    return "None";
  }
  return values.join(", ");
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    let detail = response.statusText;
    try {
      const payload = await response.json();
      detail = payload.detail || JSON.stringify(payload);
    } catch (error) {
      // ignore JSON parse errors
    }
    throw new Error(detail);
  }
  return response.json();
}

function downloadCsv(filename, rows) {
  const csvLines = rows.map((row) =>
    row
      .map((value) => `"${String(value ?? "").replaceAll('"', '""')}"`)
      .join(",")
  );
  const blob = new Blob([csvLines.join("\n")], { type: "text/csv;charset=utf-8" });
  const link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(link.href);
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
      <td>${escapeHtml(item.canonical_case_id)}</td>
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
          <th>Case</th>
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
        <div><strong>Sex / Age</strong>${escapeHtml(payload.sex_std || "n/a")} / ${escapeHtml(payload.age_value ?? "n/a")} ${escapeHtml(payload.age_unit || "")}</div>
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
  const effectiveParams = { ...params, offset };
  if (!params.drug_name || params.drug_name.length < 2) {
    setStatus("Enter a drug name with at least 2 characters.", true);
    return;
  }

  setStatus("Searching cases...");
  try {
    const payload = await fetchJson(`/cases/search?${buildQuery(effectiveParams)}`);
    renderCaseResults(payload, params);
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

async function runAggregateSearch() {
  const params = currentParams();
  if (!params.drug_name || params.drug_name.length < 2) {
    setStatus("Enter a drug name with at least 2 characters.", true);
    return;
  }

  setStatus("Loading aggregate counts...");
  try {
    const payload = await fetchJson(`/aggregates/drug-reactions?${buildQuery(params)}`);
    renderAggregateResults(payload, params);
    setStatus("Aggregate search complete.");
  } catch (error) {
    aggregateResultsEl.innerHTML = '<p class="empty-state">Aggregate search failed.</p>';
    setStatus(`Aggregate search failed: ${error.message}`, true);
  }
}

function exportCasesCsv() {
  if (!latestCasePayload || latestCasePayload.items.length === 0) {
    setStatus("Run a case search before exporting.", true);
    return;
  }

  downloadCsv("faers-case-results.csv", [
    ["source_report_id", "source_quarter", "canonical_case_id", "drugs", "reactions", "outcomes"],
    ...latestCasePayload.items.map((item) => [
      item.source_report_id,
      item.source_quarter,
      item.canonical_case_id,
      arrayText(item.drugs),
      arrayText(item.reactions),
      arrayText(item.outcomes),
    ]),
  ]);
  setStatus("Exported current case results to CSV.");
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

function clearFilters() {
  searchForm.reset();
  document.getElementById("limit").value = "25";
  caseSummaryEl.textContent = "No case search has been run yet.";
  aggregateSummaryEl.textContent = "No aggregate search has been run yet.";
  caseFilterSummaryEl.innerHTML = "";
  aggregateFilterSummaryEl.innerHTML = "";
  caseResultsEl.innerHTML = "";
  aggregateResultsEl.innerHTML = "";
  casePagerEl.innerHTML = "";
  caseDetailEl.innerHTML = '<p class="empty-state">Select a case from the table to inspect its linked drugs, reactions, and outcomes.</p>';
  latestCasePayload = null;
  latestAggregatePayload = null;
  setStatus("Filters cleared.");
}

searchForm.addEventListener("submit", runCaseSearch);
aggregateButton.addEventListener("click", runAggregateSearch);
clearFiltersButton.addEventListener("click", clearFilters);
exportCasesButton.addEventListener("click", exportCasesCsv);
exportAggregatesButton.addEventListener("click", exportAggregateCsv);
