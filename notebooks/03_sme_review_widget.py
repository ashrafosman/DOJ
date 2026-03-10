# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # DOJ Data Migration — SME Review Widget (Notebook 03)
# MAGIC
# MAGIC **Purpose**: Interactive `ipywidgets` UI for SME review of LLM-generated
# MAGIC schema mappings. Reviewers can Approve, Reject, or Edit each mapping and
# MAGIC add notes. Decisions are written back to `doj_catalog.bronze.schema_mappings`.
# MAGIC
# MAGIC **Usage**: Open this notebook in a Databricks cluster with single-user mode
# MAGIC (or shared cluster with appropriate permissions). Run all cells and interact
# MAGIC with the widgets in the output area.
# MAGIC
# MAGIC **Outputs**:
# MAGIC - Updated `review_status`, `reviewer_name`, `final_maps_to`, `reviewer_note`,
# MAGIC   `review_timestamp` columns in `doj_catalog.bronze.schema_mappings`
# MAGIC - Approved mapping export:
# MAGIC   `abfss://doj@dojstorage.dfs.core.usgovcloudapi.net/mappings/approved_v{ts}.json`

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0. Imports and Configuration

# COMMAND ----------
import json
import logging
from datetime import datetime, timezone
from typing import Optional

import ipywidgets as widgets
from IPython.display import clear_output, display

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("doj.sme_review")

CATALOG        = "doj_catalog"
BRONZE_SCHEMA  = "bronze"
MAPPING_TABLE  = f"{CATALOG}.{BRONZE_SCHEMA}.schema_mappings"
PROFILE_TABLE  = f"{CATALOG}.{BRONZE_SCHEMA}.column_profiles"
ADLS_ROOT      = "abfss://doj@dojstorage.dfs.core.usgovcloudapi.net/"

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Load Pending Mappings

# COMMAND ----------

def load_pending_mappings(mapping_version: Optional[str] = None) -> list[dict]:
    """
    Load schema mappings with review_status IS NULL or 'PENDING'.
    Optionally filter to a specific mapping_version.
    """
    version_filter = f"AND mapping_version = '{mapping_version}'" if mapping_version else ""

    df = spark.sql(f"""
        SELECT *
        FROM {MAPPING_TABLE}
        WHERE (review_status IS NULL OR review_status = 'PENDING')
        {version_filter}
        ORDER BY confidence ASC, system, source_table, source_column
    """)

    rows = [r.asDict() for r in df.collect()]
    logger.info("Loaded %d pending mappings for review", len(rows))
    return rows


def load_column_profile(system: str, table_name: str, column_name: str) -> Optional[dict]:
    """Look up profiling statistics for a specific column."""
    rows = spark.sql(f"""
        SELECT *
        FROM {PROFILE_TABLE}
        WHERE system = '{system}'
          AND table_name = '{table_name}'
          AND column_name = '{column_name}'
        ORDER BY profile_timestamp DESC
        LIMIT 1
    """).collect()
    return rows[0].asDict() if rows else None


def get_available_versions() -> list[str]:
    """Return all distinct mapping_version values from the mapping table."""
    rows = spark.sql(f"""
        SELECT DISTINCT mapping_version
        FROM {MAPPING_TABLE}
        ORDER BY mapping_version DESC
    """).collect()
    return [r["mapping_version"] for r in rows]

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Decision Persistence

# COMMAND ----------

def save_decision(
    system: str,
    source_table: str,
    source_column: str,
    mapping_version: str,
    decision: str,           # APPROVED | REJECTED | EDITED
    reviewer_name: str,
    reviewer_note: str,
    final_maps_to: str,
) -> None:
    """
    Persist a reviewer's decision by updating the schema_mappings table.
    Uses a parameterised MERGE to avoid SQL injection from widget text inputs.
    """
    review_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Escape single quotes in free-text fields
    def esc(s: str) -> str:
        return s.replace("'", "\\'") if s else ""

    spark.sql(f"""
        UPDATE {MAPPING_TABLE}
        SET
            review_status     = '{decision}',
            reviewer_name     = '{esc(reviewer_name)}',
            review_timestamp  = '{review_ts}',
            reviewer_note     = '{esc(reviewer_note)}',
            final_maps_to     = '{esc(final_maps_to)}'
        WHERE system          = '{system}'
          AND source_table    = '{esc(source_table)}'
          AND source_column   = '{esc(source_column)}'
          AND mapping_version = '{mapping_version}'
    """)
    logger.info(
        "Saved decision %s for %s.%s → %s (reviewer=%s)",
        decision, source_table, source_column, final_maps_to, reviewer_name
    )


def export_approved_mappings() -> str:
    """
    Export all APPROVED mappings to an ADLS JSON file.
    Returns the ADLS path of the exported file.
    """
    df_approved = spark.sql(f"""
        SELECT *
        FROM {MAPPING_TABLE}
        WHERE review_status = 'APPROVED' OR review_status = 'EDITED'
        ORDER BY system, source_table, source_column
    """)

    rows = [r.asDict() for r in df_approved.collect()]
    export_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    adls_path = f"{ADLS_ROOT}mappings/approved_v{export_ts}.json"

    content = json.dumps(rows, indent=2, default=str)
    dbutils.fs.put(adls_path, content, overwrite=True)
    logger.info("Exported %d approved mappings to %s", len(rows), adls_path)
    return adls_path

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Widget Layout Helper Functions

# COMMAND ----------

def confidence_badge_html(confidence: float) -> str:
    """Return a coloured HTML badge for the confidence score."""
    score = round(confidence * 100)
    if confidence >= 0.85:
        colour = "#28a745"   # green
        label  = "HIGH"
    elif confidence >= 0.75:
        colour = "#ffc107"   # amber
        label  = "MEDIUM"
    else:
        colour = "#dc3545"   # red
        label  = "LOW"

    return (
        f'<span style="background:{colour};color:white;padding:2px 8px;'
        f'border-radius:4px;font-weight:bold;font-size:12px;">'
        f'{label} {score}%</span>'
    )


def profile_summary_html(profile: Optional[dict]) -> str:
    """Render column profiling stats as a compact HTML snippet."""
    if not profile:
        return "<em>No profiling data available</em>"

    top_vals_str = ""
    try:
        top_vals = json.loads(profile.get("top_values") or "[]")
        top_vals_str = ", ".join(
            f"<code>{v['value']}</code>({v['count']})"
            for v in top_vals[:5]
        )
    except (json.JSONDecodeError, TypeError):
        top_vals_str = profile.get("top_values", "")[:100]

    return f"""
    <table style="border-collapse:collapse;font-size:12px;margin-top:4px;">
      <tr><td style="padding:2px 8px;"><b>Data type</b></td>
          <td style="padding:2px 8px;">{profile.get('dtype','')}</td></tr>
      <tr><td style="padding:2px 8px;"><b>Null rate</b></td>
          <td style="padding:2px 8px;">{profile.get('null_rate', 'N/A')}</td></tr>
      <tr><td style="padding:2px 8px;"><b>Cardinality</b></td>
          <td style="padding:2px 8px;">{profile.get('cardinality', 'N/A'):,}</td></tr>
      <tr><td style="padding:2px 8px;"><b>Uniqueness</b></td>
          <td style="padding:2px 8px;">{profile.get('uniqueness_ratio', 'N/A')}</td></tr>
      <tr><td style="padding:2px 8px;"><b>Pattern</b></td>
          <td style="padding:2px 8px;"><code>{profile.get('detected_pattern','')}</code></td></tr>
      <tr><td style="padding:2px 8px;"><b>Top values</b></td>
          <td style="padding:2px 8px;">{top_vals_str}</td></tr>
    </table>
    """

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Main Review UI

# COMMAND ----------

class SMEReviewUI:
    """
    Drives the interactive mapping review session.

    Each call to `show_next()` renders the next pending mapping with:
    - Source column stats (from profiling table)
    - LLM-proposed target and confidence badge
    - Approve / Reject / Edit buttons
    - Notes text area
    """

    def __init__(self, pending_mappings: list[dict], reviewer_name: str):
        self.mappings       = pending_mappings
        self.reviewer_name  = reviewer_name
        self.current_index  = 0
        self.session_stats  = {"approved": 0, "rejected": 0, "edited": 0}

        # --- Persistent widgets (re-used across all mapping reviews) ---

        # Progress bar
        self.progress_bar = widgets.IntProgress(
            value=0, min=0, max=max(len(pending_mappings), 1),
            description="Progress:",
            bar_style="info",
            layout=widgets.Layout(width="100%"),
        )

        # Header HTML
        self.header_html = widgets.HTML(value="<h3>Loading...</h3>")

        # Profile stats
        self.profile_html = widgets.HTML(value="")

        # Proposed mapping and confidence
        self.proposed_html = widgets.HTML(value="")

        # Rationale text
        self.rationale_html = widgets.HTML(value="")

        # Edit mapping input (shown only when "Edit" is clicked)
        self.edit_input = widgets.Text(
            placeholder="Enter alternative target (e.g. Stg_Contact.FirstName)",
            description="Override:",
            layout=widgets.Layout(width="60%", display="none"),
        )

        # Notes field
        self.notes_input = widgets.Textarea(
            placeholder="Optional reviewer notes...",
            description="Notes:",
            layout=widgets.Layout(width="70%", height="60px"),
        )

        # Action buttons
        self.btn_approve = widgets.Button(
            description="Approve",
            button_style="success",
            icon="check",
            layout=widgets.Layout(width="130px"),
        )
        self.btn_reject = widgets.Button(
            description="Reject",
            button_style="danger",
            icon="times",
            layout=widgets.Layout(width="130px"),
        )
        self.btn_edit = widgets.Button(
            description="Edit Mapping",
            button_style="warning",
            icon="edit",
            layout=widgets.Layout(width="130px"),
        )
        self.btn_skip = widgets.Button(
            description="Skip",
            button_style="",
            icon="forward",
            layout=widgets.Layout(width="100px"),
        )
        self.btn_export = widgets.Button(
            description="Export Approved",
            button_style="info",
            icon="download",
            layout=widgets.Layout(width="160px"),
        )

        # Status / feedback message area
        self.status_out = widgets.Output()

        # Wire button callbacks
        self.btn_approve.on_click(self._on_approve)
        self.btn_reject.on_click(self._on_reject)
        self.btn_edit.on_click(self._on_edit_toggle)
        self.btn_skip.on_click(self._on_skip)
        self.btn_export.on_click(self._on_export)

        # Top-level layout
        self.layout = widgets.VBox([
            self.progress_bar,
            self.header_html,
            self.profile_html,
            self.proposed_html,
            self.rationale_html,
            self.edit_input,
            self.notes_input,
            widgets.HBox([
                self.btn_approve,
                self.btn_reject,
                self.btn_edit,
                self.btn_skip,
                widgets.Label(" " * 10),
                self.btn_export,
            ]),
            self.status_out,
        ])

    # ------------------------------------------------------------------
    # Rendering
    # ------------------------------------------------------------------

    def _current_mapping(self) -> Optional[dict]:
        if self.current_index < len(self.mappings):
            return self.mappings[self.current_index]
        return None

    def _render_current(self) -> None:
        """Update all widget values to reflect the current mapping."""
        mapping = self._current_mapping()
        if not mapping:
            self.header_html.value = (
                "<h3 style='color:green'>All mappings reviewed!</h3>"
                "<p>Click <b>Export Approved</b> to save the approved mapping artifact.</p>"
            )
            for btn in (self.btn_approve, self.btn_reject, self.btn_edit, self.btn_skip):
                btn.disabled = True
            return

        self.progress_bar.value = self.current_index

        # Header
        total = len(self.mappings)
        self.header_html.value = (
            f"<h3>Mapping {self.current_index + 1} of {total}</h3>"
            f"<p><b>System:</b> {mapping['system']} &nbsp;|&nbsp; "
            f"<b>Table:</b> <code>{mapping['source_table'].split('.')[-1]}</code> &nbsp;|&nbsp; "
            f"<b>Column:</b> <code>{mapping['source_column']}</code> &nbsp;|&nbsp; "
            f"<b>Version:</b> {mapping['mapping_version']}</p>"
        )

        # Profile stats
        profile = load_column_profile(
            mapping["system"],
            mapping["source_table"],
            mapping["source_column"],
        )
        self.profile_html.value = (
            "<b>Column Profile:</b>" + profile_summary_html(profile)
        )

        # Proposed mapping and confidence
        confidence = mapping.get("confidence") or 0.0
        badge = confidence_badge_html(confidence)
        self.proposed_html.value = (
            f"<p><b>LLM Proposed Target:</b> "
            f"<code style='font-size:14px'>{mapping.get('maps_to','')}</code> "
            f"&nbsp;{badge}</p>"
        )

        # Rationale
        self.rationale_html.value = (
            f"<p><b>Rationale:</b> <em>{mapping.get('rationale','')}</em></p>"
        )

        # Reset controls
        self.edit_input.value = mapping.get("maps_to", "")
        self.edit_input.layout.display = "none"
        self.notes_input.value = ""
        for btn in (self.btn_approve, self.btn_reject, self.btn_edit, self.btn_skip):
            btn.disabled = False

        with self.status_out:
            clear_output()
            stats = self.session_stats
            print(
                f"Session: Approved={stats['approved']}  "
                f"Rejected={stats['rejected']}  "
                f"Edited={stats['edited']}  "
                f"Remaining={len(self.mappings) - self.current_index}"
            )

    # ------------------------------------------------------------------
    # Button callbacks
    # ------------------------------------------------------------------

    def _save_and_advance(self, decision: str, final_target: str) -> None:
        mapping = self._current_mapping()
        if not mapping:
            return
        try:
            save_decision(
                system=mapping["system"],
                source_table=mapping["source_table"],
                source_column=mapping["source_column"],
                mapping_version=mapping["mapping_version"],
                decision=decision,
                reviewer_name=self.reviewer_name,
                reviewer_note=self.notes_input.value.strip(),
                final_maps_to=final_target,
            )
            self.session_stats[decision.lower()] += 1
        except Exception as exc:
            with self.status_out:
                print(f"ERROR saving decision: {exc}")
            return

        self.current_index += 1
        self._render_current()

    def _on_approve(self, _btn) -> None:
        mapping = self._current_mapping()
        if mapping:
            self._save_and_advance("APPROVED", mapping.get("maps_to", ""))

    def _on_reject(self, _btn) -> None:
        self._save_and_advance("REJECTED", "NO_MATCH")

    def _on_edit_toggle(self, _btn) -> None:
        """
        First click on Edit: show the text input for the reviewer to type the
        correct target.  Second click (labelled "Confirm Edit"): save as EDITED.
        """
        if self.edit_input.layout.display == "none":
            self.edit_input.layout.display = ""
            self.btn_edit.description = "Confirm Edit"
            self.btn_edit.button_style = "success"
        else:
            override = self.edit_input.value.strip()
            if not override:
                with self.status_out:
                    print("Please enter a target column before confirming.")
                return
            self._save_and_advance("EDITED", override)
            self.btn_edit.description = "Edit Mapping"
            self.btn_edit.button_style = "warning"

    def _on_skip(self, _btn) -> None:
        self.current_index += 1
        self._render_current()

    def _on_export(self, _btn) -> None:
        with self.status_out:
            try:
                adls_path = export_approved_mappings()
                print(f"Exported approved mappings → {adls_path}")
            except Exception as exc:
                print(f"Export failed: {exc}")

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Render the UI and display it in the notebook output."""
        display(self.layout)
        self._render_current()


# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Session Configuration Widgets

# COMMAND ----------

available_versions = get_available_versions()

w_version = widgets.Dropdown(
    options=["(latest — all pending)"] + available_versions,
    description="Mapping version:",
    style={"description_width": "initial"},
    layout=widgets.Layout(width="50%"),
)

w_reviewer = widgets.Text(
    value="",
    description="Reviewer name:",
    placeholder="Enter your full name",
    style={"description_width": "initial"},
    layout=widgets.Layout(width="50%"),
)

w_start_btn = widgets.Button(
    description="Start Review Session",
    button_style="primary",
    layout=widgets.Layout(width="200px"),
)

session_output = widgets.Output()


def on_start_session(_btn):
    with session_output:
        clear_output()

        reviewer = w_reviewer.value.strip()
        if not reviewer:
            print("Please enter your reviewer name before starting.")
            return

        version = None if w_version.value == "(latest — all pending)" else w_version.value
        pending = load_pending_mappings(mapping_version=version)

        if not pending:
            print("No pending mappings found for review.")
            return

        print(f"Starting review session for {len(pending)} pending mappings...")
        print(f"Reviewer: {reviewer}")
        print("-" * 50)

        ui = SMEReviewUI(pending, reviewer_name=reviewer)
        ui.start()


w_start_btn.on_click(on_start_session)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Display Session Setup Panel

# COMMAND ----------

display(widgets.VBox([
    widgets.HTML("<h2>DOJ Schema Mapping Review</h2>"),
    widgets.HTML(
        "<p>Review LLM-proposed schema mappings before they are used in the "
        "Silver transformation pipeline. Approve, Reject, or Override each mapping.</p>"
    ),
    w_version,
    w_reviewer,
    w_start_btn,
    session_output,
]))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Summary Statistics

# COMMAND ----------

def show_review_summary() -> None:
    """Print a summary of all mapping review decisions."""
    spark.sql(f"""
        SELECT
            review_status,
            COUNT(*)                  AS count,
            ROUND(AVG(confidence), 3) AS avg_confidence,
            COUNT(DISTINCT reviewer_name) AS reviewers
        FROM {MAPPING_TABLE}
        GROUP BY review_status
        ORDER BY count DESC
    """).show(truncate=False)

    spark.sql(f"""
        SELECT
            system,
            COUNT(*) AS total,
            COUNT(CASE WHEN review_status = 'APPROVED' THEN 1 END) AS approved,
            COUNT(CASE WHEN review_status = 'REJECTED' THEN 1 END) AS rejected,
            COUNT(CASE WHEN review_status = 'EDITED'   THEN 1 END) AS edited,
            COUNT(CASE WHEN review_status = 'PENDING'
                        OR review_status IS NULL        THEN 1 END) AS pending
        FROM {MAPPING_TABLE}
        GROUP BY system
        ORDER BY system
    """).show(truncate=False)

# Run summary in its own output area (won't interfere with the widget UI)
show_review_summary()
