"""
Speckle Automate Function
Extracts BrepX properties from 'plugins' and 'Core HBx3' collections
and exports them to Excel + Google Sheets.
"""

import json
import sys
import traceback
import threading
from enum import Enum
from typing import Any, List

try:
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment
    import gspread
    from google.oauth2.service_account import Credentials
except ImportError as e:
    print(f"IMPORT ERROR: {e}", flush=True)
    traceback.print_exc()
    sys.exit(1)

from pydantic import Field
from speckle_automate import AutomateBase, AutomationContext, execute_automate_function
from specklepy.objects.base import Base
from flatten import flatten_base


# ─── Inputs ───────────────────────────────────────────────────────────────────

class OutputFormat(str, Enum):
    EXCEL_ONLY  = "excel_only"
    SHEETS_ONLY = "sheets_only"
    BOTH        = "both"


class FunctionInputs(AutomateBase):
    output_format: OutputFormat = Field(
        default=OutputFormat.BOTH,
        title="Output Format",
        description="Choose where to export the extracted data.",
    )
    google_sheet_id: str = Field(
        title="Google Sheet ID",
        description="The ID from your Sheet URL: .../spreadsheets/d/<ID>/edit",
        min_length=10,
    )
    google_service_account_json: str = Field(
        title="Google Service Account JSON",
        description="Full JSON content of your GCP service account key.",
    )


# ─── Helpers ──────────────────────────────────────────────────────────────────

def get_prop(obj: Base, *key_fragments: str) -> Any:
    props = getattr(obj, "properties", None)
    if not props:
        return None
    prop_dict = (
        {k: getattr(props, k) for k in props.get_dynamic_member_names()}
        if isinstance(props, Base)
        else props if isinstance(props, dict)
        else None
    )
    if not prop_dict:
        return None
    for fragment in key_fragments:
        for k, v in prop_dict.items():
            if fragment.lower() in k.lower().strip():
                return v
    return None


def style_header_row(ws, row: int, fill_hex: str):
    fill = PatternFill("solid", fgColor=fill_hex)
    for cell in ws[row]:
        cell.fill = fill
        cell.font = Font(bold=True, color="FFFFFF")
        cell.alignment = Alignment(horizontal="center")


def autofit_columns(ws):
    for col in ws.columns:
        max_len = max((len(str(cell.value or "")) for cell in col), default=10)
        ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 60)


# ─── Sheet builders ───────────────────────────────────────────────────────────

def build_plugins_sheet(ws, breps: List[Base]):
    headers = [
        "Index", "Speckle ID", "Application ID",
        "Geometry Area (m²)", "Geometry Volume (m³)", "Units",
        "Volume (brep #)", "Normalized Score", "Program & Density",
        "Wind Pressure (kPa)", "Incident Radiation (kWh/m²)",
    ]
    ws.append(headers)
    style_header_row(ws, 1, "2F5496")

    for i, brep in enumerate(breps, start=1):
        ws.append([
            i,
            getattr(brep, "id", None),
            getattr(brep, "applicationId", None),
            round(getattr(brep, "area",   0) or 0, 4),
            round(getattr(brep, "volume", 0) or 0, 4),
            getattr(brep, "units", "m"),
            get_prop(brep, "Volume"),
            get_prop(brep, "Normalized"),
            get_prop(brep, "Program", "Height", "density"),
            get_prop(brep, "Wind"),
            get_prop(brep, "incident", "radiation"),
        ])

    ws.append([])
    ws.append(["TOTAL BREPS", len(breps)])
    ws[ws.max_row][0].font = Font(bold=True)
    autofit_columns(ws)


def build_core_sheet(ws, breps: List[Base]):
    headers = [
        "Index", "Speckle ID", "Application ID",
        "Area (m²)", "Units",
        "Stress Pts Coordinates", "Beam Thickness (m)",
    ]
    ws.append(headers)
    style_header_row(ws, 1, "375623")

    total_length = 0.0
    for i, brep in enumerate(breps, start=1):
        stress = get_prop(brep, "Stress", "stress pts", "stress_pts")
        beam   = get_prop(brep, "Beam", "thickness", "beam thick")
        length = getattr(brep, "length", None)
        if length:
            try:
                total_length += float(length)
            except (TypeError, ValueError):
                pass
        ws.append([
            i,
            getattr(brep, "id", None),
            getattr(brep, "applicationId", None),
            round(getattr(brep, "area", 0) or 0, 4),
            getattr(brep, "units", "m"),
            str(stress) if stress is not None else None,
            beam,
        ])

    ws.append([])
    ws.append(["TOTAL BREPS", len(breps)])
    ws[ws.max_row][0].font = Font(bold=True)
    if total_length > 0:
        ws.append(["TOTAL LENGTH (m)", round(total_length, 4)])
        ws[ws.max_row][0].font = Font(bold=True)
    autofit_columns(ws)


# ─── Google Sheets sync ───────────────────────────────────────────────────────

def sync_to_google_sheets(sheet_id: str, service_account_json: str, wb: openpyxl.Workbook):
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(json.loads(service_account_json), scopes=scopes)
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(sheet_id)

    for sheet_name in wb.sheetnames:
        rows = [[cell.value for cell in row] for row in wb[sheet_name].iter_rows()]
        try:
            gs_ws = spreadsheet.worksheet(sheet_name)
            gs_ws.clear()
        except gspread.WorksheetNotFound:
            gs_ws = spreadsheet.add_worksheet(title=sheet_name, rows=200, cols=30)
        if rows:
            gs_ws.update(rows, value_input_option="USER_ENTERED")


# ─── Main function ────────────────────────────────────────────────────────────

def automate_function(
    automate_context: AutomationContext,
    function_inputs: FunctionInputs,
) -> None:
    try:
        print("DEBUG: function started", flush=True)

        # Timeout wrapper around receive_version()
        result_holder = [None]
        error_holder  = [None]

        def do_receive():
            try:
                result_holder[0] = automate_context.receive_version()
            except Exception as e:
                error_holder[0] = e

        t = threading.Thread(target=do_receive)
        t.start()
        t.join(timeout=50)

        if t.is_alive():
            print("TIMEOUT: receive_version() hung for 50s", flush=True)
            automate_context.mark_run_failed("receive_version() timed out")
            return

        if error_holder[0]:
            print(f"ERROR in receive_version: {error_holder[0]}", flush=True)
            traceback.print_exc()
            automate_context.mark_run_failed(str(error_holder[0]))
            return

        version_root_object = result_holder[0]
        print("DEBUG: version received", flush=True)

        # Split all breps: plugins have 'Volume' property, core don't
        all_breps = [e for e in flatten_base(version_root_object) if "Brep" in getattr(e, "speckle_type", "")]
        print(f"DEBUG: total breps={len(all_breps)}", flush=True)

        plugin_breps = []
        core_breps = []
        for brep in all_breps:
            props = getattr(brep, "properties", None)
            if props:
                keys = list(props.get_dynamic_member_names()) if isinstance(props, Base) else list(props.keys()) if isinstance(props, dict) else []
                if any("volume" in k.lower() for k in keys):
                    plugin_breps.append(brep)
                else:
                    core_breps.append(brep)
            else:
                core_breps.append(brep)

        print(f"DEBUG: plugins={len(plugin_breps)} core={len(core_breps)}", flush=True)

        wb = openpyxl.Workbook()
        wb.remove(wb.active)
        build_plugins_sheet(wb.create_sheet("Plugins - Volumes"),     plugin_breps)
        build_core_sheet(   wb.create_sheet("Core HBx3 - Structure"), core_breps)

        xlsx_path     = "/tmp/speckle_export.xlsx"
        sheets_status = "Google Sheets skipped."

        if function_inputs.output_format in (OutputFormat.EXCEL_ONLY, OutputFormat.BOTH):
            wb.save(xlsx_path)
            automate_context.store_file_result(xlsx_path)
            print("DEBUG: excel saved", flush=True)

        if function_inputs.output_format in (OutputFormat.SHEETS_ONLY, OutputFormat.BOTH):
            try:
                sync_to_google_sheets(
                    sheet_id=function_inputs.google_sheet_id,
                    service_account_json=function_inputs.google_service_account_json,
                    wb=wb,
                )
                sheets_status = "Google Sheets synced."
            except Exception as e:
                sheets_status = f"Google Sheets sync failed: {e}"
            print(f"DEBUG: {sheets_status}", flush=True)

        automate_context.mark_run_success(
            f"Plugins: {len(plugin_breps)} breps | Core: {len(core_breps)} breps | {sheets_status}"
        )

    except Exception as e:
        print(f"EXCEPTION: {e}", flush=True)
        traceback.print_exc()
        automate_context.mark_run_failed(f"Function crashed: {e}")


if __name__ == "__main__":
    execute_automate_function(automate_function, FunctionInputs)
    