"""
Speckle Automate Function
Extracts BrepX properties from 'plugins' and 'Core HBx3' collections
and exports them to Excel + Google Sheets.
"""

import json
from enum import Enum
from typing import Any, List

import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
import gspread
from google.oauth2.service_account import Credentials
from pydantic import Field, SecretStr
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
    google_service_account_json: SecretStr = Field(
        title="Google Service Account JSON (Secret)",
        description="Full JSON content of your GCP service account key.",
    )


# ─── Helpers ──────────────────────────────────────────────────────────────────

def get_collection_elements(root: Base, collection_name: str) -> List[Base]:
    elements = []
    for attr in root.get_dynamic_member_names():
        child = getattr(root, attr, None)
        if not isinstance(child, Base):
            continue
        display = getattr(child, "name", None) or getattr(child, "collectionType", None) or attr
        if collection_name.lower() in str(display).lower():
            for item in flatten_base(child):
                elements.append(item)
            return elements
        for sub_attr in child.get_dynamic_member_names():
            sub = getattr(child, sub_attr, None)
            if not isinstance(sub, Base):
                continue
            sub_display = getattr(sub, "name", None) or getattr(sub, "collectionType", None) or sub_attr
            if collection_name.lower() in str(sub_display).lower():
                for item in flatten_base(sub):
                    elements.append(item)
                return elements
    return elements


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
            if fragment.lower() in k.lower():
                return v
    return None


def breps_only(elements: List[Base]) -> List[Base]:
    return [e for e in elements if "Brep" in getattr(e, "speckle_type", "")]


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
            get_prop(brep, "Program", "density", "prg"),
            get_prop(brep, "Wind"),
            get_prop(brep, "incident", "radiation", "rad"),
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
    version_root_object = automate_context.receive_version()

    plugin_breps = breps_only(get_collection_elements(version_root_object, "plugins"))
    core_breps   = breps_only(get_collection_elements(version_root_object, "Core"))

    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    build_plugins_sheet(wb.create_sheet("Plugins - Volumes"),     plugin_breps)
    build_core_sheet(   wb.create_sheet("Core HBx3 - Structure"), core_breps)

    xlsx_path     = "/tmp/speckle_export.xlsx"
    sheets_status = "Google Sheets skipped."

    if function_inputs.output_format in (OutputFormat.EXCEL_ONLY, OutputFormat.BOTH):
        wb.save(xlsx_path)
        automate_context.store_file_result(xlsx_path)

    if function_inputs.output_format in (OutputFormat.SHEETS_ONLY, OutputFormat.BOTH):
        try:
            sync_to_google_sheets(
                sheet_id=function_inputs.google_sheet_id,
                service_account_json=function_inputs.google_service_account_json.get_secret_value(),
                wb=wb,
            )
            sheets_status = "Google Sheets synced."
        except Exception as e:
            sheets_status = f"Google Sheets sync failed: {e}"

    automate_context.mark_run_success(
        f"Plugins: {len(plugin_breps)} breps | Core: {len(core_breps)} breps | {sheets_status}"
    )


if __name__ == "__main__":
    execute_automate_function(automate_function, FunctionInputs)


























# """This module contains the function's business logic.

# Use the automation_context module to wrap your function in an Automate context helper.
# """

# from pydantic import Field, SecretStr
# from speckle_automate import (
#     AutomateBase,
#     AutomationContext,
#     execute_automate_function,
# )

# from flatten import flatten_base


# class FunctionInputs(AutomateBase):
#     """These are function author-defined values.

#     Automate will make sure to supply them matching the types specified here.
#     Please use the pydantic model schema to define your inputs:
#     https://docs.pydantic.dev/latest/usage/models/
#     """

#     # An example of how to use secret values.
#     whisper_message: SecretStr = Field(title="This is a secret message")
#     forbidden_speckle_type: str = Field(
#         title="Forbidden speckle type",
#         description=(
#             "If a object has the following speckle_type,"
#             " it will be marked with an error."
#         ),
#     )


# def automate_function(
#     automate_context: AutomationContext,
#     function_inputs: FunctionInputs,
# ) -> None:
#     """This is an example Speckle Automate function.

#     Args:
#         automate_context: A context-helper object that carries relevant information
#             about the runtime context of this function.
#             It gives access to the Speckle project data that triggered this run.
#             It also has convenient methods for attaching results to the Speckle model.
#         function_inputs: An instance object matching the defined schema.
#     """
#     # The context provides a convenient way to receive the triggering version.
#     version_root_object = automate_context.receive_version()

#     objects_with_forbidden_speckle_type = [
#         b
#         for b in flatten_base(version_root_object)
#         if b.speckle_type == function_inputs.forbidden_speckle_type
#     ]
#     count = len(objects_with_forbidden_speckle_type)

#     if count > 0:
#         # This is how a run is marked with a failure cause.
#         automate_context.attach_error_to_objects(
#             category="Forbidden speckle_type"
#             f" ({function_inputs.forbidden_speckle_type})",
#             affected_objects=objects_with_forbidden_speckle_type,
#             message="This project should not contain the type: "
#             f"{function_inputs.forbidden_speckle_type}",
#         )
#         automate_context.mark_run_failed(
#             "Automation failed: "
#             f"Found {count} object that have one of the forbidden speckle types: "
#             f"{function_inputs.forbidden_speckle_type}"
#         )

#         # Set the automation context view to the original model/version view
#         # to show the offending objects.
#         automate_context.set_context_view()

#     else:
#         automate_context.mark_run_success("No forbidden types found.")

#     # If the function generates file results, this is how it can be
#     # attached to the Speckle project/model
#     # automate_context.store_file_result("./report.pdf")


# def automate_function_without_inputs(automate_context: AutomationContext) -> None:
#     """A function example without inputs.

#     If your function does not need any input variables,
#      besides what the automation context provides,
#      the inputs argument can be omitted.
#     """
#     pass


# # make sure to call the function with the executor
# if __name__ == "__main__":
#     # NOTE: always pass in the automate function by its reference; do not invoke it!

#     # Pass in the function reference with the inputs schema to the executor.
#     execute_automate_function(automate_function, FunctionInputs)

#     # If the function has no arguments, the executor can handle it like so
#     # execute_automate_function(automate_function_without_inputs)

