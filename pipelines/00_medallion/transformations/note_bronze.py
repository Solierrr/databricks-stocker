"""Manual Excel ingestion for the Bronze -> Silver -> Gold notebook job.

Each workbook has one sheet per source entity. Upload both files to a Unity
Catalog volume and set the paths in the job notebook parameters. The Excel
reader is built into Databricks Runtime 17.1+.
"""

from databricks.sdk.runtime import spark


SHEETS = {
    "fornecedores": {
        "brand": ("id", "fk_owner_company", "name", "normalized_name", "visibility", "archived", "legacy", "created_at", "updated_at"),
        "model": ("id", "fk_brand", "model", "status", "product_type", "infrastructure_type", "type", "material", "power_wp", "efficiency", "width", "length", "weight"),
        "supplier": ("id", "fk_company", "status", "business_type"),
        "offer": ("id", "fk_supplier", "fk_model", "unit_price", "availability", "expiration_date", "slug", "discount_percentage", "commercial_status"),
        "inventory": ("id", "fk_supplier", "fk_model", "quantity"),
        "proposal": ("id", "fk_requester", "fk_supplier", "status", "total_amount", "created_at", "updated_at"),
        "proposal_item": ("id", "fk_proposal", "fk_offer", "quantity", "negotiated_price", "discount"),
    },
    "admins": {
        "company": ("id", "type", "status", "trade_name", "corporate_name", "slug"),
        "person": ("id", "fk_users"),
        "requester": ("id", "fk_company", "business_type"),
        "technician": ("id", "fk_person", "slug"),
        "technician_affiliation": ("id", "fk_technician", "fk_company", "affiliation_type", "active"),
        "user_company": ("id", "fk_company", "fk_users", "fk_position"),
        "proposal_unit": ("id", "fk_proposal_item", "fk_local_unit", "quantity"),
        "local_unit": ("id", "fk_requester", "fk_address", "location_type"),
        "professional_review": ("id", "fk_professional", "fk_service", "rating", "active", "created_at"),
        "project_activity": ("id", "project_id", "technical_service_id", "type", "status", "starts_at", "ends_at", "onsite"),
        "service_contract": ("id", "fk_service", "delivery_deadline", "insurance", "utility_approval"),
        "service_executor": ("id", "fk_service", "fk_technician_affiliation", "function"),
        "technical_project": ("id", "fk_requester", "fk_local_unit", "owner_company_id", "owner_user_id", "name", "status", "archived", "created_at", "start_date", "end_date", "location_state", "location_city"),
        "technical_service": ("id", "fk_technical_project", "status", "scheduled_date", "created_at", "accepted_at", "end_date"),
        "charge": ("id", "fk_subscription", "amount", "payment_method", "status", "due_date", "payment_date"),
        "company_plans": ("id", "name", "value", "cycle"),
        "subscription": ("id", "fk_supplier", "fk_plan", "status", "auto_renewal", "start_date", "end_date"),
        "energy_bill": ("id", "fk_local_unit", "consumption", "price"),
    },
}


def read_bronze(
    workbook: str, sheet: str, required: tuple[str, ...], path: str | None = None
):
    # Keep the three-argument form for notebooks that configure the Spark key.
    # The job passes the workbook path explicitly to avoid relying on session state.
    if path is None:
        path = spark.conf.get(f"solaria.excel.{workbook}", "")
    if not path:
        raise ValueError(f"Set a path for {workbook}.xlsx")
    frame = (
        spark.read.format("excel")
        .option("headerRows", 1)
        .option("dataAddress", sheet)
        .option("ignoreMissingSheet", "false")
        .load(path)
    )
    missing = sorted(set(required) - set(frame.columns))
    if missing:
        raise ValueError(f"{workbook}.xlsx / {sheet}: missing columns {missing}")
    return frame
