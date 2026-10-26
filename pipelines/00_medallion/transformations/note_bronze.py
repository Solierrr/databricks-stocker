"""Manual Excel ingestion for the Bronze -> Silver -> Gold notebook job.

Each workbook has one sheet per source entity. Upload both files to a Unity
Catalog volume and set the paths in the job notebook parameters. The Excel
reader is built into Databricks Runtime 17.1+.
"""

from databricks.sdk.runtime import spark


SHEETS = {
    "fornecedores": {
        "brand": ("id", "owner_company_id", "name", "normalized_name", "visibility", "is_archived", "is_legacy", "created_at", "updated_at"),
        "model": ("id", "brand_id", "model_name", "status", "product_type", "infrastructure_type", "type", "material", "power_wp", "efficiency", "width", "length", "weight"),
        "supplier": ("id", "company_id", "status", "business_type"),
        "offer": ("id", "supplier_id", "model_id", "unit_price", "availability", "expires_at", "slug", "discount_percentage", "commercial_status"),
        "inventory": ("id", "supplier_id", "model_id", "quantity"),
        "proposal": ("id", "requester_id", "supplier_id", "status", "total_amount", "created_at", "updated_at"),
        "proposal_item": ("id", "proposal_id", "offer_id", "quantity", "negotiated_unit_price", "discount"),
    },
    "admins": {
        "company": ("id", "type", "status", "trade_name", "corporate_name", "slug"),
        "person": ("id", "user_id"),
        "requester": ("id", "company_id", "business_type"),
        "technician": ("id", "person_id", "slug"),
        "technician_affiliation": ("id", "technician_id", "company_id", "affiliation_type", "is_active"),
        "user_company": ("id", "company_id", "user_id", "job_position_id"),
        "proposal_unit": ("id", "proposal_item_id", "local_unit_id", "quantity"),
        "local_unit": ("id", "requester_id", "address_id", "location_type"),
        "professional_review": ("id", "technician_id", "technical_service_id", "rating", "is_active", "created_at"),
        "project_activity": ("id", "technical_project_id", "technical_service_id", "type", "status", "starts_at", "ends_at", "is_onsite"),
        "service_contract": ("id", "technical_service_id", "delivery_deadline_date", "has_insurance", "has_utility_approval"),
        "service_executor": ("id", "technical_service_id", "technician_affiliation_id", "function"),
        "technical_project": ("id", "requester_id", "local_unit_id", "owner_company_id", "owner_user_id", "name", "status", "is_archived", "created_at", "starts_at", "ends_at", "location_state", "location_city"),
        "technical_service": ("id", "technical_project_id", "status", "scheduled_at", "created_at", "accepted_at", "ends_at"),
        "charge": ("id", "subscription_id", "amount", "payment_method", "status", "due_date", "paid_at"),
        "company_plan": ("id", "name", "price", "cycle"),
        "subscription": ("id", "supplier_id", "company_plan_id", "status", "has_auto_renewal", "starts_at", "ends_at"),
        "energy_bill": ("id", "local_unit_id", "consumption", "amount"),
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
