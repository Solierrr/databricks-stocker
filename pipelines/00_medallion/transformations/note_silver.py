"""Silver DataFrame builders migrated from archived SQL transformations."""

from pyspark.sql import functions as F
from databricks.sdk.runtime import spark


def _column(source: str, output: str, rule: str = "raw"):
    value = F.col(f"`{source}`")
    if rule == "upper":
        value = F.upper(F.trim(value))
    elif rule == "clean":
        value = F.nullif(F.trim(value), F.lit(""))
    return value.alias(output)


SPECS = {
    "companies": ("company", [("id", "company_id", "raw"), ("type", "company_type", "upper"), ("status", "company_status", "upper"), ("trade_name", "trade_name", "clean"), ("corporate_name", "corporate_name", "clean"), ("slug", "slug", "clean")]),
    "suppliers": ("supplier", [("id", "supplier_id", "raw"), ("fk_company", "company_id", "raw"), ("status", "supplier_status", "upper"), ("business_type", "business_type", "clean")]),
    "requesters": ("requester", [("id", "requester_id", "raw"), ("fk_company", "company_id", "raw"), ("business_type", "business_type", "clean")]),
    "technician_affiliations": ("technician_affiliation", [("id", "affiliation_id", "raw"), ("fk_technician", "technician_id", "raw"), ("fk_company", "company_id", "raw"), ("affiliation_type", "affiliation_type", "upper"), ("active", "active", "raw")]),
    "company_memberships": ("user_company", [("id", "membership_id", "raw"), ("fk_company", "company_id", "raw"), ("fk_users", "user_id", "raw"), ("fk_position", "position_id", "raw")]),
    "brands": ("brand", [("id", "brand_id", "raw"), ("fk_owner_company", "owner_company_id", "raw"), ("name", "brand_name", "clean"), ("normalized_name", "normalized_name", "clean"), ("visibility", "visibility", "upper"), ("archived", "archived", "raw"), ("legacy", "legacy", "raw"), ("created_at", "created_at", "raw"), ("updated_at", "updated_at", "raw")]),
    "product_models": ("model", [("id", "model_id", "raw"), ("fk_brand", "brand_id", "raw"), ("model", "model_name", "clean"), ("status", "model_status", "upper"), ("product_type", "product_type", "upper"), ("infrastructure_type", "infrastructure_type", "upper"), ("type", "panel_type", "upper"), ("material", "material", "clean"), ("power_wp", "power_wp", "raw"), ("efficiency", "efficiency", "raw"), ("width", "width", "raw"), ("length", "length", "raw"), ("weight", "weight", "raw")]),
    "offers": ("offer", [("id", "offer_id", "raw"), ("fk_supplier", "supplier_id", "raw"), ("fk_model", "model_id", "raw"), ("unit_price", "unit_price", "raw"), ("availability", "availability", "raw"), ("expiration_date", "expiration_date", "raw"), ("slug", "slug", "clean"), ("discount_percentage", "discount_percentage", "raw"), ("commercial_status", "commercial_status", "upper")]),
    "inventory_items": ("inventory", [("id", "inventory_id", "raw"), ("fk_supplier", "supplier_id", "raw"), ("fk_model", "model_id", "raw"), ("quantity", "quantity", "raw")]),
    "proposals": ("proposal", [("id", "proposal_id", "raw"), ("fk_requester", "requester_id", "raw"), ("fk_supplier", "supplier_id", "raw"), ("status", "proposal_status", "upper"), ("total_amount", "total_amount", "raw"), ("created_at", "created_at", "raw"), ("updated_at", "updated_at", "raw")]),
    "proposal_items": ("proposal_item", [("id", "proposal_item_id", "raw"), ("fk_proposal", "proposal_id", "raw"), ("fk_offer", "offer_id", "raw"), ("quantity", "quantity", "raw"), ("negotiated_price", "negotiated_price", "raw"), ("discount", "discount", "raw")]),
    "proposal_units": ("proposal_unit", [("id", "proposal_unit_id", "raw"), ("fk_proposal_item", "proposal_item_id", "raw"), ("fk_local_unit", "local_unit_id", "raw"), ("quantity", "quantity", "raw")]),
    "local_units": ("local_unit", [("id", "local_unit_id", "raw"), ("fk_requester", "requester_id", "raw"), ("fk_address", "address_id", "raw"), ("location_type", "location_type", "upper")]),
    "technical_projects": ("technical_project", [("id", "project_id", "raw"), ("fk_requester", "requester_id", "raw"), ("fk_local_unit", "local_unit_id", "raw"), ("owner_company_id", "owner_company_id", "raw"), ("owner_user_id", "owner_user_id", "raw"), ("name", "project_name", "clean"), ("status", "project_status", "upper"), ("archived", "archived", "raw"), ("created_at", "created_at", "raw"), ("start_date", "start_date", "raw"), ("end_date", "end_date", "raw"), ("location_state", "location_state", "upper"), ("location_city", "location_city", "clean")]),
    "project_activities": ("project_activity", [("id", "activity_id", "raw"), ("project_id", "project_id", "raw"), ("technical_service_id", "service_id", "raw"), ("type", "activity_type", "upper"), ("status", "activity_status", "upper"), ("starts_at", "starts_at", "raw"), ("ends_at", "ends_at", "raw"), ("onsite", "onsite", "raw")]),
    "technical_services": ("technical_service", [("id", "service_id", "raw"), ("fk_technical_project", "project_id", "raw"), ("status", "service_status", "upper"), ("scheduled_date", "scheduled_date", "raw"), ("created_at", "created_at", "raw"), ("accepted_at", "accepted_at", "raw"), ("end_date", "end_date", "raw")]),
    "service_contracts": ("service_contract", [("id", "contract_id", "raw"), ("fk_service", "service_id", "raw"), ("delivery_deadline", "delivery_deadline", "raw"), ("insurance", "insurance", "raw"), ("utility_approval", "utility_approval", "raw")]),
    "service_executors": ("service_executor", [("id", "executor_id", "raw"), ("fk_service", "service_id", "raw"), ("fk_technician_affiliation", "affiliation_id", "raw"), ("function", "executor_function", "clean")]),
    "professional_reviews": ("professional_review", [("id", "review_id", "raw"), ("fk_professional", "technician_id", "raw"), ("fk_service", "service_id", "raw"), ("rating", "rating", "raw"), ("active", "active", "raw"), ("created_at", "created_at", "raw")]),
    "company_plans": ("company_plans", [("id", "plan_id", "raw"), ("name", "plan_name", "clean"), ("value", "plan_value", "raw"), ("cycle", "billing_cycle", "upper")]),
    "subscriptions": ("subscription", [("id", "subscription_id", "raw"), ("fk_supplier", "supplier_id", "raw"), ("fk_plan", "plan_id", "raw"), ("status", "subscription_status", "upper"), ("auto_renewal", "auto_renewal", "raw"), ("start_date", "start_date", "raw"), ("end_date", "end_date", "raw")]),
    "subscription_charges": ("charge", [("id", "charge_id", "raw"), ("fk_subscription", "subscription_id", "raw"), ("amount", "amount", "raw"), ("payment_method", "payment_method", "upper"), ("status", "charge_status", "upper"), ("due_date", "due_date", "raw"), ("payment_date", "payment_date", "raw")]),
    "energy_bills": ("energy_bill", [("id", "energy_bill_id", "raw"), ("fk_local_unit", "local_unit_id", "raw"), ("consumption", "consumption", "raw"), ("price", "price", "raw")]),
}


def technicians():
    tech = spark.read.table("workspace.bronze_core.technician").alias("t")
    people = spark.read.table("workspace.bronze_core.person").alias("p")
    return tech.join(people, F.col("t.fk_person") == F.col("p.id"), "left").select(
        F.col("t.id").alias("technician_id"),
        F.col("t.fk_person").alias("person_id"),
        F.col("p.fk_users").alias("user_id"),
        F.nullif(F.trim(F.col("t.slug")), F.lit("")).alias("technician_slug"),
    )


def silver_frame(name: str):
    if name == "technicians":
        return technicians()
    source, columns = SPECS[name]
    return spark.read.table(f"workspace.bronze_core.{source}").select(
        *(_column(*column) for column in columns)
    )
