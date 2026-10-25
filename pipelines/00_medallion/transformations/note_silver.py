"""Silver DataFrame builders migrated from archived SQL transformations."""

from pyspark.sql import functions as F
from databricks.sdk.runtime import spark

from note_catalog import BRONZE, table


def _column(source: str, output: str, rule: str = "raw"):
    value = F.col(f"`{source}`")
    if rule == "upper":
        value = F.upper(F.trim(value))
    elif rule == "clean":
        value = F.nullif(F.trim(value), F.lit(""))
    return value.alias(output)


SPECS = {
    "companies": ("company", [("id", "company_id", "raw"), ("type", "company_type", "upper"), ("status", "company_status", "upper"), ("trade_name", "trade_name", "clean"), ("corporate_name", "corporate_name", "clean"), ("slug", "slug", "clean")]),
    "suppliers": ("supplier", [("id", "supplier_id", "raw"), ("company_id", "company_id", "raw"), ("status", "supplier_status", "upper"), ("business_type", "business_type", "clean")]),
    "requesters": ("requester", [("id", "requester_id", "raw"), ("company_id", "company_id", "raw"), ("business_type", "business_type", "clean")]),
    "technician_affiliations": ("technician_affiliation", [("id", "affiliation_id", "raw"), ("technician_id", "technician_id", "raw"), ("company_id", "company_id", "raw"), ("affiliation_type", "affiliation_type", "upper"), ("is_active", "is_active", "raw")]),
    "company_memberships": ("user_company", [("id", "membership_id", "raw"), ("company_id", "company_id", "raw"), ("user_id", "user_id", "raw"), ("job_position_id", "position_id", "raw")]),
    "brands": ("brand", [("id", "brand_id", "raw"), ("owner_company_id", "owner_company_id", "raw"), ("name", "brand_name", "clean"), ("normalized_name", "normalized_name", "clean"), ("visibility", "visibility", "upper"), ("is_archived", "is_archived", "raw"), ("is_legacy", "is_legacy", "raw"), ("created_at", "created_at", "raw"), ("updated_at", "updated_at", "raw")]),
    "product_models": ("model", [("id", "model_id", "raw"), ("brand_id", "brand_id", "raw"), ("model_name", "model_name", "clean"), ("status", "model_status", "upper"), ("product_type", "product_type", "upper"), ("infrastructure_type", "infrastructure_type", "upper"), ("type", "panel_type", "upper"), ("material", "material", "clean"), ("power_wp", "power_wp", "raw"), ("efficiency", "efficiency", "raw"), ("width", "width", "raw"), ("length", "length", "raw"), ("weight", "weight", "raw")]),
    "offers": ("offer", [("id", "offer_id", "raw"), ("supplier_id", "supplier_id", "raw"), ("model_id", "model_id", "raw"), ("unit_price", "unit_price", "raw"), ("availability", "availability", "raw"), ("expires_at", "expires_at", "raw"), ("slug", "slug", "clean"), ("discount_percentage", "discount_percentage", "raw"), ("commercial_status", "commercial_status", "upper")]),
    "inventory_items": ("inventory", [("id", "inventory_id", "raw"), ("supplier_id", "supplier_id", "raw"), ("model_id", "model_id", "raw"), ("quantity", "quantity", "raw")]),
    "proposals": ("proposal", [("id", "proposal_id", "raw"), ("requester_id", "requester_id", "raw"), ("supplier_id", "supplier_id", "raw"), ("status", "proposal_status", "upper"), ("total_amount", "total_amount", "raw"), ("created_at", "created_at", "raw"), ("updated_at", "updated_at", "raw")]),
    "proposal_items": ("proposal_item", [("id", "proposal_item_id", "raw"), ("proposal_id", "proposal_id", "raw"), ("offer_id", "offer_id", "raw"), ("quantity", "quantity", "raw"), ("negotiated_unit_price", "negotiated_unit_price", "raw"), ("discount", "discount", "raw")]),
    "proposal_units": ("proposal_unit", [("id", "proposal_unit_id", "raw"), ("proposal_item_id", "proposal_item_id", "raw"), ("local_unit_id", "local_unit_id", "raw"), ("quantity", "quantity", "raw")]),
    "local_units": ("local_unit", [("id", "local_unit_id", "raw"), ("requester_id", "requester_id", "raw"), ("address_id", "address_id", "raw"), ("location_type", "location_type", "upper")]),
    "technical_projects": ("technical_project", [("id", "project_id", "raw"), ("requester_id", "requester_id", "raw"), ("local_unit_id", "local_unit_id", "raw"), ("owner_company_id", "owner_company_id", "raw"), ("owner_user_id", "owner_user_id", "raw"), ("name", "project_name", "clean"), ("status", "project_status", "upper"), ("is_archived", "is_archived", "raw"), ("created_at", "created_at", "raw"), ("starts_at", "starts_at", "raw"), ("ends_at", "ends_at", "raw"), ("location_state", "location_state", "upper"), ("location_city", "location_city", "clean")]),
    "project_activities": ("project_activity", [("id", "activity_id", "raw"), ("technical_project_id", "project_id", "raw"), ("technical_service_id", "service_id", "raw"), ("type", "activity_type", "upper"), ("status", "activity_status", "upper"), ("starts_at", "starts_at", "raw"), ("ends_at", "ends_at", "raw"), ("is_onsite", "is_onsite", "raw")]),
    "technical_services": ("technical_service", [("id", "service_id", "raw"), ("technical_project_id", "project_id", "raw"), ("status", "service_status", "upper"), ("scheduled_at", "scheduled_at", "raw"), ("created_at", "created_at", "raw"), ("accepted_at", "accepted_at", "raw"), ("ends_at", "ends_at", "raw")]),
    "service_contracts": ("service_contract", [("id", "contract_id", "raw"), ("technical_service_id", "service_id", "raw"), ("delivery_deadline_date", "delivery_deadline_date", "raw"), ("has_insurance", "has_insurance", "raw"), ("has_utility_approval", "has_utility_approval", "raw")]),
    "service_executors": ("service_executor", [("id", "executor_id", "raw"), ("technical_service_id", "service_id", "raw"), ("technician_affiliation_id", "affiliation_id", "raw"), ("function", "executor_function", "clean")]),
    "professional_reviews": ("professional_review", [("id", "review_id", "raw"), ("technician_id", "technician_id", "raw"), ("technical_service_id", "service_id", "raw"), ("rating", "rating", "raw"), ("is_active", "is_active", "raw"), ("created_at", "created_at", "raw")]),
    "company_plans": ("company_plan", [("id", "plan_id", "raw"), ("name", "plan_name", "clean"), ("price", "plan_price", "raw"), ("cycle", "billing_cycle", "upper")]),
    "subscriptions": ("subscription", [("id", "subscription_id", "raw"), ("supplier_id", "supplier_id", "raw"), ("company_plan_id", "plan_id", "raw"), ("status", "subscription_status", "upper"), ("has_auto_renewal", "has_auto_renewal", "raw"), ("starts_at", "starts_at", "raw"), ("ends_at", "ends_at", "raw")]),
    "subscription_charges": ("charge", [("id", "charge_id", "raw"), ("subscription_id", "subscription_id", "raw"), ("amount", "amount", "raw"), ("payment_method", "payment_method", "upper"), ("status", "charge_status", "upper"), ("due_date", "due_date", "raw"), ("paid_at", "paid_at", "raw")]),
    "energy_bills": ("energy_bill", [("id", "energy_bill_id", "raw"), ("local_unit_id", "local_unit_id", "raw"), ("consumption", "consumption", "raw"), ("amount", "amount", "raw")]),
}


def technicians():
    tech = spark.read.table(table(BRONZE, "technician")).alias("t")
    people = spark.read.table(table(BRONZE, "person")).alias("p")
    return tech.join(people, F.col("t.person_id") == F.col("p.id"), "left").select(
        F.col("t.id").alias("technician_id"),
        F.col("t.person_id").alias("person_id"),
        F.col("p.user_id").alias("user_id"),
        F.nullif(F.trim(F.col("t.slug")), F.lit("")).alias("technician_slug"),
    )


def silver_frame(name: str):
    if name == "technicians":
        return technicians()
    source, columns = SPECS[name]
    return spark.read.table(table(BRONZE, source)).select(
        *(_column(*column) for column in columns)
    )
