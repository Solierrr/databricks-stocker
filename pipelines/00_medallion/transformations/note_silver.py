"""Silver DataFrame builders migrated from archived SQL transformations."""

from pyspark.sql import functions as F
from databricks.sdk.runtime import spark

from note_catalog import BRONZE, table


# Excel exports store these measures and flags as text. Keep identifiers as
# strings, but publish typed Silver columns before Gold performs arithmetic.
NUMERIC_TYPES = {
    "quantity": "bigint",
    "availability": "bigint",
    "unit_price": "decimal(18,2)",
    "negotiated_unit_price": "decimal(18,2)",
    "discount": "decimal(18,2)",
    "discount_percentage": "decimal(18,2)",
    "total_amount": "decimal(18,2)",
    "plan_price": "decimal(18,2)",
    "amount": "decimal(18,2)",
    "rating": "decimal(18,2)",
    "power_wp": "decimal(18,4)",
    "efficiency": "decimal(18,4)",
    "width": "decimal(18,4)",
    "length": "decimal(18,4)",
    "weight": "decimal(18,4)",
    "consumption": "decimal(18,4)",
}
BOOLEAN_COLUMNS = {
    "is_active", "is_archived", "is_legacy", "is_onsite",
    "has_insurance", "has_utility_approval", "has_auto_renewal",
}
DATE_COLUMNS = {"delivery_deadline_date", "due_date"}
TIMESTAMP_COLUMNS = {
    "created_at", "updated_at", "expires_at", "starts_at", "ends_at",
    "scheduled_at", "accepted_at", "paid_at",
}


def _text(source: str):
    identifier = ".".join(f"`{part}`" for part in source.split("."))
    return F.nullif(F.trim(F.col(identifier).cast("string")), F.lit(""))


def _value(source: str, output: str, rule: str = "raw"):
    value = _text(source)
    identifier = ".".join(f"`{part}`" for part in source.split("."))
    clean = f"nullif(trim(cast({identifier} as string)), '')"
    if output in NUMERIC_TYPES:
        if NUMERIC_TYPES[output] == "bigint":
            parsed = f"try_cast({clean} as decimal(38,10))"
            return F.expr(
                f"case when {parsed} = floor({parsed}) "
                f"then try_cast({parsed} as bigint) end"
            )
        return F.expr(f"try_cast({clean} as {NUMERIC_TYPES[output]})")
    if output in DATE_COLUMNS | TIMESTAMP_COLUMNS:
        kind = "date" if output in DATE_COLUMNS else "timestamp"
        return F.expr(f"try_cast({clean} as {kind})")
    if output in BOOLEAN_COLUMNS:
        normalized = F.lower(value)
        return (F.when(normalized.isin("t", "true", "1"), F.lit(True))
                .when(normalized.isin("f", "false", "0"), F.lit(False)))
    if rule == "upper":
        return F.upper(value)
    return value


def _column(source: str, output: str, rule: str = "raw"):
    return _value(source, output, rule).alias(output)


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


def _validate_source(frame, name: str, columns) -> None:
    """Reject missing fields, malformed values and duplicate or empty row IDs."""
    missing = sorted({source for source, _, _ in columns} - set(frame.columns))
    if missing:
        raise ValueError(f"Bronze {name}: missing Silver columns {missing}")

    checks = {"id": _text("id").isNull()}
    for source, output, rule in columns:
        if output in NUMERIC_TYPES or output in BOOLEAN_COLUMNS or output in DATE_COLUMNS | TIMESTAMP_COLUMNS:
            converted = _value(source, output, rule)
            invalid = converted.isNull()
            if output in NUMERIC_TYPES:
                identifier = ".".join(f"`{part}`" for part in source.split("."))
                parsed = F.expr(
                    f"try_cast(nullif(trim(cast({identifier} as string)), '') as decimal(38,10))"
                )
                invalid = invalid | (parsed.isNotNull() &
                                     (parsed != converted.cast("decimal(38,10)")))
            checks[output] = _text(source).isNotNull() & invalid

    counts = frame.agg(*(
        F.sum(F.when(condition, F.lit(1)).otherwise(F.lit(0))).alias(field)
        for field, condition in checks.items()
    )).first().asDict()
    invalid = {field: count for field, count in counts.items() if count}
    if invalid:
        raise ValueError(f"Bronze {name}: invalid Silver values (field: row count): {invalid}")

    duplicate = (frame.groupBy(_text("id").alias("row_id"))
                 .count().where(F.col("count") > 1).limit(1).collect())
    if duplicate:
        raise ValueError(f"Bronze {name}: duplicate id {duplicate[0]['row_id']!r}")


def technicians():
    tech = spark.read.table(table(BRONZE, "technician")).alias("t")
    people = spark.read.table(table(BRONZE, "person")).alias("p")
    _validate_source(tech, "technician", [("id", "technician_id", "raw"), ("person_id", "person_id", "raw"), ("slug", "technician_slug", "clean")])
    _validate_source(people, "person", [("id", "person_id", "raw"), ("user_id", "user_id", "raw")])
    return tech.join(people, F.col("t.person_id") == F.col("p.id"), "left").select(
        _column("t.id", "technician_id"),
        _column("t.person_id", "person_id"),
        _column("p.user_id", "user_id"),
        _column("t.slug", "technician_slug", "clean"),
    )


def silver_frame(name: str):
    if name == "technicians":
        return technicians()
    source, columns = SPECS[name]
    frame = spark.read.table(table(BRONZE, source))
    _validate_source(frame, source, columns)
    return frame.select(
        *(_column(*column) for column in columns)
    )
