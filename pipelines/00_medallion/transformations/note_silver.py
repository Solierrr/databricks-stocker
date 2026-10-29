"""Criação dos DataFrames Silver migrados das transformações SQL arquivadas."""

from functools import reduce

from pyspark.sql import functions as F
from databricks.sdk.runtime import spark

from note_catalog import BRONZE, table


# As exportações do Excel armazenam medidas e sinalizadores como texto.
# Mantemos os IDs como texto e definimos os tipos antes dos cálculos na Gold.
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


# Regras confirmadas no schema versionado. Estoque e disponibilidade aceitam zero.
REQUIRED_MEASURES = {
    "offer": ("availability",),
    "inventory": ("quantity",),
    "proposal_item": ("quantity",),
    "proposal_unit": ("quantity",),
}
NONNEGATIVE_MEASURES = {"offer": ("availability",), "inventory": ("quantity",)}
POSITIVE_MEASURES = {"proposal_item": ("quantity",), "proposal_unit": ("quantity",)}
STATUS_VALUES = {
    "company_status": ("UNDER_ANALYSIS", "APPROVED", "REJECTED"),
    "supplier_status": ("ACTIVE", "SUSPENDED", "DEACTIVATED"),
    "model_status": ("APPROVED", "REJECTED", "UNDER_ANALYSIS"),
    "proposal_status": ("AWAITING_SUPPLIER", "AWAITING_REQUESTER", "ACCEPTED", "REJECTED", "CANCELED"),
    "project_status": ("OPEN", "IN_PROGRESS", "COMPLETED", "CANCELED"),
    "service_status": ("OPEN", "IN_PROGRESS", "COMPLETED", "CANCELED"),
    "subscription_status": ("PAID", "IN_DEBT", "SUSPENDED"),
    "charge_status": ("PENDING", "PAID", "CANCELED", "REFUNDED"),
}


# Removendo espaços e convertendo textos vazios em nulos.
def _text(source: str):
    identifier = ".".join(f"`{part}`" for part in source.split("."))
    return F.nullif(F.trim(F.col(identifier).cast("string")), F.lit(""))


# Convertendo números, datas e booleanos e padronizando textos.
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


# Identificando perda de casas decimais na conversão.
def _loses_decimal_digits(source: str, scale: int):
    """Detecta dígitos não nulos descartados, mesmo além da precisão decimal(38, 10).

    O Excel pode fornecer células de texto, decimais ou ponto flutuante. Comparar
    duas conversões não detecta frações pequenas quando ambas arredondam para zero.
    Os dígitos originais são analisados, inclusive em notação científica.
    """
    identifier = ".".join(f"`{part}`" for part in source.split("."))
    clean = f"nullif(trim(cast({identifier} as string)), '')"
    mantissa_digits = (
        f"regexp_replace(regexp_replace({clean}, '[eE].*$', ''), '[^0-9]', '')"
    )
    integer_length = f"length(regexp_extract({clean}, '^[+-]?([0-9]*)', 1))"
    exponent = (
        f"greatest(-1000, least(1000, coalesce(try_cast(nullif(regexp_extract({clean}, "
        "'[eE]([+-]?[0-9]+)$', 1), '') as int), 0)))"
    )
    first_discarded = f"greatest({integer_length} + {exponent} + {scale} + 1, 1)"
    return F.expr(
        f"substring({mantissa_digits}, {first_discarded}, "
        f"length({mantissa_digits})) rlike '[1-9]'"
    )


def _column(source: str, output: str, rule: str = "raw"):
    return _value(source, output, rule).alias(output)


# Definindo as fontes e colunas de cada tabela Silver.
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


def _validate_source(frame, name: str, columns):
    """Retira registros inválidos da Silver e informa as quantidades na execução."""
    # Verificando as colunas necessárias na Bronze.
    missing = sorted({source for source, _, _ in columns} - set(frame.columns))
    if missing:
        raise ValueError(f"Bronze {name}: missing Silver columns {missing}")

    # Validando IDs vazios e valores preenchidos que não podem ser convertidos.
    checks = {"id": _text("id").isNull()}
    for source, output, rule in columns:
        if output in NUMERIC_TYPES or output in BOOLEAN_COLUMNS or output in DATE_COLUMNS | TIMESTAMP_COLUMNS:
            converted = _value(source, output, rule)
            invalid = converted.isNull()
            if output in NUMERIC_TYPES:
                kind = NUMERIC_TYPES[output]
                scale = 0 if kind == "bigint" else int(kind.split(",")[1][:-1])
                invalid = invalid | _loses_decimal_digits(source, scale)
            elif output in DATE_COLUMNS:
                # O Spark aceita timestamp como DATE e descarta o horário sem avisar.
                identifier = ".".join(f"`{part}`" for part in source.split("."))
                clean = f"nullif(trim(cast({identifier} as string)), '')"
                timestamp = F.expr(f"try_cast({clean} as timestamp)")
                invalid = invalid | timestamp.isNull() | (
                    timestamp != converted.cast("timestamp")
                )
            checks[output] = _text(source).isNotNull() & invalid
        if output in STATUS_VALUES:
            status = F.upper(_text(source))
            checks[output] = status.isNotNull() & ~status.isin(*STATUS_VALUES[output])
        if output in NONNEGATIVE_MEASURES.get(name, ()):
            checks[output] = checks[output] | (_value(source, output, rule) < 0)
        if output in POSITIVE_MEASURES.get(name, ()):
            checks[output] = checks[output] | (_value(source, output, rule) <= 0)

    # Campos obrigatórios vazios também tornam o registro inválido.
    for field in REQUIRED_MEASURES.get(name, ()):
        checks[f"{field}_required"] = _text(field).isNull()

    # O aceite e a conclusão de um serviço não podem anteceder sua criação.
    # Quando há aceite, a conclusão também não pode acontecer antes dele.
    if name == "technical_service":
        created = _value("created_at", "created_at")
        accepted = _value("accepted_at", "accepted_at")
        ended = _value("ends_at", "ends_at")
        checks["accepted_at_before_created_at"] = accepted < created
        checks["ends_at_before_created_at"] = ended < created
        checks["ends_at_before_accepted_at"] = ended < accepted

    # Informando quantos registros falharam em cada regra.
    counts = frame.agg(*(
        F.sum(F.when(condition, F.lit(1)).otherwise(F.lit(0))).alias(field)
        for field, condition in checks.items()
    )).first().asDict()
    invalid = {field: count for field, count in counts.items() if count}
    if invalid:
        print(f"Bronze {name}: registros retirados da Silver por regra: {invalid}")

    # Retirando todas as linhas com ID duplicado, sem escolher uma versão ao acaso.
    duplicates = (frame.groupBy(_text("id").alias("_duplicate_id"))
                  .count().where(F.col("_duplicate_id").isNotNull() & (F.col("count") > 1))
                  .select("_duplicate_id"))
    duplicate_rows = frame.join(duplicates, _text("id") == F.col("_duplicate_id"), "left_semi").count()
    if duplicate_rows:
        print(f"Bronze {name}: {duplicate_rows} linhas retiradas da Silver por ID duplicado")
    invalid_row = reduce(lambda left, right: left | F.coalesce(right, F.lit(False)),
                         checks.values(), F.lit(False))
    clean = frame.filter(~invalid_row)
    return clean.join(duplicates, _text("id") == F.col("_duplicate_id"), "left_anti")


# Relacionando técnicos às pessoas após validar as fontes.
def technicians():
    tech = spark.read.table(table(BRONZE, "technician"))
    people = spark.read.table(table(BRONZE, "person"))
    tech = _validate_source(tech, "technician", [("id", "technician_id", "raw"), ("person_id", "person_id", "raw"), ("slug", "technician_slug", "clean")]).alias("t")
    people = _validate_source(people, "person", [("id", "person_id", "raw"), ("user_id", "user_id", "raw")]).alias("p")
    return tech.join(people, _text("t.person_id") == _text("p.id"), "left").select(
        _column("t.id", "technician_id"),
        _column("t.person_id", "person_id"),
        _column("p.user_id", "user_id"),
        _column("t.slug", "technician_slug", "clean"),
    )


# Validando a fonte e aplicando o mapeamento da tabela Silver.
def silver_frame(name: str):
    if name == "technicians":
        return technicians()
    source, columns = SPECS[name]
    frame = spark.read.table(table(BRONZE, source))
    frame = _validate_source(frame, source, columns)
    return frame.select(
        *(_column(*column) for column in columns)
    )
