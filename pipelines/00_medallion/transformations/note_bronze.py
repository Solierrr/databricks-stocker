"""Ingestão manual de Excel para o job Bronze -> Silver -> Gold.

Cada planilha contém uma aba por entidade de origem. Envie os dois arquivos
para um volume do Unity Catalog e configure os caminhos nos parâmetros do job.
O leitor de Excel está disponível no Databricks Runtime 17.1 ou superior.
"""

from databricks.sdk.runtime import spark


# Abas esperadas e colunas obrigatórias de cada planilha.
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

# As planilhas de recuperação usam os cabeçalhos originais do CoreDB. A Bronze
# publica os nomes usados pela Silver e mantém as demais colunas de origem.
SOURCE_SHEETS = {"company_plan": "company_plans"}
COLUMN_ALIASES = {
    "brand": {"fk_owner_company": "owner_company_id", "archived": "is_archived", "legacy": "is_legacy"},
    "model": {"fk_brand": "brand_id", "model": "model_name"},
    "supplier": {"fk_company": "company_id"},
    "offer": {"fk_supplier": "supplier_id", "fk_model": "model_id", "expiration_date": "expires_at"},
    "inventory": {"fk_supplier": "supplier_id", "fk_model": "model_id"},
    "proposal": {"fk_requester": "requester_id", "fk_supplier": "supplier_id"},
    "proposal_item": {"fk_proposal": "proposal_id", "fk_offer": "offer_id", "negotiated_price": "negotiated_unit_price"},
    "person": {"fk_users": "user_id"},
    "requester": {"fk_company": "company_id"},
    "technician": {"fk_person": "person_id"},
    "technician_affiliation": {"fk_technician": "technician_id", "fk_company": "company_id", "active": "is_active"},
    "user_company": {"fk_company": "company_id", "fk_users": "user_id", "fk_position": "job_position_id"},
    "proposal_unit": {"fk_proposal_item": "proposal_item_id", "fk_local_unit": "local_unit_id"},
    "local_unit": {"fk_requester": "requester_id", "fk_address": "address_id"},
    "professional_review": {"fk_professional": "technician_id", "fk_service": "technical_service_id", "active": "is_active"},
    "project_activity": {"project_id": "technical_project_id", "onsite": "is_onsite"},
    "service_contract": {"fk_service": "technical_service_id", "delivery_deadline": "delivery_deadline_date", "insurance": "has_insurance", "utility_approval": "has_utility_approval"},
    "service_executor": {"fk_service": "technical_service_id", "fk_technician_affiliation": "technician_affiliation_id"},
    "technical_project": {"fk_requester": "requester_id", "fk_local_unit": "local_unit_id", "archived": "is_archived", "start_date": "starts_at", "end_date": "ends_at"},
    "technical_service": {"fk_technical_project": "technical_project_id", "scheduled_date": "scheduled_at", "end_date": "ends_at"},
    "charge": {"fk_subscription": "subscription_id", "payment_date": "paid_at"},
    "company_plan": {"value": "price"},
    "subscription": {"fk_supplier": "supplier_id", "fk_plan": "company_plan_id", "auto_renewal": "has_auto_renewal", "start_date": "starts_at", "end_date": "ends_at"},
    "energy_bill": {"fk_local_unit": "local_unit_id", "price": "amount"},
}


def read_bronze(
    workbook: str, sheet: str, required: tuple[str, ...], path: str
):
    # Validando o caminho da planilha.
    if not path:
        raise ValueError(f"Set a path for {workbook}.xlsx")
    # Lendo a aba do Excel com cabeçalho.
    frame = (
        spark.read.format("excel")
        .option("headerRows", 1)
        .option("dataAddress", SOURCE_SHEETS.get(sheet, sheet))
        .option("ignoreMissingSheet", "false")
        .load(path)
    )
    # Padronizando nomes antigos de colunas.
    aliases = {
        source: target for source, target in COLUMN_ALIASES.get(sheet, {}).items()
        if source in frame.columns and target not in frame.columns
    }
    columns = [aliases.get(column, column) for column in frame.columns]
    # Verificando as colunas obrigatórias.
    missing = sorted(set(required) - set(columns))
    if missing:
        raise ValueError(f"{workbook}.xlsx / {sheet}: missing columns {missing}")
    return frame.toDF(*columns) if aliases else frame
