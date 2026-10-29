"""Criação dos 19 DataFrames Gold migrados das transformações SQL arquivadas."""

from pyspark.sql import functions as F
from databricks.sdk.runtime import spark

from note_catalog import SECURITY, SILVER, table


# Identificando o filtro de acesso às tabelas de fornecedores.
def supplier_filter_name() -> str:
    return table(SECURITY, "can_read_supplier")


# Lendo as tabelas Silver usadas nos indicadores.
def silver(name: str):
    return spark.read.table(table(SILVER, name))


# Indicadores administrativos de cadastros, modelos e propostas.
def kpi_admin_registrations():
    suppliers = silver("suppliers").where(F.col("supplier_status") == "ACTIVE").agg(
        F.countDistinct("supplier_id").alias("active_suppliers")
    )
    requesters = silver("requesters").agg(
        F.countDistinct("requester_id").alias("registered_requesters")
    )
    return suppliers.crossJoin(requesters)


def kpi_admin_models_by_status():
    return silver("product_models").groupBy("model_status").agg(
        F.countDistinct("model_id").alias("model_count")
    )


def kpi_admin_proposals_by_status():
    return silver("proposals").groupBy("proposal_status").agg(
        F.countDistinct("proposal_id").alias("proposal_count")
    )


# Indicadores de ofertas e estoque por fornecedor.
def kpi_supplier_offers_by_status():
    return silver("offers").groupBy("supplier_id", "commercial_status").agg(
        F.countDistinct("offer_id").alias("offer_count")
    )


def kpi_supplier_stock_by_model():
    stock = silver("inventory_items").groupBy("supplier_id", "model_id").agg(
        F.sum("quantity").alias("stock_units")
    ).alias("e")
    models = silver("product_models").alias("m")
    brands = silver("brands").alias("b")
    return (stock
        .join(models, F.col("e.model_id") == F.col("m.model_id"), "left")
        .join(brands, F.col("m.brand_id") == F.col("b.brand_id"), "left")
        .select(
            F.col("e.supplier_id"), F.col("e.model_id"),
            F.col("m.model_name"), F.col("b.brand_name"), F.col("e.stock_units"),
        ))


# Indicadores de unidades, propostas e projetos dos solicitantes.
def kpi_requester_summary():
    units = silver("local_units").groupBy("requester_id").agg(
        F.countDistinct("local_unit_id").alias("registered_local_units")
    ).alias("u")
    proposals = silver("proposals").groupBy("requester_id").agg(
        F.countDistinct("proposal_id").alias("registered_proposals")
    ).alias("p")
    projects = silver("technical_projects").groupBy("requester_id").agg(
        F.countDistinct("project_id").alias("registered_projects")
    ).alias("t")
    requesters = silver("requesters").alias("r")
    return (requesters
        .join(units, F.col("u.requester_id") == F.col("r.requester_id"), "left")
        .join(proposals, F.col("p.requester_id") == F.col("r.requester_id"), "left")
        .join(projects, F.col("t.requester_id") == F.col("r.requester_id"), "left")
        .select(
            F.col("r.requester_id"), F.col("r.company_id"),
            F.coalesce(F.col("u.registered_local_units"), F.lit(0)).alias("registered_local_units"),
            F.coalesce(F.col("p.registered_proposals"), F.lit(0)).alias("registered_proposals"),
            F.coalesce(F.col("t.registered_projects"), F.lit(0)).alias("registered_projects"),
        ))


def kpi_requester_proposals_by_status():
    return (silver("proposals")
        .where(F.col("requester_id").isNotNull())
        .groupBy("requester_id", "proposal_status")
        .agg(F.countDistinct("proposal_id").alias("proposal_count")))


def kpi_requester_projects_by_status():
    return (silver("technical_projects")
        .where(F.col("requester_id").isNotNull())
        .groupBy("requester_id", "project_status")
        .agg(F.countDistinct("project_id").alias("project_count")))


# Indicadores de vínculos, serviços e avaliações dos profissionais.
def kpi_professional_summary():
    affiliations = silver("technician_affiliations").alias("a")
    technicians = silver("technicians").alias("t")
    executors = silver("service_executors").alias("e")
    return (affiliations
        .join(technicians, F.col("t.technician_id") == F.col("a.technician_id"), "left")
        .join(executors, F.col("e.affiliation_id") == F.col("a.affiliation_id"), "left")
        .select(
            F.col("a.affiliation_id"), F.col("a.technician_id"), F.col("t.user_id"),
            F.col("a.company_id"), F.col("a.affiliation_type"),
            F.col("a.is_active").alias("affiliation_active"),
            F.col("e.service_id"),
        )
        .groupBy("affiliation_id", "technician_id", "user_id", "company_id",
                 "affiliation_type", "affiliation_active")
        .agg(F.countDistinct("service_id").alias("assigned_service_count")))


def kpi_professional_services_by_status():
    affiliations = silver("technician_affiliations").alias("a")
    executors = silver("service_executors").alias("e")
    services = silver("technical_services").alias("s")
    return (affiliations
        .join(executors, F.col("e.affiliation_id") == F.col("a.affiliation_id"))
        .join(services, F.col("s.service_id") == F.col("e.service_id"))
        .select(
            F.col("a.affiliation_id"), F.col("a.technician_id"),
            F.col("a.affiliation_type"), F.col("s.service_status"),
            F.col("e.service_id"),
        )
        .groupBy("affiliation_id", "technician_id", "affiliation_type", "service_status")
        .agg(F.countDistinct("service_id").alias("service_count")))


def kpi_professional_reviews():
    return (silver("professional_reviews")
        .where(F.col("is_active") == F.lit(True))
        .groupBy("technician_id")
        .agg(
            F.countDistinct("review_id").alias("review_count"),
            F.round(F.avg("rating"), 2).alias("average_rating"),
        ))


# Resumos de ofertas, preços, vencimentos e estoque dos fornecedores.
def kpi_supplier_offers_summary():
    suppliers = silver("suppliers").alias("s")
    offers = silver("offers").alias("o")
    reference = F.current_date()
    expiry = F.to_date(F.col("o.expires_at"))
    active = ((F.col("o.commercial_status") == "ACTIVE") &
              (F.col("o.expires_at").isNull() | (expiry >= reference)))
    return (suppliers
        .join(offers, F.col("o.supplier_id") == F.col("s.supplier_id"), "left")
        .groupBy(F.col("s.supplier_id"), reference.alias("reference_date"))
        .agg(
            F.countDistinct("o.offer_id").alias("registered_offers"),
            F.countDistinct(F.when(active, F.col("o.offer_id"))).alias("active_offers"),
            F.avg("o.unit_price").alias("avg_listed_price"),
            F.avg(F.when(active, F.col("o.unit_price"))).alias("avg_active_listed_price"),
            F.sum("o.availability").alias("listed_availability"),
            F.countDistinct(F.when(expiry < reference, F.col("o.offer_id"))).alias("expired_offers"),
            F.countDistinct(F.when((expiry >= reference) & (expiry < F.date_add(reference, 30)),
                                   F.col("o.offer_id"))).alias("offers_expiring_in_30_days"),
        ))


def kpi_supplier_offer_details():
    offers = silver("offers").alias("o")
    models = silver("product_models").alias("m")
    brands = silver("brands").alias("b")
    return (offers
        .join(models, F.col("m.model_id") == F.col("o.model_id"), "left")
        .join(brands, F.col("b.brand_id") == F.col("m.brand_id"), "left")
        .select(
            F.col("o.supplier_id"), F.col("o.offer_id"), F.col("o.model_id"),
            F.col("b.brand_name"), F.col("m.model_name"), F.col("o.unit_price"),
            F.col("o.availability"), F.col("o.expires_at"), F.col("o.commercial_status"),
        ))


def kpi_supplier_stock_summary():
    stock = silver("inventory_items").groupBy("supplier_id", "model_id").agg(
        F.sum("quantity").alias("stock_units")
    ).alias("e")
    suppliers = silver("suppliers").alias("s")
    return (suppliers
        .join(stock, F.col("e.supplier_id") == F.col("s.supplier_id"), "left")
        .groupBy(F.col("s.supplier_id"))
        .agg(
            F.sum("e.stock_units").alias("stock_units"),
            F.countDistinct(F.when(F.col("e.stock_units") > 0,
                                   F.col("e.model_id"))).alias("models_in_stock"),
        ))


# Relacionando itens, ofertas e propostas dos fornecedores.
def _supplier_proposal_items():
    items = silver("proposal_items").alias("i")
    offers = silver("offers").alias("o")
    proposals = silver("proposals").alias("p")
    return (items
        .join(offers, F.col("o.offer_id") == F.col("i.offer_id"))
        .join(proposals, F.col("p.proposal_id") == F.col("i.proposal_id")))


def kpi_supplier_proposals_by_status():
    return (_supplier_proposal_items()
        .groupBy(F.col("o.supplier_id"), F.col("p.proposal_status"))
        .agg(F.countDistinct("p.proposal_id").alias("received_proposals")))


# Calculando quantidades, valores negociados e taxas das propostas.
def kpi_supplier_proposals_summary():
    source = _supplier_proposal_items()
    supplier_proposals = source.select(
        F.col("o.supplier_id").alias("supplier_id"),
        F.col("p.proposal_id").alias("proposal_id"),
        F.col("p.proposal_status").alias("proposal_status"),
    ).distinct()
    status = F.col("proposal_status")
    proposal_counts = supplier_proposals.groupBy("supplier_id").agg(
        F.countDistinct(F.when(status.isin("AWAITING_SUPPLIER", "AWAITING_REQUESTER"),
                               F.col("proposal_id"))).alias("open_proposals"),
        F.countDistinct(F.when(status == "ACCEPTED", F.col("proposal_id"))).alias("accepted_proposals"),
        F.countDistinct(F.when(status.isin("ACCEPTED", "REJECTED", "CANCELED"),
                               F.col("proposal_id"))).alias("closed_proposals"),
    ).alias("p")
    accepted = F.col("p.proposal_status") == "ACCEPTED"
    items = source.groupBy(F.col("o.supplier_id")).agg(
        F.count(F.when(accepted, F.lit(1))).alias("accepted_items"),
        F.count(F.when(accepted & F.col("i.negotiated_unit_price").isNull(), F.lit(1)))
            .alias("accepted_items_without_price"),
        F.count(F.when(accepted & F.col("i.negotiated_unit_price").isNotNull(), F.lit(1)))
            .alias("accepted_items_with_price"),
        F.sum(F.when(accepted, F.col("i.negotiated_unit_price") * F.col("i.quantity")))
            .alias("accepted_negotiated_value"),
    ).alias("i")
    suppliers = silver("suppliers").alias("s")
    return (suppliers
        .join(proposal_counts, F.col("p.supplier_id") == F.col("s.supplier_id"), "left")
        .join(items, F.col("i.supplier_id") == F.col("s.supplier_id"), "left")
        .select(
            F.col("s.supplier_id"),
            F.coalesce(F.col("p.open_proposals"), F.lit(0)).alias("open_received_proposals"),
            F.coalesce(F.col("p.accepted_proposals"), F.lit(0)).alias("accepted_proposals"),
            F.coalesce(F.col("p.closed_proposals"), F.lit(0)).alias("closed_proposals"),
            (F.lit(100.0) * F.col("p.accepted_proposals") /
             F.nullif(F.col("p.closed_proposals"), F.lit(0)))
                .alias("closed_acceptance_rate_pct"),
            F.coalesce(F.col("i.accepted_items_without_price"), F.lit(0)).alias("accepted_items_without_price"),
            (F.lit(100.0) * F.col("i.accepted_items_with_price") /
             F.nullif(F.col("i.accepted_items"), F.lit(0)))
                .alias("accepted_items_price_coverage_pct"),
            F.col("i.accepted_negotiated_value"),
        ))


# Detalhando propostas aceitas por modelo e data de criação.
def kpi_supplier_accepted_value_by_model():
    source = _supplier_proposal_items().where(F.col("p.proposal_status") == "ACCEPTED")
    models = silver("product_models").alias("m")
    brands = silver("brands").alias("b")
    return (source
        .join(models, F.col("m.model_id") == F.col("o.model_id"), "left")
        .join(brands, F.col("b.brand_id") == F.col("m.brand_id"), "left")
        .groupBy(F.col("o.supplier_id"), F.col("o.model_id"),
                 F.col("b.brand_name"), F.col("m.model_name"))
        .agg(F.sum(F.col("i.negotiated_unit_price") * F.col("i.quantity"))
             .alias("accepted_negotiated_value")))


def kpi_supplier_accepted_value_by_month():
    source = _supplier_proposal_items().where(F.col("p.proposal_status") == "ACCEPTED")
    return (source
        .groupBy(F.col("o.supplier_id"),
                 F.date_trunc("MONTH", F.col("p.created_at")).alias("proposal_created_month_proxy"))
        .agg(F.sum(F.col("i.negotiated_unit_price") * F.col("i.quantity"))
             .alias("accepted_negotiated_value")))


def kpi_supplier_accepted_offers_by_date():
    source = _supplier_proposal_items().where(F.col("p.proposal_status") == "ACCEPTED")
    return (source
        .groupBy(F.col("o.supplier_id"), F.col("o.offer_id"), F.col("o.model_id"),
                 F.to_date(F.col("p.created_at")).alias("proposal_created_date_proxy"))
        .agg(F.countDistinct("p.proposal_id").alias("accepted_proposals")))


# Registrando os indicadores e identificando os que exigem filtro de fornecedor.
GOLD_BUILDERS = {
    name: builder for name, builder in globals().items()
    if name.startswith("kpi_") and callable(builder)
}

SUPPLIER_TABLES = frozenset(
    name for name in GOLD_BUILDERS if name.startswith("kpi_supplier_")
)
