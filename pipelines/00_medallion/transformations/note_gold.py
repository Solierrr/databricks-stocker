"""The 19 Gold DataFrame builders migrated from archived SQL transformations."""

from pyspark.sql import functions as F
from databricks.sdk.runtime import spark

from note_catalog import SECURITY, SILVER, table


def supplier_filter_name() -> str:
    return table(SECURITY, "can_read_supplier")


def silver(name: str):
    return spark.read.table(table(SILVER, name))


def kpi_admin_cadastros():
    suppliers = silver("suppliers").where(F.col("supplier_status") == "ACTIVE").agg(
        F.countDistinct("supplier_id").alias("fornecedores_ativos")
    )
    requesters = silver("requesters").agg(
        F.countDistinct("requester_id").alias("demandantes_cadastrados")
    )
    return suppliers.crossJoin(requesters)


def kpi_admin_modelos_status():
    return silver("product_models").groupBy("model_status").agg(
        F.countDistinct("model_id").alias("total_modelos")
    )


def kpi_admin_propostas_status():
    return silver("proposals").groupBy("proposal_status").agg(
        F.countDistinct("proposal_id").alias("total_propostas")
    )


def kpi_fornecedor_ofertas_status():
    return silver("offers").groupBy("supplier_id", "commercial_status").agg(
        F.countDistinct("offer_id").alias("total_ofertas")
    )


def kpi_fornecedor_estoque_modelo():
    stock = silver("inventory_items").groupBy("supplier_id", "model_id").agg(
        F.sum("quantity").alias("unidades_estoque")
    ).alias("e")
    models = silver("product_models").alias("m")
    brands = silver("brands").alias("b")
    return (stock
        .join(models, F.col("e.model_id") == F.col("m.model_id"), "left")
        .join(brands, F.col("m.brand_id") == F.col("b.brand_id"), "left")
        .select(
            F.col("e.supplier_id"), F.col("e.model_id"),
            F.col("m.model_name"), F.col("b.brand_name"), F.col("e.unidades_estoque"),
        ))


def kpi_demandante_resumo():
    units = silver("local_units").groupBy("requester_id").agg(
        F.countDistinct("local_unit_id").alias("unidades_cadastradas")
    ).alias("u")
    proposals = silver("proposals").groupBy("requester_id").agg(
        F.countDistinct("proposal_id").alias("propostas_cadastradas")
    ).alias("p")
    projects = silver("technical_projects").groupBy("requester_id").agg(
        F.countDistinct("project_id").alias("projetos_cadastrados")
    ).alias("t")
    requesters = silver("requesters").alias("r")
    return (requesters
        .join(units, F.col("u.requester_id") == F.col("r.requester_id"), "left")
        .join(proposals, F.col("p.requester_id") == F.col("r.requester_id"), "left")
        .join(projects, F.col("t.requester_id") == F.col("r.requester_id"), "left")
        .select(
            F.col("r.requester_id"), F.col("r.company_id"),
            F.coalesce(F.col("u.unidades_cadastradas"), F.lit(0)).alias("unidades_cadastradas"),
            F.coalesce(F.col("p.propostas_cadastradas"), F.lit(0)).alias("propostas_cadastradas"),
            F.coalesce(F.col("t.projetos_cadastrados"), F.lit(0)).alias("projetos_cadastrados"),
        ))


def kpi_demandante_propostas_status():
    return (silver("proposals")
        .where(F.col("requester_id").isNotNull())
        .groupBy("requester_id", "proposal_status")
        .agg(F.countDistinct("proposal_id").alias("total_propostas")))


def kpi_demandante_projetos_status():
    return (silver("technical_projects")
        .where(F.col("requester_id").isNotNull())
        .groupBy("requester_id", "project_status")
        .agg(F.countDistinct("project_id").alias("total_projetos")))


def kpi_profissional_resumo():
    affiliations = silver("technician_affiliations").alias("a")
    technicians = silver("technicians").alias("t")
    executors = silver("service_executors").alias("e")
    return (affiliations
        .join(technicians, F.col("t.technician_id") == F.col("a.technician_id"), "left")
        .join(executors, F.col("e.affiliation_id") == F.col("a.affiliation_id"), "left")
        .select(
            F.col("a.affiliation_id"), F.col("a.technician_id"), F.col("t.user_id"),
            F.col("a.company_id"), F.col("a.affiliation_type"),
            F.col("a.active").alias("affiliation_active"),
            F.col("e.service_id"),
        )
        .groupBy("affiliation_id", "technician_id", "user_id", "company_id",
                 "affiliation_type", "affiliation_active")
        .agg(F.countDistinct("service_id").alias("servicos_atribuidos")))


def kpi_profissional_servicos_status():
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
        .agg(F.countDistinct("service_id").alias("total_servicos")))


def kpi_profissional_avaliacoes():
    return (silver("professional_reviews")
        .where(F.col("active") == F.lit(True))
        .groupBy("technician_id")
        .agg(
            F.countDistinct("review_id").alias("total_avaliacoes"),
            F.round(F.avg("rating"), 2).alias("nota_media"),
        ))


def kpi_fornecedor_ofertas_resumo():
    suppliers = silver("suppliers").alias("s")
    offers = silver("offers").alias("o")
    reference = F.current_date()
    expiry = F.to_date(F.col("o.expiration_date"))
    active = ((F.col("o.commercial_status") == "ACTIVE") &
              (F.col("o.expiration_date").isNull() | (expiry >= reference)))
    return (suppliers
        .join(offers, F.col("o.supplier_id") == F.col("s.supplier_id"), "left")
        .groupBy(F.col("s.supplier_id"), reference.alias("data_referencia"))
        .agg(
            F.countDistinct("o.offer_id").alias("ofertas_cadastradas"),
            F.countDistinct(F.when(active, F.col("o.offer_id"))).alias("ofertas_ativas"),
            F.avg("o.unit_price").alias("preco_medio_anunciado"),
            F.avg(F.when(active, F.col("o.unit_price"))).alias("preco_medio_anunciado_ativo"),
            F.sum("o.availability").alias("disponibilidade_anunciada"),
            F.countDistinct(F.when(expiry < reference, F.col("o.offer_id"))).alias("ofertas_expiradas"),
            F.countDistinct(F.when((expiry >= reference) & (expiry < F.date_add(reference, 30)),
                                   F.col("o.offer_id"))).alias("ofertas_vencendo_30_dias"),
        ))


def kpi_fornecedor_ofertas_detalhe():
    offers = silver("offers").alias("o")
    models = silver("product_models").alias("m")
    brands = silver("brands").alias("b")
    return (offers
        .join(models, F.col("m.model_id") == F.col("o.model_id"), "left")
        .join(brands, F.col("b.brand_id") == F.col("m.brand_id"), "left")
        .select(
            F.col("o.supplier_id"), F.col("o.offer_id"), F.col("o.model_id"),
            F.col("b.brand_name"), F.col("m.model_name"), F.col("o.unit_price"),
            F.col("o.availability"), F.col("o.expiration_date"), F.col("o.commercial_status"),
        ))


def kpi_fornecedor_estoque_resumo():
    stock = silver("inventory_items").groupBy("supplier_id", "model_id").agg(
        F.sum("quantity").alias("unidades_estoque")
    ).alias("e")
    suppliers = silver("suppliers").alias("s")
    return (suppliers
        .join(stock, F.col("e.supplier_id") == F.col("s.supplier_id"), "left")
        .groupBy(F.col("s.supplier_id"))
        .agg(
            F.sum("e.unidades_estoque").alias("unidades_estoque"),
            F.countDistinct(F.when(F.col("e.unidades_estoque") > 0,
                                   F.col("e.model_id"))).alias("produtos_com_estoque"),
        ))


def _supplier_proposal_items():
    items = silver("proposal_items").alias("i")
    offers = silver("offers").alias("o")
    proposals = silver("proposals").alias("p")
    return (items
        .join(offers, F.col("o.offer_id") == F.col("i.offer_id"))
        .join(proposals, F.col("p.proposal_id") == F.col("i.proposal_id")))


def kpi_fornecedor_propostas_status():
    return (_supplier_proposal_items()
        .groupBy(F.col("o.supplier_id"), F.col("p.proposal_status"))
        .agg(F.countDistinct("p.proposal_id").alias("propostas_recebidas")))


def kpi_fornecedor_propostas_resumo():
    source = _supplier_proposal_items()
    supplier_proposals = source.select(
        F.col("o.supplier_id").alias("supplier_id"),
        F.col("p.proposal_id").alias("proposal_id"),
        F.col("p.proposal_status").alias("proposal_status"),
    ).distinct()
    status = F.col("proposal_status")
    proposal_counts = supplier_proposals.groupBy("supplier_id").agg(
        F.countDistinct(F.when(status.isin("AWAITING_SUPPLIER", "AWAITING_REQUESTER"),
                               F.col("proposal_id"))).alias("propostas_abertas"),
        F.countDistinct(F.when(status == "ACCEPTED", F.col("proposal_id"))).alias("propostas_aceitas"),
        F.countDistinct(F.when(status.isin("ACCEPTED", "REJECTED", "CANCELED"),
                               F.col("proposal_id"))).alias("propostas_encerradas"),
    ).alias("p")
    accepted = F.col("p.proposal_status") == "ACCEPTED"
    items = source.groupBy(F.col("o.supplier_id")).agg(
        F.count(F.when(accepted, F.lit(1))).alias("itens_aceitos"),
        F.count(F.when(accepted & F.col("i.negotiated_price").isNull(), F.lit(1)))
            .alias("itens_aceitos_sem_preco"),
        F.count(F.when(accepted & F.col("i.negotiated_price").isNotNull(), F.lit(1)))
            .alias("itens_aceitos_com_preco"),
        F.sum(F.when(accepted, F.col("i.negotiated_price") * F.col("i.quantity")))
            .alias("valor_negociado_aceito"),
    ).alias("i")
    suppliers = silver("suppliers").alias("s")
    return (suppliers
        .join(proposal_counts, F.col("p.supplier_id") == F.col("s.supplier_id"), "left")
        .join(items, F.col("i.supplier_id") == F.col("s.supplier_id"), "left")
        .select(
            F.col("s.supplier_id"),
            F.coalesce(F.col("p.propostas_abertas"), F.lit(0)).alias("propostas_abertas_recebidas"),
            F.coalesce(F.col("p.propostas_aceitas"), F.lit(0)).alias("propostas_aceitas"),
            F.coalesce(F.col("p.propostas_encerradas"), F.lit(0)).alias("propostas_encerradas"),
            (F.lit(100.0) * F.col("p.propostas_aceitas") /
             F.nullif(F.col("p.propostas_encerradas"), F.lit(0)))
                .alias("taxa_aceite_encerradas_percentual"),
            F.coalesce(F.col("i.itens_aceitos_sem_preco"), F.lit(0)).alias("itens_aceitos_sem_preco"),
            (F.lit(100.0) * F.col("i.itens_aceitos_com_preco") /
             F.nullif(F.col("i.itens_aceitos"), F.lit(0)))
                .alias("cobertura_preco_itens_aceitos_percentual"),
            F.col("i.valor_negociado_aceito"),
        ))


def kpi_fornecedor_valor_aceito_produto():
    source = _supplier_proposal_items().where(F.col("p.proposal_status") == "ACCEPTED")
    models = silver("product_models").alias("m")
    brands = silver("brands").alias("b")
    return (source
        .join(models, F.col("m.model_id") == F.col("o.model_id"), "left")
        .join(brands, F.col("b.brand_id") == F.col("m.brand_id"), "left")
        .groupBy(F.col("o.supplier_id"), F.col("o.model_id"),
                 F.col("b.brand_name"), F.col("m.model_name"))
        .agg(F.sum(F.col("i.negotiated_price") * F.col("i.quantity"))
             .alias("valor_negociado_aceito")))


def kpi_fornecedor_valor_aceito_mes():
    source = _supplier_proposal_items().where(F.col("p.proposal_status") == "ACCEPTED")
    return (source
        .groupBy(F.col("o.supplier_id"),
                 F.date_trunc("MONTH", F.col("p.created_at")).alias("mes_criacao_proposta_proxy"))
        .agg(F.sum(F.col("i.negotiated_price") * F.col("i.quantity"))
             .alias("valor_negociado_aceito")))


def kpi_fornecedor_ofertas_aceitas_data():
    source = _supplier_proposal_items().where(F.col("p.proposal_status") == "ACCEPTED")
    return (source
        .groupBy(F.col("o.supplier_id"), F.col("o.offer_id"), F.col("o.model_id"),
                 F.to_date(F.col("p.created_at")).alias("data_criacao_proposta_proxy"))
        .agg(F.countDistinct("p.proposal_id").alias("propostas_aceitas")))


GOLD_BUILDERS = {
    name: builder for name, builder in globals().items()
    if name.startswith("kpi_") and callable(builder)
}

SUPPLIER_TABLES = frozenset(
    name for name in GOLD_BUILDERS if name.startswith("kpi_fornecedor_")
)
