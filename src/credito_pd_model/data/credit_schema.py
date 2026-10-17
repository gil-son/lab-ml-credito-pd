"""Schema explícito dos dados brutos de crédito (PD sintético).

Convive com ``schemas.py`` (churn herdado do template) até a Fase 3 estar completa;
ver ``synthetic.py`` para o que deve ser removido depois.
"""

from __future__ import annotations

from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

CONTRACT_ID_COLUMN = "NroContrato"
SAFRA_COLUMN = "AnoMesReferencia"
CREDIT_TARGET_COLUMN = "bad_90"

# DES = safras < OOT_START_SAFRA; OOT = safras >= OOT_START_SAFRA.
OOT_START_SAFRA = 202410

RAW_CREDIT_SCHEMA = StructType(
    [
        StructField("NroContrato", StringType(), nullable=False),
        StructField("AnoMesReferencia", IntegerType(), nullable=False),
        StructField("tipo_produto", StringType(), nullable=False),
        StructField("idade", IntegerType(), nullable=False),
        StructField("renda_mensal", DoubleType(), nullable=True),
        StructField("tempo_relacionamento_meses", IntegerType(), nullable=False),
        StructField("valor_contrato", DoubleType(), nullable=False),
        StructField("prazo_meses", IntegerType(), nullable=False),
        StructField("utilizacao_limite", DoubleType(), nullable=True),
        StructField("qtd_atrasos_12m", IntegerType(), nullable=False),
        StructField("bad_90", IntegerType(), nullable=False),
    ]
)

CREDIT_COLUMNS: tuple[str, ...] = tuple(f.name for f in RAW_CREDIT_SCHEMA.fields)

CREDIT_NUMERIC_FEATURES: tuple[str, ...] = (
    "idade",
    "renda_mensal",
    "tempo_relacionamento_meses",
    "valor_contrato",
    "prazo_meses",
    "utilizacao_limite",
    "qtd_atrasos_12m",
)

CREDIT_CATEGORICAL_FEATURES: tuple[str, ...] = ("tipo_produto",)

ALLOWED_PRODUCT_TYPES: tuple[str, ...] = ("consignado", "cartao", "pessoal")
