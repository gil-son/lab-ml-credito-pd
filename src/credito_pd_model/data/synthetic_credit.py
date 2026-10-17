"""Geração determinística de dados sintéticos de crédito (PD) em painel por safra.

Cada linha é um contrato distinto (``NroContrato`` único), com 12 safras
(``AnoMesReferencia`` 202401..202412) e alvo ``bad_90`` (~10-15% de maus).
O sinal vem de um score latente com ruído, mais uma deriva leve entre safras,
para que o OOT seja um pouco mais difícil que o DES. Nada aqui vem de dados reais.

O CSV resultante NÃO é versionado (ver ``.gitignore``): gere com
``python -m credito_pd_model.cli.generate_credit_data``.
"""

from __future__ import annotations

import math
import random

from pyspark.sql import DataFrame, SparkSession

from credito_pd_model.data.credit_schema import CREDIT_COLUMNS, RAW_CREDIT_SCHEMA

SAFRAS: tuple[int, ...] = tuple(202401 + i for i in range(12))

_PRODUCTS = ("consignado", "cartao", "pessoal")
_PRODUCT_WEIGHTS = (0.40, 0.35, 0.25)
_PRODUCT_EFFECT = {"consignado": -0.5, "cartao": 0.1, "pessoal": 0.4}
_TERMS = (12, 24, 36, 48, 60, 72, 84)
_DELAY_VALUES = (0, 1, 2, 3, 4, 5)
_DELAY_WEIGHTS = (55, 20, 11, 7, 4, 3)

_INTERCEPT = -3.15
_NOISE_STD = 0.5
_SAFRA_DRIFT = 0.03
_NULL_RATE_INCOME = 0.06
_NULL_RATE_UTILIZATION = 0.04


def _clip(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _rows_per_safra(n_rows: int) -> list[int]:
    base, extra = divmod(n_rows, len(SAFRAS))
    return [base + (1 if i < extra else 0) for i in range(len(SAFRAS))]


def generate_credit_rows(n_rows: int = 50_000, seed: int = 42) -> list[tuple]:
    """Gera ``n_rows`` contratos distintos, repartidos entre as 12 safras."""
    rng = random.Random(seed)
    rows: list[tuple] = []
    contract_index = 0
    for month_index, (safra, count) in enumerate(zip(SAFRAS, _rows_per_safra(n_rows), strict=True)):
        for _ in range(count):
            product = rng.choices(_PRODUCTS, weights=_PRODUCT_WEIGHTS)[0]
            age = int(_clip(rng.gauss(40, 12), 19, 75))
            income = round(math.exp(rng.gauss(8.0, 0.5)), 2)
            tenure = rng.randint(0, 120)
            term = rng.choice(_TERMS)
            contract_value = round(income * rng.uniform(1.0, 8.0), 2)
            utilization = round(_clip(rng.gauss(0.45, 0.25), 0.0, 1.2), 4)
            delays = rng.choices(_DELAY_VALUES, weights=_DELAY_WEIGHTS)[0]

            installment_ratio = contract_value / term / income
            score = (
                _INTERCEPT
                + 1.1 * (utilization - 0.45)
                + 0.45 * delays
                - 0.8 * (math.log(income) - 8.0)
                + 4.0 * (installment_ratio - 0.10)
                - 0.012 * (tenure - 60) / 10
                - 0.02 * (age - 40) / 10
                + _PRODUCT_EFFECT[product]
                + _SAFRA_DRIFT * month_index
                + rng.gauss(0.0, _NOISE_STD)
            )
            bad_90 = 1 if rng.random() < 1.0 / (1.0 + math.exp(-score)) else 0

            rows.append(
                (
                    f"CTR-{contract_index:07d}",
                    safra,
                    product,
                    age,
                    None if rng.random() < _NULL_RATE_INCOME else income,
                    tenure,
                    contract_value,
                    term,
                    None if rng.random() < _NULL_RATE_UTILIZATION else utilization,
                    delays,
                    bad_90,
                )
            )
            contract_index += 1
    return rows


def generate_credit_df(spark: SparkSession, n_rows: int = 50_000, seed: int = 42) -> DataFrame:
    return spark.createDataFrame(generate_credit_rows(n_rows, seed), schema=RAW_CREDIT_SCHEMA)


__all__ = ["CREDIT_COLUMNS", "SAFRAS", "generate_credit_df", "generate_credit_rows"]
