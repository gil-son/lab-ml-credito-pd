"""CLI: gera o CSV sintético de crédito (não versionado) para uso local e testes.

Não depende de Spark: escreve com ``csv`` da biblioteca padrão.
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

from credito_pd_model.data.credit_schema import CREDIT_COLUMNS
from credito_pd_model.data.synthetic_credit import generate_credit_rows
from credito_pd_model.logging_config import get_logger

logger = get_logger(__name__)

DEFAULT_OUTPUT = "data/sample/credito_pd.csv"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Gera o dataset sintético de crédito (PD) em CSV.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--n-rows", type=int, default=50_000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args(argv)

    rows = generate_credit_rows(args.n_rows, args.seed)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(CREDIT_COLUMNS)
        writer.writerows(["" if value is None else value for value in row] for row in rows)

    logger.info(
        "Dataset sintético de crédito gerado",
        extra={"extra_fields": {"path": str(output), "row_count": len(rows), "seed": args.seed}},
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
