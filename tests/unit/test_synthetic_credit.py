from __future__ import annotations

from collections import Counter

from credito_pd_model.data.credit_schema import CREDIT_COLUMNS, OOT_START_SAFRA
from credito_pd_model.data.synthetic_credit import SAFRAS, generate_credit_df, generate_credit_rows


def test_rows_are_deterministic_for_same_seed():
    assert generate_credit_rows(600, seed=1) == generate_credit_rows(600, seed=1)
    assert generate_credit_rows(600, seed=1) != generate_credit_rows(600, seed=2)


def test_contracts_are_distinct_and_spread_over_twelve_safras():
    rows = generate_credit_rows(1_200, seed=42)
    assert len(rows) == 1_200
    assert len({row[0] for row in rows}) == 1_200
    counts = Counter(row[1] for row in rows)
    assert tuple(sorted(counts)) == SAFRAS
    assert set(counts.values()) == {100}


def test_row_count_not_divisible_by_twelve_is_fully_distributed():
    assert len(generate_credit_rows(1_205, seed=42)) == 1_205


def test_bad_rate_is_in_expected_range_and_target_is_binary():
    rows = generate_credit_rows(12_000, seed=42)
    targets = [row[-1] for row in rows]
    assert set(targets) <= {0, 1}
    assert 0.08 <= sum(targets) / len(targets) <= 0.18


def test_nullable_features_have_nulls_and_required_ones_do_not():
    rows = generate_credit_rows(6_000, seed=42)
    columns = {name: [row[i] for row in rows] for i, name in enumerate(CREDIT_COLUMNS)}
    assert any(v is None for v in columns["renda_mensal"])
    assert any(v is None for v in columns["utilizacao_limite"])
    for name, values in columns.items():
        if name not in {"renda_mensal", "utilizacao_limite"}:
            assert all(v is not None for v in values), name


def test_des_and_oot_both_have_data():
    safras = [row[1] for row in generate_credit_rows(1_200, seed=42)]
    assert any(s < OOT_START_SAFRA for s in safras)
    assert any(s >= OOT_START_SAFRA for s in safras)


def test_generate_credit_df_matches_schema(spark):
    df = generate_credit_df(spark, n_rows=240, seed=3)
    assert df.columns == list(CREDIT_COLUMNS)
    assert df.count() == 240
