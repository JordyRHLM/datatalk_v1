"""Tests para el módulo de limpieza de datos."""
import pandas as pd
import pytest
from datatalk.data.cleaner import normalize_column_name, clean_dataframe


def test_normalize_column_name():
    assert normalize_column_name("Fecha Venta") == "fecha_venta"
    assert normalize_column_name("  Sucursal ID  ") == "sucursal_id"
    assert normalize_column_name("Año/Mes") == "ano_mes"
    assert normalize_column_name("Núm. Factura") == "num__factura"


def test_clean_dataframe_removes_empty_rows():
    df = pd.DataFrame({
        "fecha": ["2024-01-01", None, "2024-01-03"],
        "ventas": [100, None, 300],
        "sucursal": ["norte", None, "sur"],
    })
    clean_df, report = clean_dataframe(df)
    assert len(clean_df) == 2
    assert any(c["type"] == "empty_rows_removed" for c in report["changes"])


def test_clean_dataframe_removes_duplicates():
    df = pd.DataFrame({
        "fecha": ["2024-01-01", "2024-01-01"],
        "ventas": [100, 100],
    })
    clean_df, report = clean_dataframe(df)
    assert len(clean_df) == 1
    assert any(c["type"] == "duplicates_removed" for c in report["changes"])


def test_clean_dataframe_normalizes_columns():
    df = pd.DataFrame({"Fecha Venta": ["2024-01-01"], "Total $": [100]})
    clean_df, report = clean_dataframe(df)
    assert "fecha_venta" in clean_df.columns
    assert any(c["type"] == "columns_renamed" for c in report["changes"])
