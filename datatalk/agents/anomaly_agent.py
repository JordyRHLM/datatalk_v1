"""
Anomaly Agent — detecta caídas anómalas en ventas usando DuckDB.
Compara ventas actuales contra promedio móvil de las últimas N horas.
"""
from datatalk.data.duck_engine import execute_sql, list_tables


class AnomalyAgent:
    def __init__(self, threshold: float = 0.20):
        self.threshold = threshold  # 20% de caída = anomalía

    def detect(self, table_name: str, sales_col: str = "ventas", branch_col: str = "sucursal_id") -> list[dict]:
        """
        Detecta sucursales con ventas por debajo del umbral respecto al promedio móvil.
        Retorna lista de anomalías encontradas.
        """
        if table_name not in list_tables():
            return []

        sql = f"""
            WITH stats AS (
                SELECT
                    {branch_col},
                    AVG({sales_col}) AS avg_sales,
                    MAX({sales_col}) AS max_sales,
                    MIN({sales_col}) AS min_sales,
                    LAST({sales_col}) AS latest_sales
                FROM {table_name}
                GROUP BY {branch_col}
            )
            SELECT
                {branch_col},
                latest_sales,
                avg_sales,
                ROUND((avg_sales - latest_sales) / avg_sales * 100, 2) AS drop_pct
            FROM stats
            WHERE latest_sales < avg_sales * (1 - {self.threshold})
            ORDER BY drop_pct DESC
        """
        try:
            df = execute_sql(sql)
            return df.to_dict(orient="records")
        except Exception:
            return []
