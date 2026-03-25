"""
Forecast Agent — predicción de ventas con series temporales.
Usa DuckDB para preparar los datos y Prophet para el modelo.
TODO: instalar prophet (pip install prophet) cuando se configure el entorno.
"""
from datatalk.data.duck_engine import execute_sql


class ForecastAgent:
    def forecast(self, table_name: str, date_col: str, sales_col: str, branch_id: str, periods: int = 7) -> dict:
        """
        Genera predicción de ventas para los próximos N días.
        Retorna predicción con intervalo de confianza.
        """
        # 1. Extraer serie temporal con DuckDB
        sql = f"""
            SELECT
                DATE_TRUNC('day', {date_col}) AS ds,
                SUM({sales_col}) AS y
            FROM {table_name}
            WHERE sucursal_id = '{branch_id}'
            GROUP BY ds
            ORDER BY ds
        """
        try:
            df = execute_sql(sql)
        except Exception as e:
            return {"error": str(e)}

        if len(df) < 10:
            return {"error": "Datos insuficientes para forecast (mínimo 10 días)"}

        # 2. TODO: modelo Prophet
        # from prophet import Prophet
        # model = Prophet(daily_seasonality=True)
        # model.fit(df)
        # future = model.make_future_dataframe(periods=periods)
        # forecast = model.predict(future)

        return {
            "branch_id": branch_id,
            "periods": periods,
            "status": "Prophet pendiente de instalar — datos listos",
            "data_points": len(df),
            "date_range": {
                "start": str(df["ds"].min()),
                "end": str(df["ds"].max()),
            }
        }
