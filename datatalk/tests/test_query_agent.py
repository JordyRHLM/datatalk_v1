"""Test del Query Agent con mocks — sin gastar créditos Azure."""
import sys
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

FILE_PATH = str(Path(__file__).parent.parent / "data" / "ventas_test.xlsx")


def _mock_llm(responses):
    call_count = {"n": 0}
    def _inner(system, user):
        idx = min(call_count["n"], len(responses) - 1)
        call_count["n"] += 1
        return responses[idx]
    return _inner


def test_sql_correcto_primer_intento():
    print("\nTEST 1: SQL correcto en el primer intento")
    sql = "SELECT categoria, SUM(ventas_total) as total FROM data_table GROUP BY categoria ORDER BY total DESC"
    with patch("datatalk.agents.query_agent._call_llm", _mock_llm([sql])):
        from datatalk.agents import query_agent
        result = query_agent.run_with_validation("RANKING", "¿Categorías con más ventas?", FILE_PATH)
    assert result["success"] is True
    assert result["attempts"] == 1
    assert result["autocorrected"] is False
    print(f"  ✓ Éxito en intento 1 — {len(result['data'])} filas")


def test_autocorreccion():
    print("\nTEST 2: Autocorrección en intento 2")
    roto = "SELECT columna_inexistente FROM data_table"
    ok = "SELECT categoria, SUM(ventas_total) as total FROM data_table GROUP BY categoria"
    with patch("datatalk.agents.query_agent._call_llm", _mock_llm([roto, ok])):
        from datatalk.agents import query_agent
        result = query_agent.run_with_validation("RANKING", "¿Categorías con más ventas?", FILE_PATH)
    assert result["success"] is True
    assert result["attempts"] == 2
    assert result["autocorrected"] is True
    print(f"  ✓ Autocorregido en intento 2")


def test_tres_fallos():
    print("\nTEST 3: 3 fallos → mensaje amigable")
    roto = "SELECT col_inexistente FROM data_table"
    with patch("datatalk.agents.query_agent._call_llm", _mock_llm([roto] * 3)):
        from datatalk.agents import query_agent
        result = query_agent.run_with_validation("AGREGACION", "¿Total ventas?", FILE_PATH)
    assert result["success"] is False
    assert result["attempts"] == 3
    assert "traceback" not in result["user_message"].lower()
    assert "duckdb" not in result["user_message"].lower()
    assert result["error_detail"] is not None
    print(f"  ✓ Mensaje usuario: {result['user_message']}")


def test_generate_sql():
    print("\nTEST 4: generate_sql()")
    from datatalk.agents.schema_agent import run as schema_run
    sql_esperado = "SELECT zona, AVG(ventas_total) FROM data_table GROUP BY zona"
    with patch("datatalk.agents.query_agent._call_llm", _mock_llm([sql_esperado])):
        from datatalk.agents import query_agent
        schema = schema_run(FILE_PATH)
        sql = query_agent.generate_sql("COMPARATIVA", schema, "Promedio ventas por zona")
    assert sql == sql_esperado
    print(f"  ✓ SQL: {sql}")


if __name__ == "__main__":
    print("=" * 55)
    print("TEST: Query Agent — Validation Loop")
    print("=" * 55)
    test_generate_sql()
    test_sql_correcto_primer_intento()
    test_autocorreccion()
    test_tres_fallos()
    print("\n✓ Todos los tests del Query Agent pasaron")
