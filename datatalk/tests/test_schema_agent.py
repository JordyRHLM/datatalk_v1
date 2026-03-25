"""Test del Schema Agent. Corre sin credenciales Azure."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from datatalk.agents.schema_agent import run, schema_to_prompt_text
import datatalk.data.generate_test_data  # genera el xlsx si no existe


def test_schema_agent():
    file_path = Path(__file__).parent.parent / "data" / "ventas_test.xlsx"
    print("=" * 55)
    print("TEST: Schema Agent")
    print("=" * 55)

    schema = run(str(file_path))

    assert schema["sql_ready"] is True
    assert schema["row_count"] == 300, f"Esperaba 300, got {schema['row_count']}"
    assert len(schema["columns"]) == 5, f"Esperaba 5, got {len(schema['columns'])}"

    print(f"✓ Filas: {schema['row_count']}")
    print(f"✓ Columnas: {[c['name'] for c in schema['columns']]}")
    for col in schema["columns"]:
        print(f"  {col['name']} → {col['category']} | ej: {col['examples']}")
    print("\n--- Prompt text ---")
    print(schema_to_prompt_text(schema))
    print("\n✓ Schema Agent: todos los tests pasaron")


if __name__ == "__main__":
    test_schema_agent()
