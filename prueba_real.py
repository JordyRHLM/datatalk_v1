"""
Prueba real contra Azure OpenAI — correr DESPUÉS de configurar .env
"""
import sys
sys.path.insert(0, ".")

from datatalk.agents import query_agent

resultado = query_agent.run_with_validation(
    intent="RANKING",
    question="¿Cuáles son las categorías con más ventas?",
    file_path="datatalk/data/ventas_test.xlsx",
)

print("Éxito:", resultado["success"])
print("Intentos:", resultado["attempts"])
print("Autocorregido:", resultado["autocorrected"])
print("SQL:\n", resultado["sql_final"])
if resultado["data"] is not None:
    print("\nResultados:")
    print(resultado["data"])
print("\nMensaje:", resultado["user_message"])
