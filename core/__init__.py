import os
from dotenv import load_dotenv

# Cargar variables de entorno desde un archivo .env si existe
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
API_BASE_URL = os.getenv("API_BASE_URL")
API_KEY = os.getenv("API_KEY")
DATE = os.getenv("DATE", "2023-01-01")  # Valor por defecto si no se pasa la fecha

# Validaciones para asegurarse de que las variables críticas estén configuradas
if not DATABASE_URL:
    raise ValueError("DATABASE_URL no está definido en las variables de entorno.")
if not API_BASE_URL:
    raise ValueError("API_BASE_URL no está definido en las variables de entorno.")
if not API_KEY:
    raise ValueError("API_KEY no está definido en las variables de entorno.")

__all__ = ["DATABASE_URL", "API_BASE_URL", "API_KEY", "DATE"]