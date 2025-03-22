import os
DATABASE_URL = os.getenv("DATABASE_URL")
API_KEY = os.getenv("API_KEY")


__all__ = ["DATABASE_URL", "API_KEY"]