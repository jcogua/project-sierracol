import yaml
from sqlalchemy import create_engine
from models import Base

# Leer configuración desde YAML
def get_database_url():
    with open('config/database.yaml', 'r') as file:
        config = yaml.safe_load(file)
    pg = config['postgresql']
    return f"postgresql://{pg['user']}:{pg['password']}@{pg['host']}:{pg['port']}/{pg['database']}"

def create_db_tables():
    engine = create_engine(get_database_url())
    Base.metadata.create_all(engine)
    print("✅ Tablas creadas correctamente en la base de datos.")

if __name__ == "__main__":
    create_db_tables()
