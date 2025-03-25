# Energy-Petroleum-Pipeline

Este proyecto implementa un pipeline ETL (Extract, Transform, Load) utilizando [Prefect](https://www.prefect.io/) para la automatización y orquestación de la extracción, transformación y carga de datos sobre energía y petróleo.

## 📌 Características

- **Extracción**: Obtiene datos desde una API, archivos Excel y CSV.
- **Transformación**: Limpia, formatea y enriquece los datos.
- **Carga**: Guarda los datos en una base de datos PostgreSQL.
- **Notificaciones**: Alertas mediante Slack en caso de errores o éxito.
- **Automatización**: Configuración de despliegue con Prefect y cron jobs.

## 📂 Estructura del Proyecto

```
├── flows/
│   ├── extract.py        # Módulo de extracción de datos
│   ├── transform.py      # Módulo de transformación de datos
│   ├── load.py           # Módulo de carga de datos
│   ├── notifications.py  # Notificaciones a Slack
├── etl.py                # Definición del pipeline ETL principal
├── etl_pipeline.py       # Despliegue del pipeline
├── requirements.txt      # Dependencias del proyecto
├── .env                  # Variables de entorno
```

## 🚀 Instalación

1. **Clona el repositorio**

```sh
git clone <repo-url>
cd Energy-Petroleum-Pipeline
```

2. **Crea un entorno virtual y actívalo**

```sh
python -m venv venv
source venv/bin/activate  # En Windows usa: venv\Scripts\activate
```

3. **Instala las dependencias**

```sh
pip install -r requirements.txt
```

## ⚙️ Configuración

Crea un archivo `.env` en la raíz del proyecto con las siguientes variables:

```
DATABASE_URL=<tu_conexion_postgres>
API_BASE_URL=<url_de_la_api>
API_KEY=<tu_api_key>
DATE=<fecha_inicio>
```

## ▶️ Ejecución

Para correr el pipeline ETL de forma manual:

```sh
python etl.py
```

Para registrar el despliegue con Prefect:

```sh
python etl_pipeline.py
```

## 📊 Tecnologías Usadas

- **Prefect**: Orquestación del pipeline
- **Pandas**: Procesamiento de datos
- **SQLAlchemy**: Interacción con PostgreSQL
- **Requests**: Consumo de API
- **Python-dotenv**: Gestión de variables de entorno

## 📬 Notificaciones

El pipeline envía alertas mediante Slack en caso de errores o éxito. Asegúrate de configurar tu webhook en Prefect antes de ejecutar el pipeline.

## 🏗 Futuras Mejoras

- Implementación de pruebas unitarias
- Soporte para más fuentes de datos
- Optimización del rendimiento

---

📌 *Autor: Jesus Alberto Cogua Mayorga*

