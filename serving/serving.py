# ================================================================================
# REQUERIMIENTO 5: SERVING EN ASTRADB (CASSANDRA)
# ================================================================================

print("\n" + "="*80)
print("SERVING EN ASTRADB (CASSANDRA) - USANDO SPARK CONNECTOR")
print("="*80)

# --------------------------------------------------------------------------------
# Instalar y configurar Spark Cassandra Connector
# --------------------------------------------------------------------------------

print("Instalando Spark Cassandra Connector...")

# Reinstalar Spark con el conector de Cassandra
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Big Data - Cassandra Integration") \
    .config("spark.jars.packages",
            "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
    .config("spark.sql.extensions",
            "com.datastax.spark.connector.CassandraSparkExtensions") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("✓ Spark reiniciado con Cassandra Connector")


# --------------------------------------------------------------------------------
# Configuración de conexión a AstraDB
# --------------------------------------------------------------------------------

#TODO configurar bien la conexiona Cassandra
# COMPLETAR ESTOS CAMPOS CON TUS CREDENCIALES DE ASTRADB
ASTRA_CLIENT_ID = "TU_CLIENT_ID"  # Token: Client ID
ASTRA_CLIENT_SECRET = "TU_CLIENT_SECRET"  # Token: Client Secret
ASTRA_SECURE_BUNDLE_PATH = "/content/drive/MyDrive/Big Data - Final/secure-connect-bundle.zip"
ASTRA_DB_ID = "TU_DATABASE_ID"  # ID de tu base de datos en AstraDB
KEYSPACE = "cloud_analytics"

print("\n⚠ RECORDATORIO: Actualizar credenciales de AstraDB antes de ejecutar")
print(f"- Client ID: {ASTRA_CLIENT_ID}")
print(f"- Bundle path: {ASTRA_SECURE_BUNDLE_PATH}")
print(f"- Keyspace: {KEYSPACE}")

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Conectar para crear tablas
cloud_config = {'secure_connect_bundle': ASTRA_SECURE_BUNDLE_PATH}
auth_provider = PlainTextAuthProvider(ASTRA_CLIENT_ID, ASTRA_CLIENT_SECRET)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

print("✓ Conectado a AstraDB para crear tablas")

# Crear keyspace
create_keyspace_query = f"""
CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
WITH replication = {{'class': 'NetworkTopologyStrategy', 'datacenter1': 3}}
\"""
try:
    session.execute(create_keyspace_query)
    print(f"✓ Keyspace '{KEYSPACE}' verificado/creado")
except Exception as e:
    print(f"⚠ Keyspace ya existe o error: {e}")

session.set_keyspace(KEYSPACE)
"""

# Tabla para queries 1 y 2
create_table_1 = """
CREATE TABLE IF NOT EXISTS org_daily_usage_by_service (
    org_id text,
    usage_date date,
    service text,
    daily_cost_usd double,
    total_requests double,
    total_cpu_hours double,
    total_storage_gb_hours double,
    total_genai_tokens int,
    total_carbon_kg double,
    PRIMARY KEY ((org_id), usage_date, service)
) WITH CLUSTERING ORDER BY (usage_date DESC, service ASC);
"""
session.execute(create_table_1)
print("✓ Tabla org_daily_usage_by_service creada")


# Descomentar cuando tengas las credenciales configuradas
"""
# Configurar Spark para conectarse a AstraDB
spark.conf.set(f"spark.cassandra.connection.config.cloud.path", ASTRA_SECURE_BUNDLE_PATH)
spark.conf.set(f"spark.cassandra.auth.username", ASTRA_CLIENT_ID)
spark.conf.set(f"spark.cassandra.auth.password", ASTRA_CLIENT_SECRET)
spark.conf.set(f"spark.cassandra.connection.ssl.enabledAlgorithms", "TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA")

# Para AstraDB específicamente
spark.conf.set("spark.cassandra.connection.ssl.enabled", "true")
spark.conf.set("spark.cassandra.output.consistency.level", "LOCAL_QUORUM")

print("✓ Configuración de Spark para AstraDB completada")


# --------------------------------------------------------------------------------
# Crear tablas en Cassandra usando CQL
# --------------------------------------------------------------------------------

# Necesitamos el driver Python solo para crear las tablas (una vez)
try:
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
except ImportError:
    print("Instalando cassandra-driver...")
    !pip install cassandra-driver
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider

# Conectar para crear tablas
cloud_config = {'secure_connect_bundle': ASTRA_SECURE_BUNDLE_PATH}
auth_provider = PlainTextAuthProvider(ASTRA_CLIENT_ID, ASTRA_CLIENT_SECRET)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

print("✓ Conectado a AstraDB para crear tablas")

# Crear Keyspace (si no existe)
create_keyspace_query = f\"""
CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
WITH replication = {{'class': 'NetworkTopologyStrategy', 'datacenter1': 3}}
\"""
try:
    session.execute(create_keyspace_query)
    print(f"✓ Keyspace '{KEYSPACE}' verificado/creado")
except Exception as e:
    print(f"⚠ Keyspace ya existe o error: {e}")

session.set_keyspace(KEYSPACE)

# TABLA 1: org_daily_usage_by_service
create_table_1 = \"""
CREATE TABLE IF NOT EXISTS org_daily_usage_by_service (
    org_id TEXT,
    usage_date DATE,
    service TEXT,
    daily_cost_usd DOUBLE,
    total_requests BIGINT,
    cpu_hours DOUBLE,
    storage_gb_hours DOUBLE,
    genai_tokens BIGINT,
    carbon_kg DOUBLE,
    avg_cost_per_event DOUBLE,
    last_updated TIMESTAMP,
    PRIMARY KEY ((org_id, usage_date), service)
) WITH CLUSTERING ORDER BY (service ASC)
\"""
session.execute(create_table_1)
print("✓ Tabla org_daily_usage_by_service creada")

# TABLA 2: revenue_by_org_month
create_table_2 = \"""
CREATE TABLE IF NOT EXISTS revenue_by_org_month (
    org_id TEXT,
    month TEXT,
    revenue_usd DOUBLE,
    total_credits_usd DOUBLE,
    total_tax_usd DOUBLE,
    net_revenue_usd DOUBLE,
    revenue_local_currency DOUBLE,
    local_currency TEXT,
    fx_rate DOUBLE,
    last_updated TIMESTAMP,
    PRIMARY KEY (org_id, month)
) WITH CLUSTERING ORDER BY (month DESC)
\"""
session.execute(create_table_2)
print("✓ Tabla revenue_by_org_month creada")

# TABLA 3: cost_anomaly_mart
create_table_3 = \"""
CREATE TABLE IF NOT EXISTS cost_anomaly_mart (
    org_id TEXT,
    usage_date DATE,
    service TEXT,
    daily_cost DOUBLE,
    z_score DOUBLE,
    anomaly_score DOUBLE,
    is_anomaly BOOLEAN,
    last_updated TIMESTAMP,
    PRIMARY KEY ((org_id, usage_date), service)
) WITH CLUSTERING ORDER BY (service ASC)
\"""
session.execute(create_table_3)
print("✓ Tabla cost_anomaly_mart creada")

# TABLA 4: tickets_by_org_date
create_table_4 = \"""
CREATE TABLE IF NOT EXISTS tickets_by_org_date (
    org_id TEXT,
    ticket_date DATE,
    severity TEXT,
    ticket_count BIGINT,
    sla_breach_count BIGINT,
    sla_breach_rate DOUBLE,
    avg_csat DOUBLE,
    unique_tickets BIGINT,
    avg_resolution_hours DOUBLE,
    last_updated TIMESTAMP,
    PRIMARY KEY ((org_id, ticket_date), severity)
) WITH CLUSTERING ORDER BY (severity ASC)
\"""
session.execute(create_table_4)
print("✓ Tabla tickets_by_org_date creada")

# TABLA 5: genai_tokens_by_org_date
create_table_5 = \"""
CREATE TABLE IF NOT EXISTS genai_tokens_by_org_date (
    org_id TEXT,
    usage_date DATE,
    total_tokens BIGINT,
    total_requests BIGINT,
    avg_tokens_per_request DOUBLE,
    estimated_cost_usd DOUBLE,
    last_updated TIMESTAMP,
    PRIMARY KEY (org_id, usage_date)
) WITH CLUSTERING ORDER BY (usage_date DESC)
\"""
session.execute(create_table_5)
print("✓ Tabla genai_tokens_by_org_date creada")

# Cerrar sesión de creación de tablas
cluster.shutdown()
print("✓ Todas las tablas creadas, conexión CQL cerrada")


# --------------------------------------------------------------------------------
# Cargar datos desde Gold (Parquet) a Cassandra usando Spark Connector
# --------------------------------------------------------------------------------

print("\\n" + "="*80)
print("CARGANDO DATOS A CASSANDRA CON SPARK CONNECTOR")
print("="*80)

# Función helper para cargar con manejo de errores
def load_to_cassandra(parquet_path, table_name, description):
    \"""
    Carga datos desde Parquet a Cassandra usando Spark Cassandra Connector
    \"""
    try:
        print(f"\\n[{description}] Cargando datos...")

        # Leer desde Gold
        df = spark.read.parquet(parquet_path)
        row_count = df.count()

        print(f"  Filas leídas: {row_count}")

        # Escribir a Cassandra
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table=table_name, keyspace=KEYSPACE) \
            .save()

        print(f"✓ {row_count} filas cargadas en {table_name}")
        return True

    except Exception as e:
        print(f"✗ Error cargando {table_name}: {e}")
        return False


# CARGAR MART 1: org_daily_usage_by_service
load_to_cassandra(
    f"{datalake_path}/gold/finops/org_daily_usage_by_service",
    "org_daily_usage_by_service",
    "MART 1 - FinOps Usage"
)

# CARGAR MART 2: revenue_by_org_month
load_to_cassandra(
    f"{datalake_path}/gold/finops/revenue_by_org_month",
    "revenue_by_org_month",
    "MART 2 - FinOps Revenue"
)

# CARGAR MART 3: cost_anomaly_mart
load_to_cassandra(
    f"{datalake_path}/gold/finops/cost_anomaly_mart",
    "cost_anomaly_mart",
    "MART 3 - Cost Anomalies"
)

# CARGAR MART 4: tickets_by_org_date
load_to_cassandra(
    f"{datalake_path}/gold/support/tickets_by_org_date",
    "tickets_by_org_date",
    "MART 4 - Support Tickets"
)

# CARGAR MART 5: genai_tokens_by_org_date
load_to_cassandra(
    f"{datalake_path}/gold/product/genai_tokens_by_org_date",
    "genai_tokens_by_org_date",
    "MART 5 - GenAI Usage"
)


# --------------------------------------------------------------------------------
# Verificar datos cargados usando Spark SQL
# --------------------------------------------------------------------------------

print("\\n" + "="*80)
print("VERIFICANDO DATOS EN CASSANDRA")
print("="*80)

# Leer de vuelta desde Cassandra para verificar
print("\\n--- Verificación MART 1: org_daily_usage_by_service ---")
df_verify1 = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="org_daily_usage_by_service", keyspace=KEYSPACE) \
    .load()

print(f"Total de filas en Cassandra: {df_verify1.count()}")
df_verify1.show(5, truncate=False)


print("\\n--- Verificación MART 3: cost_anomaly_mart ---")
df_verify3 = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="cost_anomaly_mart", keyspace=KEYSPACE) \
    .load()

print(f"Total de filas en Cassandra: {df_verify3.count()}")
df_verify3.filter(col("is_anomaly") == True).show(5, truncate=False)


print("\\n--- Verificación MART 5: genai_tokens_by_org_date ---")
df_verify5 = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="genai_tokens_by_org_date", keyspace=KEYSPACE) \
    .load()

print(f"Total de filas en Cassandra: {df_verify5.count()}")
df_verify5.orderBy(col("total_tokens").desc()).show(5, truncate=False)


# --------------------------------------------------------------------------------
# Queries de demostración (las 5 obligatorias del proyecto)
# --------------------------------------------------------------------------------

print("\\n" + "="*80)
print("QUERIES DE DEMOSTRACIÓN (Requisito del Proyecto)")
print("="*80)

# QUERY 1: Costos y requests diarios por org y servicio en un rango de fechas
print("\\n[QUERY 1] Costos y requests diarios por org y servicio (últimos 7 días)")
df_query1 = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="org_daily_usage_by_service", keyspace=KEYSPACE) \
    .load() \
    .filter(col("usage_date") >= date_sub(current_date(), 7)) \
    .select("org_id", "usage_date", "service", "daily_cost_usd", "total_requests") \
    .orderBy(col("daily_cost_usd").desc())

df_query1.show(10, truncate=False)


# QUERY 2: Top-N servicios por costo acumulado en los últimos 14 días
print("\\n[QUERY 2] Top-5 servicios por costo acumulado (últimos 14 días)")
df_query2 = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="org_daily_usage_by_service", keyspace=KEYSPACE) \
    .load() \
    .filter(col("usage_date") >= date_sub(current_date(), 14)) \
    .groupBy("service") \
    .agg(sum("daily_cost_usd").alias("total_cost_14d")) \
    .orderBy(col("total_cost_14d").desc()) \
    .limit(5)

df_query2.show(truncate=False)


# QUERY 3: Evolución de tickets críticos y tasa de SLA breach por día (últimos 30 días)
print("\\n[QUERY 3] Tickets críticos y SLA breach rate (últimos 30 días)")
try:
    df_query3 = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="tickets_by_org_date", keyspace=KEYSPACE) \
        .load() \
        .filter(col("ticket_date") >= date_sub(current_date(), 30)) \
        .filter(col("severity").isin(["critical", "high"])) \
        .groupBy("ticket_date") \
        .agg(
            sum("ticket_count").alias("total_critical_tickets"),
            avg("sla_breach_rate").alias("avg_sla_breach_rate")
        ) \
        .orderBy("ticket_date")

    df_query3.show(30, truncate=False)
except Exception as e:
    print(f"⚠ Datos de tickets no disponibles: {e}")


# QUERY 4: Revenue mensual con créditos/impuestos aplicados (normalizado a USD)
print("\\n[QUERY 4] Revenue mensual por organización (normalizado a USD)")
try:
    df_query4 = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="revenue_by_org_month", keyspace=KEYSPACE) \
        .load() \
        .select("org_id", "month", "revenue_usd", "total_credits_usd",
                "total_tax_usd", "net_revenue_usd") \
        .orderBy(col("net_revenue_usd").desc())

    df_query4.show(10, truncate=False)
except Exception as e:
    print(f"⚠ Datos de revenue no disponibles: {e}")


# QUERY 5: Tokens GenAI y costo estimado por día
print("\\n[QUERY 5] Tokens GenAI y costo estimado por día (últimos 30 días)")
df_query5 = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="genai_tokens_by_org_date", keyspace=KEYSPACE) \
    .load() \
    .filter(col("usage_date") >= date_sub(current_date(), 30)) \
    .groupBy("usage_date") \
    .agg(
        sum("total_tokens").alias("daily_total_tokens"),
        sum("estimated_cost_usd").alias("daily_estimated_cost"),
        avg("avg_tokens_per_request").alias("avg_tokens_per_req")
    ) \
    .orderBy("usage_date")

df_query5.show(30, truncate=False)


# --------------------------------------------------------------------------------
# Resumen final
# --------------------------------------------------------------------------------

print("\\n" + "="*80)
print("RESUMEN DE CARGA")
print("="*80)

tables_summary = [
    "org_daily_usage_by_service",
    "revenue_by_org_month",
    "cost_anomaly_mart",
    "tickets_by_org_date",
    "genai_tokens_by_org_date"
]

for table in tables_summary:
    try:
        count = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=table, keyspace=KEYSPACE) \
            .load() \
            .count()
        print(f"✓ {table}: {count:,} filas")
    except Exception as e:
        print(f"✗ {table}: Error - {e}")

print("\\n✓ Pipeline completado: Datos cargados y verificados en Cassandra")
"""

print("\n✓ Código de Cassandra con Spark Connector preparado")
print("\n" + "="*80)
print("PASOS PARA CONFIGURAR ASTRADB")
print("="*80)
print("""
1. Crear cuenta en AstraDB: https://astra.datastax.com

2. Crear una nueva base de datos:
   - Database name: cloud-analytics-db
   - Keyspace: cloud_analytics
   - Provider: GCP (recomendado para Colab)
   - Region: us-east1 (o más cercana)

3. Crear Token de aplicación:
   - Settings → Tokens → Generate Token
   - Role: Database Administrator
   - Guardar Client ID y Client Secret

4. Descargar Secure Connect Bundle:
   - Connect → Drivers → Download Bundle
   - Subir el archivo .zip a tu Google Drive
   - Actualizar ASTRA_SECURE_BUNDLE_PATH

5. Actualizar variables en el código:
   - ASTRA_CLIENT_ID = "tu_client_id"
   - ASTRA_CLIENT_SECRET = "tu_client_secret"
   - ASTRA_SECURE_BUNDLE_PATH = "/content/drive/MyDrive/..."
   - ASTRA_DB_ID = "tu_database_id" (aparece en la URL)

6. Descomentar el código y ejecutar

NOTA: El conector Spark para Cassandra es MÁS EFICIENTE que el driver Python
para grandes volúmenes de datos, ya que distribuye la carga en paralelo.
""")

# spark.stop()