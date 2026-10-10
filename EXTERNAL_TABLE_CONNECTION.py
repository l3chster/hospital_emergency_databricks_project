user = dbutils.secrets.get("scope_blech", "postgresUser-blech")
pwd = dbutils.secrets.get("scope_blech", "postgresPassword-blech")

spark.sql(f"""
CREATE CONNECTION IF NOT EXISTS postgresql_connection
TYPE POSTGRESQL
OPTIONS (
  host 'ep-twilight-tree-b49zmtt0.c-6.us-east-2.aws.neon.tech',
  port '5432',
  user '{user}',
  password '{pwd}'
)
""")

spark.sql("""
CREATE FOREIGN CATALOG IF NOT EXISTS hospital_db
    USING CONNECTION postgresql_connection
    OPTIONS (database 'neondb')
""")
