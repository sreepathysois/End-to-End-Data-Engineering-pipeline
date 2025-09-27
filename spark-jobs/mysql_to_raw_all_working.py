from pyspark.sql import SparkSession

# MySQL connection
mysql_host = "172.18.181.60"
mysql_user = "cdc"
mysql_password = "msis@123"
mysql_db = "retaildb"

def get_spark():
    spark = (
        SparkSession.builder.appName("MySQL_List_Tables")
        .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33")  # ensure JDBC driver
        .getOrCreate()
    )
    return spark

if __name__ == "__main__":
    spark = get_spark()

    jdbc_url = f"jdbc:mysql://{mysql_host}:3306/{mysql_db}"

    # Query INFORMATION_SCHEMA to list all tables in retaildb
    tables_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "information_schema.tables")
        .option("user", mysql_user)
        .option("password", mysql_password)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .load()
        .filter(f"table_schema = '{mysql_db}'")
    )

    tables_df.select("table_name").show(truncate=False)

    spark.stop()

