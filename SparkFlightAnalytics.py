from pyspark.sql.session import SparkSession
from pyspark import SparkConf
import pyspark

class FlightAnalytics:
    def __init__(self, job_name, master, driver_memory, executor_memory):
        self.appName = job_name
        self.master = master
        self.driver_memory = driver_memory
        self.executor_memory = executor_memory

    def spark_builder(self):
        conf = SparkConf()
        conf.set("spark.driver.memory", self.driver_memory)
        conf.set("spark.executor.memory", self.executor_memory)

        spark_session = SparkSession.builder.master(self.master).appName(self.appName).config(conf=conf).getOrCreate()
        return spark_session

    def read_from_hive(self, database, table):
        query = f"select * from {database}.{table}"
        print(query)
        df = spark.sql(query)
        return df

    def dataframe_join_tables(self, df1, df2, df1_on, df2_on, join_type):
        df = df1.join(df2, df1[df1_on]==df2[df2_on], join_type)
        return df


if __name__ == '__main__':
    obj = FlightAnalytics("Spark_Flight_Analytics", "yarn", "1g", "1g")
    # obj = FlightAnalytics("Spark_Flight_Analytics", "local[*]", "1g", "1g")

    spark = obj.spark_builder()

    df_airport = obj.read_from_hive("flight_landing", "airport")
    df_airport.printSchema()

    df_routes = obj.read_from_hive("flight_landing", "routes")
    df_routes.printSchema()

    df_join: pyspark.sql.DataFrame = obj.dataframe_join_tables(df_airport, df_routes, "airport_id", "destination_airport_id", "inner")
    df_join.printSchema()

    df_final = df_join.select("airport_name", "city", "country", "stops")
    df_final.limit(5).show()

    df_group = df_final.groupby("airport_name").count().orderBy("airport_name")

    df_group.write.saveAsTable("flight_landing.aggregates", mode="overwrite")