from pyspark.sql.session import SparkSession
from pyspark import SparkConf


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



if __name__ == '__main__':
    # obj = FlightAnalytics("Spark_Flight_Analytics", "yarn", "1g", "1g")
    obj = FlightAnalytics("Spark_Flight_Analytics", "local[*]", "1g", "1g")

    spark = obj.spark_builder()

    df = spark.sql("show databases")
    df.show()

