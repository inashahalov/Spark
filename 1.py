from pyspark.sql import SparkSession

# Создание сессии Spark
spark = SparkSession.builder \
    .appName("PySpark Example") \
    .getOrCreate()

# Задействуем мощь Spark
data = [("Alice", 1), ("Bob", 2), ("Cathy", 3)]
df = spark.createDataFrame(data, ["Name", "Age"])
df.show()

# Завершение Spark сессии
spark.stop()