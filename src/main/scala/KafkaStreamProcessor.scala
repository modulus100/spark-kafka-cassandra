import org.apache.spark.sql.SparkSession

object KafkaStreamProcessor {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("KafkaStreamProcessor")
      .getOrCreate

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9090")
      .option("subscribe", "topic1")
      .load()

    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
  }
}
