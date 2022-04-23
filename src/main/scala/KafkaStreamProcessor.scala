import com.datastax.oss.driver.api.core.uuid.Uuids
import org.apache.spark.sql.cassandra.DataFrameWriterWrapper
import org.apache.spark.sql.functions.{col, from_json, udf}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

final case class MultiPlayerScoreEvent(
  playerId: Int,
  playerName: String,
  playerScore: Int
)

object KafkaStreamProcessor {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("KafkaStreamProcessor")
      .getOrCreate

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "kafka-spark-streaming")
      .option("startingOffsets", "earliest")
      .load()

    val multiPlayerSchema = new StructType()
      .add("playerId", IntegerType)
      .add("playerName", StringType)
      .add("playerScore", IntegerType)

    import spark.implicits._
    val playerScoreEvent = df
      .selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), multiPlayerSchema).as("data"))
      .select("data.*")
      .map(row => MultiPlayerScoreEvent(row.getInt(0), row.getString(1), row.getInt(2)))

    val makeUUID = udf(() => Uuids.timeBased().toString)

    val playerScoreDataFrame = playerScoreEvent
      .withColumn("id", makeUUID())
      .withColumnRenamed("playerId", "player_id")
      .withColumnRenamed("playerName", "player_name")
      .withColumnRenamed("playerScore", "player_score")

    // stream to console
//    playerScoreEvent.writeStream
//      .format("console")
//      .outputMode("append")
//      .start()
//      .awaitTermination()

    // stream to cassandra
    playerScoreDataFrame
      .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .foreachBatch { (batchDF: DataFrame, batchID: Long) =>
        println(s"Writing to Cassandra batchId: $batchID, event: $batchDF")
        batchDF
          .write
          .cassandraFormat("multiplayer_score_event", "streaming")
          .mode("append")
          .save()
      }
      .outputMode("update")
      .start()
      .awaitTermination()

    // another kafka topic
//    df.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
//      .writeStream
//      .format("kafka")
//      .outputMode("append")
//      .option("kafka.bootstrap.servers", "localhost:9092")
//      .option("topic", "kafka-spark-streaming")
//      .start()
//      .awaitTermination()
  }
}