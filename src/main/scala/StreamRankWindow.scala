import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery

object StreamRankWindow extends Serializable {
  val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
  spark.conf.set("spark.sql.shuffle.partitions", "10")
  spark.conf.set("spark.sql.default.parallelism", "10")

  implicit val sqlContext = spark.sqlContext
  import sqlContext.implicits._

  def main(args: Array[String]): Unit = {
    val inputStream: MemoryStream[GameEvent] = MemoryStream[GameEvent]

    val query = execute(inputStream.toDF())

    inputStream.addData(
      Seq(
        GameEvent("2020-09-25 14:01:31.000", 1, 65, 78, 12345, "zone11", "user1", "Windows"),
        GameEvent("2020-09-25 14:01:41.135", 2, 65, 75, 12345, "zone13", "user2", "Android"),
        GameEvent("2020-09-25 14:01:41.135", 3, 65, 75, 123456, "zone13", "user3", "Android"),
        GameEvent("2020-09-25 14:01:53.135", 4, 60, 75, 12345, "zone13", "user4", "Android"),
        GameEvent("2020-09-25 14:01:53.135", 5, 60, 75, 12346, "zone13", "user5", "Android"),
        GameEvent("2020-09-25 14:02:11.000", 1, 65, 72, 12345, "zone11", "user4", "Windows")
      )
    )
    query.processAllAvailable()
    spark.sql("SELECT * FROM stream_rank").show(false)
  }

  def execute(inputStream: DataFrame): StreamingQuery = {
    import scala.concurrent.duration._

    val rankUdf = udf((r: Seq[Row]) => {
      r.map{ case Row(user_id: Int, user_level: Int, user_exp: Int, user_zone: String, user_name: String, os: String) =>
        (user_id, user_level, user_exp, user_zone, user_name, os)}
        .sortBy{ case (user_id, user_level, user_exp, user_zone, user_name, os)  => (-user_level, -user_exp)}
        .zipWithIndex.map{ r =>
        val user = r._1
        (user._1, user._2, user._3, user._4, user._5, user._6, r._2 + 1)
      }
    })

    val userRank = inputStream
      .withColumn("event_time_ts", unix_timestamp(col("time"), "yyyy-MM-dd HH:mm:ss.SSS").cast(TimestampType))
      .withWatermark("event_time_ts", 1.minute.toString())
      .groupBy(
        window(col("event_time_ts"), 1.minute.toString()),
        col("user_server")
      )
      .agg(
        rankUdf(collect_list(struct("user_id", "user_level", "user_exp", "user_zone", "user_name", "os")))
        .as("users")
      )
      .withColumn("exploded_col", explode(col("users")))
      .drop("users")
      .withColumn("user_id", col("exploded_col._1"))
      .withColumn("user_level", col("exploded_col._2"))
      .withColumn("user_exp", col("exploded_col._3"))
      .withColumn("user_zone", col("exploded_col._4"))
      .withColumn("user_name", col("exploded_col._5"))
      .withColumn("os", col("exploded_col._6"))
      .withColumn("server_rank", col("exploded_col._7"))
      .drop("exploded_col")

    userRank
      .writeStream
      .queryName("stream_rank")
      .format("memory")
      .outputMode("update")
      .start()
  }
}