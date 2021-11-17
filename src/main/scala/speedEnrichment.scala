import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructField, StructType}

object speedEnrichment {
  def main(args:Array[String]):Unit= {

    val spark = SparkSession.builder.appName("streamDF").getOrCreate()

    val custom_schema = StructType(Array(
      StructField("Liked", BooleanType, true),
      StructField("user_id", IntegerType, true),
      StructField("Video_end_type", IntegerType, true),
      StructField("minutes_played", IntegerType, true),
      StructField("video_id", IntegerType, true),
      StructField("Geo_cd", StringType, true),
      StructField("Channel_id", IntegerType, true),
      StructField("creator_id", IntegerType, true),
      StructField("Timestamp", StringType, true),
      StructField("Disliked", BooleanType, true)
    ))

    val sparkstreamingDF: DataFrame = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "ip-70-0-31-179.us-east-2.compute.internal:9092").option("subscribe", "mobile_activity").load()
    val sparkstreamingDF_string: DataFrame = sparkstreamingDF.selectExpr("CAST(value AS STRING)")

    val json_df = sparkstreamingDF_string.select(from_json(col("value"), custom_schema).as("data")).select("data.*")
    val json_df_replacewith_null = json_df.na.replace(Seq("user_id", "video_id", "channel_id", "creator_id", "geo_cd", "timestamp"), Map("" -> null))


    val json_df_with_NULL = json_df_replacewith_null.filter("Geo_cd is null OR creator_id is null")

    val json_df_without_NULL = json_df_replacewith_null.filter("Geo_cd is NOT NULL AND creator_id is NOT NULL")
    json_df_with_NULL.createOrReplaceTempView("json_df_with_NULL")
    val enriched_json_df = spark.sql("SELECT a.liked,a.user_id ,IF (a.video_end_type IS NULL,3,a.video_end_type) AS video_end_type ,a.minutes_played,a.video_id,b.geo_cd,a.channel_id,c.creator_id ,a.timestamp ,a.disliked FROM json_df_with_NULL a LEFT JOIN channel_geocd_df b ON a.channel_id = b.channel_id LEFT JOIN video_creator_df c on a.video_id = c.video_id")
    val json_enriched_writeDF: Dataset[Row] = enriched_json_df.union(json_df_without_NULL)

    json_enriched_writeDF.writeStream.outputMode("append").format("parquet").option("path", "midproject2/output/speed_data/").option("checkpointLocation", "checkpoint").start()

  }
}
