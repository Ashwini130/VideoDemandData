import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success, Try}

object batchEnrichment{

  def main(args:Array[String]):Unit={
    val spark = SparkSession.builder.appName("batchDF").getOrCreate()

    val custom_schema = StructType(Array(
      StructField("Liked", BooleanType, true),
      StructField("user_id",IntegerType,true),
      StructField("Video_end_type", IntegerType, true),
      StructField("minutes_played", IntegerType, true),
      StructField("video_id",IntegerType,true),
      StructField("Geo_cd", StringType, true),
      StructField("Channel_id", IntegerType, true),
      StructField("creator_id",IntegerType,true),
      StructField("Timestamp", StringType, true),
      StructField("Disliked", BooleanType, true)
    ))

   val csv_DF = spark.read.format("csv").option("mode","DROPMALFORMED").schema(custom_schema).load("midproject2/flume_sink/CSV_Files")
    val xml_df = spark.read.format("com.databricks.spark.xml").option("mode","DROPMALFORMED").option("rowTag","record").load("midproject2/flume_sink/XML_Files")

    Try(csv_DF.unionByName(xml_df))
    match {

      case Success(success) => {
        println("Files read Successfully. Beginning Data Enrichment...")
        val batchDF = csv_DF.unionByName(xml_df)
        val batchDF_replacewith_null = batchDF.na.replace(Seq("user_id","video_id","channel_id","creator_id","geo_cd","timestamp"),Map(""->null))
        val video_creator_df = spark.sql("select * from video_creator")
        val channel_geocd_df = spark.sql("select * from channel_geocd")

        val batchDF_with_NULL = batchDF_replacewith_null.filter("Geo_cd is null OR creator_id is null")

        val batchDF_without_NULL=batchDF_replacewith_null.filter("Geo_cd is NOT NULL AND creator_id is NOT NULL")

        batchDF_with_NULL.join(channel_geocd_df,batchDF_with_NULL("Channel_id").cast("int") ===channel_geocd_df("channel_id"),"left_outer")

        batchDF_with_NULL.createOrReplaceTempView("batchDF_with_NULL")
        channel_geocd_df.createOrReplaceTempView("channel_geocd_df")
        video_creator_df.createOrReplaceTempView("video_creator_df")

        val enriched_df = spark.sql("SELECT a.liked as liked,a.user_id as user_id ,IF (a.video_end_type IS NULL,3,a.video_end_type) AS video_end_type ,a.minutes_played as minutes_played,a.video_id as video_id,b.geo_cd as geo_cd,a.channel_id as channel_id,c.creator_id as creator_id ,a.timestamp as timestamp,a.disliked as disliked FROM batchDF_with_NULL a LEFT JOIN channel_geocd_df b ON a.channel_id = b.channel_id LEFT JOIN video_creator_df c on a.video_id = c.video_id")
        enriched_df.show(false)
        val complete_enriched_df = enriched_df.union(batchDF_without_NULL)
        complete_enriched_df.createOrReplaceTempView("complete_enriched_df")
        val complete_df = spark.sql("select *,IF('userid' is null OR video_id is null OR timestamp is null OR geo_cd is null OR creator_id is null,'fail','pass') AS status from complete_enriched_df ")
        complete_df.write.mode("append").parquet("midproject2/enriched_data1/")


      }

      case Failure(e) => {
        println("The Files could not be read successfully, Kindly check the files exist and if in the right format as one or more fields could not be found. TERMINATING JOB ...." + e)

      }
    }

  }
}
