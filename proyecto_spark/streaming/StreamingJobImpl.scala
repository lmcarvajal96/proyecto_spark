package io.keepcoding.spark.exercise.streaming

import com.carrotsearch.hppc.Intrinsics.cast
import jdk.jfr.internal.handlers.EventHandler.timestamp
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

object StreamingJobImpl extends StreamingJob{
  override val spark: SparkSession =
    SparkSession
    .builder()
      .master("local[*]")
      .appName("Streaming Job")
      .getOrCreate()

  import spark.implicits._

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame =
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .load()


  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
    val schema = ScalaReflection.schemaFor[AntennaMessage].dataType.asInstanceOf[StructType]
    dataFrame
      .select(from_json($"value".cast(StringType), schema).as("json"))
      .select("json.*")
  }


  override def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()

  }

  override def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    antennaDF
      .join(metadataDF,
        antennaDF("id") === metadataDF("id")
      ).drop(metadataDF("id"))
  }

  override def totalBytesApp(dataFrame: DataFrame): DataFrame = {
    dataFrame

      .select($"timestamp", $"app",$"id", $"bytes")
      .withColumn("timestamp",$"timestamp".cast(TimestampType))
      .withWatermark("timestamp", "1 minute")
      .groupBy($"app", window($"timestamp","5 minutes").as("w"))
      .agg(sum($"bytes").as("total_bytes")
      )
      .select($"app",$"w.start".as("date"),$"total_bytes")

  }

  override def totalBytesAntenna(dataFrame: DataFrame): DataFrame = {
    dataFrame

      .select($"timestamp",$"antenna_id",$"id", $"bytes")
      .withColumn("timestamp",$"timestamp".cast(TimestampType))
      .withWatermark("timestamp", "1 minute")
      .groupBy($"antenna_id", window($"timestamp","1 minute").as("w"))
      .agg(sum($"bytes").as("total_bytes")
      )
      .select($"antenna_id",$"w.start".as("date"),$"total_bytes")

  }
  override def userQuotes(dataFrame: DataFrame): DataFrame = {
    dataFrame

      .select($"timestamp",$"email",$"id", $"bytes",$"quota")
      .withColumn("timestamp",$"timestamp".cast(TimestampType))
      .withWatermark("timestamp", "1 minute")
      .groupBy($"email", window($"timestamp","1 minute").as("w"),$"quota")
      .agg(sum($"bytes").as("total_bytes")
      )
      .withColumn("fraude",
        when($"total_bytes" > $"quota", "fraude")
          .when($"total_bytes" <= $"quota", " no fraude"))

      .select($"email",$"w.start".as("date"),$"total_bytes", $"fraude")

  }
  override def TotalBytesMail(dataFrame: DataFrame): DataFrame = {
    dataFrame

      .select($"timestamp",$"email",$"id", $"bytes")
      .withColumn("timestamp",$"timestamp".cast(TimestampType))
      .withWatermark("timestamp", "1 minute")
      .groupBy($"email", window($"timestamp","1 minute").as("w"))
      .agg(sum($"bytes").as("total_bytes")
      )
      .select($"email",$"w.start".as("date"),$"total_bytes")

  }
  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {
    dataFrame
      .writeStream
      .foreachBatch { (df: DataFrame, id: Long) =>
        df
          .write
          .mode(SaveMode.Append)
          .format("jdbc")
          .option("driver", "org.postgresql.Driver")
          .option("url", jdbcURI)
          .option("dbtable", jdbcTable)
          .option("user", user)
          .option("password", password)
          .save()
      }.start()
      .awaitTermination()
  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {
    dataFrame

      .withColumn("year", year($"timestamp".cast(TimestampType)))
      .withColumn("month", month($"timestamp".cast(TimestampType)))
      .withColumn("day", dayofmonth($"timestamp".cast(TimestampType)))
      .withColumn("hour", hour($"timestamp".cast(TimestampType)))
      .writeStream
      .partitionBy("year","month","day","hour")
      .format("parquet")
      .option("path", s"$storageRootPath/data")
      .option("checkpointLocation",s"$storageRootPath/checkpoint_proyecto/")
      .start()
      .awaitTermination()
  }

  // def main(args: Array[String]): Unit = run(args)

  def main(args: Array[String]): Unit = {


    val metadataDF = readAntennaMetadata("jdbc:postgresql://35.192.203.38:5432/postgres", "user_metadata", "postgres", "keepcoding")
    val antennaStreamDF = parserJsonData(readFromKafka("34.72.190.67:9092","devices"))
    totalBytesApp(enrichAntennaWithMetadata(antennaStreamDF,metadataDF))
      .writeStream
      .format("console")
      .start()


    totalBytesAntenna(enrichAntennaWithMetadata(antennaStreamDF,metadataDF))
      .writeStream
      .format("console")
      .start()

    TotalBytesMail(enrichAntennaWithMetadata(antennaStreamDF,metadataDF))
      .writeStream
      .format("console")
      .start()

    userQuotes(enrichAntennaWithMetadata(antennaStreamDF,metadataDF))
      .writeStream
      .format("console")
      .start()

    val writeToStorageFut = writeToStorage(antennaStreamDF, "/tmp/practica_proyecto/")
    val writeToJdbcFutApp = writeToJdbc(
      totalBytesApp(enrichAntennaWithMetadata(antennaStreamDF,metadataDF)),
      "jdbc:postgresql://35.192.203.38:5432/postgres", "app_sum", "postgres", "keepcoding"
    )
    val writeToJdbcFutAntenna = writeToJdbc(
      totalBytesAntenna(enrichAntennaWithMetadata(antennaStreamDF,metadataDF)),
      "jdbc:postgresql://35.192.203.38:5432/postgres", "antenna_sum", "postgres", "keepcoding"
    )
    val writeToJdbcFutEmail = writeToJdbc(
      TotalBytesMail(enrichAntennaWithMetadata(antennaStreamDF,metadataDF)),
      "jdbc:postgresql://35.192.203.38:5432/postgres", "email_sum", "postgres", "keepcoding"
    )
    val writeToJdbcFutQuotes= writeToJdbc(
      userQuotes(enrichAntennaWithMetadata(antennaStreamDF,metadataDF)),
      "jdbc:postgresql://35.192.203.38:5432/postgres", "user_quotes", "postgres", "keepcoding"
    )
    val f = Future.sequence(Seq(writeToStorageFut, writeToJdbcFutApp, writeToJdbcFutAntenna,writeToJdbcFutEmail,writeToJdbcFutQuotes ))
    //val f = Future.sequence(Seq(writeToStorageFut, writeToJdbcFutApp))
    Await.result(f, Duration.Inf)
    val d = Future.sequence(Seq(writeToStorageFut,  writeToJdbcFutAntenna))
    Await.result(d, Duration.Inf)
    val e = Future.sequence(Seq(writeToStorageFut, writeToJdbcFutEmail))
    Await.result(e, Duration.Inf)
    val g = Future.sequence(Seq(writeToStorageFut, writeToJdbcFutQuotes))
    Await.result(e, Duration.Inf)

  }



}
