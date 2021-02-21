package io.keepcoding.spark.exercise.batch
import io.keepcoding.spark.exercise.streaming.StreamingJobImpl.{readAntennaMetadata, spark}
import org.apache.spark.sql.functions.{approx_count_distinct, avg, lit, max, min, sum, when, window}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.OffsetDateTime

object BatchJobImpl  extends BatchJob{
  override val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("Streaming Job")
      .getOrCreate()

  import spark.implicits._


  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    spark
      .read
      .format("parquet")
      .load(storagePath)
      .where(
        $"year" === filterDate.getYear &&
          $"month" === filterDate.getMonthValue &&
          $"day" === filterDate.getDayOfMonth &&
          $"hour" === filterDate.getHour
      )
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

      .select($"timestamp",$"app", $"bytes")

      .groupBy($"app", window($"timestamp","1 hour").as("w"))
      .agg(
        sum($"bytes").as("total_bytes")
      )
      .select($"app",$"w.start".as("date"),$"total_bytes")
  }
  override def userQuotes(dataFrame: DataFrame): DataFrame = {
    dataFrame

      .select($"timestamp",$"email",$"id", $"bytes",$"quota")

      .groupBy($"email", window($"timestamp","1 hour").as("w"))
      .agg(sum($"bytes").as("total_bytes")
      )
      .withColumn("fraude",
        when($"total_bytes" > $"quota", "fraude")
          .when($"total_bytes" <= $"quota", " no fraude"))

      .select($"email",$"w.start".as("date"),$"total_bytes", $"fraude")

  }
  override def totalBytesAntenna(dataFrame: DataFrame): DataFrame = {
    dataFrame

      .select($"timestamp",$"antenna_id", $"bytes")

      .groupBy($"antenna_id", window($"timestamp","1 hour").as("w"))
      .agg(
        sum($"bytes").as("total_bytes")
      )
      .select($"antenna_id",$"w.start".as("date"),$"total_bytes")
  }
  override def TotalBytesMail(dataFrame: DataFrame): DataFrame = {
    dataFrame

      .select($"timestamp",$"email", $"bytes")

      .groupBy($"email", window($"timestamp","1 hour").as("w"))
      .agg(
        sum($"bytes").as("total_bytes")
      )
      .select($"email",$"w.start".as("date"),$"total_bytes")
  }

  // override def computeErrorAntennaByModelAndVersion(dataFrame: DataFrame): DataFrame = {

  import org.apache.spark.sql.functions.sum

  //dataFrame
  // .where($"metric"  === lit("status") && $"value" === lit(-1))
  // .select($"timestamp", $"model", $"version", $"id")
  // .groupBy(window($"timestamp","1 hour"), $"model", $"version")
  // .agg(approx_count_distinct($"id").as("antennas_num"))
  // .select($"antennas_num", $"w.start".as("date"), $"mode", $"version")

  //}

  //override def computePercentStatusByID(dataFrame: DataFrame): DataFrame = {
  //  dataFrame
  //  .where($"metric"  === lit("status"))
  // .withColumn("state",
  //  when($"value" === lit(0), disable)
  //  .when($"value" === lit(1), "enable")
  //  .otherwise("error"))
  //}

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .save()
  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit = {
    dataFrame
      .coalesce(1)
      .write
      .partitionBy("year","month","day","hour")
      .format("parquet")
      .save( s"$storageRootPath/historical_proyecto/")
  }

  def main(args: Array[String]): Unit = {
    val argsTime = "2021-02-15T14:00:00Z"
    val storageDF = readFromStorage("/tmp/practica/data/",OffsetDateTime.parse(argsTime))

    val metadataDF = readAntennaMetadata("jdbc:postgresql://35.192.203.38:5432/postgres", "user_metadata", "postgres", "keepcoding")
    val enrichDF = enrichAntennaWithMetadata(storageDF,metadataDF)
    val aggSumAppDF = totalBytesApp(enrichDF)

    writeToJdbc(aggSumAppDF,"jdbc:postgresql://35.192.203.38:5432/postgres", "app_1h_sum", "postgres", "keepcoding")

    writeToStorage(storageDF,"/tmp/app_proyecto/")

    val aggSumAntennaDF = totalBytesAntenna(enrichDF)

    writeToJdbc(aggSumAntennaDF,"jdbc:postgresql://35.192.203.38:5432/postgres", "antenna_1h_sum", "postgres", "keepcoding")

    writeToStorage(storageDF,"/tmp/antenna_proyecto/")

    val aggSumMailDF = TotalBytesMail(enrichDF)

    writeToJdbc(aggSumMailDF,"jdbc:postgresql://35.192.203.38:5432/postgres", "email_1h_sum", "postgres", "keepcoding")

    writeToStorage(storageDF,"/tmp/mail_proyecto/")
    val aggSumQuotesDF = userQuotes(enrichDF)

    writeToJdbc(aggSumQuotesDF,"jdbc:postgresql://35.192.203.38:5432/postgres", "userQuotes_1h", "postgres", "keepcoding")

    writeToStorage(storageDF,"/tmp/quotes_proyecto/")
  }

  override def computeErrorAntennaByModelAndVersion(dataFrame: DataFrame): DataFrame = ???

  override def computePercentStatusByID(dataFrame: DataFrame): DataFrame = ???

  override def TotalBytesApp(dataFrame: DataFrame): DataFrame = ???
  override def TotalBytesAntenna(dataFrame: DataFrame): DataFrame = ???



}
