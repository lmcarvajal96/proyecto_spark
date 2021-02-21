package io.keepcoding.spark.exercise.streaming

import java.sql.Timestamp
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SparkSession}

case class AntennaMessage( timestamp: Long, id: String, antenna_id: String, bytes: Long, app: String )

trait StreamingJob {

  val spark: SparkSession

  def readFromKafka(kafkaServer: String, topic: String): DataFrame

  def parserJsonData(dataFrame: DataFrame): DataFrame

  def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame

  def totalBytesApp(dataFrame: DataFrame): DataFrame

  def totalBytesAntenna(dataFrame: DataFrame): DataFrame
  def userQuotes(dataFrame: DataFrame): DataFrame


  def TotalBytesMail(dataFrame: DataFrame): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit]

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit]

  def run(args: Array[String]): Unit = {
    val Array(kafkaServer, topic, jdbcUri, jdbcMetadataTable, aggJdbcTable, jdbcUser, jdbcPassword, storagePath) = args
    println(s"Running with: ${args.toSeq}")

    val kafkaDF = readFromKafka(kafkaServer, topic)
    val antennaDF = parserJsonData(kafkaDF)
    val metadataDF = readAntennaMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
    val antennaMetadataDF = enrichAntennaWithMetadata(antennaDF, metadataDF)
    val storageFuture = writeToStorage(antennaDF, storagePath)
    val aggBySumAppDF = totalBytesApp(antennaMetadataDF)
    val aggFutureApp = writeToJdbc(aggBySumAppDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    val aggBySumAntennaDF = totalBytesAntenna(antennaMetadataDF)
    val aggFutureAntenna = writeToJdbc(aggBySumAntennaDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    val aggBySumEmailDF = TotalBytesMail(antennaMetadataDF)
    val aggFutureEmail = writeToJdbc(aggBySumEmailDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    val aggBySumQuotesDF = userQuotes(antennaMetadataDF)
    val aggFutureQuotes = writeToJdbc(aggBySumQuotesDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)

    Await.result(Future.sequence(Seq(aggFutureApp,aggFutureAntenna,aggFutureEmail,aggFutureQuotes , storageFuture)), Duration.Inf)

    spark.close()
  }

}
