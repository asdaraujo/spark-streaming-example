package com.cloudera.workshop

import java.net.InetAddress
import java.net.UnknownHostException
import java.nio.charset.StandardCharsets
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.HttpEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.MappingException

object SparkStreamingIoT {
  implicit val formats = DefaultFormats
  case class ModelResult(result: Integer)
  case class ModelResponse(success: Boolean, response: ModelResult)
  case class SensorReading(
    sensor_id: Integer,
    sensor_ts: Long,
    is_healthy: Option[Integer],
    response: Option[ModelResult],
    sensor_0: Double,
    sensor_1: Double,
    sensor_2: Double,
    sensor_3: Double,
    sensor_4: Double,
    sensor_5: Double,
    sensor_6: Double,
    sensor_7: Double,
    sensor_8: Double,
    sensor_9: Double,
    sensor_10: Double,
    sensor_11: Double)
  val sensorSchema = ScalaReflection.schemaFor[SensorReading].dataType.asInstanceOf[StructType]

  val ip = InetAddress.getLocalHost()
  val hostname = ip.getHostName()
  val brokers = hostname + ":9092"

  def modelScoring(data: SensorReading, cdswApiUrl: String, accessKey: String): Integer = {
    val feature = f"${data.sensor_0}%.0f, ${data.sensor_1}%.0f, ${data.sensor_2}%.0f, ${data.sensor_3}%.0f, " +
                  f"${data.sensor_4}%.0f, ${data.sensor_5}%.0f, ${data.sensor_6}%.0f, ${data.sensor_7}%.0f, " +
                  f"${data.sensor_8}%.0f, ${data.sensor_9}%.0f, ${data.sensor_10}%.0f, ${data.sensor_11}%.0f"
    val requestData = s"""{"accessKey":"$accessKey", "request":{"feature":"$feature"}}"""

    val post = new HttpPost(cdswApiUrl)
    post.setHeader("Content-type", "application/json")
    post.setEntity(new StringEntity(requestData, StandardCharsets.UTF_8))
    
    val httpClient = HttpClientBuilder.create().build()
    val response = httpClient.execute(post)
    val entity = response.getEntity()
    var modelResult: Integer = null
    if (entity != null) {
      val result = EntityUtils.toString(entity)
      try {
        modelResult = parse(result).extract[ModelResponse].response.result
      } catch {
        case e: MappingException => System.out.println("CANNOT PARSE RESPONSE:[" + result + "]")
      }
    }
    modelResult
  }

  def main(args: Array[String]) {

    if (args.length < 1) {
      System.err.println(s"""
        |Usage: SparkStreamingIoT <cdsw_access_key>
        |  <cdsw_access_key> is the access key associated with the CDSW model to use
        | 
        """.stripMargin)
      System.exit(1)
    }

    val accessKey = args(0)
    val publicIp = args(1)
    val cdswApiUrl = "http://cdsw." + publicIp + ".nip.io/api/altus-ds-1/models/call-model"

    val kuduMaster = hostname + ":7051"
    val kuduTable = "impala::default.sensors"

    // Create context with 5 second batch interval
    val spark = SparkSession.builder
      .appName("SparkStreamingIoT") 
      .getOrCreate
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))
    val kuduContext = new KuduContext(kuduMaster, spark.sparkContext)

    // Create direct kafka stream with brokers and topics
    val topics = Array("iot")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-consumer",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    // Get the events, parse, score the data point against the model and build a dataframe row
    var offsetRanges: Array[OffsetRange] = null
    val events = messages
      .transform { rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
      .map(record => parse(record.value).extract[SensorReading])
      .map(reading => Row(reading.sensor_id,
                          reading.sensor_ts,
                          modelScoring(reading, cdswApiUrl, accessKey),
                          null,
                          reading.sensor_0,
                          reading.sensor_1,
                          reading.sensor_2,
                          reading.sensor_3,
                          reading.sensor_4,
                          reading.sensor_5,
                          reading.sensor_6,
                          reading.sensor_7,
                          reading.sensor_8,
                          reading.sensor_9,
                          reading.sensor_10,
                          reading.sensor_11))

    // Store data in Kudu and commit the processed offset
    events.foreachRDD { rdd =>
      val df = spark.createDataFrame(rdd, sensorSchema)
      df.show(100)
      kuduContext.upsertRows(df, kuduTable)
      messages.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
