package io.geoant.trackstreams

import akka.actor.AbstractLoggingActor
import akka.actor.ActorRef
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.japi.pf.ReceiveBuilder
import com.databricks.spark.avro.SchemaConverters
import com.databricks.spark.xml.XmlReader
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component
import java.util.*
import org.apache.spark.sql.functions.callUDF
import org.apache.spark.sql.functions.col
import java.util.concurrent.ConcurrentHashMap
import kotlin.collections.ArrayList
import net.simon04.jelementtree.ElementTree as ET

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
class KafkaStreamingActor : AbstractLoggingActor {

    override fun createReceive() =
            ReceiveBuilder().match(String::class.java) {
                log().info(it)
            }.build()

    var sqlc: SQLContext
    var sc: JavaSparkContext
    var ssc: JavaStreamingContext
//    var count: Long = 0

    val mediator = DistributedPubSub.get(context.system).mediator()

    constructor(ssc: JavaStreamingContext, kProps: KafkaProperties, listener: ActorRef) {

        var bootstrap = kProps?.bootstrapServers?.joinToString(separator = ",")
        if (bootstrap == null || bootstrap.contentEquals("")) {
            bootstrap = "bootstrap.kafka:9092"
        }

        this.sc = ssc.sparkContext()
        this.ssc = ssc
        this.sqlc = SQLContext(ssc.sparkContext())

        val xmlStream = this.sqlc.readStream().format("kafka")
//                .option("kafka.bootstrap.servers", kProps.bootstrapServers.joinToString (separator = ","))
                .option("kafka.bootstrap.servers", bootstrap)
//                .option("kafka.bootstrap.servers", "bootstrap.kafka:9092")
                .option("subscribe", "geoant-raw")
//                .option("startingOffsets", "latest")
                .load()

        val values = xmlStream.selectExpr("CAST(value AS STRING)").filter {
            !(it.getString(0).contains("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>"))
        }

        val schema = StructType(
                arrayOf(StructField("id", DataTypes.StringType, false, Metadata.empty()),
                        StructField("xml", DataTypes.StringType, false, Metadata.empty()))
        )

        this.sqlc.udf().register("xmlParse", XmlParser, schema)

        val queryDf = values
                .withColumn("data", callUDF("xmlParse", col("value")))
                .select(Column("data").getItem("id").`as`("id"), Column("data").getItem("xml").`as`("xml"))
                .writeStream()
                .outputMode("append")
                .format("memory")
                .queryName("queryDf")
                .trigger(Trigger.ProcessingTime(1000))
                .start()

        while (queryDf.isActive) {

            Parser.files.forEach {

                if (it.value) {

                    log().info("Parser found EOF...")
                    log().info("Working with id: ${it.key}")
                    val queryDfTable = this.sqlc.sql("select xml from queryDf where id = '${it.key}'").filter {
                        !(it.getString(0).contains("\"mark\":\"START\"")) && !(it.getString(0).contains("\"mark\":\"END\""))
                    }
                    val cnt = queryDfTable.count()
                    log().info("********************************************************************************* COUNT: $cnt")

                    var xmlString = StringBuilder()
                    queryDfTable.toJavaRDD().toLocalIterator().forEach {
                        xmlString.append(it.getString(0).trim().replaceFirst("^([\\W]+)<", "<"))
                    }

                    mediator.tell(DistributedPubSubMediator.Send("/user/geowave-writer", xmlString.toString(), false), self)

                    val xmlRdd = this.sc.parallelize(mutableListOf(xmlString.toString()))

                    val xmlDf = XmlReader().withRowTag("TrackMessage").xmlRdd(sqlc, xmlRdd.rdd())

                    var json = StringBuilder()
                    xmlDf.toJSON().toJavaRDD().toLocalIterator().forEach {
                        json.append(it)
                    }

                    mediator.tell(DistributedPubSubMediator.Send("/user/elastic-writer", json.toString(), false), self)

                    log().info("Reset Parser...")
                    Parser.files.remove(it.key)

                    queryDfTable.show()
                }

            Thread.sleep(10000)
            }
        }

        listener.tell("pipe initialized", self)
    }

    object XmlParser : UDF1<String, Row> {

        var id = UUID.randomUUID()

        override fun call(t1: String?): Row {
            val line = t1!!.split("\"text/plain\"")[1].trim()
            if (line.contains("\"mark\":\"START\"")) {
                id = UUID.randomUUID()
                Parser.files.put(id.toString(), false)
            }
            if (line.contains("\"mark\":\"END\"")) {
                Parser.files.put(id.toString(), true)
            }

            return RowFactory.create(id.toString(), line)
        }
    }

    object Parser {
        var files = ConcurrentHashMap<String, Boolean>()
    }

}

