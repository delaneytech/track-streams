package io.geoant.trackstreams

import akka.actor.AbstractLoggingActor
import akka.actor.ActorRef
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.japi.pf.ReceiveBuilder
import com.databricks.spark.avro.SchemaConverters
import com.databricks.spark.xml.XmlReader
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component
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
    var count: Long = 0

    val mediator = DistributedPubSub.get(context.system).mediator()

    constructor(ssc: JavaStreamingContext, kProps: KafkaProperties, listener: ActorRef) {
//        constructor(ssc: JavaStreamingContext, kProps : KafkaProperties, elasticWriter : ActorRef, geoWaveWriter : ActorRef, listener: ActorRef) {

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

//        val startStopFlag = xmlStream.selectExpr("CAST(value AS STRING)").filter {
//
//            it.getString(0).contains("\"mark\":\"START\"") || it.getString(0).contains("\"mark\":\"END\"")
//        }

//        if ((startStopFlag.select(col("value")).collect()[0] as String).contains("\"mark\":\"START\"")) {
//            count = 0
//        }

        val values = xmlStream.selectExpr("CAST(value AS STRING)").filter {
            !(it.getString(0).contains("\"mark\":\"START\"")) && !(it.getString(0).contains("\"mark\":\"END\"")) && !(it.getString(0).contains("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>"))
        }

        this.sqlc.udf().register("xmlParse", XmlParser, DataTypes.StringType)

        val queryDf = values
                .selectExpr("xmlParse(value) as xml")
                .writeStream()
                .outputMode("append")
                .format("memory")
                .queryName("queryDf")
                .trigger(Trigger.ProcessingTime(1000))
                .start()

        while (queryDf.isActive) {

//            if (cnt.compareTo(0) != 0 && cnt <= this.count) {
            if (Parser.eof) {

                log().info("Parser found EOF...")
                val queryDfTable = this.sqlc.sql("select * from queryDf")
                queryDf.stop()
                val cnt = queryDfTable.count()
                log().info("********************************************************************************* COUNT: $cnt")


//                val xmlRows: Array<Row> = queryDfTable.collect() as Array<Row>
                var xmlString = StringBuilder()
                queryDfTable.toJavaRDD().toLocalIterator().forEach {
                    xmlString.append(it.getString(0).trim().replaceFirst("^([\\W]+)<", "<"))
                }

//                val xmlString = (rowsAsStrings as Array<String>).joinToString(separator = "")

//                val xmlString = xmlRows.map { row ->
//                    row.getString(0).trim().replaceFirst("^([\\W]+)<","<")
//                }.joinToString(separator = "")

                mediator.tell(DistributedPubSubMediator.Send("/user/geowave-writer", xmlString.toString(), false), self)
//                geoWaveWriter.tell(xmlString.toString(), self)

                val xmlRdd = this.sc.parallelize(mutableListOf(xmlString.toString()))

                val xmlDf = XmlReader().withRowTag("TrackMessage").xmlRdd(sqlc, xmlRdd.rdd())

//                saveToCassandra(xmlDf)
//                xmlDf.printSchema()
//                val json = xmlDf.write().format("json").save()
//                val jsonArray = xmlDf.toJSON().collect() as Array<String>

                var json = StringBuilder()
                val jsonArray = xmlDf.toJSON().toJavaRDD().toLocalIterator().forEach {
                    json.append(it)
                }
//                log().info(json)

                mediator.tell(DistributedPubSubMediator.Send("/user/elastic-writer", json.toString(), false), self)
//                elasticWriter.tell(json, self)
//                CassandraJavaUtil.javaFunctions(xmlDf.javaRDD()).writerBuilder("lake", "tracks", mapToRow(Row::class.java)).saveToCassandra()

                log().info("Reset Parser...")
                count = 0
                Parser.eof = false
//                queryDf.stop()

                queryDfTable.show()
            }

//            Thread.sleep(10000)
        }

//        CassandraStreamingJavaUtil.javaFunctions(
//                kafkaStream.flatMap {
//                    mutableListOf(
//                            XmlEntry(
//                                    UUID.randomUUID(),
//                                    it.toString().split("\"text/plain\"")[1])
//                    ).iterator()
//                })
//                .writerBuilder("lake",
//                        "xml_entries",
//                        mapToRow(
//                                XmlEntry::class.java,
//                                mapOf("id" to "entry_id", "xml" to "entry_data")))
//                .saveToCassandra()

        listener.tell("pipe initialized", self)
    }


    object XmlParser : UDF1<String, String> {
        override fun call(t1: String?): String {

            val line = t1!!.split("\"text/plain\"")[1].trim()
            if (line.contains("</TrackMessage>")) {
                Parser.eof = true
            }
            return line
        }
    }

    object Parser {
        var eof = false
    }

//    fun saveToCassandra(df : Dataset<Row?>) = {
//
//          CassandraJavaUtil.javaFunctions(df.javaRDD()).flatMap {
//
//
//          }
//              .writerBuilder("lake", "tracks", mapToRow(df.schema().javaClass)).saveToCassandra()
//
////        df.write().format("org.apache.spark.sql.cassandra").options(mapOf("keyspace" to "lake", "table" to "tracks"))
//          }
//    }

}

