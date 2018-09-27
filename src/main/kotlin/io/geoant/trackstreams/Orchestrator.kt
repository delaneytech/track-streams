package io.geoant.trackstreams

import akka.actor.*
import akka.japi.pf.ReceiveBuilder
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.boot.autoconfigure.cassandra.CassandraProperties
import org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchProperties
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
class Orchestrator : AbstractLoggingActor {

    var kafkaActorRef : ActorRef
//    var elasticWriterActorRef : ActorRef
//    var geoWaveWriterActorRef : ActorRef

    var ssc : JavaStreamingContext

//    @Autowired
    var cProps : CassandraProperties
//    @Autowired
    var kProps : KafkaProperties
//    @Autowired
    var eProps : ElasticsearchProperties

//    var elasticWriterInitialized = false
//    var geoWaveWriterInitialized = false
    var pipeInitialized = false

    constructor(ssc : JavaStreamingContext, cProps: CassandraProperties, kProps: KafkaProperties, eProps : ElasticsearchProperties) {

        this.ssc = ssc

        this.eProps = eProps
        this.kProps = kProps
        this.cProps = cProps

//        this.elasticWriterActorRef = context.actorOf(
//                Props.create(ElasticRawWriterActor::class.java, this.eProps, self), "elastic-writer")
//        this.geoWaveWriterActorRef = context.actorOf(
//                Props.create(GeoWaveWriterActor::class.java, this.cProps, self), "geowave-writer")

            this.kafkaActorRef = context.actorOf(
                    Props.create(KafkaStreamingActor::class.java, ssc, kProps,  self), "pipe")
    }

    override fun createReceive() =
            ReceiveBuilder().match(
                String::class.java) {
                when (it) {
//                    "elastic writer initialized" -> {
//                        elasticWriterInitialized = true
//                        checkActorsAndInitializeKafkaActor()
//                    }
//                    "geowave writer initialized" -> {
//                        geoWaveWriterInitialized = true
//                        checkActorsAndInitializeKafkaActor()
//                    }
                    "pipe initialized" -> {
                        pipeInitialized = true
                        this.ssc.start()
                        sender().tell("started stream context", self)
                    }
                }
            }.build()


//    fun checkActorsAndInitializeKafkaActor () {
//
//        if (elasticWriterInitialized && geoWaveWriterInitialized && !pipeInitialized) {
//            this.kafkaActorRef = context.actorOf(
//                    Props.create(KafkaStreamingActor::class.java, ssc, kProps, elasticWriterActorRef, geoWaveWriterActorRef, self), "pipe")
//            pipeInitialized = true;
//        }
//    }
}

