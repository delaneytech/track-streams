package io.geoant.trackstreams

import akka.actor.AbstractLoggingActor
import akka.actor.ActorRef
import akka.japi.pf.ReceiveBuilder
import org.apache.http.HttpHost
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.xcontent.XContentType
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchProperties
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component
import java.lang.Exception
import java.util.*

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
class ElasticRawWriterActor : AbstractLoggingActor {

    override fun createReceive() =
            ReceiveBuilder().matchAny {

                val content = it.toString()
                val size = content.length

                client.indexAsync(
                        IndexRequest(
                                "lake",
                                "track_message",
                                UUID.randomUUID().toString())
                                .source(
                                        content,
                                        XContentType.JSON),
                        Listener)

            }.build()

    var client : RestHighLevelClient

    constructor(eProps : ElasticsearchProperties, listener : ActorRef) {

        listener.tell("elastic writer initialized", self)

        val host = eProps.clusterNodes.split(":")
        client = RestHighLevelClient(
                RestClient
                        .builder(HttpHost(host[0], host[1].toInt(), "http"))
                        .setRequestConfigCallback {
                            it.setSocketTimeout(500000)
                                    .setConnectTimeout(500000)
                                    .setConnectionRequestTimeout(0)
                        })
    }

    object Listener : ActionListener<IndexResponse> {

        val logger = LoggerFactory.getLogger(Listener::class.java)

        override fun onFailure(e: Exception?) {
            logger.error(e!!.message)
        }

        override fun onResponse(response: IndexResponse?) {
            logger.info(response.toString())
        }

    }
}