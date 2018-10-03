package io.geoant.trackstreams

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.cluster.Cluster
import akka.management.AkkaManagement
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.stream.ActorMaterializer
import io.geoant.trackstreams.SpringExtension.Companion.SPRING_EXTENSION_PROVIDER
import org.apache.kafka.streams.StreamsConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.cassandra.CassandraProperties
import org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchProperties
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.web.embedded.netty.NettyReactiveWebServerFactory
import org.springframework.boot.web.reactive.server.ReactiveWebServerFactory
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories
import org.springframework.http.server.reactive.HttpHandler
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.annotation.EnableKafkaStreams
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration
import org.springframework.web.reactive.DispatcherHandler
import org.springframework.web.server.WebHandler
import org.springframework.web.server.adapter.WebHttpHandlerBuilder

@Configuration
@EnableKafka
@EnableKafkaStreams
@EnableCassandraRepositories
@EnableConfigurationProperties(CassandraProperties::class, KafkaProperties::class, ElasticsearchProperties::class, SparkKubernetesProperties::class)
class TrackStreamsConfiguration {

    @Autowired
    lateinit var applicationContext: ApplicationContext

    val CLUSTER_NAME = "geoant-akka-cluster"

    @Bean
    @Primary
    fun cProps(cp: CassandraProperties?): CassandraProperties {
        var p: CassandraProperties?
        if (cp?.contactPoints == null || cp?.contactPoints.isEmpty()) {
            p = CassandraProperties()
            p.contactPoints.add("geoant-cassandra.geoant")
            p.port = 9042
        } else {
            p = cp
        }
        return p
    }

    @Bean
    @Primary
    fun kProps(kp: KafkaProperties?): KafkaProperties {
        var p: KafkaProperties?
        if (kp?.bootstrapServers == null || kp?.bootstrapServers.isEmpty()) {
            p = KafkaProperties()
            p.bootstrapServers.add("bootstrap.kafka:9092")
        } else {
            p = kp
        }
        return p
    }

    @Bean
    @Primary
    fun eProps(ep: ElasticsearchProperties?): ElasticsearchProperties {
        var p: ElasticsearchProperties?
        if (ep?.clusterNodes.isNullOrBlank()) {
            p = ElasticsearchProperties()
            p.clusterNodes = "geoant-service.geoant:9200"
        } else {
            p = ep
        }
        return p as ElasticsearchProperties
    }

    //    @Value("\${spark.appName:track-streams}")
    var appName: String = "track-streams"
    //    @Value("\${spark.master:k8s://https://tcp.cloudolympus.io:1080}")
    var master: String = "k8s://https://tcp.cloudolympus.io:1080"
    //    @Value("\${spark.mode:cluster}")
    var mode: String = "cluster"
    //    @Value("\${spark.kubernetes.namespace:default}")
    var namespace: String = "default"
    //    @Value("\${spark.kubernetes.driver.docker.image:kubespark/spark-driver:v2.2.0-kubernetes-0.5.0}")
    var driverImage: String = "kubespark/spark-driver:v2.2.0-kubernetes-0.5.0"
    //    @Value("\${spark.kubernetes.executor.docker.image:kubespark/spark-executor:v2.2.0-kubernetes-0.5.0}")
    var executorImage: String = "kubespark/spark-executor:v2.2.0-kubernetes-0.5.0"
    //    @Value("\${spark.kubernetes.initcontainer.docker.image:kubespark/spark-init:v2.2.0-kubernetes-0.5.0}")
    var initImage: String = "kubespark/spark-init:v2.2.0-kubernetes-0.5.0"
    //    @Value("\${spark.kubernetes.resourceStagingServer.uri:https://tcp.cloudolympus.io:1031}")
    var resourceServer: String = "https://tcp.cloudolympus.io:1031"

    @Bean
    fun nettyReactiveWebServerFactory(): ReactiveWebServerFactory {
        return NettyReactiveWebServerFactory()
    }

    @Bean
    fun webHandler(): WebHandler {
        return DispatcherHandler(this.applicationContext)
    }

    @Bean
    fun httpHandler(): HttpHandler {
        return WebHttpHandlerBuilder.applicationContext(this.applicationContext)
                .build()
    }

    @Bean
    fun actorSystem(): ActorSystem {
        val system = ActorSystem.create(CLUSTER_NAME)
        SPRING_EXTENSION_PROVIDER.get(system)
                .initialize(this.applicationContext)
        return system
    }

    @Bean
    fun orchestratorActorRef(actorSystem: ActorSystem, scc: JavaStreamingContext, cProps: CassandraProperties, kProps: KafkaProperties, eProps: ElasticsearchProperties) =
            actorSystem.actorOf(
                    Props.create(Orchestrator::class.java, scc, cProps, kProps, eProps), "orchestrator")

    @Bean
    fun streamingContext(conf: SparkConf) =
            JavaStreamingContext(conf, Duration(100))

    @Bean
    fun sparkConfig(cProps: CassandraProperties?): SparkConf {

        var host = cProps?.contactPoints
        var port = cProps?.port
        if (host == null || host.isEmpty() || host[0].contentEquals("")) {
            host = mutableListOf("geoant-cassandra.geoant")
        }
        if (port == null || port.compareTo(0) <= 0) {
            port = 9042
        }

        val sparkConf: SparkConf = SparkConf()
        sparkConf.setAppName(appName)
//        sparkConf.setMaster("k8s://https://tcp.cloudolympus.io:1080")
        sparkConf.setMaster("local")
//        sparkConf.setMaster(master)
//        sparkConf.set("spark.cassandra.connection.host", host[0])
//        sparkConf.set("spark.cassandra.connection.port", port.toString())
//        sparkConf.set("spark.cassandra.connection.host", "geoant-cassandra.geoant")
//        sparkConf.set("spark.cassandra.connection.port", "9042")
        sparkConf.set("spark.cassandra.connection.host", "tcp.cloudolympus.io")
        sparkConf.set("spark.cassandra.connection.port", "1042")
        return sparkConf
    }

    @Bean(name = [KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME])
    fun kafkaStreamsConfig(kProps: KafkaProperties?): StreamsConfig {

        var bootstrap = kProps?.bootstrapServers?.joinToString(separator = ",")
        if (bootstrap == null || bootstrap.contentEquals("")) {
            bootstrap = "bootstrap.kafka:9092"
        }

        return StreamsConfig(mapOf(
                StreamsConfig.APPLICATION_ID_CONFIG to appName,
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrap
//                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to "bootstrap.kafka:9092"
        ))
    }

    @Bean
    fun cluster(orchestratorActorRef: ActorRef, actorSystem: ActorSystem) =
            Cluster.get(actorSystem)

    @Bean
    fun serverStart(cluster: Cluster, management: AkkaManagement, bootstrap: ClusterBootstrap): String {
//        management.start()
//        bootstrap.start()
        return "Management and Bootstrap Started"
    }

    @Bean
    fun materializer(actorSystem: ActorSystem) =
            ActorMaterializer.create(actorSystem)

    @Bean
    fun akkaManagement(actorSystem: ActorSystem) =
            AkkaManagement.get(actorSystem)

    @Bean
    fun clusterBootstrap(actorSystem: ActorSystem) =
            ClusterBootstrap.get(actorSystem)
}