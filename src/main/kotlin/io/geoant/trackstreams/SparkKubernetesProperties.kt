package io.geoant.trackstreams

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("spark")
class SparkKubernetesProperties {

    var name : String = ""
    var master : String = ""
    var mode : String = ""
}