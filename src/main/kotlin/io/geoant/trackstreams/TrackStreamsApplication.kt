package io.geoant.trackstreams

import org.springframework.boot.CommandLineRunner
import org.springframework.boot.SpringApplication
import org.springframework.boot.WebApplicationType
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchAutoConfiguration
import org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchDataAutoConfiguration
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration
import org.springframework.context.annotation.ComponentScan


@ComponentScan
@EnableAutoConfiguration(exclude = [])
class TrackStreamsApplication : CommandLineRunner {

    override fun run(vararg args: String?) {
    }

    companion object {
        @JvmStatic fun main (args: Array<String>) {
            val application = SpringApplication(TrackStreamsApplication::class.java)
                application.webApplicationType = WebApplicationType.REACTIVE
                application.run(*args)
            }
        }

}

