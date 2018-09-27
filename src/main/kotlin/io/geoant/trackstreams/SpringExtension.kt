package io.geoant.trackstreams

import akka.actor.AbstractExtensionId
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.Props
import org.springframework.context.ApplicationContext


class SpringExtension : AbstractExtensionId<SpringExtension.SpringExt>() {

    override fun createExtension(system: ExtendedActorSystem): SpringExt {
        return SpringExt()
    }

    class SpringExt : Extension {
        @Volatile
        private var applicationContext: ApplicationContext? = null

        fun initialize(applicationContext: ApplicationContext) {
            this.applicationContext = applicationContext
        }

        fun props(actorBeanName: String): Props {
            return Props.create(
                    SpringActorProducer::class.java, applicationContext, actorBeanName)
        }
    }

    companion object {

        val SPRING_EXTENSION_PROVIDER = SpringExtension()
    }
}