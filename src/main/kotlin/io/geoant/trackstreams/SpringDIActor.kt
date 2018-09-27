package io.geoant.trackstreams

import akka.actor.Actor
import akka.actor.IndirectActorProducer


class SpringDIActor : IndirectActorProducer {

    private var type: Class<out Actor>? = null
    private var actorInstance: Actor? = null

    constructor(type: Class<out Actor>) {
        this.type = type
    }

    constructor(actorInstance: Actor) {
        this.actorInstance = actorInstance
    }

    /**
     * This factory method must produce a fresh actor instance upon each
     * invocation. **It is not permitted to return the same instance more than
     * once.**
     */
    override fun produce(): Actor? {
        var newActor = actorInstance
        actorInstance = null
        if (newActor == null) {
            try {
                newActor = type?.newInstance()
            } catch (e: InstantiationException) {
//                LOG.error("Unable to create actor of type:{}", type, e)
            } catch (e: IllegalAccessException) {
//                LOG.error("Unable to create actor of type:{}", type, e)
            }

        }
//        ApplicationContextProvider.getApplicationContext().getAutowireCapableBeanFactory().autowireBean(newActor)
        return newActor
    }

    /**
     * This method is used by [[Props]] to determine the type of actor which will
     * be created. This means that an instance of this `IndirectActorProducer`
     * will be created in order to call this method during any call to
     * [[Props#actorClass]]; it should be noted that such calls may
     * performed during actor set-up before the actual actor’s instantiation, and
     * that the instance created for calling `actorClass` is not necessarily reused
     * later to produce the actor.
     */
    override fun actorClass(): Class<out Actor>? {
        return type
    }

    companion object {

    }
}