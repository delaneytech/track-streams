package io.geoant.trackstreams

import akka.actor.ActorRef
import org.springframework.stereotype.Component
import java.util.*


class SimpleCommunicator {

    constructor(kafkaActor : ActorRef) {
        kafkaActor.tell(XmlEntry(UUID.randomUUID(), "hello cassandra, from kafka"), ActorRef.noSender())
    }
}

