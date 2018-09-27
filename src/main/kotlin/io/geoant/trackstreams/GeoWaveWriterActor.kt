package io.geoant.trackstreams

import akka.actor.AbstractLoggingActor
import akka.actor.ActorRef
import akka.japi.pf.ReceiveBuilder
import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider
import mil.nga.giat.geowave.core.ingest.avro.WholeFile
import mil.nga.giat.geowave.datastore.cassandra.CassandraDataStore
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations
import mil.nga.giat.geowave.datastore.cassandra.operations.config.CassandraOptions
import mil.nga.giat.geowave.datastore.cassandra.operations.config.CassandraRequiredOptions
import mil.nga.giat.geowave.format.stanag4676.Stanag4676IngestPlugin
import org.opengis.feature.simple.SimpleFeature
import org.springframework.beans.factory.config.ConfigurableBeanFactory
import org.springframework.boot.autoconfigure.cassandra.CassandraProperties
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component
import java.nio.ByteBuffer
import java.nio.charset.Charset
import net.simon04.jelementtree.ElementTree as ET

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
class GeoWaveWriterActor : AbstractLoggingActor {

    override fun createReceive() =
            ReceiveBuilder().matchAny {

                writeToGeoWave(it as String)
            }.build()

    val charset = Charset.forName("UTF-8")
    val encoder = charset.newEncoder()
    var store : CassandraDataStore

    constructor(cProps : CassandraProperties, listener: ActorRef) {

        val requiredOptions = CassandraRequiredOptions()
        requiredOptions.contactPoint = cProps.contactPoints.get(0)
//        requiredOptions.contactPoint = "geoant-cassandra.geoant"
//        requiredOptions.contactPoint = "localhost"
        requiredOptions.geowaveNamespace = "tracks"

        val operations = CassandraOperations(requiredOptions)

        val options = CassandraOptions()
        options.isPersistDataStatistics = false
        options.isCreateTable = true

        store = CassandraDataStore(operations, options)

        listener.tell("geowave writer initialized", self)
    }




    fun writeToGeoWave(xmlMsg: String) = try {

        val startLength = xmlMsg.length
        val fixedXml = xmlMsg.replace("AUTOMATIC ESTIMATED", "AUTOMATIC_ESTIMATED")
        val endLength = fixedXml.length
        val xml = ET.fromString(fixedXml)
        var elemTree = ET("TrackMessage")
        xml.findAll("messageSecurity").forEach {
            elemTree.addChild(it)
        }
        xml.findAll("msgCreatedTime").forEach {
            elemTree.addChild(it)
        }
        xml.findAll("stanagVersion").forEach {
            elemTree.addChild(it)
        }
        xml.findAll("senderId").forEach {
            elemTree.addChild(it)
        }
        xml.findAll("tracks").forEach {
            val tracks = ET("tracks")
            it.children.forEach {
                if ("items" != it.tag) {
                    tracks.addChild(it.clone())
                } else if ("TrackPoint" == it.getAttribute("type")) {
                    val trackPoint = it.clone()
                    val trackPointItems = ET("items").setAttribute("type", "TrackPoint")
                    trackPointItems.addChild(trackPoint.find("trackItemUUID").clone())
                    trackPointItems.addChild(trackPoint.find("trackItemSecurity").clone())
                    trackPointItems.addChild(trackPoint.find("trackItemTime").clone())
                    trackPointItems.addChild(trackPoint.find("trackItemSource").clone())
                    trackPointItems.addChild(trackPoint.find("trackItemComment").clone())
                    trackPointItems.addChild(trackPoint.find("trackPointPosition").clone())
                    trackPointItems.addChild(trackPoint.find("trackPointSpeed").clone())
                    trackPointItems.addChild(trackPoint.find("trackPointCourse").clone())
                    trackPointItems.addChild(trackPoint.find("trackPointType").clone())
                    trackPointItems.addChild(trackPoint.find("trackPointSource").clone())
                    trackPointItems.addChild(trackPoint.find("trackPointObjectMask").clone())
                    val tPDC = trackPoint.find("TrackPointDetail").clone()
                    val tPD = ET("TrackPointDetail")
                    tPD.addChild(tPDC.find("pointDetailVelocity").clone())
                    tPD.addChild(tPDC.find("pointDetailCovarianceMatrix").clone())
                    if ("LocalCartesianPosition" == tPDC.find("pointDetailPosition").getAttribute("type")) {
                        var detail = ET("pointDetailPosition").setAttribute("type", "GeodeticPosition")
                        val pdp = tPDC.find("pointDetailPosition").clone()
                        val origin = pdp.find("localSystem").find("origin").clone()
                        origin.children.forEach {
                            detail.addChild(it.clone())
                        }
                        tPD.addChild(detail)
                    } else {
                        tPD.addChild(tPDC.find("pointDetailPosition").clone())
                    }
                    trackPointItems.addChild(tPD)

                    tracks.addChild(trackPointItems.clone())
                } else if ("VideoInformation" != it.getAttribute("type")) {
                    tracks.addChild(it.clone())
                }
            }
            elemTree.addChild(tracks.clone())
        }

        val xmlBytes = elemTree.root.toXML().toByteArray(Charsets.UTF_8)
        val len = xmlBytes.size
        val byteBuffer = ByteBuffer.wrap(xmlBytes)
//                encoder.reset()
//                val byteBuffer = ByteBuffer.allocate(xml.length + 1)
//                encoder.encode(CharBuffer.wrap(xml.toCharArray()),byteBuffer, false)
//        encoder.encode(CharBuffer.wrap(""), byteBuffer, true)
//        encoder.flush(byteBuffer)

        val wholeFile = WholeFile(byteBuffer, "stream")

        val iterator = Stanag4676IngestPlugin().ingestWithMapper().toGeoWaveData(
                wholeFile,
                mutableListOf(SpatialTemporalDimensionalityTypeProvider.SpatialTemporalIndexBuilder().createIndex().id),
                null)


        iterator.forEach {
            val adapter = FeatureDataAdapter((it.value as SimpleFeature).featureType)
            val index = SpatialTemporalDimensionalityTypeProvider.SpatialTemporalIndexBuilder().createIndex()
            val writer = store.createWriter(
                    adapter,
                    index)
            writer.write(it.value as SimpleFeature)
        }
    } catch (ex: Exception) {
        ex.printStackTrace()
    }

}