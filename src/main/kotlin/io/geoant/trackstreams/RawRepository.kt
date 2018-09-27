package io.geoant.trackstreams

import org.springframework.data.cassandra.core.mapping.Column
import org.springframework.data.cassandra.core.mapping.PrimaryKey
import org.springframework.data.cassandra.core.mapping.Table
import org.springframework.data.cassandra.repository.ReactiveCassandraRepository
import scala.Tuple2
import java.io.Serializable
import java.util.*


@Table("xml_entries")
data class XmlEntry (@PrimaryKey("entry_id") val id : UUID = UUID.randomUUID(), @Column("entry_data") val xml : String)

data class XmlMsg( val line : String ) : Serializable

interface RawRepository : ReactiveCassandraRepository<XmlEntry,UUID>