package filodb.repair

import java.io.File

import com.typesafe.config.ConfigFactory
import filodb.cassandra.DefaultFiloSessionProvider
import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.core.GlobalConfig
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.downsample.OffHeapMemory
import filodb.core.memstore.{TimeSeriesPartition, TimeSeriesShardStats}
import filodb.core.metadata.{Dataset, Schema, Schemas}
import filodb.core.store.{PartKeyRecord, StoreConfig}
import filodb.memory.format.ZeroCopyUTF8String
import filodb.memory.format.ZeroCopyUTF8String._
import monix.reactive.Observable
import org.apache.spark.SparkConf
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class PartitionKeysCopierSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll with ScalaFutures {
  implicit val defaultPatience = PatienceConfig(timeout = Span(15, Seconds), interval = Span(250, Millis))

  implicit val s = monix.execution.Scheduler.Implicits.global

  val configPath = "conf/timeseries-filodb-server.conf"

  val sysConfig = GlobalConfig.systemConfig.getConfig("filodb")

  val config = ConfigFactory.parseFile(new File(configPath)).getConfig("filodb").withFallback(sysConfig)

  // TODO: change this!! how do we pick the right file path here? Or we read all of them?
  val sourceConfigPath = config.getStringList("dataset-configs").get(0)
  val shards = ConfigFactory.parseFile(new File(sourceConfigPath)).getInt("num-shards")

  lazy val session = new DefaultFiloSessionProvider(config.getConfig("cassandra")).session
  lazy val colStore = new CassandraColumnStore(config, s, session)

  //  val lastSampleTime = 74373042000L
  //  val pkUpdateHour = hour(lastSampleTime)
  var gauge1PartKeyBytes: Array[Byte] = _
  var gauge2PartKeyBytes: Array[Byte] = _
  var gauge3PartKeyBytes: Array[Byte] = _
  var gauge4PartKeyBytes: Array[Byte] = _
  val rawDataStoreConfig = StoreConfig(ConfigFactory.parseString(
    """
      |flush-interval = 1h
      |shard-mem-size = 1MB
      |ingestion-buffer-mem-size = 30MB
                """.stripMargin))
  val offheapMem = new OffHeapMemory(Seq(Schemas.gauge, Schemas.promCounter, Schemas.promHistogram, Schemas.untyped),
    Map.empty, 100, rawDataStoreConfig)
  val seriesTags1 = Map("_ws_".utf8 -> "my_ws1".utf8, "_ns_".utf8 -> "my_ns1".utf8)
  val seriesTags2 = Map("_ws_".utf8 -> "my_ws2".utf8, "_ns_".utf8 -> "my_ns2".utf8)
  val seriesTags3 = Map("_ws_".utf8 -> "my_ws3".utf8, "_ns_".utf8 -> "my_ns3".utf8)

  val sourceDataset = Dataset("source", Schemas.gauge)
  val targetDataset = Dataset("target", Schemas.gauge)

  val shardStats = new TimeSeriesShardStats(sourceDataset.ref, -1)

  def hour(millis: Long = System.currentTimeMillis()): Long = millis / 1000 / 60 / 60

  override def beforeAll(): Unit = {
    colStore.initialize(sourceDataset.ref, 1).futureValue
    colStore.truncate(sourceDataset.ref, 1).futureValue

    colStore.initialize(targetDataset.ref, 1).futureValue
    colStore.truncate(targetDataset.ref, 1).futureValue
  }

  override def afterAll(): Unit = {
    offheapMem.free()
  }

  it("should write test data to cassandra source table") {
    def writePartKeys(pk: PartKeyRecord, shard: Int): Unit = {
      colStore.writePartKeys(sourceDataset.ref, shard, Observable.now(pk), 259200, 0L, false).futureValue
    }

    def tsPartition(schema: Schema,
                    gaugeName: String,
                    seriesTags: Map[ZeroCopyUTF8String, ZeroCopyUTF8String]): TimeSeriesPartition = {
      val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
      val partKey = partBuilder.partKeyFromObjects(schema, gaugeName, seriesTags)
      val part = new TimeSeriesPartition(0, schema, partKey,
        0, offheapMem.bufferPools(schema.schemaHash), shardStats,
        offheapMem.nativeMemoryManager, 1)
      part
    }

    gauge1PartKeyBytes = tsPartition(Schemas.gauge, "my_gauge1", seriesTags1).partKeyBytes
    writePartKeys(PartKeyRecord(gauge1PartKeyBytes, 1507923801000L, Long.MaxValue, Some(150)), 0) // 2017

    gauge2PartKeyBytes = tsPartition(Schemas.gauge, "my_gauge2", seriesTags2).partKeyBytes
    writePartKeys(PartKeyRecord(gauge2PartKeyBytes, 1539459801000L, Long.MaxValue, Some(150)), 0) // 2018

    gauge3PartKeyBytes = tsPartition(Schemas.gauge, "my_gauge3", seriesTags3).partKeyBytes
    writePartKeys(PartKeyRecord(gauge3PartKeyBytes, 1602554400000L, 1602561600000L, Some(150)), 0) // 2020-10-13 02:00-04:00 GMT

    gauge4PartKeyBytes = tsPartition(Schemas.gauge, "my_gauge4", seriesTags3).partKeyBytes
    writePartKeys(PartKeyRecord(gauge4PartKeyBytes, 1602549000000L, 1602561600000L, Some(150)), 0) // 2020-10-13 00:30-04:00 GMT
  }

  it("should run a simple Spark job") {
    // This test verifies that the configuration can be read and that Spark runs. A full test
    // that verifies chunks are copied correctly is found in CassandraColumnStoreSpec.

    val sparkConf = new SparkConf(loadDefaults = true)
    sparkConf.setMaster("local[2]")

    sparkConf.set("spark.filodb.partitionkeys.copier.source.configFile", configPath)
    sparkConf.set("spark.filodb.partitionkeys.copier.source.dataset", "source")

    sparkConf.set("spark.filodb.partitionkeys.copier.target.configFile", configPath)
    sparkConf.set("spark.filodb.partitionkeys.copier.target.dataset", "target")

    sparkConf.set("spark.filodb.partitionkeys.copier.ingestionTimeStart", "2020-10-13T00:00:00Z")
    sparkConf.set("spark.filodb.partitionkeys.copier.ingestionTimeEnd", "2020-10-13T05:00:00Z")
    sparkConf.set("spark.filodb.partitionkeys.copier.diskTimeToLive", "7d")

    PartitionKeysCopierMain.run(sparkConf).close()

    //    colStore.truncate(dataset.ref, 1).futureValue
    //    colStore.truncate(targetDataset.ref, 1).futureValue
  }

  it("verify data written onto cassandra target table") {
//    def partKeyBytes(pk: PartKeyRecord) : Array[Byte] = {
//      pk.
//    }

    val partKeys = Await.result(colStore.scanPartKeys(targetDataset.ref, 0).toListL.runAsync,
      Duration(1, "minutes"))
    partKeys.size shouldEqual 2



//    partKeys.map()
    for(pk <- partKeys) {

    }
  }
}
