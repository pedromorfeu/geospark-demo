import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}

object Main {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    .setAppName("GeoSparkRunnableExample") // Change this to a proper name
    .setMaster("local[*]") // Delete this if run in cluster mode
    // Enable GeoSpark custom Kryo serializer
    .set("spark.serializer", classOf[KryoSerializer].getName)
    .set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val shapesRDD = ShapefileReader.readToGeometryRDD(sc, "/Users/pedromorfeu/Downloads/portugal-latest-free.shp/gis_osm_pois_a_free_1.shp")

    println("total: " + shapesRDD.count())

    println("first: " + shapesRDD.first())

    val shapesSpatialRDD = new SpatialRDD[Geometry]
    shapesSpatialRDD.rawSpatialRDD = shapesRDD

    shapesSpatialRDD.CRSTransform("epsg:4326", "epsg:3035")

    val buildings = shapesSpatialRDD.rawSpatialRDD.take(2)

    buildings.toArray.foreach(println)
    println("distance: " + buildings.get(0).distance(buildings.get(1)))


    shapesSpatialRDD.analyze()
    println(shapesSpatialRDD.boundaryEnvelope)

    shapesSpatialRDD.buildIndex(IndexType.RTREE, false)

    shapesSpatialRDD.spatialPartitioning(GridType.KDBTREE, 5)
    println(shapesSpatialRDD.spatialPartitionedRDD.partitions.size())

    val start = System.currentTimeMillis()
    val resultRDD = JoinQuery.SpatialJoinQuery(shapesSpatialRDD, shapesSpatialRDD, true, true)
    println(resultRDD.count())
    println("took "  + ((System.currentTimeMillis() - start)/1000) + " secs")


    sc.stop()

  }

}
