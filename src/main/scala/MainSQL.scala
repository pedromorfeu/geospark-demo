import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}

object MainSQL {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder()
      .master("local[*]") // Delete this if run in cluster mode
      .appName("readTestScala") // Change this to a proper name
      // Enable GeoSpark custom Kryo serializer
      .config("geospark.join.numpartition", "5")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()
    GeoSparkSQLRegistrator.registerAll(session)
    session.sparkContext.setLogLevel("ERROR")

    System.setProperty("geospark.global.charset","utf8")

    val shapesRDD = ShapefileReader.readToGeometryRDD(session.sparkContext, "/Users/pedromorfeu/Downloads/portugal-latest-free.shp/pois")

    println("total: " + shapesRDD.countWithoutDuplicates())
    println("first: " + shapesRDD.rawSpatialRDD.first())

    //val shapesSpatialRDD = new SpatialRDD[Geometry]
    //shapesSpatialRDD.rawSpatialRDD = shapesRDD.asInstanceOf[JavaRDD[Geometry]]
    val shapesSpatialRDD = shapesRDD

    shapesSpatialRDD.CRSTransform("epsg:4326", "epsg:3035")

    val buildings = shapesSpatialRDD.rawSpatialRDD.take(2)

    buildings.toArray.foreach(println)
    println("distance: " + buildings.get(0).distance(buildings.get(1)))


    shapesSpatialRDD.analyze()
    println(shapesSpatialRDD.boundaryEnvelope)

    val shapesDF = Adapter.toDf(shapesSpatialRDD, session).select("geometry").withColumnRenamed("geometry", "rddshape")
    shapesDF.printSchema()
    shapesDF.show(3, false)
    shapesDF.createOrReplaceTempView("shapes")

    val start = System.currentTimeMillis()
    val resultsDF = session.sql(
      """
        | SELECT distinct s.rddshape
        | FROM shapes s
        | WHERE ST_Contains(ST_GeomFromWKT(s.rddshape), ST_GeomFromWKT(s.rddshape))
      """.stripMargin)
    println(resultsDF.count())
    println("took "  + ((System.currentTimeMillis() - start)/1000) + " secs")

    session.close()

  }

}
