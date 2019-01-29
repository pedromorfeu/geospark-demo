import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.geometryObjects.Circle
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.{CircleRDD, SpatialRDD}
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS

object Distance {
  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .master("local[*]")
      .appName("myGeoSparkSQLdemo").getOrCreate()
    GeoSparkSQLRegistrator.registerAll(session)
    session.sparkContext.setLogLevel("ERROR")

    System.setProperty("geospark.global.charset", "utf8")
    System.setProperty("org.geotools.referencing.forceXY", "true")


    // Circle of 200m arround POINT (1.04125843698 41.6345227856)
    // Should intersect/contain POINT (1.0436518037 41.6345011605)

    // PostGIS:
    // select ST_within(
    //	 cast(ST_SetSRID(ST_GeomFromText('POINT (1.0436518037 41.6345011605)')::geography,4326) as geometry),
    //	 cast(st_buffer(ST_SetSRID(ST_GeomFromText('POINT (1.04125843698 41.6345227856)')::geography,4326), 200::float) as geometry)
    // )
    // returns true

    val pointASpatialRDD = new SpatialRDD[Geometry]
    val pointA1 = new GeometryFactory().createPoint(new Coordinate(1.04125843698, 41.6345227856))
    pointA1.setUserData(pointA1.toString)
    val pointA2 = new GeometryFactory().createPoint(new Coordinate(1.0436518037, 41.6345011605))
    pointA2.setUserData(pointA2.toString)
    val pointA3 = new GeometryFactory().createPoint(new Coordinate(1.04245510301, 41.6345119795))
    pointA3.setUserData(pointA3.toString)
    pointASpatialRDD.rawSpatialRDD = session.sparkContext.parallelize(Array(pointA1, pointA2, pointA3)).toJavaRDD().asInstanceOf[JavaRDD[Geometry]]
    pointASpatialRDD.CRSTransform("epsg:4326", "epsg:3035")
    println("first pointA:" + pointASpatialRDD.rawSpatialRDD.first())
    pointASpatialRDD.analyze()
    pointASpatialRDD.spatialPartitioning(GridType.KDBTREE, 1)

    val pointBSpatialRDD = new SpatialRDD[Geometry]
    val pointB = new GeometryFactory().createPoint(new Coordinate(1.04125843698, 41.6345227856))
    pointB.setUserData(pointB.toString)
    pointBSpatialRDD.rawSpatialRDD = session.sparkContext.parallelize(Array(pointB)).toJavaRDD().asInstanceOf[JavaRDD[Geometry]]
    pointBSpatialRDD.CRSTransform("epsg:4326", "epsg:3035")
    println("first pointB:" + pointASpatialRDD.rawSpatialRDD.first())
    val circleRDD = new CircleRDD(pointBSpatialRDD, 200D)
    circleRDD.spatialPartitioning(pointASpatialRDD.getPartitioner)

    val res = JoinQuery.SpatialJoinQueryFlat(pointASpatialRDD, circleRDD, false, false)

    println(res.count())
    res.rdd.collect().foreach(println)


    val sourceCRS = CRS.decode("epsg:4326")
    val targetCRS = CRS.decode("epsg:3035")
    val transform = CRS.findMathTransform(sourceCRS, targetCRS, false)
    val point1Meters = JTS.transform(pointA2, transform)
    val point2Meters = JTS.transform(pointB, transform)
    val circle = new Circle(point2Meters, 200D)

    println(point2Meters.buffer(200D).contains(point1Meters))

    //    println("circle contains point: " + circle.contains(point1Meters))
    //    println("point within circle: " + point1Meters.within(circle))
    //    println("point intersects circle: " + point1Meters.intersects(circle))

    val resDistance = JoinQuery.DistanceJoinQueryFlat(pointASpatialRDD, circleRDD, false, false)
    println(resDistance.count())
    resDistance.collect.toArray.foreach(println)


    session.close()

  }

}
