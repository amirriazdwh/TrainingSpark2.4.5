import java.io.File
import org.apache.spark.sql.SparkSession

object SparkPersonDS extends  App{

  val warehouseLocation = new File("spark-warehouse").getAbsolutePath

  val spark = SparkSession.builder()
                          .appName("SparkDetSet")
                          .config("spark.sql.warehouse.dir", "file:/home/cloudera/spark")
                          .master("spark://192.168.56.103:7077")
                          .enableHiveSupport()
    // enable hive support will force the spark to read hive metastore and read the database tables
                          .getOrCreate()

 // val name = classOf[Person]
  println(warehouseLocation)
  spark.sessionState.catalog.listDatabases().foreach(println)
  println(spark.sharedState.warehousePath)
  spark.catalog.listTables().show

  val sqlText =
    """select id,
            | name ,
            | age
            | from vwEmployee""".stripMargin

  val engineSpark = Person.registerPerson(spark)
  spark.catalog.listTables().show
  spark.sql("show tables").show
  engineSpark.sql(sqlText).show()


}