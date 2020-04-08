import SparkPersonDS.spark
import org.apache.spark.sql._

object SparkStandAlone extends App {
  // System.setProperty("hadoop.home.dir", "C:\\CustomProgram\\hadoop")
  val spark = SparkSession.builder()
    .appName("FirstDeve")
    .master("spark://192.168.56.103:7077")
    .enableHiveSupport()
    //.config("spark.blockManager.port", "10025")
    // .config("spark.driver.blockManager.port", "10026")
    // .config("spark.driver.port", "10027")
    .config("inferschema", true)
    .config("spark.executor.cores", "1")
    .config("spark.cores.max", "1")
    //.config("spark.default.parallelism","1")
    .config("spark.executor.memory", "1g")
    // .config("hive.metastore.warehouse.dir", "/home/cloudera/spark/spark-warehouse/metastore_db")
    .config("spark.sql.warehouse.dir", "file://192.168.56.103/home/cloudera/spark")
    .getOrCreate()
  spark.sessionState.catalog.listDatabases().foreach(println)
  println(spark.sharedState.warehousePath)
  //spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
  spark.sql("show tables").show
  // spark.sql("select * from src").show
  val df = spark.read.csv("file:///home/cloudera/spark/employee.txt").toDF("id", "name", "age")
  spark.sql("drop table Employee")
  df.write.saveAsTable("Employee")
  spark.sql("select * from Employee").show
  spark.catalog.listTables().show



}
