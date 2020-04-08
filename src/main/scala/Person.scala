import org.apache.spark.sql.SparkSession

case class Person(val id:Int, val name:String, val age:Int)

object Person{
  def registerPerson(spark:SparkSession)={
    // read speak data set
    // implicits ensure that rest of the encoders of as are passed implicitly
    import spark.implicits._
    val path="file:///home/cloudera/spark/employee.txt"
    val ds = spark.read.option("inferschema",true).csv(path).toDF("id","name","age").as[Person]
    ds.createOrReplaceTempView("vwEmployee")

    spark
  }
}
