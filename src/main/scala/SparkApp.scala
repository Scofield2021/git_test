import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, StringType, StructField, StructType}
import sun.util.resources.cldr.fa.LocaleNames_fa
case class Person(name: String, age: Long)

object SparkApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Simple Application")
      .config("spark.master", "local[*]")
      .getOrCreate()

    val df = spark.read.csv("data/people.csv")

    //df.createOrReplaceTempView("people")
    //df.printSchema()
    df.filter(col("age") > 20).show()










  }
}