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
    spark.sparkContext.setLogLevel("ERROR")

    /*val df = spark.read.json("data/people.json")

    df.createOrReplaceTempView("people")
    df.printSchema()
    df.filter(col("age") > 20).show()*/

    val sc = spark.sparkContext
    val textFile = sc.textFile("/Users/ludenghui/IdeaProjects/spark-test/测试文本")

    //val nums = textFile.map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b)
    val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    wordCount.collect().foreach(println)











  }
}