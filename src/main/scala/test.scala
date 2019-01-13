import scala.math.random
import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi").setMaster("spark://192.168.43.120:50003")
      .setJars(List("D:\\Project\\IntelliJIDEAProjects\\testm\\target\\scala-2.11\\testm_2.11-0.1.jar"))
      .setIfMissing("spark.driver.host", "192.168.43.120")
      .set("spark.executor.memory", "471859200")
      .set("spark.testing.memory", "2147480000")
      .set("spark.driver.memory", "200m")
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = 100000 * slices
    val count = spark.parallelize(1 to n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    println("spark Test")
    spark.stop()
  }
}
