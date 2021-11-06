import org.apache.spark.SparkContext

object Test {

  def main(args: Array[String]) {
val sc: SparkContext

    val nums = sc.parallelize(List(1, 2, 3, 4))
    val squared = nums.map(x => x * x).collect()


  }
}
