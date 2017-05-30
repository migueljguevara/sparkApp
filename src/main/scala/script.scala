import org.apache.spark.SparkContext, org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {
  	//println("Hola Mundo")
    val conf = new SparkConf().setAppName("scala spark").setMaster(args(0))// este parametro es yarn
    val sc = new SparkContext(conf)
    val i = List(1, 2, 3, 4, 5)
    val dataRDD = sc.parallelize(i)
    dataRDD.saveAsTextFile(args(1)) //este parametro es gs://midepo/dataoutput
  }
}