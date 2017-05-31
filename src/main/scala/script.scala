// ATENCION: Recibe 1 argumentos 
import org.apache.spark.SparkContext, org.apache.spark.SparkConf
object SparkSQLExample {
  def main(args: Array[String]) {
	val conf = new SparkConf().setAppName("scala spark").setMaster(args(0))
	val sc = new SparkContext(conf)
	val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    //Nuevo
    val csvFile = sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").load("gs://midepo/datos/dataset.csv")
    csvFile.registerTempTable("clima")
    val distinctCountries = sqlContext.sql("select * from clima where TEMPERATURA_AMBIENTE = (select max(TEMPERATURA_AMBIENTE) from clima)")
    distinctCountries.collect.foreach(println)

    //val jsonRDD = sc.wholeTextFiles("gs://midepo/datos/dataset.json").map(x => x._2)
    //val df = sqlContext.read.json(jsonRDD)
	//df.show()    
  }
 }

/*import org.apache.spark.SparkContext, org.apache.spark.SparkConf
// ATENCION: Recibe 2 argumentos 
object SimpleApp {
  def main(args: Array[String]) {
  	//println("Hola Mundo")
    val conf = new SparkConf().setAppName("scala spark").setMaster(args(0))// este parametro es yarn
    val sc = new SparkContext(conf)
    val i = List(1, 2, 3, 4, 5)
    val dataRDD = sc.parallelize(i)
    dataRDD.saveAsTextFile(args(1)) //este parametro es gs://midepo/dataoutput
  }
}*/
