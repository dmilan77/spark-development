import com.anthem.hca.ebp.etl.{ETLStrategy, ImportTrait}
import com.anthem.hca.util.io.HadoopFileUtil
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * @author Milan Das
  * @version 1.0
  
  */
class SampleSparkJob  {


  override def importData: Unit = {
    val sc: SparkContext = getSparkContext
    val outboundPath = ConfigFactory.load().getString("location.outbound")

    val sqltext = """select * from sampletable"""

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val df = new HiveContext(sc).sql(sqltext)
    val dfwindow = Window.partitionBy("state").orderBy($"empid".desc)
    val orderedDf = df.withColumn("rank",rank().over(dfwindow)).filter($"rank"===1)

    orderedDf.collect().foreach(log.info(_))





  }

  def openFileRaw = ???
  def ??? : Nothing = throw new NotImplementedError

}
