import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.hadoop.fs._
import java.io.File
import java.net.URI

import util.Try

/**
  * Created by dmilan on 8/25/17.
  */
object CreateCSVFileFromHive{
   def main(args: Array[String]): Unit = {
    val sc = new SparkContext()
    //val sqlContext = new SQLContext(sc)
    val sqlContext = new HiveContext(sc)
    val df = sqlContext.sql("SELECT * FROM mycars")
     val srcpath = "hdfs://quickstart.cloudera:8020/user/cloudera/myfile000.csv"
     val destPath = "file:/tmp/cloudera/myfile.csv"
    df
      //.coalesce(1)
        .write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .save(srcpath)
     MyFileUtils.merge(sc,srcpath,destPath)




   }

}
