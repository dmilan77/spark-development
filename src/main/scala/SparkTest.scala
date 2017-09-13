import java.net.URI

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.TimestampType

object SparkTest {
  def main(args: Array[String]): Unit = {
    implicit val sc = new SparkContext()
    val hiveContext = new HiveContext(sc)
    val outputDF = hiveContext.sql("select * from employeehive limit 10")
    val col_select_list = outputDF.schema.fields
      .map(structField => {
        structField.dataType match {
          case _: TimestampType =>  "date_format(" + structField.name.toString() + ",\"YYYY/MM/dd\")"
          case _ => structField.name.toString()
        }
      })
      .mkString(",")
    println("***********************************************")
    println(col_select_list)

    println("***********************************************")
    val r = scala.util.Random.nextInt(99999).toString
    val tempTableName = "tmptsvxport".concat(r)
    // scala.util.Try(hiveContext.dropTempTable(tempTableName))

    outputDF.registerTempTable(tempTableName)

    val outputDF_1 =
      hiveContext.sql("select " + col_select_list + " from " + tempTableName)
//      hiveContext.sql("select " + "*" + " from " + tempTableName)
    outputDF_1.write
      .mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      //.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .option("inferSchema", "true")
      .option("quoteMode", "NON_NUMERIC")
      .option("timestampformat", "yyyy/mm/dd")
      .save("hdfs://quickstart.cloudera:8020/user/cloudera/employee.tsv")
    merge("hdfs://quickstart.cloudera:8020/user/cloudera/employee.tsv","file:///tmp/cloudera/out/employee.tsv")
    scala.util.Try(hiveContext.dropTempTable(tempTableName))


  }

  private def merge(srcPath: String, dstPath: String)(
    implicit sc: SparkContext): Unit = {
    val srcFileSystem = FileSystem.get(new URI(srcPath), sc.hadoopConfiguration)
    val dstFileSystem = FileSystem.get(new URI(dstPath), sc.hadoopConfiguration)
    dstFileSystem.delete(new Path(dstPath), true)
    FileUtil.copyMerge(srcFileSystem,
      new Path(srcPath),
      dstFileSystem,
      new Path(dstPath),
      true,
      sc.hadoopConfiguration,
      null)

  }

}