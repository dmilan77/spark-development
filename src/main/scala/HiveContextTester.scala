import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by dmilan on 9/12/17.
  */
object HiveContextTester {
  def main(args: Array[String]): Unit = {
    implicit val sc = new SparkContext()
    val hiveContext = new HiveContext(sc)
    hiveContext.sql(
      s"""create table b1(col0 string,col1 string,col2 string,col3 string,col4 string,col5 string,col6 string)
                                    		       |clustered by (col0) into 32 buckets
                                               |
		     """.stripMargin)
    }


}
