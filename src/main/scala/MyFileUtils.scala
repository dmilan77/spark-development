import java.net.URI

import org.apache.hadoop.fs.{FileUtil, Path, FileSystem}
import org.apache.spark.SparkContext

object  MyFileUtils {



   def merge(sc: SparkContext,
                    srcPath: String,
                    dstPath: String): Unit = {
    val srcFileSystem = FileSystem.get(new URI(srcPath),
      sc.hadoopConfiguration)
    val dstFileSystem = FileSystem.get(new URI(dstPath),
      sc.hadoopConfiguration)
    dstFileSystem.delete(new Path(dstPath), true)
    FileUtil.copyMerge(srcFileSystem, new Path(srcPath),
      dstFileSystem, new Path(dstPath),
      true, sc.hadoopConfiguration, null)
  }


}
