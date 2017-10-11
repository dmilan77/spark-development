

import java.io._
import java.nio.charset.Charset
import java.util.zip.GZIPOutputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

/**
  * @author Milan Das : milan.das@anthem.com
  * @version 1.0
  */
object HadoopFileUtil {
  val log = Logger.getLogger(getClass.getName)

  /** Read files from  HDFS and merge it into singe file.
    *
    * @example 1
    *          {{{HadoopFileUtil.copyMerge(srcFileSystem,
    *                                        new Path(srcPath),
    *                                        dstFileSystem,
    *                                        new Path(dstPath),
    *                                        true,
    *sc.hadoopConfiguration,
    *                                        null,headerRecord,isPrintHeader, isGzipFile)
    *          }}}
    * @param srcFS          : Source FileSystem : {{{FileSystem.get(new URI(srcPath), sc.hadoopConfiguration)}}}
    * @param srcDir         : Source Directory
    * @param  dstFS         : Destination fileSystem
    * @param  destFile      : Destination fileSystem {{{FileSystem.get(new URI(destPath), sc.hadoopConfiguration)}}}
    * @param  deleteSource  : true if source file need to be deleted
    * @param  conf          : hadoop configuration
    * @param  addString     : adding extra string at the end of the line
    * @param  headerString  : header String
    * @param  isPrintHeader : print header record if flag is true
    * @param  isGzipFile    : true if file need to be gzip compressed format.
    */

  @throws[IOException]
  def copyMerge(srcFS: FileSystem,
                srcDir: Path,
                dstFS: FileSystem,
                destFile: Path,
                deleteSource: Boolean,
                conf: Configuration,
                addString: String,
                headerString: String,
                isPrintHeader: Boolean,
                isGzipFile: Boolean): Either[String, Boolean] = {
    checkDest(srcDir.getName, dstFS, destFile, false) match {
      case Left(error) => Left(error)
      case Right(dstFile) => {
        if (!srcFS.getFileStatus(srcDir).isDirectory) return Left(s"$srcDir is not a directory")
        var out: OutputStream =
          if (isGzipFile) new GZIPOutputStream(dstFS.create(dstFile))
          else dstFS.create(dstFile)
        if (headerString != null && isPrintHeader)
          out.write(headerString.getBytes(Charset.forName("UTF-8")))

        var contents: Array[FileStatus] = srcFS.listStatus(srcDir)
        contents = contents.sortBy(_.getPath.toString)
        contents.map { content => {
          if (content.isFile) {
            val in = srcFS.open(content.getPath)
            scala.util.Try(IOUtils.copyBytes(in, out, conf, false))
            if (addString != null) scala.util.Try(out.write(addString.getBytes("UTF-8")))
            in.close()
          }
          content
        }
        }

        scala.util.Try(out.close())
        if (deleteSource) srcFS.delete(srcDir, true) else true
        Right(true)
      }
    }


  }

  private[HadoopFileUtil] def checkDest(srcName: String,
                                        dstFS: FileSystem,
                                        dst: Path,
                                        overwrite: Boolean): Either[String, Path] = {
    if (dstFS.exists(dst)) {
      val sdst = dstFS.getFileStatus(dst)
      if (sdst.isDirectory) {
        if (null == srcName) Left(s"Target  $dst  is a directory")
        checkDest(null, dstFS, new Path(dst, srcName), overwrite)
      } else if (!overwrite)
        Left(s"Target  $dst  already exists")
    }

    Right(dst)
  }
  /** Read files from  HDFS and merge it into singe file.
    *
    * @example 1
    *          {{{HadoopFileUtil.renameFiles("/user/hive/testrename/wk_clm_rndrg_prov",
    *                                        "toNewFileName",
    *                                        sc)
    *          }}}
    * @param srcHdfsDir          : Source hdfs dir
    * @param targetFilePrefix         : Target File Name
    * @param  sc         : spark Context
    */
  def renameFiles(srcHdfsDir:String, targetFilePrefix:String,  fileExtension: String = "tsv.gz")( sc:SparkContext): Either[String, String]={
    log.info(s"""Renaming files from srcHdfsDir= $srcHdfsDir as targetFilePrefix= $targetFilePrefix """)
    val srcHdfsDirPath = new Path(srcHdfsDir)
    val hadoopConfig = sc.hadoopConfiguration
    val fileSystem = srcHdfsDirPath.getFileSystem(hadoopConfig)
    val srcDirStatus = fileSystem.listStatus(srcHdfsDirPath)
    if (srcDirStatus.isEmpty ) Left(s"$srcHdfsDir  is Empty")
    else {
      srcDirStatus.zipWithIndex.par.foreach{case(fileStatus, counter) => {

        val paddedCounter =  "000" + counter takeRight 4
        val srcPathString = fileStatus.getPath.toString
        if (srcPathString.contains("part-")) {
          //log.info(s"""srcPathString = $srcPathString""")
          val destPathString = srcPathString.substring(0, srcPathString.lastIndexOf("part-")).concat(s"""$targetFilePrefix.$paddedCounter.${fileExtension}""")

          val srcPath = new Path(srcPathString)
          val destPath = new Path(destPathString)
          fileStatus.getPath.getFileSystem(hadoopConfig).rename(srcPath, destPath)
        }
        else log.info(s"""Skipping : $srcPathString""")

      }}
      Right("Files renamed Successfully.. ")
    }

  }
}
