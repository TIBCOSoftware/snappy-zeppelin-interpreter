package org.apache.zeppelin.interpreter

import org.apache.zeppelin.spark.ZeppelinContext
import org.apache.spark.sql._
import org.apache.spark.sql.{SnappySession, SparkSession}
import scala.collection.mutable.{ListBuffer, Map}
import org.apache.spark.sql._


object QueryBuilder {

  case class ParamText(name: String, desc: String, opts: Option[List[(String, String)]] = None) {

    def render(z: ZeppelinContext) = opts match {
      case Some(select) => z.select(desc,select.toSeq).asInstanceOf[String]
      case _ => z.input(desc).asInstanceOf[String]
    }
  }

  def parseAndRender(pList : List[ParamText], z : ZeppelinContext) = {
    val paramMap = scala.collection.mutable.Map.empty[String, String]
    for (p <- pList)
      paramMap += (p.name -> p.render(z))
    paramMap
  }

  def printHTML(deps : List[String], src : String){
    println(s"""%html <div><h5><span style="font-weight: bold;"> $src</h5> """)
    deps.length match {
      case 0 => println(s"""%html None""")
      case _ => deps.map(d => println(s"""%html $d"""))
     }
  }

/*  def getPathFromParams(params : scala.collection.mutable.Map[String,String]) = {
    case `hdfs` => ???
    case `aws` => ???
    case `gcs` => ???
    case `adls` => ???
    case _ => ???
  }*/

  // File formats
  lazy val csv = "CSV"
  lazy val prq = "Parquet"
  lazy val json = "JSON"
  lazy val orc = "ORC"
  lazy val avro = "Avro"
  lazy val xml = "XML"
  lazy val txt = "Text"

  // Data sources and parameters
  lazy val hdfs = "HDFS"
  lazy val hdfs_namenode = "namenode"
  lazy val hdfs_path = "path"

  lazy val aws = "AWS"
  lazy val aws_id = "id"
  lazy val aws_secret = "secret"
  lazy val aws_location = "location"

  lazy val lfs = "LFS"
  lazy val lfs_path = "path"

  lazy val gcs = "GCS"
  lazy val gcs_projID = "projectid"
  lazy val gcs_keyPath = "keypath"
  lazy val gcs_bucket = "bucket"
  lazy val gcs_path = "path"

  lazy val adls = "ADLS"
  lazy val adls_storage= "storageAccount"
  lazy val adls_key = "key"
  lazy val adls_container = "container"
  lazy val adls_filepath = "filepath"


  private lazy val fileFormats = List((csv, csv),(prq,prq),(json,json),(orc,orc),(avro,avro),(xml,xml),(txt,txt))
  private lazy val booleanOpts = List(("true", "true"), ("false", "false"))
  private lazy val sources = List((hdfs, "Hadoop File System"),(aws, "Amazon S3"),(lfs, "Local File System"),
                        (gcs, "Google Cloud Storage"),(adls, "Microsoft Azure Store"))

  def getFileFormatParams(f : String) = f match {
    case `csv` => List(ParamText("encoding", s"Encoding $csv"),
      ParamText("delimiter", s"Delimiter in $csv"),
      ParamText("header", s"Header present in $csv", Some(booleanOpts)),
      ParamText("inferSchema", s"Infer Schema from $csv" , Some(booleanOpts)),
      ParamText("mode", s"Mode for bad records in $csv", Some(List(("DROPMALFORMED", "DROPMALFORMED"), ("FAILFAST", "FAILFAST"))))
    )
    case `txt` => List(ParamText("text_header","Header in Text file",Some(booleanOpts)),
                     ParamText("text_delimiter","Delimiter in Text File"))
    case `xml` => List(ParamText("rowTag","Row Tag in XML file"))
    case _ => List()
  }

  def getDataSources = sources

  def getDataSourceParams(d : String) = d match {
    case `hdfs` => List(ParamText(hdfs_namenode,"HDFS Name node"),ParamText(hdfs_path,"Path of file in HDFS "))
    case `aws` => List(ParamText(aws_id,"S3 access ID"),ParamText(aws_secret,"S3 access secret"),ParamText(aws_location,"S3 bucket location"))
    case `adls` => List(ParamText(adls_storage,"Azure Storage Account"),ParamText(adls_container,"Azure Container"),
      ParamText(adls_key,"Azure Key"),ParamText(adls_filepath,"Azure File path in the container"))
    case `gcs` => List(ParamText(gcs_projID,"GCS Project ID"),ParamText(gcs_keyPath,"Path of Key File"),
      ParamText(gcs_bucket,"GCS Bucket Name"),ParamText(gcs_path,"File path in the GCS bucket"))
    case `lfs` => List(ParamText(lfs_path,"Path on the local file system"))
    case _ => List()
  }

  def getDependencyJarNames (d: String) = d match {
    case `adls` => List("hadoop-azure*.jar","azure-storage*.jar")
    case `gcs`  => List("gcs-connector-hadoop2-latest.jar")
    case `avro` => List("Spark-Avro package")
    case `xml`  => List("Spark-XML package")
    case _      => List()
  }

  def getFileFormats = fileFormats

  /*def connectSource(z: ZeppelinContext): Unit ={
    import org.apache.hadoop.conf.Configuration._
    import org.apache.hadoop.fs.FileSystem._
    import org.apache.hadoop.fs.FileSystem
    import org.apache.hadoop.fs.Path
    import org.apache.hadoop.fs.permission.FsPermission
    import org.apache.hadoop.util.Progressable
    val ds = z.get("dataSource").asInstanceOf[String]
    val path = ds match {
      case `hdfs` =>
      case `aws` =>
      case `adls` =>
      case `gcs` =>
      case `lfs` =>
      case _ => ""
    }
  }*/
}