package org.apache.zeppelin.interpreter

import org.apache.zeppelin.spark.ZeppelinContext
import org.apache.spark.sql._
import org.apache.spark.sql.{SnappySession, SparkSession}
import scala.collection.mutable.{ListBuffer, Map}

class QueryBuilder{

}

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

/*  case class DataSource(shortName: String, fullName: String,
      jarDeps: Option[List[String]] = None, params : Option[List[ParamText]] = None) {
    def getDisplayDetails = (shortName, fullName)
    def render (z : ZeppelinContext) = params match {
      case Some(params) => parseAndRender(params,z)
      case _ =>
    }
  }*/

  // File formats
  val csv = "CSV"
  val prq = "Parquet"
  val json = "JSON"
  val orc = "ORC"
  val avro = "Avro"
  val xml = "XML"
  val txt = "Text"

  // Data sources
  val hdfs = "HDFS"
  val aws = "AWS"
  val lfs = "LFS"
  val gcs = "GCS"
  val adls = "ADLS"

  private val fileFormats = List((csv, csv),(prq,prq),(json,json),(orc,orc),(avro,avro),(xml,xml),(txt,txt))
  private val booleanOpts = List(("true", "true"), ("false", "false"))
  private val sources = List((hdfs, "Hadoop File System"),(aws, "Amazon S3"),(lfs, "Local File System"),
                        (gcs, "Google Cloud Storage"),(adls, "Microsoft Azure Store"))

  def getFileFormatParams(f : String) = f match {
    case `csv` => List(ParamText("encoding", "Encoding"),
      ParamText("delimiter", "Delimiter"),
      ParamText("header", "Header present", Some(booleanOpts)),
      ParamText("inferSchema", "Infer Schema", Some(booleanOpts)),
      ParamText("mode", "Mode for bad records", Some(List(("DROPMALFORMED", "DROPMALFORMED"), ("FAILFAST", "FAILFAST"))))
    )
    case `txt` => List(ParamText("text_header","Header in Text file",Some(booleanOpts)),
                     ParamText("text_delimiter","Delimiter in Text File"))
    case `xml` => List(ParamText("rowTag","Row Tag in XML file"))
    case _ => List()
  }

  def getDataSources = sources


  def getDataSourceParams(d : String) = d match {
    case `hdfs` => List(ParamText("namenode","HDFS Name node"))
    case `aws` => List(ParamText("id","S3 access ID"),ParamText("secret","S3 access secret"),ParamText("location","S3 bucket location"))
    case `adls` =>List(ParamText("storageAccount","Azure Storage Account"),ParamText("container","Azure Container"),
      ParamText("key","Azure Key"),ParamText("filepath","Azure File path in the container"))
    case `gcs` => List(ParamText("projectid","Project ID"),ParamText("keypath","Path of Key File"),
      ParamText("bucket","Bucket Name"),ParamText("path","File path in the bucket"))
    case `lfs` => List(ParamText("path","Path on the file system"))
    case _ => List()
  }

  def getDependencyJarNames (d: String) = d match {
    case `adls` =>List("hadoop-azure*.jar","azure-storage*.jar from mvnrepository.com")
    case `gcs` => List("gcs-connector-hadoop2-latest.jar from mvnrepository.com")
    case `avro` => List("Spark-Avro package from spark-packages.org")
    case `xml` => List("Spark-XML package from spark-packages.org")
    case _ => List()
  }

  def getFileFormats = fileFormats

  def connectSource(z: ZeppelinContext){}
}