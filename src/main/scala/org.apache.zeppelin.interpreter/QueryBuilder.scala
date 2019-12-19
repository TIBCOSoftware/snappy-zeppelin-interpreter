/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2017 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package org.apache.zeppelin.interpreter

import scala.collection.mutable

import org.apache.zeppelin.spark.ZeppelinContext

import org.apache.spark.sql._
import org.apache.spark.sql.{SnappySession, SparkSession}
import scala.collection.mutable.{ListBuffer, Map}

import org.apache.spark.sql._
import org.apache.spark.SparkContext

/**
 * An utility package for AngularJS code generation and basic query string formation based on user inputs
 * gathered using Zeppelin Notebooks using AngularJS UI controls and some of the Zeppelin APIs.
 * # Uses Zeppelin Context as the inter paragraph data exchange mechanism.
 * # Using AngularBind API of Zeppelin context to limit the access of variables to limited paragraphs.
 */
object QueryBuilder {

  // File formats
  lazy val csv = "CSV"
  lazy val csv_delimiter = "delimiter"
  lazy val csv_charset = "charset"
  lazy val csv_quote = "quote"
  lazy val csv_escape = "escape"
  lazy val csv_comment = "comment"
  lazy val csv_header = "header"
  lazy val csv_inferSchema = "inferSchema"
  lazy val csv_mode = "mode"
  lazy val csv_ignoreLeadingWhiteSpace = "ignoreLeadingWhiteSpace"
  lazy val csv_ignoreTrailingWhiteSpace = "ignoreTrailingWhiteSpace"
  lazy val csv_nullValue = "nullValue"
  lazy val csv_nanValue = "nanValue"
  lazy val csv_positiveInf = "positiveInf"
  lazy val csv_negativeInf = "negativeInf"
  lazy val csv_compressionCodec = "compression"
  lazy val csv_dateFormat = "dateFormat"
  lazy val csv_timestampFormat = "timestampFormat"
  lazy val csv_maxColumns = "maxColumns"
  lazy val csv_maxCharsPerColumn = "maxCharsPerColumn"
  lazy val csv_escapeQuotes = "escapeQuotes"
  lazy val csv_maxMalformedLogPerPartition = "maxMalformedLogPerPartition"
  lazy val csv_quoteAll = "quoteAll"

  lazy val prq = "Parquet"
  lazy val prq_merge_schema = "mergeSchema"
  lazy val prq_compression = "compression"

  lazy val json = "JSON"
  lazy val json_sampling_ratio = "samplingRatio"
  lazy val json_primitivesAsString = "primitivesAsString"
  lazy val json_prefersDecimal = "prefersDecimal"
  lazy val json_allowComments = "allowComments"
  lazy val json_allowUnquotedFieldNames = "allowUnquotedFieldNames"
  lazy val json_allowSingleQuotes = "allowSingleQuotes"
  lazy val json_allowNumericLeadingZeros = "allowNumericLeadingZeros"
  lazy val json_allowNonNumericNumbers = "allowNonNumericNumbers"
  lazy val json_allowBackslashEscapingAnyCharacter = "allowBackslashEscapingAnyCharacter"
  lazy val json_compressionCodec = "compression"
  lazy val json_parseMode = "parseMode"
  lazy val json_dateFormat = "dateFormat"
  lazy val json_timeStampFormat = "timeStampFormat"

  lazy val orc = "ORC"
  lazy val orc_compressionCodec = "compression"

  lazy val avro = "Avro"
  lazy val avro_source = "com.databricks.spark.avro"

  lazy val xml = "XML"
  lazy val xml_rowtag = "rowTag"
  lazy val xml_source = "com.databricks.spark.xml"

  lazy val txt = "Text"
  lazy val txt_header = "text_header"
  lazy val txt_delimiter = "text_delimiter"

  // Data sources and parameters
  lazy val hdfs = "hdfs"
  lazy val hdfs_namenode = "namenode"
  lazy val hdfs_path = "path"

  lazy val aws = "AWS"
  lazy val aws_id = "id"
  lazy val aws_secret = "secret"
  lazy val aws_location = "location"
  lazy val aws_accessor = "accessor"

  lazy val lfs = "LFS"
  lazy val lfs_path = "path"

  lazy val gcs = "GCS"
  lazy val gcs_projID = "projectid"
  lazy val gcs_keyPath = "keypath"
  lazy val gcs_bucket = "bucket"
  lazy val gcs_path = "path"
  lazy val gcs_accessor = "gs"

  lazy val adls = "ADLS"
  lazy val adls_storage = "storageAccount"
  lazy val adls_key = "key"
  lazy val adls_container = "container"
  lazy val adls_filepath = "filepath"
  lazy val adls_accessor = "wasb"
  lazy val adls_domain = ".blob.core.windows.net"

  // Some reusable strings
  private lazy val fileFormats = List((csv, csv), (prq, prq), (json, json), (orc, orc), (avro, avro), (xml, xml), (txt, txt))
  private lazy val booleanOpts = List(("true", "true"), ("false", "false"))
  private lazy val sources = List((hdfs, "Hadoop File System"), (aws, "Amazon S3"), (lfs, "Local File System"),
    (gcs, "Google Cloud Storage"), (adls, "Microsoft Azure Store"))

  private val dlSlash = "/"
  private val dlColon = ":"
  private val dlColonSlash = dlColon + dlSlash + dlSlash
  private val dlAtRate = "@"
  private val system_default = "system_default"
  private val html = "html"
  private val query = "query"

  // AngularJS related element properties and functions
  private lazy val selectAll = "selectAll"
  //FIXME : overflow parameter for tables not reflecting when set here.
  private lazy val tableStyle =
    s"""
     <style>
      table, th, td {
        border: 1px solid black;
      }
      th, td {
        padding: 2px;
      }
     </style>"""

  private lazy val confirmMessage = s"""confirm('Values updated successfully.')"""
  private lazy val resetMessage = s"""confirm('Values being reset. Restarting in a new copy of this notebook recommended.')"""
  private lazy val confirmOnClickFunction =
    s"""
       |function confirm(message){
       |  alert(message)
       |}
     """.stripMargin
  private lazy val btnSubmit = "submit"
  private lazy val btnReset = "reset"
  private lazy val btnText = "text"
  private lazy val btnSuccess = "btn-success"
  private lazy val btnPrimary = "btn-primary"
  private lazy val btnDanger = "btn-danger"

  /**
   * Helper function for generating the buttons related section of the final string to be submitted to Angular.
   * @param buttons
   * @return
   */
  private def renderButtons(buttons: List[AngularButton]): String = {
    val render = new StringBuilder()
    val script = new StringBuilder(s"""<script>""")
    for (b <- buttons) {
      render ++= s"""<button type="${b.btType}" class="btn ${b.btClass}" """
      b.btNgClick match {
        case Some(x) => render ++= s"""ng-click = "$x""""
        case _ =>
      }
      (b.btOnClickSignature, b.btOnClickFunction) match {
        case (Some(x), Some(y)) => render ++= s"""onClick = "$x""""
          script ++= y
        case (_, _) =>
      }
      render ++= s">${b.btLabel}</button>&emsp;"
    }
    script ++= (s"""</script>""")
    render.toString + script.toString
  }

  /**
   * A representation of parameters that need to be passed to the file readers.
   *
   * @param name    name of the property as specified in the Spark (or reader library) documentation
   * @param desc    Description for the label that accompanies the input UI control.
   * @param default Default value if any, as specified in the Spark (or reader library) documentation
   * @param opts    List of options in case the property values can be defined in a limited set of known values.
   * @param secure  Whether parameter captures sensitive information that needs to be obscured on the UI.
   */
  case class ParamText(
      name: String,
      desc: String,
      default: String = system_default,
      opts: Option[List[(String, String)]] = None,
      secure: Option[String] = None)

  /**
   * Representation of a AngularJS button.
   *
   * @param btType    Type of button (text,submit,reset)
   * @param btLabel   Text to be populated in the button highlighting its purpose
   * @param btClass   Class of the button (https://www.w3schools.com/bootstrap/bootstrap_buttons.asp)
   * @param btNgClick String representation of the code that needs to be executed on click.
   *                  Used for binding with zeppelin context for now.
   * @param btOnClickSignature Function that gets registered with the OnClick property of the button.
   * @param btOnClickFunction Complete function definition that gets added to the "script" section of the Angular code.
   */
  case class AngularButton(
      btType: String,
      btLabel: String,
      btClass: String,
      btNgClick: Option[String] = None,
      btOnClickSignature: Option[String] = None,
      btOnClickFunction: Option[String] = None
  )

  /**
   * Utility function to render list of parameters using AngularJS controls.
   * @param pList List of ParamText
   * @param paraIDs
   * @return
   */
  def renderParamList(pList: List[ParamText], paraIDs: List[String]): String = {
    val s = new StringBuilder()
    val bind = new StringBuilder()
    val unbind = new StringBuilder()
    val labelWidth = 250
    pList match {
      case Nil =>
        s"""%angular
            <h4 style="color:blue;font-style:italic;">
            Current selection does not need any parameters to be initialized.</h4>""".stripMargin
      case l : List[ParamText] => for (p <- l) {
        p.opts match {
          case Some(x) =>
            s ++=
                s"""<mat-form-field><label for="select${p.name}" style="width:${labelWidth}px">${p.name}
                    <span style="font-style:italic;" >(default: ${p.default})</span></label>
                    <select matNativeControl required ng-model="${p.name}">"""
            for (m <- x)
              s ++= s"""<option value="${m._1}">${m._2}</option>""" //FIXME:Default using "selected" not working.
            s ++= s"""</select></mat-form-field><br/>"""
            for (para <- paraIDs) {
              bind ++= s"""z.angularBind('${p.name}',${p.name},'$para');"""
              unbind ++= s"""z.angularUnbind('${p.name}','$para');"""
            }

          case _ =>
            s ++=
                s"""<label style="width:${labelWidth}px" for="${p.name}">${p.desc}</label> <input type=${
                  p.secure match {
                    case Some(y) => y
                    case _ => "text"
                  }
                } class="form-control" id="${p.name}" ng-model="${p.name}" ng-init="${p.name}='${p.default}'"}
                </input><br/>"""
            for (para <- paraIDs) {
              bind ++= s"""z.angularBind('${p.name}',${p.name},'$para');"""
              unbind ++= s"""z.angularUnbind('${p.name}','$para');"""
            }

        }
      }

        s"""%angular
        <form class="form-inline">
        <div>
            ${s.toString}
            ${
          renderButtons(List(AngularButton(btnSubmit, "Confirm", btnSuccess, Some(bind.toString), Some(confirmMessage), Some(confirmOnClickFunction)),
            AngularButton(btnSubmit, "Reset", btnDanger, Some(unbind.toString), None, None)))
        }
        </div>
        </form>
        """
    }
  }


  /**
   * Function to create the final path string based on user inputs provided for a particular data source.
   * @param z Zeppelin Context
   * @param ds
   * @return
   */
  def getPathFromParams(z: ZeppelinContext, ds: String): String = ds match {
    case `hdfs` => hdfs + dlColonSlash + z.angular(hdfs_namenode).asInstanceOf[String] + dlSlash +
        z.angular(hdfs_path).asInstanceOf[String]
    case `aws` => z.angular(aws_accessor) + dlColonSlash + z.angular(aws_id).asInstanceOf[String] + dlColon +
        z.angular(aws_secret).asInstanceOf[String] + dlAtRate + z.angular(aws_location).asInstanceOf[String]
    case `gcs` => gcs_accessor + dlColonSlash + z.angular(gcs_bucket).asInstanceOf[String] +
        dlSlash + z.angular(gcs_path).asInstanceOf[String]
    case `adls` => adls_accessor + dlColonSlash + z.angular(adls_container).asInstanceOf[String] +
        dlAtRate + z.angular(adls_storage).asInstanceOf[String] +
        adls_domain + dlSlash + z.angular(adls_filepath).asInstanceOf[String]
    case `lfs` => z.angular(lfs_path).asInstanceOf[String]
    case _ => ""
  }

  /**
   * Set parameters in the Spark Context to allow access without having to expose sensitive string in path.
   * @param sc Spark Context provided by Zeppelin.
   * @param z Zepplin Context
   * @param ds Data source identifier string
   */
  def configureDataSourceEnvParams(sc: org.apache.spark.SparkContext, z: ZeppelinContext, ds: String): Unit = ds match {
    case `gcs` =>
      sc.hadoopConfiguration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      sc.hadoopConfiguration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
      sc.hadoopConfiguration.set("fs.gs.project.id", z.angular(gcs_projID).asInstanceOf[String])
      sc.hadoopConfiguration.set("google.cloud.auth.service.account.enable", "true")
      sc.hadoopConfiguration.set("google.cloud.auth.service.account.json.keyfile", z.angular(gcs_keyPath).asInstanceOf[String])
    case `adls` =>
      sc.hadoopConfiguration.set("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      sc.hadoopConfiguration.set("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb")
      sc.hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      sc.hadoopConfiguration.set("fs.azure.account.key." + z.angular(adls_storage).asInstanceOf[String] + adls_domain, z.angular(adls_key).asInstanceOf[String])
    case `aws` =>
      sc.hadoopConfiguration.set("fs." + z.angular(aws_accessor).asInstanceOf[String] + ".awsAccessKeyId", z.angular(aws_id).asInstanceOf[String])
      sc.hadoopConfiguration.set("fs." + z.angular(aws_accessor).asInstanceOf[String] + ".awsSecretAccessKey", z.angular(aws_secret).asInstanceOf[String])
    case _ =>
  }

  /**
   * Get list of parameters associated with a file format. Parameters reference from Spark (or reader library) docs.
   * @param f File format identifier string
   * @return
   */
  def getFileFormatParams(f: String): List[ParamText] = f match {
    case `csv` => List(
      ParamText(csv_delimiter, csv_delimiter, ","),
      ParamText(csv_charset, csv_charset, "UTF-8"),
      ParamText(csv_quote, csv_quote,system_default),
      ParamText(csv_escape, csv_escape,system_default),
      ParamText(csv_comment, csv_comment),
      ParamText(csv_header, csv_header, "false", Some(booleanOpts)),
      ParamText(csv_inferSchema, csv_inferSchema, "false", Some(booleanOpts)),
      ParamText(csv_mode, csv_mode, "DROPMALFORMED", Some(List(("DROPMALFORMED", "DROPMALFORMED"),
        ("FAILFAST", "FAILFAST"),
        ("PERMISSIVE", "PERMISSIVE")))),
      ParamText(csv_ignoreLeadingWhiteSpace, csv_ignoreLeadingWhiteSpace, "false", Some(booleanOpts)),
      ParamText(csv_ignoreTrailingWhiteSpace, csv_ignoreTrailingWhiteSpace, "false", Some(booleanOpts)),
      ParamText(csv_nullValue, csv_nullValue),
      ParamText(csv_nanValue, csv_nanValue, "NaN"),
      ParamText(csv_positiveInf, csv_positiveInf, "Inf"),
      ParamText(csv_negativeInf, csv_negativeInf, "-Inf"),
      ParamText(csv_compressionCodec, csv_compressionCodec + "(or codec)", "uncompressed",
        Some(List(("uncompressed", "uncompressed"),
          ("bzip2", "bzip2"),
          ("deflate", "deflate"),
          ("gzip", "gzip"),
          ("lz4", "lz4"),
          ("snappy", "snappy")
        ))),
      ParamText(csv_dateFormat, csv_dateFormat, "yyyy-MM-dd"),
      ParamText(csv_timestampFormat, csv_timestampFormat),//FIXME : default parsing issue yyyy-MM-dd'T'HH:mm:ss.SSSZZ
      ParamText(csv_maxColumns, csv_maxColumns, "20480"),
      ParamText(csv_maxCharsPerColumn, csv_maxCharsPerColumn, "-1"),
      ParamText(csv_escapeQuotes, csv_escapeQuotes, "true", Some(booleanOpts)),
      ParamText(csv_maxMalformedLogPerPartition, csv_maxMalformedLogPerPartition, "10"),
      ParamText(csv_quoteAll, csv_quoteAll, "false", Some(booleanOpts))
    )
    case `prq` => List(ParamText(prq_merge_schema,"Merge Schema","false",Some(booleanOpts)),
      ParamText(prq_compression,prq_compression,"uncompressed",
        Some(List(("uncompressed","uncompressed"),
          ("snappy", "snappy"),
          ("gzip", "gzip"),
          ("lzo","lzo")
        ))))
    case `txt` => List(ParamText(txt_header, "Header in Text file", "true", Some(booleanOpts)),
      ParamText(txt_delimiter, "Delimiter in Text File", ","))
    case `xml` => List(ParamText(xml_rowtag, "Row Tag in XML file", "breakfast_menu"))
    case `json` => List(ParamText(json_sampling_ratio,json_sampling_ratio,"1.0"),
      ParamText(json_primitivesAsString,json_primitivesAsString,"false",Some(booleanOpts)),
      ParamText(json_prefersDecimal,json_prefersDecimal,"false",Some(booleanOpts)),
      ParamText(json_allowComments,json_allowComments,"false",Some(booleanOpts)),
      ParamText(json_allowUnquotedFieldNames,json_allowUnquotedFieldNames,"false",Some(booleanOpts)),
      ParamText(json_allowSingleQuotes,json_allowSingleQuotes,"true",Some(booleanOpts)),
      ParamText(json_allowBackslashEscapingAnyCharacter,json_allowBackslashEscapingAnyCharacter,"false",Some(booleanOpts)),
      ParamText(json_compressionCodec, json_compressionCodec, "uncompressed",
        Some(List(("uncompressed", "uncompressed"),
          ("bzip2", "bzip2"),
          ("deflate", "deflate"),
          ("gzip", "gzip"),
          ("lz4", "lz4"),
          ("snappy", "snappy")
        ))),
      ParamText(json_parseMode, json_parseMode, "DROPMALFORMED", Some(List(("DROPMALFORMED", "DROPMALFORMED"),
        ("FAILFAST", "FAILFAST"),
        ("PERMISSIVE", "PERMISSIVE")))),
      ParamText(json_dateFormat,json_dateFormat),
      ParamText(json_timeStampFormat,json_timeStampFormat)
    )
    case `orc` => List(ParamText(orc_compressionCodec,orc_compressionCodec,"snappy",
      Some(List(("snappy","snappy"),
        ("snappy", "snappy"),
        ("uncompressed","uncompressed"),
        ("zlib", "zlib"),
        ("lzo","lzo")
      ))))
    case _ => List()
  }

  /**
   * Get the list of data sources currently supported by the functions.
   * @return
   */
  def getDataSources: List[(String, String)] = sources

  /**
   * Get parameters associated with a particular data source.
   * @param d String identifier for data source.
   * @return
   */
  def getDataSourceParams(d: String): List[ParamText] = d match {
    case `hdfs` => List(ParamText(hdfs_namenode, "HDFS Name node", "localhost:9000"), ParamText(hdfs_path, "Path of file in HDFS", ""))
    case `aws` => List(ParamText(aws_id, "S3 access ID", "", None, Some("password")), ParamText(aws_secret, "S3 access secret", "", None, Some("password"))
      , ParamText(aws_location, "S3 bucket location", ""),ParamText(aws_accessor,"AWS Accessor","s3a"))
    case `adls` => List(ParamText(adls_storage, "Azure Storage Account", "", None, Some("password")), ParamText(adls_container, "Azure Container", ""),
      ParamText(adls_key, "Azure Key", ""), ParamText(adls_filepath, "Azure File path in the container", ""))
    case `gcs` => List(ParamText(gcs_projID, "GCS Project ID", "", None, Some("password")), ParamText(gcs_keyPath, "Path of Key File", "", None, Some("password")),
      ParamText(gcs_bucket, "GCS Bucket Name", ""), ParamText(gcs_path, "File path in the GCS bucket", ""))
    case `lfs` => List(ParamText(lfs_path, "Path on the local file system", ""))
    case _ => List()
  }

  /**
   * Get the list of file formats currently supported by functions.
   * @return
   */
  def getFileFormats: List[(String, String)] = fileFormats

  /**
   * Get the string that needs to be passed as the options field in Query being submitted to spark.read.
   * @param ff file format identifier string
   * @param z Zeppelin Context
   * @return
   */
  def getFileFormatOptionsForSparkReader(ff: String, z: ZeppelinContext): Predef.Map[String, String] = {
    val opts = mutable.Map.empty[String, String]
    getFileFormatParams(ff) match {
      case Nil =>
      case x: List[ParamText] =>
        x.foreach(p => z.angular(p.name).asInstanceOf[String] match {
          case null => p.default match {
            case `system_default` =>
            case _ => opts += (p.name -> p.default)
          }
          case _ => opts += (p.name -> z.angular(p.name).asInstanceOf[String])
        })
    }
    opts.toMap
  }

  /**
   * Get the string that needs to be passed as the options field in Query being submitted to SnappySession.
   * @param ff File format identifier string
   * @param z Zeppelin Context
   * @return
   */
  def getFileFormatOptionsForSnappy(ff: String, z: ZeppelinContext): String = {
    val opts = new scala.collection.mutable.ListBuffer[String]
    getFileFormatParams(ff) match {
      case Nil =>
      case x : List[ParamText] => x.foreach(p =>
         z.angular(p.name).asInstanceOf[String] match {
          case null => p.default match {
            case `system_default` =>
            case _ => opts += s"""${p.name} '${p.default}'"""}
          case _ => opts += s"${p.name} '${z.angular(p.name).asInstanceOf[String]}'"
        })
      }
    opts.toList.mkString(",")
  }

  /**
   * Generate angular code of a table for schema with checkboxes for selection.
   * @param z Zeppelin Context
   * @param df Data frame from which schema will be inferred.
   * @param paraIDs Zeppelin paragraph with which these variables need to be bound.
   * @return
   */
  def generateTabularSchema(z: ZeppelinContext, df: org.apache.spark.sql.DataFrame, paraIDs: List[String]): String = {
    val cols = scala.collection.mutable.ListBuffer[(String, String, String)]()
    val jsTableName = "TabularSchema"
    for (s <- df.schema) {
      cols += ((s.name.toString, s.dataType.toString, s.nullable.toString))
    }
    val s = new StringBuilder()
    val bindCheckboxes = new StringBuilder()
    val unbind = new StringBuilder()
    for (c <- cols) {
      s ++=
          s"""
            <tr>
            <td style="text-align:center"><input type="checkbox" ng-model="${c._1}"/></td>
            <td style="text-align:left">${c._1}</td>
            <td style="text-align:left">${c._2.replaceAll("Type", "").toLowerCase}</td>
            <td style="text-align:center">${c._3}</td>
            </tr>
            """
      for (para <- paraIDs) {
        bindCheckboxes ++= s"""z.angularBind('${c._1}',${c._1},'$para');"""
        unbind ++= s"""z.angularUnbind('${c._1}','$para');"""
      }
    }

    for (para <- paraIDs) {
      bindCheckboxes ++= s"""z.angularBind('${selectAll}',${selectAll},'$para');"""
      unbind ++= s"""z.angularUnbind('${selectAll}','$para');"""
    }

    val fnName = "updateAll"
    val updateAllFunction =
      s"""function $fnName(select){
           var checkBoxes = document.getElementsByTagName("INPUT");
           for (var i = 0; i < checkBoxes.length; i++) {
            checkBoxes[i].checked = select;
           }
         }"""

    s"""%angular
        $tableStyle
        <input type="checkbox" ng-model="${selectAll}">Select All</input>
        <h4> OR select a subset using the following table. </h4>
        <div style="height:300px;overflow:auto">
        <table cellspacing="0" rules="all" id=$jsTableName style="border-collapse: collapse;">
        <tr>
            <th style="width:120px;text-align:center">Import</th>
            <th style="width:120px;text-align:center">Column Name</th>
            <th style="width:120px;text-align:center">Type</th>
            <th style="width:120px;text-align:center">Nullable</th>
        </tr>
        ${s.toString}
        </table>
        </div>
        </br>
        <br/>
        ${
      renderButtons(List(AngularButton(btnSubmit, "Confirm", btnSuccess, Some(bindCheckboxes.toString), Some(confirmMessage), Some(confirmOnClickFunction)),
        AngularButton(btnReset, "Reset", btnDanger, Some(unbind.toString), Some(fnName + "(false)"), Some(updateAllFunction))))
    }
        """
  }

  /**
   * This function became necessary to tackle the inconsistent response type being derived from Zeppelin context.
   * @param v
   * @tparam T
   * @return
   */
  private def typeToBool[T](v: T) = v match {
    case _: Boolean => v.asInstanceOf[Boolean]
    case _ => v.asInstanceOf[String] match {
      case "true" => true
      case _ => false
    }
  }

  /**
   * Generate a table for existing schema and text boxes that allow user to update schema before final query formation.
   * @param z Zeppelin Context
   * @param df Data Frame from which schema will be inferred.
   * @param paraIDs List of Zeppelin notebook paragraphs with which these variables should be bound.
   * @return
   */
  def generateSchemaSelector(z: ZeppelinContext, df: org.apache.spark.sql.DataFrame, paraIDs: List[String]): String = {
    df.schema.isEmpty match {
      case true => "Empty Dataframe provided. No schema to be rendered."
      case _ =>
        val cols = new StringBuilder()
        val bind = new StringBuilder()
        val unbind = new StringBuilder()
        for (s <- df.schema) {
          (typeToBool(z.angular(s.name)),typeToBool(z.angular(selectAll))) match {
            case (true,_) | (_,true) => {
              cols ++=
                  s"""
                    <tr>
                    <td style="text-align:left">${s.name.toString}</td>
                    <td style="text-align:left">
                        ${s.dataType.toString.replaceAll("Type", "").toLowerCase}
                    </td>
                    <td style="text-align:center">${s.nullable.toString}</td>
                    <td> <input type="text" class="form-control" ng-model="${s.name.toString + "_name"}"
                         ng-init="${s.name.toString + "_name"}='${s.name.toString}'" value="${s.name.toString}">
                         </input></td>
                    <td> <input type="text" class="form-control" ng-model="${s.name.toString + "_dataType"}"
                         ng-init="${s.name.toString + "_dataType"}=
                         '${s.dataType.toString.replaceAll("Type", "").toLowerCase}'"
                         value="${s.dataType.toString.replaceAll("Type", "").toLowerCase}">
                         </input> </td>
                    <td><input type="text" class="form-control" ng-model="${s.name.toString + "_nullable"}"
                         ng-init="${s.name.toString + "_nullable"}='${s.nullable.toString}'" value="${s.nullable.toString}">
                    </input> </td>
                    </tr>"""
                      for (para <- paraIDs) {
                        bind ++=
                            s"""z.angularBind('${s.name.toString + "_name"}',${s.name.toString + "_name"},'$para');
                        z.angularBind('${s.name.toString + "_dataType"}',${s.name.toString + "_dataType"},'$para');
                        z.angularBind('${s.name.toString + "_nullable"}',${s.name.toString + "_nullable"},'$para');
                        """
                      }
                    }
                    case (_,_) =>
                  }
                  for (para <- paraIDs) {
                    unbind ++=
                        s"""z.angularUnbind('${s.name.toString + "_name"}','$para');
                        z.angularUnbind('${s.name.toString + "_dataType"}','$para');
                        z.angularUnbind('${s.name.toString + "_nullable"}','$para');
                        z.angularUnbind('${selectAll}','$para')"""
                  }
                }

                s"""%angular
                    $tableStyle
                    <div style="height:300px;overflow:auto">
                    <table cellspacing="0" rules="all" id="SchemaSelector" style="border-collapse: collapse;">
                        <tr>
                             <th style="width:240px;text-align:center" colspan="3">Inferred from data</th>
                             <th style="width:240px;text-align:center" colspan="3">[Optional] User specified</th>
                        <tr>
                        <tr>
                            <th style="width:120px;text-align:center">Column Name</th>
                            <th style="width:60px;text-align:center">Type</th>
                            <th style="width:60px;text-align:center">Nullable</th>
                            <th style="width:120px;text-align:center">Column Name</th>
                            <th style="width:60px;text-align:center">Type</th>
                            <th style="width:60px;text-align:center">Nullable</th>
                        </tr>
                        ${cols.toString}
                    </table>
                    </div>
                    <br/>
                    ${
                  renderButtons(List(AngularButton(btnSubmit, "Confirm", btnSuccess, Some(bind.toString),
                    Some(confirmMessage), Some(confirmOnClickFunction)),
                    AngularButton(btnReset, "Reset", btnDanger, Some(unbind.toString), Some(resetMessage), Some(confirmOnClickFunction))))
                }
        """
    }
  }

  /**
   * Generate the projection based on schema.
   * @param df Data frame from which schema will be inferred.
   * @param z Zeppelin Context
   * @return
   */
  def getProjectionFromScehma(df: org.apache.spark.sql.DataFrame, z: ZeppelinContext): String = {
    val p = new scala.collection.mutable.ListBuffer[String]()
    for (s <- df.schema) {
      if (z.angular(s.name.toString + "_name").asInstanceOf[String] != null) {
        p += List(z.angular(s.name.toString + "_name").asInstanceOf[String],
          z.angular(s.name.toString + "_dataType").asInstanceOf[String],
          if (z.angular(s.name.toString + "_nullable").asInstanceOf[String] == "false")
            "NOT NULL" else "NULL").mkString(" ")
      }
    }
    "(" + p.toList.mkString(",") + ")"
  }

  /**
   * Combines all parameters and options string to generate the final query to be executed for creation
   * of the external table.
   * @param z Zeppelin context
   * @param df Dataframe
   * @return The final query that will be executed for creation of the external table representing the data.
   */
  def getCreateExternalTableQuery(z: ZeppelinContext, df: org.apache.spark.sql.DataFrame): String = {
    val space = " "
    // TODO: Confirm the selective projection creation from file formats and apply it here.
    "CREATE EXTERNAL TABLE" + space + z.get("dataset").asInstanceOf[String] + space + "USING" + space +
        (z.get("fileFormat").asInstanceOf[String] match {
          case `avro` => avro_source
          case `xml` => xml_source
          case f => f
        }) + space +
        "OPTIONS (" + s"""path '${z.get("path")}' """ +
          (getFileFormatOptionsForSnappy(z.get("fileFormat").asInstanceOf[String], z) match {
            case "" => ""
            case x: String => "," + x
          }) +
        ")"
  }
}