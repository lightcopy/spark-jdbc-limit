package com.github.sadikovi

import java.util.Properties
import scala.collection.JavaConverters._
import org.apache.spark.sql.DataFrameWriter

package object jdbc {
  implicit class JdbcDataFrameReader(writer: DataFrameWriter[_]) {
    def extjdbc(url: String, table: String, props: Properties = new Properties()): Unit = {
      writer.
        format("org.apache.spark.sql.jdbc.patch.ExtJdbcRelationProvider").
        option("url", url).
        option("dbtable", table).
        options(props.asScala).
        save()
    }
  }
}
