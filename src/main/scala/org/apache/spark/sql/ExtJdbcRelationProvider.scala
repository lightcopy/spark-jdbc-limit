package org.apache.spark.sql.jdbc.patch

import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.execution.datasources.jdbc._
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}

class ExtJdbcRelationProvider extends JdbcRelationProvider {
  override def shortName(): String = "extjdbc"

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      df: DataFrame): BaseRelation = {
    val jdbcOptions = new JDBCOptions(parameters)
    val url = jdbcOptions.url
    val table = jdbcOptions.table
    val createTableOptions = jdbcOptions.createTableOptions
    val isTruncate = jdbcOptions.isTruncate

    val conn = ExtJdbcUtils.createConnectionFactory(jdbcOptions)()
    try {
      val tableExists = JdbcUtils.tableExists(conn, jdbcOptions)
      if (tableExists) {
        mode match {
          case SaveMode.Overwrite =>
            if (isTruncate && JdbcUtils.isCascadingTruncateTable(url) == Some(false)) {
              // In this case, we should truncate table and then load.
              JdbcUtils.truncateTable(conn, table)
              ExtJdbcUtils.saveTable(df, url, table, jdbcOptions)
            } else {
              // Otherwise, do not truncate the table, instead drop and recreate it
              JdbcUtils.dropTable(conn, table)
              JdbcUtils.createTable(conn, df, jdbcOptions)
              ExtJdbcUtils.saveTable(df, url, table, jdbcOptions)
            }

          case SaveMode.Append =>
            ExtJdbcUtils.saveTable(df, url, table, jdbcOptions)

          case SaveMode.ErrorIfExists =>
            throw new AnalysisException(
              s"Table or view '$table' already exists. SaveMode: ErrorIfExists.")

          case SaveMode.Ignore =>
            // With `SaveMode.Ignore` mode, if table already exists, the save operation is expected
            // to not save the contents of the DataFrame and to not change the existing data.
            // Therefore, it is okay to do nothing here and then just return the relation below.
        }
      } else {
        JdbcUtils.createTable(conn, df, jdbcOptions)
        ExtJdbcUtils.saveTable(df, url, table, jdbcOptions)
      }
    } finally {
      conn.close()
    }

    createRelation(sqlContext, parameters)
  }
}
