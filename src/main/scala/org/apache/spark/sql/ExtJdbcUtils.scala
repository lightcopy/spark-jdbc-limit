package org.apache.spark.sql.execution.datasources.jdbc

import java.sql.{Connection, Driver, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData, SQLException}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._

object ExtJdbcUtils extends Logging {
  // sleep interval in milliseconds
  val sleep = 100L
  // timeout in nanoseconds
  val timeout = 60 * 1e9.toLong

  def createConnectionFactory(options: JDBCOptions): () => Connection = {
    val driverClass: String = options.driverClass
    () => {
      DriverRegistry.register(driverClass)
      val driver: Driver = DriverManager.getDrivers.asScala.collectFirst {
        case d: DriverWrapper if d.wrapped.getClass.getCanonicalName == driverClass => d
        case d if d.getClass.getCanonicalName == driverClass => d
      }.getOrElse {
        throw new IllegalStateException(
          s"Did not find registered driver with class $driverClass")
      }
      // this can fail to connect when too many connections open, we, instead, will wait when any
      // other connection slot is available with explicitly set timeout
      val start = System.nanoTime
      var keepTrying = true
      var connection: Connection = null
      while (keepTrying && System.nanoTime - start <= timeout) {
        try {
          connection = driver.connect(options.url, options.asConnectionProperties)
          keepTrying = false
        } catch {
          case NonFatal(err) if err.getMessage.contains("too many clients") =>
            Thread.sleep(sleep)
          case other: Throwable =>
            throw other
        }
      }
      if (connection == null) {
        throw new IllegalStateException(s"Could not find connection after ${timeout/1e6} ms")
      } else {
        connection
      }
    }
  }

  private def getJdbcType(dt: DataType, dialect: JdbcDialect): JdbcType = {
    dialect.getJDBCType(dt).orElse(JdbcUtils.getCommonJDBCType(dt)).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for ${dt.simpleString}"))
  }

  /**
   * Saves the RDD to the database in a single transaction.
   */
  def saveTable(
      df: DataFrame,
      url: String,
      table: String,
      options: JDBCOptions) {
    val dialect = JdbcDialects.get(url)
    val nullTypes: Array[Int] = df.schema.fields.map { field =>
      getJdbcType(field.dataType, dialect).jdbcNullType
    }

    val rddSchema = df.schema
    val getConnection: () => Connection = createConnectionFactory(options)
    val batchSize = options.batchSize
    val isolationLevel = options.isolationLevel
    df.foreachPartition(iterator => JdbcUtils.savePartition(
      getConnection, table, iterator, rddSchema, nullTypes, batchSize, dialect, isolationLevel)
    )
  }
}
