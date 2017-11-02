package org.apache.spark.sql.execution.datasources.jdbc

import java.sql.{Connection, Driver, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData, SQLException}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row}
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

  def insertStatement(conn: Connection, table: String, rddSchema: StructType, dialect: JdbcDialect)
      : PreparedStatement = {
    val columns = rddSchema.fields.map(x => dialect.quoteIdentifier(x.name)).mkString(",")
    val placeholders = rddSchema.fields.map(_ => "?").mkString(",")
    val sql = s"INSERT INTO $table ($columns) VALUES ($placeholders)"
    conn.prepareStatement(sql)
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
    val transactions = df.rdd.mapPartitions { iterator =>
      savePartition(
        getConnection, table, iterator, rddSchema, nullTypes, batchSize, dialect, isolationLevel)
    }.collect
    val tix = transactions.flatten
    if (tix.exists(_ == Long.MinValue)) {
      val committed = tix.filter(_ != Long.MinValue)
      logWarning("Found errors while saving, rolling back following transactions: " +
        s"${committed.mkString("[", ", ", "]")}")
      val conn = getConnection()
      try {
        deleteTransactions(table, conn, committed)
        logInfo("Removed transactions successfully")
      } catch {
        case NonFatal(err) =>
          logWarning("Failed to clean up transaction state, clean table manually")
          throw err
      } finally {
        conn.close()
      }
      throw new RuntimeException(
        "Failed to save into database, see error logs above for any upstream errors")
    } else {
      logInfo("Completed successfully")
    }
  }

  private type JDBCValueSetter = (PreparedStatement, Row, Int) => Unit

  private def makeSetter(
      conn: Connection,
      dialect: JdbcDialect,
      dataType: DataType): JDBCValueSetter = dataType match {
    case IntegerType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setInt(pos + 1, row.getInt(pos))

    case LongType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setLong(pos + 1, row.getLong(pos))

    case DoubleType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setDouble(pos + 1, row.getDouble(pos))

    case FloatType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setFloat(pos + 1, row.getFloat(pos))

    case ShortType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setInt(pos + 1, row.getShort(pos))

    case ByteType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setInt(pos + 1, row.getByte(pos))

    case BooleanType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setBoolean(pos + 1, row.getBoolean(pos))

    case StringType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setString(pos + 1, row.getString(pos))

    case BinaryType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setBytes(pos + 1, row.getAs[Array[Byte]](pos))

    case TimestampType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setTimestamp(pos + 1, row.getAs[java.sql.Timestamp](pos))

    case DateType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setDate(pos + 1, row.getAs[java.sql.Date](pos))

    case t: DecimalType =>
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        stmt.setBigDecimal(pos + 1, row.getDecimal(pos))

    case ArrayType(et, _) =>
      // remove type length parameters from end of type name
      val typeName = getJdbcType(et, dialect).databaseTypeDefinition
        .toLowerCase.split("\\(")(0)
      (stmt: PreparedStatement, row: Row, pos: Int) =>
        val array = conn.createArrayOf(
          typeName,
          row.getSeq[AnyRef](pos).toArray)
        stmt.setArray(pos + 1, array)

    case _ =>
      (_: PreparedStatement, _: Row, pos: Int) =>
        throw new IllegalArgumentException(
          s"Can't translate non-null value for field $pos")
  }

  def savePartition(
      getConnection: () => Connection,
      table: String,
      iterator: Iterator[Row],
      rddSchema: StructType,
      nullTypes: Array[Int],
      batchSize: Int,
      dialect: JdbcDialect,
      isolationLevel: Int): Iterator[Option[Long]] = {
    val conn = getConnection()
    var committed = false

    var finalIsolationLevel = Connection.TRANSACTION_NONE
    if (isolationLevel != Connection.TRANSACTION_NONE) {
      try {
        val metadata = conn.getMetaData
        if (metadata.supportsTransactions()) {
          // Update to at least use the default isolation, if any transaction level
          // has been chosen and transactions are supported
          val defaultIsolation = metadata.getDefaultTransactionIsolation
          finalIsolationLevel = defaultIsolation
          if (metadata.supportsTransactionIsolationLevel(isolationLevel))  {
            // Finally update to actually requested level if possible
            finalIsolationLevel = isolationLevel
          } else {
            logWarning(s"Requested isolation level $isolationLevel is not supported; " +
                s"falling back to default isolation level $defaultIsolation")
          }
        } else {
          logWarning(s"Requested isolation level $isolationLevel, but transactions are unsupported")
        }
      } catch {
        case NonFatal(e) => logWarning("Exception while detecting transaction support", e)
      }
    }
    val supportsTransactions = finalIsolationLevel != Connection.TRANSACTION_NONE
    var transactionId: Option[Long] = None

    try {
      if (supportsTransactions) {
        conn.setAutoCommit(false) // Everything in the same db transaction.
        conn.setTransactionIsolation(finalIsolationLevel)
      }
      val stmt = insertStatement(conn, table, rddSchema, dialect)
      val setters: Array[JDBCValueSetter] = rddSchema.fields.map(_.dataType)
        .map(makeSetter(conn, dialect, _)).toArray
      val numFields = rddSchema.fields.length

      try {
        var rowCount = 0
        while (iterator.hasNext) {
          val row = iterator.next()
          var i = 0
          while (i < numFields) {
            if (row.isNullAt(i)) {
              stmt.setNull(i + 1, nullTypes(i))
            } else {
              setters(i).apply(stmt, row, i)
            }
            i = i + 1
          }
          stmt.addBatch()
          rowCount += 1
          if (rowCount % batchSize == 0) {
            if (transactionId.isEmpty) {
              transactionId = getCurrentTransactionId(conn)
            }
            stmt.executeBatch()
            rowCount = 0
          }
        }
        if (rowCount > 0) {
          if (transactionId.isEmpty) {
            transactionId = getCurrentTransactionId(conn)
          }
          stmt.executeBatch()
        }
      } finally {
        stmt.close()
      }
      if (supportsTransactions) {
        conn.commit()
      }
      committed = true
      Iterator(transactionId)
    } catch {
      case e: SQLException =>
        val cause = e.getNextException
        if (cause != null && e.getCause != cause) {
          if (e.getCause == null) {
            e.initCause(cause)
          } else {
            e.addSuppressed(cause)
          }
        }
        Iterator(Some(Long.MinValue))
      case NonFatal(err) =>
        logError(s"Failed with general error: $err", err)
        Iterator(Some(Long.MinValue))
    } finally {
      if (!committed) {
        // The stage must fail.  We got here through an exception path, so
        // let the exception through unless rollback() or close() want to
        // tell the user about another problem.
        if (supportsTransactions) {
          conn.rollback()
        }
        conn.close()
      } else {
        // The stage must succeed.  We cannot propagate any exception close() might throw.
        try {
          conn.close()
        } catch {
          case e: Exception => logWarning("Transaction succeeded, but closing failed", e)
        }
      }
    }
  }

  // get current transaction id to clear table after failures
  private def getCurrentTransactionId(conn: Connection): Option[Long] = {
    val sql = "SELECT txid_current()"
    try {
      val stmt = conn.prepareStatement(sql)
      try {
        val rs = stmt.executeQuery()
        if (!rs.next()) {
          throw new IllegalStateException("Result is empty")
        }
        val tix = rs.getLong(1)
        logInfo(s"Found transaction id $tix")
        Some(tix)
      } finally {
        stmt.close()
      }
    } catch {
      case e: Exception =>
        logWarning("Failed to extract transaction id", e)
        Some(Long.MinValue)
    }
  }

  // delete committed transactions
  private def deleteTransactions(table: String, conn: Connection, tix: Array[Long]): Unit = {
    if (tix.nonEmpty) {
      var stmt: PreparedStatement = null
      try {
        // TODO: will fail if not postgresql, sanitize input
        stmt = conn.prepareStatement(s"DELETE FROM $table WHERE xmin in (${tix.mkString(",")})")
        stmt.executeUpdate()
      } finally {
        if (stmt != null) {
          stmt.close()
        }
      }
    }
  }
}
