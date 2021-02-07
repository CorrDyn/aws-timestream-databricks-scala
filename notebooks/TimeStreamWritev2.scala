// Databricks notebook source
//Define Package for AWS TimeStream Class

package com.corrdyn.timestream.v2

import java.util.ArrayList
import java.util.List
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient
import software.amazon.awssdk.services.timestreamwrite.model.ConflictException
import software.amazon.awssdk.services.timestreamwrite.model.CreateDatabaseRequest
import software.amazon.awssdk.services.timestreamwrite.model.CreateTableRequest
import software.amazon.awssdk.services.timestreamwrite.model.Database
import software.amazon.awssdk.services.timestreamwrite.model.DeleteDatabaseRequest
import software.amazon.awssdk.services.timestreamwrite.model.DeleteDatabaseResponse
import software.amazon.awssdk.services.timestreamwrite.model.DeleteTableRequest
import software.amazon.awssdk.services.timestreamwrite.model.DeleteTableResponse
import software.amazon.awssdk.services.timestreamwrite.model.DescribeDatabaseRequest
import software.amazon.awssdk.services.timestreamwrite.model.DescribeDatabaseResponse
import software.amazon.awssdk.services.timestreamwrite.model.DescribeTableRequest
import software.amazon.awssdk.services.timestreamwrite.model.DescribeTableResponse
import software.amazon.awssdk.services.timestreamwrite.model.Dimension
import software.amazon.awssdk.services.timestreamwrite.model.ListDatabasesRequest
import software.amazon.awssdk.services.timestreamwrite.model.ListDatabasesResponse
import software.amazon.awssdk.services.timestreamwrite.model.ListTablesRequest
import software.amazon.awssdk.services.timestreamwrite.model.ListTablesResponse
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValueType
import software.amazon.awssdk.services.timestreamwrite.model.Record
import software.amazon.awssdk.services.timestreamwrite.model.RejectedRecord
import software.amazon.awssdk.services.timestreamwrite.model.RejectedRecordsException
import software.amazon.awssdk.services.timestreamwrite.model.ResourceNotFoundException
import software.amazon.awssdk.services.timestreamwrite.model.RetentionProperties
import software.amazon.awssdk.services.timestreamwrite.model.Table
import software.amazon.awssdk.services.timestreamwrite.model.UpdateDatabaseRequest
import software.amazon.awssdk.services.timestreamwrite.model.UpdateTableRequest
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsRequest
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsResponse
import software.amazon.awssdk.services.timestreamwrite.paginators.ListDatabasesIterable
import software.amazon.awssdk.services.timestreamwrite.paginators.ListTablesIterable
// import com.amazonaws.services.timestream.Main.DATABASE_NAME
// import com.amazonaws.services.timestream.Main.TABLE_NAME

// import CrudAndSimpleIngestionExample._

//remove if not needed
import scala.collection.JavaConverters._

// object CrudAndSimpleIngestionExample {

  

// }

class CrudAndSimpleIngestionExample(client: TimestreamWriteClient, DATABASE_NAME: String, TABLE_NAME: String) {

  var timestreamWriteClient: TimestreamWriteClient = client
  val HT_TTL_HOURS: Long = 24L
  val CT_TTL_DAYS: Long = 7L
  
  def createDatabase(): Unit = {
    println("Creating database")
    val request: CreateDatabaseRequest =
      CreateDatabaseRequest.builder().databaseName(DATABASE_NAME).build()
    try {
      timestreamWriteClient.createDatabase(request)
      println("Database [" + DATABASE_NAME + "] created successfully")
    } catch {
      case e: ConflictException =>
        println(
          "Database [" + DATABASE_NAME + "] exists. Skipping database creation")

    }
  }

  def describeDatabase(): Unit = {
    println("Describing database")
    val describeDatabaseRequest: DescribeDatabaseRequest =
      DescribeDatabaseRequest.builder().databaseName(DATABASE_NAME).build()
    try {
      val response: DescribeDatabaseResponse =
        timestreamWriteClient.describeDatabase(describeDatabaseRequest)
      val databaseRecord: Database = response.database()
      val databaseId: String = databaseRecord.arn()
      println("Database " + DATABASE_NAME + " has id " + databaseId)
    } catch {
      case e: Exception => {
        println("Database doesn't exist = " + e)
        throw e
      }

    }
  }

  def listDatabases(): Unit = {
    println("Listing databases")
    val request: ListDatabasesRequest =
      ListDatabasesRequest.builder().maxResults(2).build()
    val listDatabasesIterable: ListDatabasesIterable =
      timestreamWriteClient.listDatabasesPaginator(request)
    for (listDatabasesResponse <- listDatabasesIterable.asScala) {
      val databases: List[Database] = listDatabasesResponse.databases()
      databases.forEach((database) => println(database.databaseName()))
    }
  }

  def updateDatabase(kmsKeyId: String): Unit = {
    if (kmsKeyId == null) {
      println("Skipping UpdateDatabase because KmsKeyId was not given")
      return
    }
    println("Updating database")
    val request: UpdateDatabaseRequest = UpdateDatabaseRequest
      .builder()
      .databaseName(DATABASE_NAME)
      .kmsKeyId(kmsKeyId)
      .build()
    try {
      timestreamWriteClient.updateDatabase(request)
      println(
        "Database [" + DATABASE_NAME + "] updated successfully with kmsKeyId " +
          kmsKeyId)
    } catch {
      case e: ResourceNotFoundException =>
        println(
          "Database [" + DATABASE_NAME + "] does not exist. Skipping UpdateDatabase")

      case e: Exception => println("UpdateDatabase failed: " + e)

    }
  }

  def createTable(): Unit = {
    println("Creating table")
    val retentionProperties: RetentionProperties = RetentionProperties
      .builder()
      .memoryStoreRetentionPeriodInHours(HT_TTL_HOURS)
      .magneticStoreRetentionPeriodInDays(CT_TTL_DAYS)
      .build()
    val createTableRequest: CreateTableRequest = CreateTableRequest
      .builder()
      .databaseName(DATABASE_NAME)
      .tableName(TABLE_NAME)
      .retentionProperties(retentionProperties)
      .build()
    try {
      timestreamWriteClient.createTable(createTableRequest)
      println("Table [" + TABLE_NAME + "] successfully created.")
    } catch {
      case e: ConflictException =>
        println(
          "Table [" + TABLE_NAME + "] exists on database [" + DATABASE_NAME +
            "] . Skipping database creation")

    }
  }

  def updateTable(): Unit = {
    println("Updating table")
    val retentionProperties: RetentionProperties = RetentionProperties
      .builder()
      .memoryStoreRetentionPeriodInHours(HT_TTL_HOURS)
      .magneticStoreRetentionPeriodInDays(CT_TTL_DAYS)
      .build()
    val updateTableRequest: UpdateTableRequest = UpdateTableRequest
      .builder()
      .databaseName(DATABASE_NAME)
      .tableName(TABLE_NAME)
      .retentionProperties(retentionProperties)
      .build()
    timestreamWriteClient.updateTable(updateTableRequest)
    println("Table updated")
  }

  def describeTable(): Unit = {
    println("Describing table")
    val describeTableRequest: DescribeTableRequest = DescribeTableRequest
      .builder()
      .databaseName(DATABASE_NAME)
      .tableName(TABLE_NAME)
      .build()
    try {
      val response: DescribeTableResponse =
        timestreamWriteClient.describeTable(describeTableRequest)
      val tableId: String = response.table().arn()
      println("Table " + TABLE_NAME + " has id " + tableId)
    } catch {
      case e: Exception => {
        println("Table " + TABLE_NAME + " doesn't exist = " + e)
        throw e
      }

    }
  }

  def listTables(): Unit = {
    println("Listing tables")
    val request: ListTablesRequest = ListTablesRequest
      .builder()
      .databaseName(DATABASE_NAME)
      .maxResults(2)
      .build()
    val listTablesIterable: ListTablesIterable =
      timestreamWriteClient.listTablesPaginator(request)
    for (listTablesResponse <- listTablesIterable.asScala) {
      val tables: List[Table] = listTablesResponse.tables()
      tables.forEach((table) => println(table.tableName()))
    }
  }

  def writeRecords(): Unit = {
    println("Writing records")
// Specify repeated values for all records
    val records: List[Record] = new ArrayList[Record]()
    val time: Long = System.currentTimeMillis()
    val dimensions: List[Dimension] = new ArrayList[Dimension]()
    val region: Dimension =
      Dimension.builder().name("region").value("us-east-1").build()
    val az: Dimension = Dimension.builder().name("az").value("az1").build()
    val hostname: Dimension =
      Dimension.builder().name("hostname").value("host1").build()
    dimensions.add(region)
    dimensions.add(az)
    dimensions.add(hostname)
    val cpuUtilization: Record = Record
      .builder()
      .dimensions(dimensions)
      .measureValueType(MeasureValueType.DOUBLE)
      .measureName("cpu_utilization")
      .measureValue("13.5")
      .time(String.valueOf(time))
      .build()
    val memoryUtilization: Record = Record
      .builder()
      .dimensions(dimensions)
      .measureValueType(MeasureValueType.DOUBLE)
      .measureName("memory_utilization")
      .measureValue("40")
      .time(String.valueOf(time))
      .build()
    records.add(cpuUtilization)
    records.add(memoryUtilization)
    val writeRecordsRequest: WriteRecordsRequest = WriteRecordsRequest
      .builder()
      .databaseName(DATABASE_NAME)
      .tableName(TABLE_NAME)
      .records(records)
      .build()
    try {
      val writeRecordsResponse: WriteRecordsResponse =
        timestreamWriteClient.writeRecords(writeRecordsRequest)
      println(
        "WriteRecords Status: " +
          writeRecordsResponse.sdkHttpResponse().statusCode())
    } catch {
      case e: RejectedRecordsException => printRejectedRecordsException(e)

      case e: Exception => println("Error: " + e)

    }
  }

  def writeRecordsWithCommonAttributes(): Unit = {
    println("Writing records with extracting common attributes")
// Specify repeated values for all records
    val records: List[Record] = new ArrayList[Record]()
    val time: Long = System.currentTimeMillis()
    val dimensions: List[Dimension] = new ArrayList[Dimension]()
    val region: Dimension =
      Dimension.builder().name("region").value("us-east-1").build()
    val az: Dimension = Dimension.builder().name("az").value("az1").build()
    val hostname: Dimension =
      Dimension.builder().name("hostname").value("host1").build()
    dimensions.add(region)
    dimensions.add(az)
    dimensions.add(hostname)
    val commonAttributes: Record = Record
      .builder()
      .dimensions(dimensions)
      .measureValueType(MeasureValueType.DOUBLE)
      .time(String.valueOf(time))
      .build()
    val cpuUtilization: Record = Record
      .builder()
      .measureName("cpu_utilization")
      .measureValue("13.5")
      .build()
    val memoryUtilization: Record = Record
      .builder()
      .measureName("memory_utilization")
      .measureValue("40")
      .build()
    records.add(cpuUtilization)
    records.add(memoryUtilization)
    val writeRecordsRequest: WriteRecordsRequest = WriteRecordsRequest
      .builder()
      .databaseName(DATABASE_NAME)
      .tableName(TABLE_NAME)
      .commonAttributes(commonAttributes)
      .records(records)
      .build()
    try {
      val writeRecordsResponse: WriteRecordsResponse =
        timestreamWriteClient.writeRecords(writeRecordsRequest)
      println(
        "writeRecordsWithCommonAttributes Status: " +
          writeRecordsResponse.sdkHttpResponse().statusCode())
    } catch {
      case e: RejectedRecordsException => printRejectedRecordsException(e)

      case e: Exception => println("Error: " + e)

    }
  }

  def writeRecordsWithUpsert(): Unit = {
    println("Writing records with upsert")
// Specify repeated values for all records
    val records: List[Record] = new ArrayList[Record]()
    val time: Long = System.currentTimeMillis()
// To achieve upsert (last writer wins) semantic, one example is to use current time as the version if you are writing directly from the data source
    var version: Long = System.currentTimeMillis()
    val dimensions: List[Dimension] = new ArrayList[Dimension]()
    val region: Dimension =
      Dimension.builder().name("region").value("us-east-1").build()
    val az: Dimension = Dimension.builder().name("az").value("az1").build()
    val hostname: Dimension =
      Dimension.builder().name("hostname").value("host1").build()
    dimensions.add(region)
    dimensions.add(az)
    dimensions.add(hostname)
    var commonAttributes: Record = Record
      .builder()
      .dimensions(dimensions)
      .measureValueType(MeasureValueType.DOUBLE)
      .time(String.valueOf(time))
      .version(version)
      .build()
    var cpuUtilization: Record = Record
      .builder()
      .measureName("cpu_utilization")
      .measureValue("13.5")
      .build()
    var memoryUtilization: Record = Record
      .builder()
      .measureName("memory_utilization")
      .measureValue("40")
      .build()
    records.add(cpuUtilization)
    records.add(memoryUtilization)
    val writeRecordsRequest: WriteRecordsRequest = WriteRecordsRequest
      .builder()
      .databaseName(DATABASE_NAME)
      .tableName(TABLE_NAME)
      .commonAttributes(commonAttributes)
      .records(records)
      .build()
// write records for first time
    try {
      val writeRecordsResponse: WriteRecordsResponse =
        timestreamWriteClient.writeRecords(writeRecordsRequest)
      println(
        "WriteRecords Status for first time: " +
          writeRecordsResponse.sdkHttpResponse().statusCode())
    } catch {
      case e: RejectedRecordsException => printRejectedRecordsException(e)

      case e: Exception => println("Error: " + e)

    }
// Successfully retry same writeRecordsRequest with same records and versions, because writeRecords API is idempotent.
    try {
      val writeRecordsResponse: WriteRecordsResponse =
        timestreamWriteClient.writeRecords(writeRecordsRequest)
      println(
        "WriteRecords Status for retry: " +
          writeRecordsResponse.sdkHttpResponse().statusCode())
    } catch {
      case e: RejectedRecordsException => printRejectedRecordsException(e)

      case e: Exception => println("Error: " + e)

    }
// upsert with lower version, this would fail because a higher version is required to update the measure value.
    version -= 1
    commonAttributes = Record
      .builder()
      .dimensions(dimensions)
      .measureValueType(MeasureValueType.DOUBLE)
      .time(String.valueOf(time))
      .version(version)
      .build()
    cpuUtilization = Record
      .builder()
      .measureName("cpu_utilization")
      .measureValue("14.5")
      .build()
    memoryUtilization = Record
      .builder()
      .measureName("memory_utilization")
      .measureValue("50")
      .build()
    val upsertedRecords: List[Record] = new ArrayList[Record]()
    upsertedRecords.add(cpuUtilization)
    upsertedRecords.add(memoryUtilization)
    var writeRecordsUpsertRequest: WriteRecordsRequest = WriteRecordsRequest
      .builder()
      .databaseName(DATABASE_NAME)
      .tableName(TABLE_NAME)
      .commonAttributes(commonAttributes)
      .records(upsertedRecords)
      .build()
    try {
      val writeRecordsResponse: WriteRecordsResponse =
        timestreamWriteClient.writeRecords(writeRecordsUpsertRequest)
      println(
        "WriteRecords Status for upsert with lower version: " +
          writeRecordsResponse.sdkHttpResponse().statusCode())
    } catch {
      case e: RejectedRecordsException => {
        println("WriteRecords Status for upsert with lower version: ")
        printRejectedRecordsException(e)
      }

      case e: Exception => println("Error: " + e)

    }
// upsert with higher version as new data is generated
    version = System.currentTimeMillis()
    commonAttributes = Record
      .builder()
      .dimensions(dimensions)
      .measureValueType(MeasureValueType.DOUBLE)
      .time(String.valueOf(time))
      .version(version)
      .build()
    writeRecordsUpsertRequest = WriteRecordsRequest
      .builder()
      .databaseName(DATABASE_NAME)
      .tableName(TABLE_NAME)
      .commonAttributes(commonAttributes)
      .records(upsertedRecords)
      .build()
    try {
      val writeRecordsUpsertResponse: WriteRecordsResponse =
        timestreamWriteClient.writeRecords(writeRecordsUpsertRequest)
      println(
        "WriteRecords Status for upsert with higher version: " +
          writeRecordsUpsertResponse.sdkHttpResponse().statusCode())
    } catch {
      case e: RejectedRecordsException => printRejectedRecordsException(e)

      case e: Exception => println("Error: " + e)

    }
  }

  def deleteTable(): Unit = {
    println("Deleting table")
    val deleteTableRequest: DeleteTableRequest = DeleteTableRequest
      .builder()
      .databaseName(DATABASE_NAME)
      .tableName(TABLE_NAME)
      .build()
    try {
      val response: DeleteTableResponse =
        timestreamWriteClient.deleteTable(deleteTableRequest)
      println(
        "Delete table status: " + response.sdkHttpResponse().statusCode())
    } catch {
      case e: ResourceNotFoundException => {
        println("Table " + TABLE_NAME + " doesn't exist = " + e)
        throw e
      }

      case e: Exception => {
        println("Could not delete table " + TABLE_NAME + " = " + e)
        throw e
      }

    }
  }

  def deleteDatabase(): Unit = {
    println("Deleting database")
    val deleteDatabaseRequest: DeleteDatabaseRequest =
      DeleteDatabaseRequest.builder().databaseName(DATABASE_NAME).build()
    try {
      val response: DeleteDatabaseResponse =
        timestreamWriteClient.deleteDatabase(deleteDatabaseRequest)
      println(
        "Delete database status: " + response.sdkHttpResponse().statusCode())
    } catch {
      case e: ResourceNotFoundException => {
        println("Database " + DATABASE_NAME + " doesn't exist = " + e)
        throw e
      }

      case e: Exception => {
        println(
          "Could not delete Database " + DATABASE_NAME + " = " +
            e)
        throw e
      }

    }
  }

  private def printRejectedRecordsException(
      e: RejectedRecordsException): Unit = {
    println("RejectedRecords: " + e)
    e.rejectedRecords().forEach(System.out.println)
  }

}

// COMMAND ----------

import com.corrdyn.timestream.v2._

import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import java.time.Duration
import software.amazon.awssdk.regions.Region

def buildWriteClient(): TimestreamWriteClient = {
    val httpClientBuilder: ApacheHttpClient.Builder =
      ApacheHttpClient.builder()
    httpClientBuilder.maxConnections(5000)
    val retryPolicy: RetryPolicy.Builder = RetryPolicy.builder()
    retryPolicy.numRetries(10)
    val overrideConfig: ClientOverrideConfiguration.Builder =
      ClientOverrideConfiguration.builder()
    overrideConfig.apiCallAttemptTimeout(Duration.ofSeconds(20))
    overrideConfig.retryPolicy(retryPolicy.build())
    TimestreamWriteClient
      .builder()
      .httpClientBuilder(httpClientBuilder)
      .overrideConfiguration(overrideConfig.build())
      .region(Region.US_EAST_1)
      .build()
  }

val DATABASE_NAME: String = "corrdynSample"
val TABLE_NAME: String = "someTable"

val writeClient: TimestreamWriteClient = buildWriteClient()
val crudAndSimpleIngestionExample: CrudAndSimpleIngestionExample =
      new CrudAndSimpleIngestionExample(writeClient, DATABASE_NAME, TABLE_NAME)


// COMMAND ----------

crudAndSimpleIngestionExample.createDatabase()
crudAndSimpleIngestionExample.describeDatabase()
crudAndSimpleIngestionExample.listDatabases()
crudAndSimpleIngestionExample.createTable()
crudAndSimpleIngestionExample.describeTable()
crudAndSimpleIngestionExample.listTables()
crudAndSimpleIngestionExample.updateTable()
crudAndSimpleIngestionExample.writeRecords()
crudAndSimpleIngestionExample.writeRecordsWithCommonAttributes()
crudAndSimpleIngestionExample.writeRecordsWithUpsert()