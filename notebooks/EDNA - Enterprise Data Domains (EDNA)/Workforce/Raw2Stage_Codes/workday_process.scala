// Databricks notebook source
// DBTITLE 1,Declare Variables
val timeformat = "yyyy-MM-dd HH:mm:ss.000"
val cntrl_table = "workforce_cntrl_mdt.rsa_r2s_cntrl"
val adls_storage_account_name = "bdaze1iednadl01"
val adls_container_name = "edna-workforcedatadomain" 
val path_cntrl_table = "abfss://"+adls_container_name+"@"+adls_storage_account_name+".dfs.core.windows.net/Control_Metadata/rsa_r2s_cntrl"
val source_table= "workforce_raw.employee_workday"
val business_name="workday"
val wd_employee_raw= "workforce_raw.wd_employee"
val wd_employee= "workforce_stage.wd_employee_stg"
val wd_employee_inc= "workforce_stage.wd_employee_inc"
val pswd=dbutils.secrets.get(scope = "AKV_EDNA", key = "DataOpsDW-elancoadmin")
val key=dbutils.secrets.get(scope = "AKV_EDNA", key = "ADLS-WORKFORCEDATADOMAIN")
val secret=dbutils.secrets.get(scope = "AKV_EDNA", key = "DataOpsServicePrincipal01-Secret")
val clintid=dbutils.secrets.get(scope = "AKV_EDNA", key = "DataOpsServicePrincipal01-ClintID")
val tenantid=dbutils.secrets.get(scope = "AKV_EDNA", key = "DataOpsServicePrincipal01-TenantID")
//val path_cntrl_table="abfss://edna-workforcedatadomain@bdaze1iednadl01.dfs.core.windows.net/Control_Metadata/rsa_r2s_cntrl"

// COMMAND ----------

// DBTITLE 1,Configure source(ADLS) 
dbutils.widgets.text("valsource_filename_WD", "ELAN_WFDD_Demographic_2020-04-08_125005.JSON")
val source_filename_wd = dbutils.widgets.get("valsource_filename_WD")
spark.conf.set("fs.azure.account.auth.type."+adls_storage_account_name+".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type."+adls_storage_account_name+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id."+adls_storage_account_name+".dfs.core.windows.net", clintid)
spark.conf.set("fs.azure.account.oauth2.client.secret."+adls_storage_account_name+".dfs.core.windows.net",secret)
//"Elanco-DataOps-NonProd-EDNA-Secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+adls_storage_account_name+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenantid+"/oauth2/token")
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import scala.util.matching.Regex
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType}
//spark.conf.set("fs.azure.account.key.bdaze1iednadl01.blob.core.windows.net","Fw0hRjUPtpRmjD5DoiAJfWqS6J4KZ6NTKt6UQtPhkEXIkT64CTneMvzGeLBNBWmSUtkL76y/UCxfaC5mtsNbCw==")


// COMMAND ----------

// DBTITLE 1,Configure source(ADLS) - hadoop conf
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.auth.type."+adls_storage_account_name+"", "OAuth")
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth.provider.type."+adls_storage_account_name+".dfs.core.windows.net","org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.id."+adls_storage_account_name+".dfs.core.windows.net", clintid)
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.secret."+adls_storage_account_name+".dfs.core.windows.net",secret)
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.endpoint."+adls_storage_account_name+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenantid+"/oauth2/token")

// COMMAND ----------

// DBTITLE 1,create Dataframe
val wd_rawdataframe=spark.read.option("multiLine", true).json(s"abfss://${adls_container_name}@${adls_storage_account_name}.dfs.core.windows.net/Raw/Workday/landing_workday_files/${source_filename_wd}")
//rawextract.printSchema()

import org.apache.spark.sql.functions
val wd_flattendataframe = wd_rawdataframe.withColumn("Emp_data",explode($"Report_Entry")).withColumn("Load_Datetime",lit(current_timestamp())).withColumn("sourcefile", substring_index(input_file_name(), "/", -1))   //.withColumn("Emp_data",explode($"Report_Entry"))
//final_df_temp2.printSchema()
//final_df_temp2.show()
wd_flattendataframe.createOrReplaceTempView("employee_workday")
//wd_flattendataframe.printSchema()

// COMMAND ----------

// MAGIC %sql
// MAGIC select Emp_data.* from employee_workday

// COMMAND ----------

// DBTITLE 1,Assign column names
val workday_dataframe=wd_flattendataframe.select(
col("Emp_data.Employee_ID").as("Employee_ID"),
col("Emp_data.User_name").as("User_name"),
col("Emp_data.First_Name").as("First_Name"),
col("Emp_data.Middle_Name").as("Middle_Name"),
col("Emp_data.Last_Name").as("Last_Name"),
col("Emp_data.Preferred_Name_-_First_Name").as("Preferred_Name_First_Name"),
col("Emp_data.Preferred_Name_-_Middle_Name").as("Preferred_Name_Middle_Name"),
col("Emp_data.Preferred_Last_Name").as("Preferred_Name_Last_Name"),
col("Emp_data.Phone_Number").as("Phone_Number"),
col("Emp_data.Home_Phone_Number").as("Home_Phone_Number"),
col("Emp_data.primary_Work_Email").as("primary_Work_Email"),
col("Emp_data.Location").as("Location"),
col("Emp_data.Location_Address_-_Country").as("Location_Address_Country"),
col("Emp_data.Location_Reference_ID").as("Location_Reference_ID"),
col("Emp_data.Position_ID").as("Position_ID"),
col("Emp_data.Position_Title").as("Position_Title"),
col("Emp_data.Previous_System_ID").as("Previous_System_ID"),
col("Emp_data.Is_Manager").as("Is_Manager"),
col("Emp_data.Hire_Date").as("Hire_Date"),
col("Emp_data.Original_Hire_Date").as("Original_Hire_Date"),
col("Emp_data.Job_Classification_ID").as("Job_Classification_ID"),
col("Emp_data.Job_Code").as("Job_Code"),
col("Emp_data.Job_Family").as("Job_Family"),
col("Emp_data.Job_Family_Group").as("Job_Family_Group"),
col("Emp_data.Active_Status").as("Active_Status"),
col("Emp_data.Contract_End_Date").as("Contract_End_Date"),
col("Emp_data.Manager_Employee_ID").as("Manager_Employee_ID"),
col("Emp_data.Supervisory_Org_Name").as("Supervisory_Org_Name"),
col("Emp_data.Supervisory_Organization_-_ID").as("Supervisory_Organization_ID"),
col("Emp_data.Cost_Center_ID").as("Cost_Center_ID"),
col("Emp_data.Cost_Center").as("Cost_Centre"),
col("Emp_data.Cost_Center_Name").as("Cost_Center_Name"),
col("Emp_data.Contingent_Worker_Supplier").as("Contingent_Worker_Supplier"),
col("Emp_data.Time_Type").as("Time_Type"),
col("Emp_data.Worker_Type").as("Worker_Type"),
col("Emp_data.Worker_Sub-Type").as("Worker_Sub_Type"),
col("Load_Datetime").as("Load_Datetime"),
col("sourcefile").as("sourcefile")).withColumn("datepart",lit(current_date())).createOrReplaceTempView("df_employee_wd")
//col("sourcefile").as("sourcefile")).createOrReplaceTempView("df_employee_wd")
//workday_dataframe.show()
val workday_dataframe_tbl=spark.sql("select * from df_employee_wd")//.persist(StorageLevel.MEMORY_AND_DISK)
//workday_dataframe_tbl.persist()

// COMMAND ----------

workday_dataframe_tbl.write.mode(SaveMode.Append).insertInto(s"${wd_employee_raw}")

// COMMAND ----------

val processstage="raw"
val val_check= spark.sql(s"select business_name as check_business_name,table_name as check_table_name, max(raw_load_tm) as max_raw_load_tm, max(load_txn_tm)as max_load_txn_tm from ${cntrl_table} where business_name='${business_name}' and stage='${processstage}' group by business_name,table_name")

// COMMAND ----------

val val_filter = val_check.select(col("max_raw_load_tm"))

// COMMAND ----------

val final_val_filter = if(!val_filter.rdd.isEmpty()) (val_filter.head(1)(0)(0)) else lit("1900-01-01 00:00:00").cast(TimestampType)
//val final_frame_inc= if (!val_filter.rdd.isEmpty()) eleminatenochangerecords.where(col("Load_Datetime") > final_val_filter) else eleminatenochangerecords

// COMMAND ----------

val wd_raw_dataframe = spark.sql(s"select * from ${wd_employee_raw}")

// COMMAND ----------

val final_frame_temp=  wd_raw_dataframe.select(
col("Employee_ID")
,col("User_name")
,col("First_Name")
,col("Middle_Name")
,col("Last_Name")
,col("Preferred_Name_First_Name")
,col("Preferred_Name_Middle_Name")
,col("Preferred_Name_Last_Name")
,col("Phone_Number")
,col("Home_Phone_Number")
,col("primary_Work_Email")
,col("Location")
,col("Location_Address_Country")
,col("Location_Reference_ID")
,col("Position_ID")
,col("Position_Title")
,col("Previous_System_ID")
,col("Is_Manager")
,col("Hire_Date")
,col("Original_Hire_Date")
,col("Job_Classification_ID")
,col("Job_Code")
,col("Job_Family")
,col("Job_Family_Group")
,col("Active_Status")
,col("Contract_End_Date")
,col("Manager_Employee_ID")
,col("Supervisory_Org_Name")
,col("Supervisory_Organization_ID")
,col("Cost_Center_ID")
,col("Cost_Centre")
,col("Cost_Center_Name")
,col("Contingent_Worker_Supplier")
,col("Time_Type")
,col("Worker_Type")
,col("Worker_Sub_Type")
,col("Load_Datetime")
,col("file_name")
,col("datepart"))

// COMMAND ----------

val final_frame= if (!val_filter.rdd.isEmpty()) final_frame_temp.where(col("Load_Datetime") > final_val_filter) else wd_raw_dataframe

// COMMAND ----------

final_frame.createOrReplaceTempView("df_employee_wd_raw")

// COMMAND ----------

spark.sql("""select
Employee_ID
,case when upper(User_name)='''NULL''' then  null  else User_name End as User_name
,case when upper(First_Name)='''NULL''' then  null  else First_Name End as First_Name
,case when upper(Middle_Name)='''NULL''' then  null  else Middle_Name End as Middle_Name
,case when upper(Last_Name)='''NULL''' then  null  else Last_Name End as Last_Name
,case when upper(Preferred_Name_First_Name)='''NULL''' then  null  else Preferred_Name_First_Name End as Preferred_Name_First_Name
,case when upper(Preferred_Name_Middle_Name)='''NULL''' then  null  else Preferred_Name_Middle_Name End as Preferred_Name_Middle_Name
,case when upper(Preferred_Name_Last_Name)='''NULL''' then  null  else Preferred_Name_Last_Name End as Preferred_Name_Last_Name
,case when upper(Phone_Number)='''NULL''' then  null  else Phone_Number End as Phone_Number
,case when upper(Home_Phone_Number)='''NULL''' then  null  else Home_Phone_Number End as Home_Phone_Number
,case when upper(primary_Work_Email)='''NULL''' then  null  else primary_Work_Email End as primary_Work_Email
,case when upper(Location)='''NULL''' then  null  else Location End as Location
,case when upper(Location_Address_Country)='''NULL''' then  null  else Location_Address_Country End as Location_Address_Country
,case when upper(Location_Reference_ID)='''NULL''' then  null  else Location_Reference_ID End as Location_Reference_ID
,case when upper(Position_ID)='''NULL''' then  null  else Position_ID End as Position_ID
,case when upper(Position_Title)='''NULL''' then  null  else Position_Title End as Position_Title
,case when upper(Previous_System_ID)='''NULL''' then  null  else Previous_System_ID End as Previous_System_ID
,case when upper(Is_Manager)='''NULL''' then  null  else Is_Manager End as Is_Manager
,case when upper(Hire_Date)='''NULL''' then  null  else Hire_Date End as Hire_Date
,case when upper(Original_Hire_Date)='''NULL''' then  null  else Original_Hire_Date End as Original_Hire_Date
,case when upper(Job_Classification_ID)='''NULL''' then  null  else Job_Classification_ID End as Job_Classification_ID
,case when upper(Job_Code)='''NULL''' then  null  else Job_Code End as Job_Code
,case when upper(Job_Family)='''NULL''' then  null  else Job_Family End as Job_Family
,case when upper(Job_Family_Group)='''NULL''' then  null  else Job_Family_Group End as Job_Family_Group
,case when upper(Active_Status)='''NULL''' then  null  else Active_Status End as Active_Status
,case when upper(Contract_End_Date)='''NULL''' then  null  else Contract_End_Date End as Contract_End_Date
,case when upper(Manager_Employee_ID)='''NULL''' then  null  else Manager_Employee_ID End as Manager_Employee_ID
,case when upper(Supervisory_Org_Name)='''NULL''' then  null  else Supervisory_Org_Name End as Supervisory_Org_Name
,case when upper(Supervisory_Organization_ID)='''NULL''' then  null  else Supervisory_Organization_ID End as Supervisory_Organization_ID
,case when upper(Cost_Center_ID)='''NULL''' then  null  else Cost_Center_ID End as Cost_Center_ID
,case when upper(Cost_Centre)='''NULL''' then  null  else Cost_Centre End as Cost_Centre
,case when upper(Cost_Center_Name)='''NULL''' then  null  else Cost_Center_Name End as Cost_Center_Name
,case when upper(Contingent_Worker_Supplier)='''NULL''' then  null  else Contingent_Worker_Supplier End as Contingent_Worker_Supplier
,case when upper(Time_Type)='''NULL''' then  null  else Time_Type End as Time_Type
,case when upper(Worker_Type)='''NULL''' then  null  else Worker_Type End as Worker_Type
,case when upper(Worker_Sub_Type)='''NULL''' then  null  else Worker_Sub_Type End as Worker_Sub_Type
,Load_Datetime
,file_name as sourcefile
,datepart
from df_employee_wd_raw""").createOrReplaceTempView("cleannulldata_tbl")

// COMMAND ----------

val cleannulls=spark.sql("select * from cleannulldata_tbl")

// COMMAND ----------

val df_aftercoalesce=cleannulls.withColumn("Record_status",coalesce($"First_Name",$"Middle_Name",$"Last_Name",$"Preferred_Name_First_Name",$"Preferred_Name_Middle_Name",$"User_name",$"Preferred_Name_Last_Name",$"Phone_Number",$"Home_Phone_Number",$"primary_Work_Email",$"Location",$"Location_Address_Country",$"Location_Reference_ID",$"Position_ID",$"Position_Title",$"Previous_System_ID",$"Is_Manager",$"Hire_Date",$"Original_Hire_Date",$"Job_Classification_ID",$"Job_Code",$"Job_Family",$"Job_Family_Group",$"Active_Status",$"Contract_End_Date",$"Manager_Employee_ID",$"Supervisory_Org_Name",$"Supervisory_Organization_ID",$"Cost_Center_ID",$"Cost_Centre",$"Cost_Center_Name",$"Contingent_Worker_Supplier",$"Time_Type",$"Worker_Type",$"Worker_Sub_Type",lit("No_Record")))


// COMMAND ----------

val eleminatenochangerecords_temp=df_aftercoalesce.filter($"Record_status" =!= "No_Record")

// COMMAND ----------

val eleminatenochangerecords=eleminatenochangerecords_temp.drop("Record_status")

// COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic") 
//spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")

// COMMAND ----------

//employee_consolidated_dataframe_inc.write.mode("overwrite").insertInto(s"${stg_employee_consolidated_inc}") 
if (!eleminatenochangerecords.rdd.isEmpty()) eleminatenochangerecords.write.mode("overwrite").insertInto(s"${wd_employee_inc}") else null
//eleminatenochangerecords.write.mode("overwrite").insertInto(s"${wd_employee_inc}")


// COMMAND ----------

eleminatenochangerecords.write.mode(SaveMode.Append).insertInto(s"${wd_employee}")

// COMMAND ----------

spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id", clintid)
spark.conf.set("fs.azure.account.oauth2.client.secret", secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/"+tenantid+"/oauth2/token")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")

// COMMAND ----------

spark.conf.set("fs.azure.account.auth.type."+adls_storage_account_name+".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type."+adls_storage_account_name+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id."+adls_storage_account_name+".dfs.core.windows.net", "" + clintid + "")
spark.conf.set("fs.azure.account.oauth2.client.secret."+adls_storage_account_name+".dfs.core.windows.net", "" + secret + "")
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+adls_storage_account_name+".dfs.core.windows.net", "https://login.microsoftonline.com/" + tenantid + "/oauth2/token")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
//dbutils.fs.ls("abfss://" + fileSystemName  + "@""+adls_storage_account_name+".dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

// COMMAND ----------

spark.conf.set("fs.azure.account.auth.type."+adls_storage_account_name+".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type."+adls_storage_account_name+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id."+adls_storage_account_name+".dfs.core.windows.net", clintid)
spark.conf.set("fs.azure.account.oauth2.client.secret."+adls_storage_account_name+".dfs.core.windows.net",secret)
//"Elanco-DataOps-NonProd-EDNA-Secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+adls_storage_account_name+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenantid+"/oauth2/token")

// COMMAND ----------

spark.sparkContext.hadoopConfiguration.set("fs.azure.account.auth.type."+adls_storage_account_name+".dfs.core.windows.net", "OAuth")
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth.provider.type."+adls_storage_account_name+".dfs.core.windows.net",  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.id."+adls_storage_account_name+".dfs.core.windows.net", clintid)
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.secret."+adls_storage_account_name+".dfs.core.windows.net", secret)
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.endpoint."+adls_storage_account_name+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenantid+"/oauth2/token")

// COMMAND ----------

// DBTITLE 1,Load WD data to ADW(synapse)

spark.conf.set("fs.azure.account.key.bdaze1iednadl01.blob.core.windows.net",key)
/*
spark.conf.set("fs.azure.account.auth.type."+adls_storage_account_name+".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type."+adls_storage_account_name+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id."+adls_storage_account_name+".dfs.core.windows.net", clintid)
spark.conf.set("fs.azure.account.oauth2.client.secret."+adls_storage_account_name+".dfs.core.windows.net",secret)
//"Elanco-DataOps-NonProd-EDNA-Secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+adls_storage_account_name+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenantid+"/oauth2/token")


spark.sparkContext.hadoopConfiguration.set("fs.azure.account.auth.type."+adls_storage_account_name+".dfs.core.windows.net", "OAuth")
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth.provider.type."+adls_storage_account_name+".dfs.core.windows.net",  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.id."+adls_storage_account_name+".dfs.core.windows.net", clintid)
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.secret."+adls_storage_account_name+".dfs.core.windows.net", secret)
spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.endpoint."+adls_storage_account_name+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenantid+"/oauth2/token")
*/





//wd_final_dataframe.write
eleminatenochangerecords.write
  .format("com.databricks.spark.sqldw")
  .option("url", "jdbc:sqlserver://bdaze1isqdwdb01.database.windows.net:1433;database=BDAZE1ISQDWSV01;user=elancoadmin;password="+pswd+";encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;")
  .option("tempDir", "wasbs://edna-workforcedatadomain@bdaze1iednadl01.blob.core.windows.net/Raw/Workday/scratchpad/")
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", "EDNAWorkforce.Employee_Workday_History")
  //.option("tempDir", "wasbs://"+adls_container_name+"@"+adls_storage_account_name+".blob.core.windows.net/Raw/Workday/scratchpad")
  //.option("forwardSparkAzureStorageCredentials", "true")
  .mode("append")
  .save()

// COMMAND ----------

val max_load_date_for_run=workday_dataframe_tbl.agg(max("Load_Datetime"))
//val dateee=workday_dataframe_tbl.select(max("Load_Datetime"))
val max_load_date = if(!max_load_date_for_run.rdd.isEmpty()) (max_load_date_for_run.head(1)(0)(0)) else lit("1900-01-01 00:00:00").cast(TimestampType)

// COMMAND ----------

import org.apache.spark.sql.functions._
//val max_load_date=spark.sql("select max(load_datetime) as load_date from df_employee_wd")
val cntrl_metadata_frame = if(!workday_dataframe_tbl.rdd.isEmpty) workday_dataframe_tbl.select(lit(s"${business_name}").as("business_name"),lit(s"${wd_employee}").as("table_name"),lit(s"${processstage}").as("stage"),lit(s"${max_load_date}").as("raw_load_tm")).withColumn("load_txn_tm",lit(current_timestamp())).createOrReplaceTempView("cntrl_metadata_table")  else null

// COMMAND ----------

val df_cntrl_mtd = if (!workday_dataframe_tbl.rdd.isEmpty()) spark.sql("select business_name,table_name,stage, max(raw_load_tm) as raw_load_tm, load_txn_tm from cntrl_metadata_table group by  business_name, table_name,stage,load_txn_tm")
else null

// COMMAND ----------

/*
if (final_frame.rdd.isEmpty()) null
else
if(!val_filter.rdd.isEmpty()) if(max_load_date.head(1)(0)(0) == val_filter.head(1)(0)(0)) null else df_cntrl_mtd.write.format("delta").mode("append").save(s"${path_cntrl_table}")
else 
df_cntrl_mtd.write.format("delta").mode("append").save(s"${path_cntrl_table}")
*/
if (final_frame.rdd.isEmpty()) null
else
if(!val_filter.rdd.isEmpty()) if(df_cntrl_mtd.head(1)(0)(2) == val_filter.head(1)(0)(0)) null else df_cntrl_mtd.write.format("delta").mode("append").insertInto(s"${cntrl_table}")
else 
df_cntrl_mtd.write.format("delta").mode("append").insertInto(s"${cntrl_table}")

//final_frame.write.mode("overwrite").insertInto(s"${wd_employee_inc}")