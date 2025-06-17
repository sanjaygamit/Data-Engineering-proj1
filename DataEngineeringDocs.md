Data Engineering Study

1. Project Desc
2. Your role
3. Technology
4. Outcome

“ My project is to move the data from on premises to cloud and to build these scalable data pipelines.
For this my role was Senior Specialist Data Engineering and I used GCP, DATABRICKS, PYSAPRK
to build this data pipeline. project has helped to improve the performance by 30% because earlier there no big data technology involved and now we were using the big data technology.
overall we have proceesed around one terabyte of data on a monthly basis. “

Video : https://www.youtube.com/watch?v=5peQThvQmQk

APACHE AIRFLOW - “Pipeline as a Code”. “ Workflow Management Tool” ¯
“ A workflow is like a series of task that needs to be executed in specific order” (Extract- Transform-Load ETL)
DAG - Directed Acyclic Graph
Directed :- Task moves in one direction
Acyclic :- There are no loops (Task do not move in circles)
Graph :- Visual representation of Different Tasks. - BashOperator “executes a bash command” - PythonOperator “calls an arbitrary Python function” - EmailOperator - Sends an Email. - Use the @task decorators to execute an arbitrary Python function

- ## Build, Schedule , Run “Data Pipeline”

Interview Question Video : Azure Data Engineer : https://www.youtube.com/watch?v=44sWlTR-yK0&list=PLG18wEwc7jq37sc_Ncl5j_M2ycW6G4_bL

1.  Explain incremental Load :- CDC (Change Data Capture Load)
    Instead of loading the full data from the source system to the target in the ETL pipeline, will try to load the data in incremental fashion and that is on every day basis. What are the change of triggers that was happen from last fetched data will take only that triggers and will take it out and will process through our data pipelines and will load into the target , this process is nothing but the incremental process. This process runs on everyday basis but the full load will run on once that is one time load when we set up the initial pipeline.

2.  SCD Type 1 and SCD Type 2  
     SCD TYPE 1 : It is kind of overwrite mode os it do not maintain any sort of history. It will just overwrite or update the information with the existing information.
    SCD TYPE 2 : It maintain the full history like it can comes with the start date and end date and some current fact so if any new change was happened to the existing record we will insert a new recored and we can soft delete the existing record. How we will soft delete, as we will enter the end-date as current date and put indicator for an example flag ’N’ means record is no longer in use. We can add another record entry of the same record with the starting date as today and end date as a max end-date and we can keep the current indicator flag as ‘Y’ as active so that when we query the data on particular record we can use the filter as current indicator record as ‘Y’ so that we can only get the latest record from the SCD TYPE 2 and also it can maintain the historical data. So we do not loose the data in SCD TYPE 2 but we lose the historical data in SCD TYPE 1.

3.  DELTA TABLES. : https://www.youtube.com/watch?v=dludPEu1lIo&t=190s
    Delta tables are type of tables that are available in databricks, that are part of a lake house architecture. So delta tables are a kind of open source kind of table models underline the delta tables the data will be stored as spark of the parquet files and the metadata under the log kind of information will be stored on the json files. So this delta tables can support the ACID properties and it is ACID compilance, it can support version rollback, if you want to get the previous versions of the data like it basically will support the time travel feature. If you wanted to get the previous versions of the data , we can go and we can get the data based on timestamp based on the version as of now keywords we can use it.

4.  DELTA TABLES SUPPORT WHICH FILE FORMAT.
    Delta Table support PARQUET file format. The actual data will be stored in the PARQUET and the Meta Data are kind of a change log that was happened over the table will be stored under the Json log files.

5.  OPTIMIZATION ON DELTA TABLE.  
     Partition the data based on the date ways so whatever the date that we wanted to fetch we can use the filters as a date column. So that we will try to avoid the unnecessary scanning of all the partitions this is optimisation that we have enabled on delta table. We can also perform the z-ordering.

        z-ordering is nothing but reorganise the data files by sorting them based on the specific columns. It also improves the read performance of the queries that we fire on the database.

        Vaccum Command - So Vaccum commands are actually helpful to release the deleted data or soft deleted kind of data so that will actually remove from the system and it will release 		the physical disk space , that helps avoiding the scanning of the all the data and other stuff.

6.  First Name and Last name club - Separate or club using spark Dataframe.
    So inside this spark dataframe we do have a Split function. If we apply split function under name column based on the space so the first position 0th Position we can consider as FIRST NAME, and 1st Position we can consider as a LAST NAME. We can do it with withColumn Function of the data frame API.

7.  ADF in SnowFlake. Why used ADF, why can’t we get the data from Databricks. (Data Factory | Data lake | Datawarehouse)

8.  SQL already normalised Data - What transformation required ? (Fact Tables Dimension Tables : https://www.youtube.com/watch?v=dKyA6oBlKwo )

9.  Metadata will be different for each table how would you handled that ?

Window Function Interview Questions : https://www.youtube.com/watch?v=aBMwAEeRTT0&list=PLP1_hACWUYHxV0r2alS0i_H7oLOH5DXh3

10 . DEEP CLONE AND SHALLOW CLONES. : https://www.youtube.com/watch?v=bmVElHDhBP8

11. 26 DLT aka Delta Live Tables | DLT Part 1 | Streaming Tables & Materialized Views in DLT pipeline :- https://www.youtube.com/watch?v=iqf_QHC7tgQ&t=11s | https://www.youtube.com/watch?v=q543k7YrMfY
12. Delta Live Tables: Building Reliable ETL Pipelines with Azure Databricks : https://www.youtube.com/watch?v=nh-vEj_285M&t=14s
13. Delta Live Tables : Databricks : https://www.youtube.com/watch?v=PIFL7W3DmaY
14. Recursion : https://www.youtube.com/watch?v=XkL3SUioNvo
15. Remove duplicate in Pyspark : https://www.youtube.com/watch?v=_p5RSg_NT6U
16. New column in Pyspark in Databricks : https://www.youtube.com/watch?v=8ZBpaeFhq7E
17. Add | Rename | Drop Column : https://www.youtube.com/watch?v=2SzrgwVhsy0
18. how to take table ddl backup in databricks : https://www.youtube.com/watch?v=yukhCLUo1Qk&list=PLG18wEwc7jq37sc_Ncl5j_M2ycW6G4_bL&index=2
19.

• 5+ years of Data Engineering and Analysis: Proven experience in both data engineering and data analysis roles.
• 2+ years of network data experience: familiar with the concept of network node metrics, performance management and fault management.
• Strong Communication Skills: Ability to effectively communicate technical concepts to both technical and non-technical audiences.
• SQL Expertise: Advanced proficiency in SQL for data manipulation and querying.
• Database and Analytics Process Familiarity: Deep understanding of database structures, data warehousing concepts, and various analytical methodologies.
• Colab and Python Programming: Practical experience with Colab and Python for data processing, analysis, and automation.
• Dashboarding Skills: Proven ability to create informative and user-friendly dashboards using tools like Looker.
• BigQuery Database Usage: Hands-on experience working with the Google BigQuery database.
• Cloud data pipeline and processing: experience with data pipelines in a cloud environment, GCP is preferred, including using cloud computing and storage resources, and other native data pipeline capabilities.

Interview :

Write a function to remove duplicates from a list of integers and return the unique elements.
Example Input:
numbers = [1, 2, 2, 3, 4, 4, 5]
Expected Output:
[1, 2, 3, 4, 5]
Follow-up: How can you do this without using any additional data structures?

numbers = [1, 2, 2, 3, 4, 4, 5]

Def rem_dup(s):
ds = set(s)
return ds

rem_dup(nubmber):
numbers = [1, 2, 2, 3, 4, 4, 5]
Ds = spark.createDataframe(number)
Ds1 = spark.dropDuplicate()

numbers = [1, 2, 2, 3, 4, 4, 5]

Def rem_dup(s):
ds = {}
for I in s:
if I not in s:
ds[i] == 0
else:
ds[i] = d[i] + 1
rerturn ds

Def rem_dup(s):
ds = []
for I in s:
if I not in s:
ds = ds.append(I)
else:
pass:
rerturn ds

Create a query that displays the monthly average sales per product category for the last year. The output should include the month, category name, and average sales amount.

Product Category- partition | monthly

    	Month | Category | Name | average of sales amount


              Select month, categloty, Name,
           sum(sales_amount)over(partition by month, Category)
          from product_details;

Daily data

With ds as (Select Catefgory, Name, day_date, Sales_amount, o_date(day_date,’MM-YYYY’) mon1
sum()over(partition by to_date(day_date,’MM-YYYY’) order to_date(day_date,’MM-YYYY’) ) month_sum_data
From product_data)
Select mon1 “Month”,Catefgory, Name,
sum(month_sum_data)over();

;

1. different stages in snowflake. : https://www.youtube.com/watch?v=reWO0qqnpN0
2. different type of table in snowflake.
3. Different type of views in snowflake.
4. How you will implement pipepine in snowflake.

PYSPARK INTERVIEW QUESTION : https://www.youtube.com/watch?v=EsCv8c0l9XQ&list=PLNRxk1s77zfj2M2MuCuEPy_k25bUbv1Py

######################################## SnowFlake ################################################

- https://www.youtube.com/watch?v=4JYirMYRQC8
- https://www.youtube.com/watch?v=Ir-8g7TQbcU
  Q What is snowflake.
  Snowflake is a cloud data warehouseing solution.
  Snowflake enables data storage, processing and analytic solutions that are faster, easier to use, and far more flexible than traditional offerings.
  Q Is snowflake available on premises ?

  - No, Snowflake data warehouse is a true Saas offering which can be hosted only on public cloud like aws, azure or gcp.

  Q What are the three key layers on snowflake architecture?

  - Database
  - Query processing(Compute)
  - Cloud Services

  1. What is Snowpipe

     Snowpipe enables loading data from files as soon as they are available in a stage.

  2. What data security features are inbuilt in Snowflake?
     Data security plays a prominent role in all enterprises. Snowflake adapts the best-in-class security standards for encrypting and securing the customer accounts and data that we store in the Snowflake.
     1. Network security
     2. allow or block IP
     3. Mask the data dynamically
     4. Role based access control for authorisation.
     5. all the data by default encrypted.

3.  How to load data in snowflake.
    Data can be loaded in Snowflake using various ETL tools or COPY command.

4.  Explain Virtual warehouse coast in Snowflake.

5.  Permanent tabble, temporary table, transient table,

    #### AWS to Snowflake

    1. How to upload the Data in S3 Service.
       How to create the external stage in snowflake:
       CREATE STAGE AWS_STG
       URL = ''
       STORAGE_INTEGRATION =

### Ingestion Options

    - Copy
        Efficient bulk loading
        Control your own warehouse
        Deterministic latency
    - Snowpipe
        Continuous ingestion of files
        Serverless
        Median latency ~ 30s

    - Snowpipe Streaming
        Near real-time ingestion of rowsets
        Client application needed
        < 5s median latency

    What is Snowpipe?
        Snowpipe is Snowflake's continuous data ingestion service that automatically loads data as it becomes available, ensuring near real-time data availability for analysis.
    How does Snowflake ensure data security and compliance with requlations?
        End-to-end encryption,
        Role based access control
        network policies
        compliance with standard HIPPA,
        GDPR
        SOC2 type II ensuring data protection and regulatory adherence.

    What are the different caching mechanisms in Snowflake, and how do they enhence performance?
        Snowflake employs
        result caching,
        metadata caching
        query caching
            and
        data caching
                        to enhance performance by reducing query response times and minimizing the need to reprocess data.

    How does Snowflake's data sharing feature work, and what are its benefits?
        Snowflake's secure Data sharing allows organizations to share data in real time with other Snowflake accounts without data duplication, enhancing collaboration and reducing storage costs.
    What strategies can be employed to optimize query performance in Snowflake ?
        Optimzing query performance in Snowflake can be achieved through
            proper data clustering
            using materializeed views
            leveraging result caching
            minimizing data movement by filtering data in queries.
    How does Snowflake integrate with ETL tools and supports data integration workflows ?
        Snowflake integrates seamlessly with various ETL tools through connectors and drivers, supporting data integration workflows by allowing efficient data loading, transformation and unloading operations.

######################################## SnowFlakeEnd ################################################
