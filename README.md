# Databricks Fundamentals Bootcamp
### Building end-to-end data engineering solutions <br/>
<b>Live class at O'Reilly</b>: https://learning.oreilly.com/live-events/databricks-fundamentals-bootcamp/0642572193034/
<br/>
<b>Instructor</b>: Mohit Batra https://www.linkedin.com/in/mohitbatra/
<br/><br/>

## What you'll learn!

<li> Set up and navigate the Databricks platform
<li> Use Apache Spark to build ETL pipelines by extracting, cleaning, and transforming the data
<li> Build data warehouse-like features on top of a data lake by reliably storing the data using Delta format
<li> Use Databricks Unity Catalog to manage and govern data
<li> Build production-ready pipelines using Databricks Workflows

<br/>

## Schedule
The timeframes are only estimates and may vary according to how the class is progressing.

### Day 1: Exploring, Cleaning, and Transforming Data with Databricks

#### Introduction to Databricks (20 minutes)

<li> Presentation: Overview of Databricks platform and its features
<li> Group discussion: Benefits of using Databricks platform for organizations
<li> Q&A

#### Understanding Apache Spark architecture (30 minutes)

<li> Presentation: How Apache Spark performs distributed data processing; Spark on Databricks
<li> Group discussion: How organizations are using Spark
<li> Q&A
<li> Break

#### Setting up and exploring Databricks (60 minutes)

<li> Demos: Setting up Databricks workspace; walkthrough of Databricks workspace; creating and using interactive clusters; using serverless compute; using notebooks; working with dbutils
<li> Hands-on exercises: Set up Databricks workspace; configure cluster; use dbutils to work with file system
<li> Q&A
<li> Break

#### Reading data from multiple file formats (50 minutes)

<li> Presentation: Connecting to external storage; understanding Databricks File System (DBFS); understanding Spark DataFrames; working with file formats like CSV, Parquet, and JSON
<li> Hands-on exercises: Upload files to Databricks File System; read files using Spark's DataFrames API; infer or apply schema to files
<li> Q&A
<li> Break

#### Cleaning and transforming data using PySpark (55 minutes)

<li> Demos: Operations to clean and transform data; using Databricks Assistant for generating code
<li> Hands-on exercises: Apply clean-up operations (removing duplicates and nulls, filling missing values, and filtering records); apply transformations (selecting or renaming columns, creating derived columns, etc.)
<li> Q&A

#### Working with Spark SQL and visualizing data (25 minutes)

<li> Demos: Running SQL queries on DataFrames; creating visualizations
<li> Hands-on exercises: Create temporary SQL views on DataFrames; build reports; create charts; add charts to dashboards
<li> Q&A

### Day 2: Storing Data with Delta Lake and Building Workflows with Databricks

#### Introduction to Delta Lake (30 minutes)

<li> Presentation: Challenges with data lakes; Delta format and transaction log; ACID guarantees on data lakes; competitors
<li> Group discussion: How Delta Lake can help build a data warehouse on a data lake
<li> Q&A

#### Storing data in data lake using Delta format (40 minutes)

<li> Demos: Writing DataFrames in Delta format; checking transaction log; creating and managing Delta tables; audit history; table constraints
<li> Q&A
<li> Break

#### Working with Delta Lake features (55 minutes)

<li> Demos: Working with Delta Lake features
<li> Hands-on exercises: Perform CRUD operations, schema enforcement and evolution, time travel, and optimization; compare performance with Parquet
<li> Q&A
<li> Break

#### Working with Unity Catalog (60 minutes)

<li> Presentation: Why Unity Catalog is required; setting up a metastore and catalog; writing a table to catalog and schema
<li> Hands-on exercises: Create a metastore; assign workspace to metastore; create catalog and schema; add a table to catalog
<li> Q&A
<li> Break

#### Building workflows with Databricks (55 minutes)

<li> Presentation: Understanding different types of pipelines; difference between all-purpose and job cluster; orchestrating tasks and sharing clusters
<li> Hands-on exercises: Create job; connect tasks; create job cluster; schedule job
<li> Q&A

