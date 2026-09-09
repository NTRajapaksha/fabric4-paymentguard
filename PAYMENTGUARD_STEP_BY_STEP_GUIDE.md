# PaymentGuard: End-to-End Microsoft Fabric Implementation Guide
## Real-Time Transaction Fraud Detection (Lakehouse + Eventhouse + Warehouse Fused)

---

## Executive Summary & Architectural Review

This implementation guide translates the architecture defined in [`fabric-e2e-paymentguard-project.md`](file:///c:/Users/VICTUS/Desktop/fabric/fabric-e2e-paymentguard-project.md) into a step-by-step blueprint. You will build a multi-engine fraud detection platform in Microsoft Fabric using the provided historical Kaggle dataset ([`creditcard.csv`](file:///c:/Users/VICTUS/Desktop/fabric/creditcard.csv)) and a live streaming simulator ([`simulate_transactions.py`](file:///c:/Users/VICTUS/Desktop/fabric/simulate_transactions.py)).

### Architectural Strengths & Technical Keys
1. **Multi-Engine Zero-Copy Fusion**: Rather than ETLing data between engines, **OneLake Shortcuts** allow the T-SQL Warehouse to join Lakehouse Spark Delta tables directly with Eventhouse KQL tables in a single SQL query.
2. **Multi-Cloud Data Virtualization (AWS S3 / Azure ADLS Gen2 Shortcut)**: Fabric OneLake shortcuts connect directly to external cloud object stores. Storing historical data in **AWS S3** and creating an S3 Shortcut in Fabric gives you a true multi-cloud architecture with zero data duplication.
3. **Cloud Cost Breakdown (150 MB dataset)**:
   - **AWS S3 Standard**: 150 MB = 0.15 GB. At \$0.023 / GB / month, S3 storage costs **\$0.0034 / month (less than half a cent)**. In the AWS 12-Month Free Tier (5 GB storage + 20,000 GET requests), it is **$0.00 (completely free)**.
   - **Azure Blob / ADLS Gen2 Hot**: 150 MB costs **~$0.003 / month**, within Azure free tier.
   - **Fabric Ingress**: Microsoft Fabric does not charge any ingestion fee for OneLake shortcuts; Spark reads Parquet/CSV via standard S3/Azure REST API calls.
4. **KQL OneLake Availability (Crucial Fabric Nuance)**: By default, Eventhouse tables reside in Kusto native index storage. To query them from Fabric Warehouse or Lakehouse via shortcuts, you **must enable OneLake Availability** on the KQL table, which automatically mirrors raw Parquet/Delta files into OneLake.
5. **Historical to Live Schema Alignment**: The historical dataset contains PCA features (`V1`–`V28`), `Time`, `Amount`, and `Class`. In Phase 1, our PySpark notebook enriches this data with realistic business dimensions (`merchant_category`, `customer_segment`, `hour_of_day`) and precomputes segment-level baseline fraud probabilities. This allows the Warehouse to merge live streaming transactions with historical risk baselines seamlessly.

---

## Project Structure & File Manifest

| File | Description |
|---|---|
| [`fabric-e2e-paymentguard-project.md`](file:///c:/Users/VICTUS/Desktop/fabric/fabric-e2e-paymentguard-project.md) | Original architectural specification & requirements document |
| [`creditcard.csv`](file:///c:/Users/VICTUS/Desktop/fabric/creditcard.csv) | Historical Kaggle fraud dataset (284,807 transactions, 150 MB) |
| [`simulate_transactions.py`](file:///c:/Users/VICTUS/Desktop/fabric/simulate_transactions.py) | Python live transaction generator supporting Event Hubs / Eventstream |
| `PAYMENTGUARD_STEP_BY_STEP_GUIDE.md` | This complete implementation manual |

---

## Phase 0: Fabric Environment & Workspace Provisioning

### Step 0.1: Sign in & Fabric Trial Activation
1. Navigate to **[https://app.fabric.microsoft.com](https://app.fabric.microsoft.com)** and log in with your Power BI / Microsoft 365 organizational or developer account.
2. If you do not have an active Fabric capacity, click on your **Account Manager avatar** (top right) -> select **Start trial** (free 60-day Fabric trial capacity, equivalent to F64).

### Step 0.2: Create Dedicated Workspace
1. In the left navigation rail, click **Workspaces** -> **+ New workspace**.
2. Set the workspace properties:
   - **Name**: `PaymentGuard-Fabric`
   - **Description**: `End-to-End Real-Time & Historical Fraud Detection Engine`
   - **Advanced**: Ensure **License mode** is set to **Trial** or **Fabric capacity**.
3. Click **Apply**.

### Step 0.3: Provision the 3 Core Storage Engines
Within the newly created `PaymentGuard-Fabric` workspace:

1. **Create Lakehouse**:
   - Click **+ New item** -> **Lakehouse**.
   - Name: `lh_paymentguard`.
   - Leave default settings and click **Create**.
2. **Create Eventhouse**:
   - Click **+ New item** -> **Eventhouse**.
   - Name: `eh_paymentguard`.
   - Under the Eventhouse, the default KQL database `kql_paymentguard` is created automatically (or rename it to `kql_paymentguard`).
3. **Create Warehouse**:
   - Click **+ New item** -> **Warehouse**.
   - Name: `wh_paymentguard_gold`.
   - Click **Create**.

> 💡 **Why We Did This:**  
> Real-world fraud detection demands three fundamentally different processing engines:
> - **Lakehouse (Spark)**: Ideal for heavy batch transformation, complex feature engineering, and processing uncurated historical data without rigid schemas.
> - **Eventhouse (KQL)**: Purpose-built for streaming ingestion with sub-second sliding-window analytics, catching in-flight card bursts instantly.
> - **Warehouse (T-SQL)**: Standard ACID star schema for enterprise BI, financial reconciliation, and cross-engine querying.  
> Provisioning all three in the same Fabric workspace creates a unified data fabric where data can be queried across engines without moving a single file.

---

## Phase 1: Historical Data Ingestion & Lakehouse Engineering (Bronze -> Silver)

You have three options to land [`creditcard.csv`](file:///c:/Users/VICTUS/Desktop/fabric/creditcard.csv) into the Lakehouse. **Option A (AWS S3 Shortcut)** is highly recommended to showcase multi-cloud data virtualization on your CV:

```
┌────────────────────────────────────────────────────────────────────────┐
│                        INGESTION OPTIONS                               │
│                                                                        │
│  [Option A - Multi-Cloud Flagship]                                     │
│  AWS S3 Bucket ──(Amazon S3 OneLake Shortcut)──► lh_paymentguard/Files │
│                                                                        │
│  [Option B - Azure Native Cloud]                                       │
│  Azure ADLS Gen2 ──(ADLS Gen2 OneLake Shortcut)─► lh_paymentguard/Files │
│                                                                        │
│  [Option C - Local Direct Upload]                                      │
│  Local creditcard.csv ──(Direct Browser Upload)──► lh_paymentguard/Files │
└────────────────────────────────────────────────────────────────────────┘
```

---

### Step 1.1A: Option A — AWS S3 OneLake Shortcut (Recommended)

#### 1. Create S3 Bucket & Upload Data (AWS Console)
1. Log in to the [AWS Management Console](https://console.aws.amazon.com/s3/).
2. Navigate to **Amazon S3** -> Click **Create bucket**.
   - **Bucket name**: `paymentguard-fraud-data-<your-unique-suffix>` (e.g. `paymentguard-fraud-data-98745`).
   - **AWS Region**: Select an AWS region close to you (e.g. `us-east-1` or `eu-west-1`).
   - Leave all other default settings (Block Public Access ON, SSE-S3 encryption).
   - Click **Create bucket**.
3. Click into your new bucket -> Click **Upload** -> **Add files** -> Select local [`creditcard.csv`](file:///c:/Users/VICTUS/Desktop/fabric/creditcard.csv) -> Click **Upload**.

#### 2. Create Minimal IAM Credentials for Fabric
1. In AWS Console, open **IAM** -> **Users** -> Click **Create user** (e.g., `fabric-s3-shortcut-user`).
2. In **Set permissions**, select **Attach policies directly** -> Click **Create policy** (JSON):
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "FabricOneLakeS3ReadAccess",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": [
                "arn:aws:s3:::paymentguard-fraud-data-<your-unique-suffix>",
                "arn:aws:s3:::paymentguard-fraud-data-<your-unique-suffix>/*"
            ]
        }
    ]
}
```
3. Name policy `FabricOneLakeS3ReadPolicy` -> Click **Create policy** and attach it to the user.
4. Open the created user -> **Security credentials** tab -> **Access keys** -> **Create access key** -> Choose **Third-party service** -> Download / Copy:
   - **Access key ID**
   - **Secret access key**

#### 3. Create the Amazon S3 Shortcut in Fabric Lakehouse
1. In Microsoft Fabric, open Lakehouse **`lh_paymentguard`**.
2. In the explorer pane, hover over **Tables** (or **Files**) -> click `...` -> **New shortcut**.
   *(Placing it under **Tables** registers it directly as a managed table catalog object `Tables/dbo/s3_bronze`).*
3. Select **Amazon S3**.
4. Fill in the connection settings:
   - **Connection**: Create new connection.
   - **Amazon S3 root URL**: `https://paymentguard-fraud-data-<your-unique-suffix>.s3.<region>.amazonaws.com`  
     *(Or: `https://s3.<region>.amazonaws.com/paymentguard-fraud-data-<your-unique-suffix>`)*
   - **Authentication kind**: `Key`.
   - **Access key**: Paste your AWS Access key ID.
   - **Secret key**: Paste your AWS Secret access key.
5. Click **Next**.
6. **Shortcut Settings (Crucial Cost Optimization)**:
   - **Enable cache for shortcuts**: Toggle to **ON**.
   - **Retention Period**: Set to **1 day**.
7. Set **Shortcut name**: `s3_bronze` and **Subpath**: `/`.
8. Click **Create**.
9. Expand **Tables** (or **Files**) -> **`s3_bronze`**: You will immediately see `creditcard.csv` available in Fabric OneLake!

> 💡 **Why We Did This:**  
> - **Multi-Cloud Data Mesh**: Instead of migrating 150MB of historical records into Microsoft Azure, the Amazon S3 shortcut leaves data at rest in AWS while exposing it as a native table in OneLake.
> - **Shortcut Caching (1 Day)**: Because your Fabric capacity is in East Asia and your S3 bucket might be in US/EU, shortcut caching fetches data once and caches it locally in OneLake. This eliminates cross-cloud egress charges on repeated notebook and query executions.

---

### Step 1.1B: Option B — Azure ADLS Gen2 / Blob Storage Shortcut

If you prefer using Microsoft Azure:
1. In [Azure Portal](https://portal.azure.com), create a Storage Account (e.g., `stpaymentguarddata`).
2. Under **Containers**, create container `bronze` and upload `creditcard.csv`.
3. In `lh_paymentguard` -> **Tables** or **Files** -> `...` -> **New shortcut** -> Select **Azure Data Lake Storage Gen2** or **Azure Blob Storage**.
4. Provide URL: `https://stpaymentguarddata.dfs.core.windows.net/bronze`.
5. Authenticate with **Organizational account** or **Account key** -> Name the shortcut `adls_bronze`.

---

### Step 1.1C: Option C — Direct Upload to Fabric Files (Zero Cloud Account Needed)
1. In `lh_paymentguard` -> **Files** -> `...` -> **New subfolder** -> `bronze`.
2. Click `bronze` `...` -> **Upload** -> **Upload files** -> select local [`creditcard.csv`](file:///c:/Users/VICTUS/Desktop/fabric/creditcard.csv).

---

### Troubleshooting: Spark Pool Cold-Start Loop (`[object CloseEvent]` / WebSocket Disconnect)

> [!TIP]
> If your notebook fails on launch with `Your compute session was disconnected` or `[object CloseEvent]`, the shared regional Starter Pool is stuck or cold-starting.
> 
> **Fix in 60 seconds (Create a Dedicated Mini Pool)**:
> 1. Click workspace name `PaymentGuard-Fabric` -> **Workspace settings** (gear icon).
> 2. Go to **Data Engineering** -> **Spark settings** -> **Pools** tab.
> 3. Under **Custom pools**, click **+ New pool**:
>    - Name: `fast-pool`
>    - Node size: `Small (4 vCores, 28 GB)`
>    - Autoscale: `OFF` (Min: 1, Max: 1)
>    - Click **Apply**.
> 4. Set `fast-pool` as the **Default pool** -> click **Save**.
> 5. Close notebook, reopen it, confirm top ribbon displays `fast-pool`, and run Cell 1.

> 💡 **Why We Did This:**  
> Fabric's default "Starter Pool" is a shared multi-tenant resource. During peak regional load (e.g. in East Asia), node acquisition can exceed the Livy Jupyter WebSocket timeout (30 seconds), causing an instant disconnect loop before any Python code runs. Provisioning a dedicated single-node custom pool (`fast-pool`) guarantees immediate executor allocation with zero queuing.

---

### Step 1.2: Create Feature Engineering Notebook (`nb_clean_fraud`)
1. From the top ribbon of `lh_paymentguard`, click **Open notebook** -> **New notebook**.
2. Rename notebook to **`nb_clean_fraud`**.
3. Ensure `lh_paymentguard` is attached in the Lakehouse explorer.
4. Run the following cells in order:

#### Cell 1: Ingestion, Schema Casting & Validation
```python
import pyspark.sql.functions as F
from pyspark.sql.types import *
import os

# 1. Read directly from the shortcut (supports Tables catalog or Files path)
try:
    # First attempt: Read directly via Fabric Metastore Table API
    df_raw = spark.read.table("s3_bronze")
    print("[SUCCESS] Loaded using spark.read.table('s3_bronze')")
except Exception:
    try:
        # Second attempt: Read from Tables/dbo path if created as raw file shortcut under Tables
        df_raw = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load("Tables/dbo/s3_bronze")
        print("[SUCCESS] Loaded using path 'Tables/dbo/s3_bronze'")
    except Exception:
        # Fallback: Read from Files path
        df_raw = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load("Files/s3_bronze/creditcard.csv")
        print("[SUCCESS] Loaded using path 'Files/s3_bronze/creditcard.csv'")

total_count = df_raw.count()
print(f"Total raw transactions ingested: {total_count:,}")
df_raw.printSchema()
display(df_raw.limit(5))
```

#### Cell 2: Data Quality Checks & Exceptions Logging
```python
# Check for nulls in critical fields
null_counts = df_raw.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c) 
    for c in ["Time", "Amount", "Class"]
]).toPandas()

print("Null counts in critical columns:")
print(null_counts)

# Check for abnormal amounts
negative_amounts = df_raw.filter(F.col("Amount") < 0).count()
extreme_outliers = df_raw.filter(F.col("Amount") > 50000).count()
print(f"Negative amounts: {negative_amounts}, Outliers (> $50,000): {extreme_outliers}")

# Log any bad records into a bronze_dq_exceptions table if found
dq_bad_records = df_raw.filter((F.col("Amount") < 0) | (F.col("Amount").isNull()))
if dq_bad_records.count() > 0:
    dq_bad_records.write.format("delta").mode("overwrite").saveAsTable("bronze_dq_exceptions")
    print("Logged data quality anomalies to bronze_dq_exceptions.")
else:
    print("Data quality verification passed: 0 invalid records.")
```

#### Cell 3: Business Dimension Enrichment & Feature Engineering
```python
# The Kaggle dataset contains Time (seconds elapsed from start of dataset) and PCA V1-V28.
# We enrich it with synthetic, realistic business dimensions to enable relational joins:
# 1. hour_of_day: (Time / 3600) % 24
# 2. merchant_category: deterministic pseudo-hash based on PCA feature sign & amount
# 3. customer_segment: segment based on transaction amount tiers
# 4. transaction_id: unique GUID

df_enriched = df_raw.withColumn("transaction_id", F.expr("uuid()")) \
    .withColumn("hour_of_day", (F.col("Time") / 3600).cast("int") % 24) \
    .withColumn("is_fraud", F.col("Class").cast("int")) \
    .withColumn(
        "merchant_category",
        F.when(F.col("Amount") > 1000, "Luxury Goods")
         .when(F.col("V1") > 1.0, "Online Retail")
         .when(F.col("V2") < -1.0, "Travel & Airline")
         .when(F.col("V3") > 1.0, "Electronics")
         .when(F.col("Amount") < 25, "Grocery")
         .when(F.col("Amount") < 75, "Dining & Entertainment")
         .otherwise("General Merchandise")
    ) \
    .withColumn(
        "customer_segment",
        F.when(F.col("Amount") >= 1500, "Corporate")
         .when(F.col("Amount") >= 400, "Premium")
         .when(F.col("Amount") >= 50, "Standard")
         .otherwise("New Account")
    )

display(df_enriched.select("transaction_id", "hour_of_day", "Amount", "merchant_category", "customer_segment", "is_fraud").limit(10))
```

#### Cell 4: Save Clean Delta Table (`silver_transactions`)
```python
# Write the cleaned historical transactions into Silver Delta Lake table
df_enriched.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_transactions")

print("Successfully written table: silver_transactions")
```

#### Cell 5: Compute Merchant Segment Baseline Risk (`silver_merchant_fraud_stats`)
```python
# Precompute historical fraud rates by merchant category and customer segment
# This serves as the baseline lookup table joined with live streaming alerts in the Warehouse
df_stats = df_enriched.groupBy("merchant_category", "customer_segment") \
    .agg(
        F.count("transaction_id").alias("total_historical_txns"),
        F.sum("is_fraud").alias("total_historical_fraud"),
        F.round(F.avg("Amount"), 2).alias("avg_historical_amount"),
        F.round(F.sum("is_fraud") / F.count("transaction_id"), 5).alias("historical_fraud_rate")
    )

df_stats.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_merchant_fraud_stats")

print("Successfully written table: silver_merchant_fraud_stats")
display(spark.table("silver_merchant_fraud_stats"))
```

5. Click **Run all** on the notebook. Confirm both `silver_transactions` and `silver_merchant_fraud_stats` appear in the Lakehouse **Tables** section.

> 💡 **Why We Did This:**  
> - **Schema Bridging**: Raw Kaggle fraud data is PCA-anonymized (`V1`–`V28`), which cannot be joined with live transactions. By deriving `merchant_category` and `customer_segment` using PySpark, we bridge the gap between machine learning features and relational star schemas.
> - **Precomputing Baselines (`silver_merchant_fraud_stats`)**: Calculating baseline fraud probabilities on 285,000 rows inside a streaming or warehouse query would cause high query latency. Precomputing it into a clean Silver lookup table enables the Warehouse to perform instantaneous risk fusion.

---

## Phase 2: Eventhouse Real-Time Streaming & KQL Analytics

### Step 2.1: Create & Configure Eventstream (`es_livetxns`)
1. In the `PaymentGuard-Fabric` workspace, click **+ New item** -> **Eventstream**.
2. Name: `es_livetxns` -> click **Create**.
3. In the Eventstream canvas:
   - Click **Add source** -> select **Custom App**.
   - Source name: `src_custom_python`.
   - Click **Add**.
4. Click on the `src_custom_python` node in the canvas, and in the bottom pane under **Keys**, copy the **Connection string-primary key** (looks like `Endpoint=sb://...;SharedAccessKeyName=...`). Save this for `simulate_transactions.py`.

---

### Step 2.2: Add Eventhouse Destination
1. On the Eventstream canvas, click **Add destination** -> **Eventhouse**.
2. Select destination settings:
   - **Eventhouse**: `eh_paymentguard`
   - **KQL Database**: `kql_paymentguard`
   - **Destination table**: Select **New table** -> name it **`LiveTransactions`**.
   - **Input data format**: `JSON`.
3. Click **Save**.
4. In the top ribbon, click **Publish** to start the Eventstream route.

> 💡 **Why We Did This:**  
> Eventstream acts as the managed ingestion gateway into Fabric, exposing an Azure Event Hubs-compatible endpoint (`Endpoint=sb://...`). This decouples the streaming producer (`simulate_transactions.py`) from the downstream analytical storage, allowing events to be buffered and routed without data loss.

---

### Step 2.3: CRITICAL STEP — Enable OneLake Availability on KQL Table
> [!IMPORTANT]
> To allow the Microsoft Fabric Warehouse to create a zero-copy shortcut to this KQL table, **OneLake Availability** must be turned on.

1. Open your **`kql_paymentguard`** KQL database in the Eventhouse.
2. In the left tree view, expand **Tables** -> click `...` next to **`LiveTransactions`** (or create the table schema if it has not received events yet).
3. If the table is not created yet, execute the following KQL command in the query window:
```kql
.create table LiveTransactions (
    transaction_id: string,
    account_id: string,
    customer_segment: string,
    merchant_id: string,
    merchant_category: string,
    amount: real,
    channel: string,
    timestamp: datetime,
    risk_score: real,
    is_simulated_anomaly: int,
    anomaly_reason: string
)
```
4. Now enable OneLake Availability policy via KQL command:
```kql
.alter table LiveTransactions policy mirroring data_format=parquet
```
*(Alternatively, in the UI: Click the `...` next to `LiveTransactions` -> **OneLake availability** -> Toggle to **Active** -> **Done**).*

> 💡 **Why We Did This:**  
> Eventhouse stores data in proprietary Kusto columnar index shards optimized for sub-second text and time-series queries. Fabric Warehouse cannot natively read Kusto indexes. Enabling **OneLake Availability** instructs Eventhouse to continuously mirror incoming streaming events into open Delta Parquet files in OneLake. This is the exact mechanism that unlocks zero-copy cross-engine querying!

---

### Step 2.4: Real-Time Fraud Detection Queries in KQL
Run these queries in the KQL query editor of `kql_paymentguard` (explicit casts ensure zero semantic type errors):

#### 1. Account Velocity Check (Rolling 1-Minute Window)
Detects card testing attacks (multiple charges on the same card within 60 seconds):
```kql
LiveTransactions
| extend txn_time = todatetime(timestamp)
| extend txn_amount = toreal(amount)
| where txn_time > ago(1h)
| summarize 
    txn_count_1m = count(), 
    total_spent_1m = sum(txn_amount), 
    categories = make_set(merchant_category) 
    by account_id, bin(txn_time, 1m)
| where txn_count_1m >= 3
| project txn_time, account_id, txn_count_1m, total_spent_1m, categories
| order by txn_time desc
```

#### 2. High-Amount Anomaly Scoring
Flags transactions exceeding \$3,000 or with synthetic risk score > 0.70:
```kql
LiveTransactions
| extend txn_time = todatetime(timestamp)
| extend txn_amount = toreal(amount)
| extend txn_risk = toreal(risk_score)
| where txn_time > ago(1h)
| extend is_amount_anomaly = iff(txn_amount > 3000.0, 1, 0)
| extend is_risk_anomaly = iff(txn_risk > 0.70, 1, 0)
| where is_amount_anomaly == 1 or is_risk_anomaly == 1
| project txn_time, transaction_id, account_id, merchant_category, txn_amount, txn_risk, anomaly_reason
| order by txn_time desc
```

#### 3. Create Flagged Rollup Table (`FlaggedTransactions5min`)
Execute the following KQL command to maintain an automated flagged rollup:
```kql
.set-or-append FlaggedTransactions5min <|
LiveTransactions
| extend txn_time = todatetime(timestamp)
| extend txn_amount = toreal(amount)
| extend txn_risk = toreal(risk_score)
| where txn_time > ago(1d)
| extend is_velocity_flag = iff(txn_risk > 0.65 or toint(is_simulated_anomaly) == 1, 1, 0)
| extend is_amount_flag = iff(txn_amount > 3000.0, 1, 0)
| where is_velocity_flag == 1 or is_amount_flag == 1
| summarize 
    last_seen = max(txn_time),
    flag_count = count(),
    max_amount = max(txn_amount),
    max_risk_score = max(txn_risk),
    reasons = make_set(anomaly_reason)
    by transaction_id, account_id, merchant_category, customer_segment
```

Also enable OneLake availability on `FlaggedTransactions5min`:
```kql
.alter table FlaggedTransactions5min policy mirroring data_format=parquet
```

> 💡 **Why We Did This:**  
> - **Explicit Type Casting (`todatetime`, `toreal`)**: Eventstream ingests raw JSON payloads where numeric and datetime fields often arrive as strings. Casting upfront prevents KQL runtime semantic type mismatch errors against time functions like `ago()`.
> - **Rolling Velocity Windows (`bin(txn_time, 1m)`)**: Card-testing fraud bots submit 4-5 small transactions in seconds to check card validity before making large fraudulent purchases. KQL's native time-bin aggregation identifies these rapid bursts within sub-second query response times.

---

### Step 2.5: Run the Live Python Simulator
1. In your local terminal or PowerShell in `c:\Users\VICTUS\Desktop\fabric`:
2. Install the lightweight Event Hubs library:
```powershell
pip install azure-eventhub faker
```
3. Set your connection string from Step 2.1:
```powershell
$env:FABRIC_EVENTSTREAM_CONNECTION_STRING="<PASTE_YOUR_EVENTSTREAM_CONNECTION_STRING_HERE>"
python simulate_transactions.py
```
4. Watch the terminal output as normal transactions and injected velocity attacks / anomalies stream into Fabric.
5. In Eventhouse, re-run `LiveTransactions | count` to observe live data streaming in.
6. **Note**: Stop the script with `Ctrl+C` when you want to pause live ingestion to conserve capacity.

> 💡 **Why We Did This:**  
> To simulate realistic financial traffic, the Python script injects genuine fraud patterns alongside baseline noise:
> - **Velocity Bursts**: 3-5 rapid charges on the same `account_id` within seconds.
> - **Extreme Amount Outliers**: \$5,000–\$15,000 spikes on luxury goods.
> - **Capacity Awareness**: Stopping the script stops capacity consumption, keeping trial capacity utilization low.

---

## Phase 3: Warehouse Gold Layer Fusion via Cross-Engine Querying

> [!NOTE]
> In Microsoft Fabric Warehouse, you **do not create manual shortcuts under the Warehouse Tables folder**. 
> Instead, Fabric Warehouse natively uses **Cross-Database T-SQL Querying** over OneLake Delta tables! A single T-SQL query in the Warehouse can directly read from `lh_paymentguard.dbo.TableName` with zero data copying or movement.

### Step 3.1: Shortcut Eventhouse into Lakehouse (`lh_paymentguard`)
To make Eventhouse real-time streams accessible to the entire workspace:
1. Open Lakehouse **`lh_paymentguard`**.
2. In the explorer pane, right-click **Tables** -> **New shortcut**.
3. Select **Microsoft OneLake**.
4. Select your KQL database **`kql_paymentguard`** (under `eh_paymentguard`).
5. Check **`LiveTransactions`** (and `FlaggedTransactions5min` if created).
6. Click **Create**.
7. Now `lh_paymentguard` contains both:
   - `silver_merchant_fraud_stats` (Historical Lakehouse Delta Table)
   - `LiveTransactions` (Real-Time Streaming Shortcut)

---

### Step 3.2: Add Lakehouse to Warehouse Explorer
1. Open Warehouse **`wh_paymentguard_gold`**.
2. In the top ribbon (or at the top of the left explorer panel), click **`+ Warehouses`** (or **Add data**).
3. Check **`lh_paymentguard`** -> Click **Add**.
4. You will now see `lh_paymentguard` directly inside your left object explorer, right next to `wh_paymentguard_gold`!

> 💡 **Why We Did This:**  
> Rather than writing an ETL pipeline to export streaming data from Eventhouse into Warehouse tables, OneLake allows Fabric Warehouse to query Lakehouse tables and Eventhouse shortcuts directly using 3-part naming (`lh_paymentguard.dbo.LiveTransactions`). This provides true **zero-copy virtualization**: one single copy of Delta files on disk, queried concurrently by Spark and T-SQL.

---

### Step 3.3: Create Gold Star Schema Tables (T-SQL DDL)
Run this T-SQL script in a new query window in `wh_paymentguard_gold`:

```sql
-- 1. Date Dimension (Fabric-Native Subquery, No CTE, Zero Red Squiggles)
IF OBJECT_ID('dbo.dim_date', 'U') IS NOT NULL DROP TABLE dbo.dim_date;

CREATE TABLE dbo.dim_date (
    date_key INT NOT NULL,
    full_date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day_of_month INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BIT NOT NULL
);

INSERT INTO dbo.dim_date
SELECT 
    YEAR(d) * 10000 + MONTH(d) * 100 + DAY(d) AS date_key,
    CAST(d AS DATE) AS full_date,
    YEAR(d) AS year,
    DATEPART(QUARTER, d) AS quarter,
    MONTH(d) AS month,
    DATENAME(MONTH, d) AS month_name,
    DAY(d) AS day_of_month,
    DATENAME(WEEKDAY, d) AS day_name,
    CASE WHEN DATENAME(WEEKDAY, d) IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END AS is_weekend
FROM (
    SELECT DATEADD(DAY, a.d + b.d * 10 + c.d * 100, CAST('2026-01-01' AS DATE)) AS d
    FROM (SELECT 0 AS d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a
    CROSS JOIN (SELECT 0 AS d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b
    CROSS JOIN (SELECT 0 AS d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) c
    WHERE a.d + b.d * 10 + c.d * 100 < 365
) Dates;

-- 2. Merchant Dimension (Explicit Integer Keys)
IF OBJECT_ID('dbo.dim_merchant', 'U') IS NOT NULL DROP TABLE dbo.dim_merchant;

CREATE TABLE dbo.dim_merchant (
    merchant_key INT NOT NULL,
    merchant_category VARCHAR(50) NOT NULL,
    risk_tier VARCHAR(20) NOT NULL,
    sla_hours INT NOT NULL
);

INSERT INTO dbo.dim_merchant (merchant_key, merchant_category, risk_tier, sla_hours)
VALUES 
    (1, 'Luxury Goods', 'HIGH', 1),
    (2, 'Online Retail', 'HIGH', 2),
    (3, 'Electronics', 'MEDIUM', 4),
    (4, 'Travel & Airline', 'MEDIUM', 4),
    (5, 'Dining & Entertainment', 'LOW', 12),
    (6, 'Grocery', 'LOW', 24),
    (7, 'General Merchandise', 'LOW', 24);

-- 3. Customer Segment Dimension (Explicit Integer Keys)
IF OBJECT_ID('dbo.dim_customer_segment', 'U') IS NOT NULL DROP TABLE dbo.dim_customer_segment;

CREATE TABLE dbo.dim_customer_segment (
    segment_key INT NOT NULL,
    customer_segment VARCHAR(50) NOT NULL,
    credit_limit_tier VARCHAR(20) NOT NULL,
    monitoring_level VARCHAR(20) NOT NULL
);

INSERT INTO dbo.dim_customer_segment (segment_key, customer_segment, credit_limit_tier, monitoring_level)
VALUES 
    (1, 'Corporate', 'UNLIMITED', 'ENTERPRISE'),
    (2, 'Premium', 'HIGH', 'ELEVATED'),
    (3, 'Standard', 'MEDIUM', 'STANDARD'),
    (4, 'New Account', 'LOW', 'WATCHLIST');

-- 4. Gold Fact Table: Fuses Real-Time & Historical Risk
IF OBJECT_ID('dbo.fact_fraud_case', 'U') IS NOT NULL DROP TABLE dbo.fact_fraud_case;

CREATE TABLE dbo.fact_fraud_case (
    case_id VARCHAR(64) NOT NULL,
    transaction_id VARCHAR(64) NOT NULL,
    account_id VARCHAR(50) NOT NULL,
    merchant_category VARCHAR(50) NOT NULL,
    customer_segment VARCHAR(50) NOT NULL,
    amount DECIMAL(18,2) NOT NULL,
    transaction_timestamp DATETIME2(6) NOT NULL,
    date_key INT NOT NULL,
    live_risk_score DECIMAL(5,4) NOT NULL,
    is_live_flagged INT NOT NULL,
    historical_fraud_rate DECIMAL(7,5) NOT NULL,
    fused_fraud_exposure_usd DECIMAL(18,2) NOT NULL,
    case_status VARCHAR(30) NOT NULL,
    ingested_at DATETIME2(6) NOT NULL
);

-- 5. Data Quality Exceptions Log (Fabric BIGINT IDENTITY)
IF OBJECT_ID('dbo.gold_dq_log', 'U') IS NOT NULL DROP TABLE dbo.gold_dq_log;

CREATE TABLE dbo.gold_dq_log (
    log_id BIGINT IDENTITY NOT NULL,
    check_name VARCHAR(100) NOT NULL,
    failed_value VARCHAR(255) NOT NULL,
    logged_at DATETIME2(6) NOT NULL
);
```

> 💡 **Why We Did This:**  
> - **Arithmetic Date Generation**: CTEs preceding `INSERT INTO` statements and `WHILE` loops are restricted in Fabric Warehouse MPP. The derived Cartesian join generates all 365 calendar dates instantaneously in a single set-based transaction without window functions.
> - **Explicit Integer Keys**: Distributed MPP architectures distribute auto-incrementing `IDENTITY` numbers across nodes in non-sequential blocks. Using deterministic natural keys (`1, 2, 3...`) for reference dimension tables ensures predictable lookups and clean Power BI star schema relationships.
> - **`DATETIME2(6)` Precision**: Standard SQL defaults `DATETIME2` to 7 decimal places, which Fabric Warehouse rejects. Enforcing precision `(6)` aligns with Fabric's MPP microsecond storage format.

---

### Step 3.4: T-SQL Stored Procedure (`sp_refresh_fraud_gold`)
This stored procedure joins the live Eventhouse streaming data with the Lakehouse historical stats table via **3-part naming (`lh_paymentguard.dbo....`)**, applies risk fusion rules, and performs an idempotent insert into `fact_fraud_case`:

```sql
CREATE OR ALTER PROCEDURE dbo.sp_refresh_fraud_gold
AS
BEGIN
    SET NOCOUNT ON;

    -- 1. Insert new/updated flagged fraud cases fusing live + historical baseline
    INSERT INTO dbo.fact_fraud_case (
        case_id,
        transaction_id,
        account_id,
        merchant_category,
        customer_segment,
        amount,
        transaction_timestamp,
        date_key,
        live_risk_score,
        is_live_flagged,
        historical_fraud_rate,
        fused_fraud_exposure_usd,
        case_status,
        ingested_at
    )
    SELECT 
        CONCAT('CASE-', live.transaction_id) AS case_id,
        live.transaction_id,
        live.account_id,
        live.merchant_category,
        live.customer_segment,
        ISNULL(TRY_CAST(live.amount AS DECIMAL(18,2)), 0.00) AS amount,
        -- Resilient datetime parsing (Fabric Warehouse requires DATETIME2(6))
        COALESCE(
            TRY_CAST(LEFT(live.timestamp, 19) AS DATETIME2(6)),
            TRY_CAST(live.timestamp AS DATETIME2(6)),
            SYSUTCDATETIME()
        ) AS transaction_timestamp,
        -- High-speed integer date key (e.g. 20260908)
        COALESCE(
            TRY_CAST(REPLACE(LEFT(live.timestamp, 10), '-', '') AS INT),
            YEAR(SYSUTCDATETIME()) * 10000 + MONTH(SYSUTCDATETIME()) * 100 + DAY(SYSUTCDATETIME())
        ) AS date_key,
        ISNULL(TRY_CAST(live.risk_score AS DECIMAL(5,4)), 0.0000) AS live_risk_score,
        CASE WHEN ISNULL(TRY_CAST(live.risk_score AS DECIMAL(5,4)), 0) > 0.60 OR ISNULL(TRY_CAST(live.amount AS DECIMAL(18,2)), 0) > 3000 THEN 1 ELSE 0 END AS is_live_flagged,
        ISNULL(TRY_CAST(hist.historical_fraud_rate AS DECIMAL(7,5)), 0.00170) AS historical_fraud_rate,
        ROUND(ISNULL(TRY_CAST(live.amount AS DECIMAL(18,2)), 0) * (1.0 + ISNULL(TRY_CAST(hist.historical_fraud_rate AS DECIMAL(7,5)), 0.00170) * 10), 2) AS fused_fraud_exposure_usd,
        CASE 
            WHEN ISNULL(TRY_CAST(live.amount AS DECIMAL(18,2)), 0) > 5000 OR ISNULL(TRY_CAST(live.risk_score AS DECIMAL(5,4)), 0) > 0.85 THEN 'IMMEDIATE_ESCALATION'
            WHEN ISNULL(TRY_CAST(live.risk_score AS DECIMAL(5,4)), 0) > 0.60 THEN 'PENDING_ANALYST_REVIEW'
            ELSE 'AUTO_CLEARED'
        END AS case_status,
        SYSUTCDATETIME() AS ingested_at
    -- Querying directly across engines via 3-part naming without data movement:
    FROM lh_paymentguard.dbo.LiveTransactions live
    LEFT JOIN lh_paymentguard.dbo.silver_merchant_fraud_stats hist
        ON live.merchant_category = hist.merchant_category
        AND live.customer_segment = hist.customer_segment
    -- High-performance MPP Anti-Join (avoids NOT IN subquery performance traps):
    LEFT JOIN dbo.fact_fraud_case existing
        ON live.transaction_id = existing.transaction_id
    WHERE existing.transaction_id IS NULL;

    -- 2. Safely capture @@ROWCOUNT into a local variable before logging
    DECLARE @RowsLoaded INT = @@ROWCOUNT;

    INSERT INTO dbo.gold_dq_log (check_name, failed_value, logged_at)
    VALUES ('sp_refresh_fraud_gold_execution', CONCAT('Rows loaded: ', @RowsLoaded), SYSUTCDATETIME());
END;
```

Test run the stored procedure manually:
```sql
EXEC dbo.sp_refresh_fraud_gold;
SELECT * FROM dbo.fact_fraud_case ORDER BY ingested_at DESC;
```

> 💡 **Why We Did This:**  
> - **MPP Anti-Join (`LEFT JOIN ... WHERE existing.transaction_id IS NULL`)**: The traditional `WHERE NOT IN (SELECT ...)` triggers full table scans and fails on distributed MPP architectures if nulls exist. An indexed anti-join ensures sub-second idempotent upserts.
> - **Resilient Parsing (`TRY_CAST(LEFT(timestamp, 19) AS DATETIME2(6))`)**: Real streaming payloads contain varying ISO 8601 formats (`Z`, `+00:00`). Standard `CAST` will crash with conversion errors, whereas string-length normalization guarantees bulletproof ingestion.
> - **Capturing `@@ROWCOUNT`**: In distributed T-SQL, `@@ROWCOUNT` must be captured immediately into a local variable before executing subsequent audit logging statements.

---

### Step 3.5: Master Orchestration Pipeline (`pl_master_paymentguard`)
Create a single end-to-end data pipeline in Fabric that orchestrates all three storage engines:

1. In workspace `PaymentGuard-Fabric`, click **+ New item** -> **Data pipeline** -> Name: **`pl_master_paymentguard`**.
2. Add **3 Activities** connected sequentially with green success arrows:
   - **Activity 1: Notebook Activity (`Run_nb_clean_fraud`)**:
     * Type: Fabric Notebook
     * Settings: Select `nb_clean_fraud` (Executes Lakehouse Spark bronze-to-silver feature pipeline).
   - **Activity 2: Stored Procedure Activity (`Run_sp_refresh_fraud_gold`)**:
     * Type: Stored Procedure
     * Settings: Select Data Warehouse `wh_paymentguard_gold` -> Stored procedure: `[dbo].[sp_refresh_fraud_gold]` (Fuses Lakehouse & Eventhouse data).
   - **Activity 3: Semantic Model Refresh Activity (`Refresh_DirectLake_Model`)**:
     * Type: Semantic Model Refresh
     * Settings: Select `sm_paymentguard_directlake` (Ensures Direct Lake memory sync).
3. Click **Schedule** (top ribbon) -> Set recurrence: Every 15 minutes.
4. Click **Save** and **Run**.

> 💡 **Why We Did This:**  
> A master orchestration pipeline guarantees data consistency across the multi-engine fabric: Lakehouse Spark jobs complete first, the Warehouse stored procedure fuses the fresh Silver tables with live streaming alerts second, and the Direct Lake semantic model is signaled to refresh last.

---

## Phase 4: Direct Lake Semantic Model & Power BI

### Step 4.1: Direct Lake Semantic Model Configuration
1. Open `wh_paymentguard_gold`.
2. In the top ribbon, click **New semantic model**.
3. Name: `sm_paymentguard_directlake`.
4. Select the tables to include:
   - `fact_fraud_case`
   - `dim_merchant`
   - `dim_customer_segment`
   - `dim_date`
5. Click **Confirm**.
6. In the model view, establish relationships:
   - `dim_date[date_key]` 1 ─── * `fact_fraud_case[date_key]` (Single cross-filter)
   - `dim_merchant[merchant_category]` 1 ─── * `fact_fraud_case[merchant_category]` (Single cross-filter)
   - `dim_customer_segment[customer_segment]` 1 ─── * `fact_fraud_case[customer_segment]` (Single cross-filter)

> 💡 **Why We Did This:**  
> **Direct Lake mode** is Microsoft Fabric's breakthrough storage format for Power BI. Instead of duplicating data into Power BI VertiPaq memory (Import Mode) or translating visuals into slow SQL queries (DirectQuery), Direct Lake reads Delta Parquet columns directly from OneLake memory. You get Import-level sub-second performance with DirectQuery-level freshness.

---

### Step 4.2: Add Key DAX Measures
Create the following DAX measures inside `fact_fraud_case`:

```dax
// 1. Total Case Count
Total Cases = COUNTROWS(fact_fraud_case)

// 2. High Risk Escalated Cases
Escalated Cases = 
CALCULATE(
    COUNTROWS(fact_fraud_case), 
    fact_fraud_case[case_status] = "IMMEDIATE_ESCALATION"
)

// 3. Flagged Transaction Rate
Flagged Rate = 
DIVIDE(
    CALCULATE(COUNTROWS(fact_fraud_case), fact_fraud_case[is_live_flagged] = 1),
    [Total Cases],
    0
)

// 4. Average Live Risk Score
Avg Live Risk Score = AVERAGE(fact_fraud_case[live_risk_score])

// 5. Total Estimated Fraud Exposure ($)
Total Fraud Exposure USD = SUM(fact_fraud_case[fused_fraud_exposure_usd])

// 6. SLA Breach Risk (Cases pending review > 2 hours)
Pending Review Count = 
CALCULATE(
    COUNTROWS(fact_fraud_case),
    fact_fraud_case[case_status] = "PENDING_ANALYST_REVIEW"
)
```

> 💡 **Why We Did This:**  
> Explicit DAX measures ensure centralized, consistent business logic across all executive dashboards. Calculating metrics like `[Total Fraud Exposure USD]` dynamically weights transaction amounts by historical segment fraud rates, providing executive leadership with actionable financial exposure figures.

---

### Step 4.3: Power BI Report Design
Click **New report** from the semantic model ribbon:

#### Page 1: "Fraud Portfolio & Risk Overview" (Direct Lake Mode)
- **Top KPI Cards**:
  - `[Total Cases]` (Formatted as integer)
  - `[Escalated Cases]` (Formatted as integer)
  - `[Flagged Rate]` (Formatted as percentage `0.0%`)
  - `[Total Fraud Exposure USD]` (Formatted as currency `$#,0`)
- **Donut Chart (Case Action Status)**:
  - **Legend**: `fact_fraud_case[case_status]` (`IMMEDIATE_ESCALATION`, `PENDING_ANALYST_REVIEW`, `AUTO_CLEARED`)
  - **Values**: `[Total Cases]`
- **Bar Chart (Exposure by Category)**:
  - **Y-axis**: `dim_merchant[merchant_category]`
  - **X-axis**: `[Total Fraud Exposure USD]`
- **Column Chart (Distribution by Customer Segment)**:
  - **X-axis**: `dim_customer_segment[customer_segment]`
  - **Y-axis**: `[Total Cases]`
- **Interactive Slicers**:
  - `dim_merchant[risk_tier]`
  - `dim_customer_segment[monitoring_level]`

> 💡 **Why We Did This:**  
> **Categorical Distribution vs. Annual Time Trends**: When demonstrating live simulated streaming data, all transactions arrive in an intra-day window. Plotting them against a 365-day calendar (`full_date`) produces a flat single-day line. Categorical breakdown charts (`case_status`, `merchant_category`, `customer_segment`) deliver immediate operational visibility into active fraud vectors.

#### Page 2: "Live Fraud Investigation Queue"
- Table view with conditional formatting:
  * Columns: `case_id`, `account_id`, `merchant_category`, `amount`, `live_risk_score`, `historical_fraud_rate`, `case_status`, `transaction_timestamp`.
  * Highlight rows in RED where `case_status == "IMMEDIATE_ESCALATION"`.

---

## Phase 5: Real-Time Dashboard & Data Activator Reflex

### Step 5.1: Create Real-Time Dashboard in Eventhouse
In your **`kql_paymentguard`** KQL database query editor, run each query below and click **Pin to dashboard** $\rightarrow$ select or create dashboard **`PaymentGuard Live Ops`**:

#### Tile 1: Live Transaction Volume & Flagged Spikes (Area / Line Chart)
```kql
LiveTransactions
| extend txn_time = todatetime(timestamp)
| extend txn_amount = toreal(amount)
| extend txn_risk = toreal(risk_score)
| where txn_time > ago(2h)
| summarize 
    TotalTransactions = count(),
    FlaggedFrauds = countif(txn_risk > 0.60 or txn_amount > 3000.0 or toint(is_simulated_anomaly) == 1)
    by bin(txn_time, 1m)
| order by txn_time asc
```
*(Pin as: Area Chart or Line Chart | X-axis: `txn_time`, Y-axis: `TotalTransactions`, `FlaggedFrauds`)*

#### Tile 2: Live Critical Incident Feed (Table / Grid)
```kql
LiveTransactions
| extend txn_time = todatetime(timestamp)
| extend txn_amount = toreal(amount)
| extend txn_risk = toreal(risk_score)
| where txn_time > ago(2h)
| where txn_risk > 0.60 or txn_amount > 3000.0 or toint(is_simulated_anomaly) == 1
| project 
    Timestamp = txn_time,
    Account = account_id,
    Category = merchant_category,
    Amount = strcat("$", tostring(round(txn_amount, 2))),
    Channel = channel,
    RiskScore = round(txn_risk, 3),
    Reason = anomaly_reason
| order by Timestamp desc
| take 25
```
*(Pin as: Table)*

#### Tile 3: Card Velocity Watchlist (Table / Grid)
```kql
LiveTransactions
| extend txn_time = todatetime(timestamp)
| extend txn_amount = toreal(amount)
| where txn_time > ago(2h)
| summarize 
    ChargeCount = count(),
    TotalSpent = strcat("$", tostring(round(sum(txn_amount), 2))),
    LastSeen = max(txn_time),
    TargetCategories = make_set(merchant_category)
    by account_id, bin(txn_time, 2m)
| where ChargeCount >= 3
| project LastSeen, account_id, ChargeCount, TotalSpent, TargetCategories
| order by LastSeen desc
```
*(Pin as: Table — flags accounts with 3+ rapid repeat transactions within 2 minutes)*

#### Tile 4: Dollars at Risk in Last 2 Hours (Single Stat KPI Card)
```kql
LiveTransactions
| extend txn_time = todatetime(timestamp)
| extend txn_amount = toreal(amount)
| extend txn_risk = toreal(risk_score)
| where txn_time > ago(2h)
| where txn_risk > 0.60 or txn_amount > 3000.0 or toint(is_simulated_anomaly) == 1
| summarize TotalExposure = strcat("$", tostring(round(sum(txn_amount), 2)))
```
*(Pin as: Card / Stat visual)*

#### Tile 5: Risk Exposure by Channel (Column / Bar Chart)
```kql
LiveTransactions
| extend txn_time = todatetime(timestamp)
| extend txn_amount = toreal(amount)
| extend txn_risk = toreal(risk_score)
| where txn_time > ago(2h)
| summarize 
    TotalVolume = count(),
    FlaggedVolume = countif(txn_risk > 0.60 or txn_amount > 3000.0)
    by channel
| order by FlaggedVolume desc
```
*(Pin as: Column Chart | Category: `channel`, Values: `FlaggedVolume`, `TotalVolume`)*

> [!TIP]
> In your `PaymentGuard Live Ops` dashboard, click the **Auto refresh** dropdown in the top header and set it to **30 seconds** for continuous live operational monitoring.

> 💡 **Why We Did This:**  
> Power BI is built for scheduled analytical reporting, not sub-second stream monitoring. Eventhouse Real-Time Dashboards run natively on KQL indexing without querying the relational warehouse, enabling fraud analysts to watch card velocity spikes and active attack feeds update every 30 seconds with zero backend load.

---

### Step 5.2: Create Data Activator Reflex Trigger (Multi-Condition Alert)
1. In workspace `PaymentGuard-Fabric`, click **+ New item** -> **Reflex (Data Activator)**.
2. Name: `rx_fraud_escalation_alert`.
3. Set the trigger condition on Eventstream `es_livetxns`:
   - **Condition**: `amount > 5000` **AND** `risk_score > 0.75`.
   - **Action**: Send Teams notification to Fraud Operations channel or Email with card details:
     * Subject: `[URGENT FRAUD ALERT] Transaction Escalation for Account {account_id}`
     * Message body: `Transaction {transaction_id} of ${amount} in {merchant_category} exceeded risk threshold (Risk: {risk_score}). Immediate freeze required.`

> 💡 **Why We Did This:**  
> Real-world fraud operations suffer from alert fatigue when single-variable thresholds are used. A multi-condition trigger (`amount > 5000` **AND** `risk_score > 0.75`) requires both financial severity and algorithmic anomaly confidence before waking up an on-call fraud analyst.

---

## Phase 6: Production Hardening & Enterprise Governance

### 6.1 Dynamic Data Masking (DDM) on Card/Account Identifiers
In `wh_paymentguard_gold`, mask the sensitive account identifier to protect PII according to PCI-DSS:
```sql
ALTER TABLE dbo.fact_fraud_case 
ALTER COLUMN account_id ADD MASKED WITH (FUNCTION = 'partial(4, "XXXXXXX", 2)');
```
*Result*: Non-privileged analysts will see `ACC_XXXXXXX19` instead of the full account number.

> 💡 **Why We Did This:**  
> PCI-DSS regulations mandate that primary account numbers (PANs) and account identifiers must never be readable in plain text by non-privileged staff. Fabric Warehouse Dynamic Data Masking obfuscates the data on-the-fly at query execution time without altering the underlying Parquet storage.

---

### 6.2 Row-Level Security (RLS) for Regional Fraud Teams
```sql
-- Create security predicate function
CREATE SCHEMA security;
GO

CREATE FUNCTION security.fn_securitypredicate(@segment AS VARCHAR(50))
    RETURNS TABLE
WITH SCHEMABINDING
AS
    RETURN SELECT 1 AS fn_securitypredicate_result
    WHERE USER_NAME() = 'lead_fraud_analyst@company.com'
       OR (USER_NAME() = 'corporate_analyst@company.com' AND @segment = 'Corporate');
GO

-- Apply filter predicate
CREATE SECURITY POLICY security.FraudCaseFilter
ADD FILTER PREDICATE security.fn_securitypredicate(customer_segment)
ON dbo.fact_fraud_case
WITH (STATE = ON);
GO
```

> 💡 **Why We Did This:**  
> In enterprise financial institutions, corporate fraud analysts should only inspect high-exposure corporate accounts, while standard consumer operations handle retail transactions. Row-Level Security enforces data tenancy directly at the database engine level, preventing unauthorized case exposure in Power BI.

---

### 6.3 Delta Lake Maintenance
In `lh_paymentguard` notebook, run regular Delta table maintenance:
```python
# Optimize file layouts and compact small parquet files
spark.sql("OPTIMIZE silver_transactions ZORDER BY (merchant_category, hour_of_day)")

# Clean old versions beyond retention period
spark.sql("VACUUM silver_transactions RETAIN 168 HOURS")
```

> 💡 **Why We Did This:**  
> - **Compaction & Z-Ordering (`OPTIMIZE`)**: Frequent streaming or micro-batch writes create thousands of small Parquet files. `OPTIMIZE` compacts small files into ~128MB chunks and clusters data along `merchant_category` and `hour_of_day`, slashing Spark query scan times.
> - **Storage Reclamation (`VACUUM`)**: Delta Lake retains previous snapshots for time travel. `VACUUM` purges uncommitted or deleted Parquet files older than 7 days (168 hours), reclaiming OneLake capacity.

---

## End-to-End Validation Checklist

| # | Step / Component | Validation Command / Action | Expected Result | Checked |
|---|---|---|---|:---:|
| 1 | `lh_paymentguard` Bronze | Check `Files/s3_bronze/creditcard.csv` (or `Files/bronze/`) | File accessible (~143.8 MB) | [ ] |
| 2 | AWS S3 Shortcut | Verify Amazon S3 shortcut reads external bucket | Zero-copy access in Fabric | [ ] |
| 3 | `silver_transactions` | `SELECT count(*) FROM silver_transactions` | 284,807 rows | [ ] |
| 4 | `silver_merchant_fraud_stats` | `SELECT count(*) FROM silver_merchant_fraud_stats` | 28 segment rows with fraud rates | [ ] |
| 5 | `es_livetxns` | Run `simulate_transactions.py` | Terminal shows sent batches | [ ] |
| 6 | Eventhouse `LiveTransactions` | `LiveTransactions \| count` in KQL | Row count increasing live | [ ] |
| 7 | OneLake Availability | Check `LiveTransactions` table settings in KQL | Status = Active (Parquet) | [ ] |
| 8 | Warehouse Shortcuts | Run `SELECT TOP 5 * FROM dbo.LiveTransactions` in Warehouse | Returns live data without ETL | [ ] |
| 9 | Stored Procedure | `EXEC dbo.sp_refresh_fraud_gold;` | Rows populated in `fact_fraud_case` | [ ] |
| 10 | Direct Lake Semantic Model | Test refresh & query in Power BI Service | Direct Lake mode active | [ ] |
| 11 | Real-Time Dashboard & Reflex | Trigger simulator anomaly batch | Dashboard updates, alert fires | [ ] |

---
