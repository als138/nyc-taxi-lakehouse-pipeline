
# 🗽 NYC Taxi Lakehouse Project

This project implements a Lakehouse architecture using PySpark and Delta Lake on a local environment. It processes the NYC Taxi Trip dataset through structured data engineering stages — ingest, clean, transform, and aggregate — while persisting everything in Delta format.


## 🎯 Project Objective

Build a modern data processing pipeline that transforms raw taxi trip data into meaningful analytical insights. The pipeline is designed with three structured layers:

- **Bronze Layer**: Stores raw CSV data
- **Silver Layer**: Cleans data, removes outliers and duplicates
- **Gold Layer**: Aggregates data for analytical consumption


## 🧰 Tools & Technologies

- Python 3.10+
- PySpark 3.5.3
- Delta Lake 3.3.2
- NYC Yellow Taxi Trip Dataset
- Ubuntu 22.04 / Windows 11


## 📁 Folder Structure


/user/ali/
├── taxi_tripdata.csv          # Raw input file
├── delta/
│   ├── nyc_taxi_raw           # Bronze layer
│   ├── nyc_taxi_clean         # Silver layer
│   └── nyc_taxi_summary       # Gold layer



## 🚀 How to Run

### 1. Install Dependencies

```bash
pip install pyspark==3.5.3 delta-spark==3.3.2
```

### 2. Execute the main script

```bash
python3 main.py
```

---

## 🔄 Processing Stages

### 🔹 Bronze Layer

- Load the CSV file using Spark
- Save raw data to Delta format

### 🔸 Silver Layer

- Filter invalid rows (`fare_amount <= 0`, `trip_distance <= 0`)
- Remove duplicates
- Identify outliers using percentiles
- Add `trip_type` column: `short`, `medium`, `long`

### 🟡 Gold Layer

- Group by pickup and dropoff locations
- Calculate average fare per route
- Save summary table in Delta format



## 📊 Sample Output

Top 10 average fares per route:
+-------------+-------------+----------+
|PULocationID |DOLocationID |avg_fare  |
+-------------+-------------+----------+
|117          |152          |73.90     |
|220          |200          |73.90     |
|168          |259          |73.69     |


## ✅ Features

- ACID transaction support with Delta Lake
- Scalable for growing datasets and future analytics
- Ready for extension to RAG workflows, ETL scheduling, or BI integrations


## 🐛 Issues & Solutions

| Issue                        | Solution                                |
|-----------------------------|-----------------------------------------|
| JAR download errors         | Used Maven Central repository           |
| Version mismatches          | Aligned PySpark and Delta Lake versions |
| SSL connection problems     | Used static IP or internal mirrors      |


## 👤 Author

**Ali Salimi**  
Email: alisalimi6205@yahoo.com

## 📄 License

This project is licensed under the MIT License.
