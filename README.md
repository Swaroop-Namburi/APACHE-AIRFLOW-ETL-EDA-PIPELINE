# APACHE-AIRFLOW-ETL&EDA-PIPELINE
Designed and implemented an end-to-end data pipeline using Apache Airflow to automate the ETL (Extract, Transform, Load) workflow and perform exploratory data analysis (EDA). The project focused on automating data processing tasks, ensuring smooth data flow between stages, and generating analytical insights from the processed dataset.

## 📌 Project Overview

This project demonstrates the implementation of an end-to-end ETL (Extract, Transform, Load) pipeline using Apache Airflow. The pipeline automates the process of extracting data from a Netflix titles dataset, performing data cleaning and transformation, loading the processed data into a target location, and generating analytical insights through Exploratory Data Analysis (EDA).

The workflow is orchestrated using Apache Airflow's DAG architecture, ensuring scalability, reliability, and automation of the complete data pipeline.

---

## 🎯 Objective

The primary objective of this project is to:

* Automate the ETL process using Apache Airflow.
* Perform data cleaning and transformation on the Netflix dataset.
* Generate meaningful insights through Exploratory Data Analysis.
* Demonstrate workflow orchestration using DAGs and task dependencies.

---

## 🛠️ Technologies Used

* Python
* Apache Airflow
* Pandas
* NumPy
* Linux Environment
* Airflow Scheduler
* Airflow Web UI

---

## 📂 Dataset Description

The project uses the Netflix Titles Dataset containing information about movies and TV shows available on Netflix.

### Key Columns

| Column Name  | Description                         |
| ------------ | ----------------------------------- |
| show_id      | Unique identifier for each title    |
| type         | Content type (Movie / TV Show)      |
| title        | Name of the content                 |
| director     | Director of the content             |
| cast         | Actors involved                     |
| country      | Country of production               |
| date_added   | Date added to Netflix               |
| release_year | Year of release                     |
| rating       | Content maturity rating             |
| duration     | Movie duration or number of seasons |
| listed_in    | Genre / Category                    |
| description  | Summary of content                  |

---

## 🔄 ETL Pipeline Architecture

The pipeline follows a four-stage workflow:

```text
Extract → Transform → Load → EDA
```

### 1️⃣ Extract

* Reads raw Netflix dataset from a CSV file.
* Validates and counts records.
* Passes extracted data to downstream tasks using XCom.

### 2️⃣ Transform

#### Data Cleaning

* Removed duplicate records.
* Handled missing values:

  * Director → "Unknown"
  * Cast → "Not Available"
  * Country → "Unknown"

#### Data Standardization

* Converted:

  * show_id
  * type
  * country
  * rating

  to uppercase format.

* Converted title column to Title Case.

* Ensured proper string formatting across text columns.

#### Data Type Conversion

* Converted release_year into numeric format.
* Validated data types for analytical processing.

#### Feature Engineering

Created additional columns:

* duration_min → Duration in minutes for Movies
* seasons → Number of seasons for TV Shows

### 3️⃣ Load

* Stored transformed dataset into a CSV file.
* Generated clean and analysis-ready output.

### 4️⃣ Exploratory Data Analysis (EDA)

Generated insights including:

* Top countries producing Netflix content.
* Distribution of Movies vs TV Shows.
* Most popular genres/categories.
* Year-wise content addition trends.

---

## ⚙️ Airflow DAG Features

The project leverages several Apache Airflow capabilities:

* `@dag` decorator
* `@task.python`
* Task dependencies using `>>`
* XCom for inter-task communication
* Automated scheduling and monitoring through Airflow UI

---

## 📊 Sample Insights Generated

* Content distribution by type.
* Most active content-producing countries.
* Popular genres available on Netflix.
* Growth trends in content additions over time.


## 📈 Key Learning Outcomes

* Workflow orchestration using Apache Airflow.
* Building scalable ETL pipelines.
* Data cleaning and transformation using Pandas.
* Feature engineering techniques.
* Automating data analysis workflows.
* Monitoring and managing pipelines using Airflow UI.

---

## ✅ Conclusion

This project successfully demonstrates how Apache Airflow can be used to automate a complete ETL workflow. The Netflix dataset was extracted, transformed, analyzed, and loaded through a structured pipeline, reducing manual effort and improving scalability. The implementation showcases industry-standard data engineering practices and provides a strong foundation for building production-grade data pipelines.
