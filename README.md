# STEDI-Human-Balance-Analytics
Project Introduction: STEDI Human Balance Analytics In this project, you'll act as a data engineer for the STEDI team to build a data lakehouse solution for sensor data that trains a machine learning model.

### Project Overview

This project is an AWS Glue data pipeline designed to process and transform raw data from STEDI exercise devices into a clean, organized format, ready for analytics and machine learning. The project follows a standard three-layer data lake architecture: **Landing**, **Trusted**, and **Curated**.

---

### Pipeline Stages

The data pipeline consists of three main Glue jobs that are executed in a specific, sequential order:

#### **1. Customer Landing to Trusted**

-   **Objective:** To move customer data from the `Landing` zone to the `Trusted` zone.
-   **Logic:** The data is filtered to include only customers who have consented to share their information for research. The data is converted to the **Parquet** format for optimized query performance.

#### **2. Accelerometer Landing to Trusted**

-   **Objective:** To move accelerometer data from the `Landing` zone to the `Trusted` zone.
-   **Logic:** This job joins the raw accelerometer data with the trusted customer data to ensure that only data from privacy-consenting users is processed. The data is then converted to **Parquet**.

#### **3. Step Trainer Trusted to Curated**

-   **Objective:** To combine data from the `Step Trainer Trusted` and `Accelerometer Trusted` zones to create a final dataset ready for machine learning.
-   **Logic:** The two datasets are joined based on their common `timestamp` field to link sensor readings with accelerometer movements. The final, curated data is written to the `Curated` zone in **Parquet** format.

---

### AWS S3 Folder Structure

The project uses a standard data lake folder structure within your S3 bucket:

-   `s3://zsmbucket360/customer/landing/`
-   `s3://zsmbucket360/customer/trusted/`
-   `s3://zsmbucket360/accelerometer/landing/`
-   `s3://zsmbucket360/accelerometer/trusted/`
-   `s3://zsmbucket360/step_trainer/landing/`
-   `s3://zsmbucket360/step_trainer/trusted/`
-   `s3://zsmbucket360/ML_curated/`

---

### Setup and Execution

#### **Prerequisites**

-   An AWS account with sufficient permissions for AWS Glue and S3.
-   An S3 bucket named `zsmbucket360`.

#### **Execution Steps**

1.  Upload your raw JSON data files to the corresponding `landing` folders in your S3 bucket.
2.  Create three separate jobs in AWS Glue, each using one of the provided Python scripts:
    * **Job 1:** `customer_landing_to_trusted.py`
    * **Job 2:** `accelerometer_landing_to_trusted.py`
    * **Job 3:** `step_trainer_trusted_to_curated.py`
3.  Run the jobs in the specified sequential order (1 -> 2 -> 3) to build the complete data pipeline.
