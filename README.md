# Large-Scale Data Analysis of Amazon Datasets

## Overview

This project combines two distinct parts: one utilizing Apache Spark for data analysis and transformation, and the other using Dask on AWS EC2 for distributed computing. Below is a detailed guide for both sections of the project.

## Table of Contents

- [Apache Spark](#apache-spark)
  - [Overview](#overview-apache-spark)
  - [Prerequisites](#prerequisites-apache-spark)
  - [Usage](#usage-apache-spark)
  - [Task Details](#task-details-apache-spark)
  - [Results](#results-apache-spark)
- [Dask](#dask)
  - [Overview](#overview-dask)
  - [Prerequisites](#prerequisites-dask)
  - [Usage](#usage-dask)
  - [Task Details](#task-details-dask)
  - [Results](#results-dask)

---

## Apache Spark

### Overview (Apache Spark)

The Apache Spark portion of this project is centered around analyzing and processing data using the Apache Spark framework. It encompasses a series of tasks, each addressing specific data analysis and transformation objectives.

### Prerequisites (Apache Spark)

Before executing the Apache Spark tasks, ensure you have the following prerequisites installed:

- Apache Spark
- PySpark
- Python 3.x
- Other necessary Python libraries (refer to the 'utilities.py' file for specific libraries)

### Usage (Apache Spark)

1. Clone the repository:

```bash
git clone <repository_url>
```

2. Navigate to the project directory:

```bash
cd <project_directory>
```

3. Run the tasks individually by executing the corresponding Python files. For example, to run Task 1:

```bash
python task_1.py
```

4. Follow the same pattern to run the other tasks.

### Task Details (Apache Spark)

The Apache Spark segment of the project comprises several tasks, each with specific objectives:

#### Task 1

- Compute statistics on product ratings and counts.

#### Task 2

- Analyze sales rank and product categories.

#### Task 3

- Compute statistics on related products' prices and counts.

#### Task 4

- Impute missing values in the 'price' column and handle unknown titles.

#### Task 5

- Create word embeddings for product titles.

#### Task 6

- Encode and reduce dimensionality of product categories.

### Results (Apache Spark)

The results of each task are saved and can be accessed through the 'data_io' object. Each task provides specific results as outlined in the task descriptions.

### Acknowledgments (Apache Spark)

- [Apache Spark](https://spark.apache.org/) - For the distributed data processing framework.
- [PySpark](https://spark.apache.org/docs/latest/api/python/index.html) - For the Python API for Apache Spark.
- [Python](https://www.python.org/) - For the programming language used in this project.

---

## Dask

### Overview (Dask)

The Dask segment of the project involves using Dask on AWS EC2 to process and analyze data. It includes tasks that compute statistics, analyze data, and handle missing values using Dask on a remote AWS EC2 instance.

### Prerequisites (Dask)

Before running the Dask tasks, ensure you have the following prerequisites installed and configured:

- Dask
- Dask Dataframe
- Python 3.x
- AWS EC2 instance configured with Dask (Follow AWS documentation for setup)

### Usage (Dask)

1. Clone the repository:

```bash
git clone <repository_url>
```

2. Navigate to the project directory:

```bash
cd <project_directory>
```

3. Ensure your AWS EC2 instance is running and configured with Dask.

4. Run the Dask tasks by executing the `PA1` function in your Python environment. Provide the paths to the user reviews CSV and products CSV as arguments to the function:

```python
from dask_ec2 import PA1

user_reviews_csv = 'path/to/user_reviews.csv'
products_csv = 'path/to/products.csv'

runtime = PA1(user_reviews_csv, products_csv)
print(f"Total runtime: {runtime} seconds")
```

### Task Details (Dask)

The Dask segment of the project consists of several tasks aimed at processing and analyzing data. Here are the details of each task:

#### Task 1

- Compute statistics on product ratings and counts.

#### Task 2

- Analyze sales rank and product categories.

#### Task 3

- Compute statistics on related products' prices and counts.

#### Task 4

- Impute missing values in the 'price' column and handle unknown titles.

#### Task 5

- Check for matching keys between two DataFrames.

#### Task 6

- Check for matching keys within DataFrame columns.

### Results (Dask)

The results of each task are saved to a 'results_PA1.json' file. This JSON file contains the computed values for each task. You can access and analyze the results by reading the JSON file.

### Acknowledgments (Dask)

- [Dask](https://dask.org/) - For the distributed computing framework.
- [Amazon Web Services (AWS)](https://aws.amazon.com/) - For providing cloud infrastructure for running Dask on EC2 instances.

---
