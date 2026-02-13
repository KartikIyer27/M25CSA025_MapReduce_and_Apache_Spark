# M25CSA025_MapReduce_and_Apache_Spark

# Big Data Assignment 1

This repository contains my work for Assignment 1 of the Big Data course.

The assignment consists of two main parts:

1. Hadoop MapReduce (Java)
2. Apache Spark using PySpark (Q10–Q12)

---

## Files in this Repository

- `WordCount.java` → Java implementation for MapReduce (Q1–Q9)
- `Q10_Q12_pyspark.py` → PySpark implementation for Q10–Q12
- `Assignment_Report.pdf` → Final report with explanations and screenshots

---

## Dataset Used

Project Gutenberg dataset (D184MB)

- Around 425 books
- Each book is a text file
- Metadata like Author, Release Date, Language is embedded in the text

---

## Q1–Q9 (Hadoop – Java)

For the first part of the assignment, I used Hadoop MapReduce.

- Implemented WordCount in Java
- Compiled and executed using Hadoop
- Verified output from reducer
- Tested different configurations like input split size

---

## Q10 – Metadata Extraction (PySpark)

For Q10, 

- Loaded the dataset using `spark.read.text()`
- Combined lines using `groupBy` and `concat_ws`
- Extracted:
  - Author
  - Release Date
  - Language
- Extracted year using regular expressions
- Performed basic analysis such as:
  - Number of books
  - Most common language

---

## Q11 – TF-IDF and Cosine Similarity

For Q11,

- Converted text to lowercase
- Removed special characters using regex
- Tokenized words
- Removed stopwords
- Generated TF-IDF vectors using HashingTF and IDF
- Computed cosine similarity
- Displayed the top similar books

---

## Q12 – Author Influence Network

For Q12,

- Extracted `(author, year)` pairs
- Removed null or invalid values
- Created influence edges using a self-join


- Computed in-degree and out-degree
- Identified authors who influenced many others

---

## Issues Faced

During the assignment, I faced a few issues:

- Spark trying to connect to HDFS (`localhost:9000`)
- Deadlock when using `wholeTextFiles()`
- Py4J Java gateway errors during joins
- Some books missing proper metadata formatting

These were resolved by:

- Running Spark in local mode
- Switching to `read.text()` instead of `wholeTextFiles()`
- Cleaning metadata before performing joins

---

## How to Run

### Java (Hadoop)

Compile and run WordCount using Hadoop.

### PySpark

Run:

- Computed in-degree and out-degree
- Identified authors who influenced many others

---

## Issues Faced

During the assignment, I faced a few issues:

- Spark trying to connect to HDFS (`localhost:9000`)
- Deadlock when using `wholeTextFiles()`
- Py4J Java gateway errors during joins
- Some books missing proper metadata formatting

These were resolved by:

- Running Spark in local mode
- Switching to `read.text()` instead of `wholeTextFiles()`
- Cleaning metadata before performing joins

---






