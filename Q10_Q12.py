from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, input_file_name, regexp_extract, concat_ws, collect_list,
    lower, regexp_replace, count, avg
)
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF


def main():
    
    # Spark session
    
    spark = SparkSession.builder \
        .appName("CSL7110_Assignment1_Q10_Q12") \
        .getOrCreate()

    
    folder = "/mnt/d/Users/Lenovo/Desktop/IITJ sem2/Big Data/D184MB"
    data_path = f"{folder}/*.txt"

    
    # Q10: Load + Metadata
    
    print("\n[Q10] Loading dataset ...")
    raw_df = spark.read.text(data_path) \
        .withColumn("file_name", input_file_name())

    
    print("[Q10] Total lines (raw_df.count):", raw_df.count())

    print("[Q10] Combining lines into one row per book ...")
    books_df = raw_df.groupBy("file_name") \
        .agg(concat_ws("\n", collect_list("value")).alias("text"))

    print("[Q10] Total books (books_df.count):", books_df.count())  # you got 425

    print("[Q10] Extracting metadata ...")
    meta_df = books_df.select(
        "file_name",
        regexp_extract("text", r"Author:\s*(.*)", 1).alias("author"),
        regexp_extract("text", r"Release Date:\s*(.*)", 1).alias("release_raw"),
        regexp_extract("text", r"Language:\s*(.*)", 1).alias("language")
    )

    
    meta_df = meta_df.withColumn(
        "year",
        regexp_extract("release_raw", r"(\d{4})", 1).cast(IntegerType())
    )

    print("\n[Q10] Sample metadata rows:")
    meta_df.show(10, truncate=False)

    print("\n[Q10] Top 5 languages:")
    meta_df.filter(col("language") != "") \
        .groupBy("language") \
        .count() \
        .orderBy(col("count").desc()) \
        .show(5, truncate=False)

    
    
    print("\n[Q10] Average text length (rough check):")
    books_df.select(avg(col("text").rlike(".").cast("int")).alias("avg_title_length")).show()

    
    # Q11: TF-IDF + Similarity Pipeline (same steps)
   
    print("\n[Q11] Cleaning text (lowercase + remove non-letters) ...")
    clean_df = books_df.withColumn(
        "clean_text",
        regexp_replace(lower(col("text")), "[^a-z\\s]", "")
    )

    print("[Q11] Tokenizing ...")
    tokenizer = Tokenizer(inputCol="clean_text", outputCol="words")
    words_df = tokenizer.transform(clean_df)

    print("[Q11] Removing stopwords ...")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    filtered_df = remover.transform(words_df)

    print("[Q11] Building TF-IDF features ...")
    hashingTF = HashingTF(inputCol="filtered_words", outputCol="rawFeatures", numFeatures=10000)
    tf_df = hashingTF.transform(filtered_df)

    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idf_model = idf.fit(tf_df)
    tfidf_df = idf_model.transform(tf_df)

    print("\n[Q11] TF-IDF schema:")
    tfidf_df.printSchema()

    
    # Q12: Influence network + in/out degree (same join logic)
    
    print("\n[Q12] Preparing (author, year) ...")
    meta_small = meta_df.select("author", "year") \
        .filter(col("author") != "") \
        .filter(col("year").isNotNull()) \
        .distinct()

    X = 5
    print(f"[Q12] Creating influence edges with X = {X} years ...")

    edges_df = meta_small.alias("a") \
        .join(
            meta_small.alias("b"),
            (col("a.author") != col("b.author")) &
            (col("b.year") > col("a.year")) &
            ((col("b.year") - col("a.year")) <= X)
        ) \
        .select(
            col("a.author").alias("author1"),
            col("b.author").alias("author2"),
            col("a.year").alias("year1"),
            col("b.year").alias("year2")
        ) \
        .dropDuplicates(["author1", "author2"])

    print("[Q12] Total edges:", edges_df.count())
    print("\n[Q12] Edge sample:")
    edges_df.show(10, truncate=False)

    print("\n[Q12] Top 5 by in-degree:")
    in_degree = edges_df.groupBy("author2") \
        .agg(count("*").alias("in_degree")) \
        .orderBy(col("in_degree").desc())
    in_degree.show(5, truncate=False)

    print("\n[Q12] Top 5 by out-degree:")
    out_degree = edges_df.groupBy("author1") \
        .agg(count("*").alias("out_degree")) \
        .orderBy(col("out_degree").desc())
    out_degree.show(5, truncate=False)

    spark.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()
