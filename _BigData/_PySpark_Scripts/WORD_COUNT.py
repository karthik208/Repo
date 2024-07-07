from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("Word Count").getOrCreate()

# Path to the input text file
input_path = "path/to/your/input.txt"

# Read the input file
lines = spark.read.text(input_path).rdd.map(lambda r: r[0])

# Split each line into words
words = lines.flatMap(lambda line: line.split(" "))

# Map each word to a (word, 1) pair
word_pairs = words.map(lambda word: (word, 1))

# Reduce by key (word) to count occurrences
word_counts = word_pairs.reduceByKey(lambda a, b: a + b)

# Collect the results
results = word_counts.collect()

# Display the word counts
for word, count in results:
    print(f"{word}: {count}")

# Stop the Spark Session
spark.stop()
