from pyspark.sql import SparkSession

def get_sparkses():
    # Initialize Spark Session
    sparkses = SparkSession.builder.appName("Word Count").getOrCreate()
    return sparkses

def main():
    spark = get_sparkses()

    try:
        # Path to the input text file
        input_path = "file:///home/hduser/Desktop/query.txt"

        # Read the input file
        lines = spark.read.text(input_path).rdd.map(lambda r: r[0])

        # Split each line into words
        words = lines.flatMap(lambda line: line.split(" "))

        # Map each word to a (word, 1) pair
        word_pairs = words.map(lambda word: (word, 1))
        #print(word_pairs.collect())

        # Reduce by key (word) to count occurrences
        word_counts = word_pairs.reduceByKey(lambda a, b: a + b)

        # Collect the results
        results = word_counts.collect()
        #print(results)

        # Display the word counts
        total_count = 0
        for word, count in results:
            # print(f"{word}: {count}")
            total_count += count

        print(f"The total word count from the file {input_path} is: {total_count}")

    except Exception as ex:
        print(f"Exception Occurred: {str(ex)}")

    finally:
        spark.stop()

main()







