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
        #or text_rdd = spark.sparkContext.textFile(file_path)

        #Define the word to search for
        search_word = "select"

        # Filter lines that contain the search word
        matching_lines = lines.filter(lambda line: search_word in line)

        # Count the number of matching lines
        count = matching_lines.count()

        # Display the result
        if count > 0:
            print(f"The word '{search_word}' was found {count} times in the file.")
        else:
            print(f"The word '{search_word}' was not found in the file.")

    except Exception as ex:
        print(f"Exception Occurred: {str(ex)}")

    finally:
        spark.stop()

main()







