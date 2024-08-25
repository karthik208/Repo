from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, ArrayType
from pyspark.sql.functions import col, explode

import sys
import requests
import json

#check the given string is null or empty
def check_string(s):
    if s is None or s.strip() == "" :
        raise ValueError("The provided string is null or empty.")
    return s

# Initialize Spark Session
def get_sparkses():
    sparkses = SparkSession.builder.appName("API Streaming To HDFS")\
        .config("hive.metastore.uris", "thrift://localhost:9083")\
        .enableHiveSupport()\
        .getOrCreate()
    return sparkses

def get_sparkcon():
    spark = get_sparkses()
    sc = spark.sparkContext
    sc.setLogLevel('ERROR')
    return sc


# Function to fetch data from API
def get_api_data(api_url):
    try :
        check_string(api_url)
        response = requests.get(api_url)
        # Check if the request was successful
        response.raise_for_status()
        data = response.json()
        return json.dumps(data)

    except requests.exceptions.HTTPError as http_err:
        raise Exception(f"Failed to read the data from API:{response.url};Status Code:{response.status_code};Message:{response.text}") from http_err

    except Exception as err:
        raise Exception(f"An error occurred while reading data from API: {err}") from err


def main():
    print(sys.executable)
    try:
        #api_url = "https://randomuser.me/api/?exc=login"
        api_url = "https://randomuser.me/api/"
        response_data = get_api_data(api_url)
        #print(response_data)
        spark = get_sparkses()
        sc = get_sparkcon()

        json_rdd = sc.parallelize([response_data])

        schema = StructType([
            StructField("results", ArrayType(
                StructType([
                    StructField("gender", StringType()),
                    StructField("name", StructType([
                        StructField("first", StringType()),
                        StructField("last", StringType()),
                        StructField("title", StringType())
                    ])
                    ),
                    StructField("location", StructType([
                        StructField("street", StructType([
                            StructField("number", LongType()),
                            StructField("name", StringType())
                        ])
                        )
                    ])
                    ),
                    StructField("city", StringType()),
                    StructField("state", StringType()),
                    StructField("country", StringType()),
                    StructField("postcode", LongType()),
                    StructField("coordinates", StructType([
                        StructField("latitude", StringType()),
                        StructField("longitude", StringType()),
                        StructField("timezone", StructType([
                            StructField("offset", StringType()),
                            StructField("description", StringType())
                        ])
                        )
                    ])
                    ),
                    StructField("email", StringType()),
                    StructField("login", StructType([
                        StructField("uuid", StringType()),
                        StructField("username", StringType()),
                        StructField("password", StringType()),
                        StructField("salt", StringType()),
                        StructField("md5", StringType()),
                        StructField("sha1", StringType()),
                        StructField("sha256", StringType())
                    ])
                    ),
                    StructField("dob", StructType([
                        StructField("date", StringType()),
                        StructField("age", LongType())
                    ])
                                ),
                    StructField("registered", StructType([
                        StructField("date", StringType()),
                        StructField("age", LongType())
                    ])
                    ),
                    StructField("phone", StringType()),
                    StructField("cell", StringType()),
                    StructField("id", StructType([
                        StructField("name", StringType()),
                        StructField("value", StringType())
                    ])
                    ),
                    StructField("picture", StructType([
                        StructField("large", StringType()),
                        StructField("medium", StringType()),
                        StructField("thumbnail", StringType())
                    ])
                    ),
                    StructField("nat", StringType())
                ])
            )
            ),
            StructField("info", StructType([
                StructField("seed", StringType()),
                StructField("results", LongType()),
                StructField("page", LongType()),
                StructField("version", StringType())
            ])
            )
        ])

        # Convert JSON strings to dictionaries
        dict_rdd = json_rdd.map(lambda x: json.loads(x))

        df = dict_rdd.toDF(schema)

       # print(df.collect())
        flattened_df = df.withColumn("results", explode(col("results"))).select(
            col("results.gender").alias("gender"),
            col("results.name.title").alias("title"),
            col("results.name.first").alias("first_name"),
            col("results.name.last").alias("last_name"),
            col("results.location.street.name").alias("street_name"),
            col("results.location.street.number").alias("street_number"),
            col("results.city").alias("city"),
            col("results.state").alias("state"),
            col("results.country").alias("country"),
            col("results.postcode").alias("postcode"),
            col("results.coordinates.latitude").alias("latitude"),
            col("results.coordinates.longitude").alias("longitude"),
            col("results.coordinates.timezone.offset").alias("timezone_offset"),
            col("results.coordinates.timezone.description").alias("timezone_description"),
            col("results.email").alias("email"),
            col("results.login.uuid").alias("uuid"),
            col("results.login.username").alias("login_username"),
            col("results.login.password").alias("login_password"),
            col("results.login.salt").alias("login_salt"),
            col("results.login.md5").alias("login_md5"),
            col("results.login.sha1").alias("login_sha1"),
            col("results.login.sha256").alias("login_sha256"),
            col("results.dob.date").alias("dob_date"),
            col("results.dob.age").alias("dob_age"),
            col("results.registered.date").alias("reg_date"),
            col("results.registered.age").alias("reg_age"),
            col("results.phone").alias("phone"),
            col("results.cell").alias("cell"),
            col("results.id.name").alias("id_name"),
            col("results.id.value").alias("id_value"),
            col("results.picture.large").alias("picture_large"),
            col("results.picture.medium").alias("picture_medium"),
            col("results.picture.thumbnail").alias("picture_thumbnail"),
            col("results.nat").alias("nationality"),
            col("info.seed").alias("seed"),
            col("info.page").alias("page"),
            col("info.results").alias("total_results"),
            col("info.version").alias("version")
        )

        #Show the DataFrame schema and data
        #df.printSchema()
        #print(flattened_df.show(truncate=False))

        spark.sql("CREATE DATABASE IF NOT EXISTS pulsesurvey")
        spark.sql("USE pulsesurvey")
        spark.sql("CREATE TABLE IF NOT EXISTS user_data ("
                  "txtgender STRING, txttitle STRING, txtfirstname STRING, txtlastname STRING, txtstreetname STRING, intstreetnumber INT,  txtcity STRING, txtstate STRING, txtcountry STRING, intpostalcode INT,"
                  "txtlatitude STRING, txtlongitude STRING, txttimezoneoffset STRING, txttimezonedescription STRING, txtemail STRING, txtloginuuid STRING, txtloginusername STRING, txtloginpassword STRING, txtloginsalt STRING,txtloginmd5 STRING,"
                  "txtloginsha1 STRING, txtsha256 STRING,txtdobdate STRING, intdobage INT, txtregdate STRING, txtregage STRING, txtphone STRING, txtcell STRING, txtidname STRING, txtidvalue STRING, "
                  "txtpicturelarge STRING, txtpicturemedium STRING, txtpicturethumbnail STRING,txtnationality STRING, txtinfoseed STRING, txtresults STRING, txtpage STRING, txtversion STRING) STORED AS PARQUET")

        flattened_df.write.mode("append").insertInto("pulsesurvey.user_data")

        #to view the sample data
        print(spark.sql("SELECT * FROM pulsesurvey.user_data LIMIT 5").show(truncate=False))


    except Exception as ex:
        print(f"Error occured at main function: {str(ex)}")

main()