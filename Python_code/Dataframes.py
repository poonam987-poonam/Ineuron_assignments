from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from os.path import abspath
import logging
logging.basicConfig(level=logging.INFO)

from pyspark.sql.types import StructType,StructField, StringType, IntegerType


warehouse_location = abspath('spark-warehouse')

spark = SparkSession.builder.master("spark://localhost:7077").appName("demo").config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport().getOrCreate()

#1.	Prepare Movies data: Extracting the Year and Genre from the Text
def movies():

    schema = StructType([ \
    StructField("MovieID",IntegerType(),True), \
    StructField("Title",StringType(),True), \
    StructField("Genres",StringType(),True), 
    ])

    movies = spark.read.format("csv")\
    .option("header", False) \
    .option("delimiter", "::") \
    .schema(schema) \
    .load("hdfs:///input/movies.dat")

    movies = movies.withColumn('Title_1', split(movies.Title, "\(").getItem(0)) \
                .withColumn('year', split(movies.Title, "\(").getItem(1)) \
                .withColumn("year", F.regexp_replace("year", "\)", "")) 

    movies = movies.drop("Title")
    movies = movies.withColumn('genres',split(movies.Genres, '\|'))
    movies = movies.select(col('MovieID'), col("Title_1"), col('year'),explode(col("genres")).alias("genres"))
    #create view
    movies.createOrReplaceTempView("movies")
    logging.info("creating a movies table without DDL")
    movies.write.mode('overwrite').saveAsTable("movies")
    movies.show()
    return movies
    
    
    

#3.	Prepare Ratings data: Programmatically specifying a schema for the data frame
def ratings():

    schema = StructType([ \
    StructField("UserID",IntegerType(),True),\
    StructField("MovieID" , IntegerType() , True),\
    StructField("Rating", IntegerType(), True),\
    StructField("Timestamp" , IntegerType() , True)
    ])

    ratings = spark.read.format("csv") \
    .option("header", False)\
    .schema(schema) \
    .option("delimiter", "::") \
    .load("hdfs:///input/ratings.dat")

    #create view
    ratings.createOrReplaceTempView("ratings")
    logging.info("creating a ratings table without DDL")
    ratings.write.mode('overwrite').saveAsTable("ratings")
    ratings.show()
    return ratings
    
#2.	Prepare Users data: Loading a double delimited csv file   
def users():
    df3 = spark.read.format("csv").option("header", False).option("inferschema", True).option("delimiter", "::").load("hdfs:///input/users.dat")
    users = df3.withColumnRenamed("_c0", "UserID") \
           .withColumnRenamed("_c1", "Gender") \
           .withColumnRenamed("_c2", "Age") \
           .withColumnRenamed("_c3", "Occupation") \
           .withColumnRenamed("_c4", "Zip-code")

    #create view
    users.createOrReplaceTempView("users")
    logging.info("creating a users table without DDL")
    users.write.mode('overwrite').saveAsTable("users")
    users.show()
    return users

def occupation():
    df4 = spark.read.format("csv") \
      .option("header", False) \
      .option("quote", "") \
      .option("delimiter", ",") \
      .option("ignoreLeadingWhiteSpace", True).load("hdfs:///input/occupation.txt")

    Occupation = df4.withColumnRenamed("_c0", "occupation_id").withColumnRenamed("_c1", "occupation_name")
    Occupation.show()

# def broadcast_variables():
#     movies()
#     users()
#     ratings()
#     new=ratings.join(movies,ratings.MovieID == movies.MovieID,"inner").show()

    # df1 = movies.join(ratings , movies.MovieID == ratings.MovieID, "inner").show()
    


  