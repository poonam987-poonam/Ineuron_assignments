from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import Dataframes
from pyspark.sql.types import IntegerType,BooleanType,DateType
from pyspark.sql import functions as F
from os.path import abspath
import logging
logging.basicConfig(level=logging.INFO)


warehouse_location = abspath('spark-warehouse')

spark = SparkSession \
        .builder.master("spark://localhost:7077") \
        .appName("Spark_project") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .enableHiveSupport() \
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark.sparkContext.setLogLevel("INFO")
spark.sparkContext.setLogLevel("WARN")

#What are the top 10 most viewed movies?

def most_view_movies():
    ratings = Dataframes.ratings()
    movies = Dataframes.movies()
    logging.info("top 10 most viewed movies")
    movie_count = ratings.groupBy('MovieID') \
             .agg(count("MovieID").alias("movie_count")) \
             .orderBy(col("movie_count").desc()).show(10)

##What are the distinct list of genres available?
def genres():
    movies = Dataframes.movies()
    movies.select('genres').distinct().show()
    
#How many movies for each genre?
def movie_count_genres():
    movies = Dataframes.movies()
    movies.groupBy("genres").count().orderBy("count" , ascending = False).show()

# #List the latest released movies
# def latest_release_movies():
#     movies = Dataframes.movies()
#     df1 = movies.withColumn('Title_2', split(movies.Title, "\(").getItem(0)) \
#                 .withColumn('year', split(movies.Title, "\(").getItem(1)) \
#                 .withColumn("year", F.regexp_replace("year", "\)", "")) 
             
#     df2 =  df1.drop("Title" , "Genres" , "MovieID")
#     df3  = df2.withColumn("year",df2.year.cast(IntegerType())).orderBy("year" , ascending=False ).show()
#     return df2

#How many movies are starting with numbers or letters (Example: Starting with 1/2/3../A/B/C..Z)?
def movies_start_with():
    movies = Dataframes.movies()
    movies_start_with_alphabate = movies.filter(F.col("Title").rlike("^[A-Z]|[a-z]"))
    logging.info('Number of movies which start with alphabate : ',movies_start_with_alphabate.count())
    movies_strats_with_numbers=movies.filter(col("title").rlike("^[0-9]"))
    logging.info('Number of movies which start with number: ',movies_strats_with_numbers.count())

             





