from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import Dataframes
from pyspark.sql.types import IntegerType,BooleanType,DateType
from pyspark.sql import functions as F
from os.path import abspath
import anaytics
import logging
logging.basicConfig(level=logging.INFO)

warehouse_location = abspath('spark-warehouse')

spark = SparkSession \
        .builder.master("spark://localhost:7077") \
        .appName("Spark_project") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .enableHiveSupport() \
        .getOrCreate()



#	Find the list of the oldest released movies.
def oldest_releases_movies_1():
    movies = Dataframes.movies()
    logging.info("The list of the oldest released movies")
    spark.sql('select * from movies').show()


#3.	How many movies are released each year
def release_each_year():
    movies = Dataframes.movies()
    logging.info("The list of the oldest released movies")
    spark.sql('select year , count(*) as movie_count   from movies group by year').show()

#4.	How many number of movies are there for each rating
def movie_rating():
    ratings = Dataframes.ratings()
    logging.info("number of movies are there for each rating")
    spark.sql('select  Rating , count(*) as movie_count from ratings group by Rating order by Rating').show()

#5.	How many users have rated each movie?
def user_count():
    ratings = Dataframes.ratings()
    logging.info("users have rated each movie")
    spark.sql('select  MovieID , count(UserID) as user_count from ratings group by MovieID').show()

#6.	What is the total rating for each movie
def total_ratings():
    ratings = Dataframes.ratings()
    movies = Dataframes.movies()
    logging.info("total rating for each movie")
    spark.sql('select MovieID , sum(Rating) as total_ratings from ratings group by MovieID').show()


#7.	What is the average rating for each movie
def avg_ratings():
    ratings = Dataframes.ratings()
    logging.info('average rating for each movie')
    spark.sql('select MovieID , avg(Rating) as avg_ratings from ratings group by MovieID').show()

 
    

if __name__ == '__main__':
    
    Dataframes.movies()
    Dataframes.ratings()
    Dataframes.users()
    Dataframes.occupation()
    anaytics.most_view_movies()
    anaytics.genres()
    anaytics.movie_count_genres()
    anaytics.movies_start_with()
    oldest_releases_movies_1()
    # release_each_year() -----> have to check
    movie_rating()
    user_count()
    total_ratings()
    avg_ratings()



     

