import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
from pyspark import SparkContext
sc = SparkContext("local[*]","movie-rating-analysis")
lines = sc.textFile("/Users/Admin/Documents/Spark/moviedata.data")

