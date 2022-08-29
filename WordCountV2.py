import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
from pyspark import SparkContext
#commonlines
sc = SparkContext("local[*]", "wordcount")
sc.setLogLevel("INFO")
data = sc.textFile("/Users/Admin/Desktop/Dataset.txt")

words = data.flatMap(lambda x: x.split(" "))
word_counts = words.map(lambda x: (x.lower()))

finalCount = word_counts.countByValue()

print(finalCount)
sys.stdin.readline()