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
word_counts = words.map(lambda x: (x.lower(),1))

finalCount = word_counts.reduceByKey(lambda x, y: x+y).map(lambda x: (x[1],x[0]))
result = finalCount.sortByKey(False).map(lambda x: (x[1],x[0])).collect()
for a in result:
    print(a)
sys.stdin.readline()