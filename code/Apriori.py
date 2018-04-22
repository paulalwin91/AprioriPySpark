import time
import csv
import random
import operator
import sys

from scipy.linalg._interpolative import id_srand
from scipy.spatial import distance
from pyspark.sql import SparkSession
from pyspark.sql import  Row


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
sc = spark.sparkContext

start_time = time.time()
supp = 250
candidate_sets = []
current_set_let = 1
 # read ['1 5', '2 3 4 5', '1 3 5 2', '2 5 1']



input_rdd =  sc.textFile("/home/paul/Documents/SOEN 691/Project/venv/data/Presentation/retail.dat")

def checkeqq(list):
    return len(set(list)) <=1


def splitstr(x):
    a = ''
    for i in x:
        a = a + ''.join(i.split(" "))
    return a.strip()


# group to counts to filter 1st candidate set - Split
fr_one = input_rdd.flatMap(lambda x:x.split(" "))
fr_two = fr_one.groupBy(lambda x:x).map(lambda x:(x[0],list(x[1])))
# key count pairs[('4', 1), ('1', 3), ('5', 4), ('3', 2), ('2', 3)]
fr_three = fr_two.map(lambda x: (x[0],len(x[1])))
# remove less than thresho;d
fr_four = fr_three.filter(lambda x: x[1] >= supp)
fr_five = fr_four.map(lambda x:str(x[0])).filter(lambda x: x.strip() != '')

# populate 1st guys
for item in fr_five.collect():
    candidate_sets.append(item)

# print("First level Frequent set - " + str(candidate_sets))

if candidate_sets.__len__() > 0:
    current_candidate_set = sc.parallelize(candidate_sets)
    # current_candidate_set = current_candidate_set.map(lambda x: str(x))
while True:
  try:
    # print("\n\n\nInteration for set lengths " + str(current_set_let+1))
    # Find the current_set_let - length pais for this iteration
    current_set_let = current_set_let + 1
    apr_one = current_candidate_set.cartesian(current_candidate_set.flatMap(lambda x: x.split(" ")).distinct())  #('1', '1'), ('1', '5')
    apr_two = apr_one.map(lambda x: list(x)).map(lambda x: splitstr(x)).map(lambda x: set(x)).filter(lambda x: len(x) == current_set_let)
    apr_three = apr_two.map(lambda x: list(x)).map(lambda x: ' '.join(str(e) for e in x)).distinct()
    # print("Candidate sets of lenths " + str(current_set_let) + " is -" + str(apr_three.collect()))

    # Check this count and get a filteret count.
    input_rdd_Set = input_rdd.map(lambda x: x.split()).map(set)
    current_candidate_set_Set = apr_three.map(lambda x: x.split()).map(set)
    apr_four = current_candidate_set_Set.cartesian(input_rdd_Set).filter(lambda l: set(l[0]).issubset(set(l[1])))
    apr_five = apr_four.map(lambda x: (' '.join(list(x[0])),1)).reduceByKey(lambda x,y: x+ y)
    apr_six = apr_five.filter(lambda x: x[1] >= supp).map(lambda x:x[0])
    ap_six_val = apr_six.collect()

    # print("Frequent sets for lengths " + str(current_set_let) + " is -" + str(ap_six_val))

    if len(ap_six_val) > 0:
        for candidate in ap_six_val:
            candidate_sets.append(candidate)
        current_candidate_set = apr_six
    else:
        break
  except Exception as e:
      print(e)
      break
#
# print("\n\nSupport is " + str(supp))
# print("So the frequent items are " + str(candidate_sets))
print("Count of the frequent items are " + str(len(candidate_sets)))
print("Time in Mili Seconds " + str((time.time() - start_time)* 1000))
# print("Time in mili seconds" + str(current_milli_time_End - current_milli_time_Start))


























