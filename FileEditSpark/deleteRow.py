from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import os


local = True

if local:
    input_file = "../TinyData/action/action_data_20161.csv000"
else:
    behanceDataDir = "wasb://testing@adobedatascience.blob.core.windows.net/behance/data"
    input_file = "wasb://testing@adobedatascience.blob.core.windows.net/behance/data/action/action_data.csv"
    filename = "published_project_data.csv"
    parquetFile = ""




if __name__ == "__main__":

    sc = SparkContext(appName="deleteRow")
    sc.setLogLevel("ERROR")

    lines = sc.textFile(input_file).map(lambda x: x.split(',')).filter(lambda x: x[6]!='ProjectView')
    print lines.count()
    lines.cache()
    print lines.count()
    print lines.take(10)
    # counts = lines.map(lambda x: x.split(','))
