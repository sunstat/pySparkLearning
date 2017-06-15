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


# no return value
def replaceCol(x, scr_str, des_str, col_ind):
    x[col_ind] = x[col_ind].replace(scr_str, des_str)
    return x






if __name__ == "__main__":


    sc = SparkContext(appName="replaceRow")
    sc.setLogLevel("ERROR")

    lines = sc.textFile(input_file).map(lambda x: x.split(',')).map(lambda x: replaceCol(x, "ProjectView", "V", 6))
    print lines.count()
    lines.cache()
    print lines.take(5)

    #lines.saveAsTextFile('dataNoView.csv')
    # counts = lines.map(lambda x: x.split(','))
