from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests
from selenium import webdriver
from bs4 import BeautifulSoup
import time

spark = SparkSession.builder.appName("try").master("local").getOrCreate()

# df = spark.read.option("header", True).option("inferSchema", True).csv("trial.csv")
# df = df.withColumn('City', lit('Pune'))
# df.show()

url = 'https://www.naukri.com/java-developer-jobs-in-pune?k=java%20developer&l=pune'
# https://enterprise.naukri.com/recruit/login?msg=TO&URL=https%3A%2F%2Fresdex.naukri.com%2Fv2%2Fsearch%2FpageChange%3FSRCHTYPE%3Dadv%26sid%3D4875578459

page = requests.get(url)
# print(page.text)

driver = webdriver.Chrome('../chromedriver_win32/chromedriver.exe')
driver.get(url)

time.sleep(3)

soup = BeautifulSoup(driver.page_source, 'html.parser')

# print(soup.prettify())

driver.close()

schema = StructType([
    StructField('Title', StringType()),
    StructField('URL', StringType())
])
df = spark.createDataFrame([('','')], schema)

results = soup.find(class_='list')
job_elems = results.find_all('article',class_='jobTuple bgWhite br4 mb-8')
job_list = []

for job_elem in job_elems:
    URL=job_elem.find('a', class_='title fw500 ellipsis').get('href')
    Title = job_elem.find('a', class_='title fw500 ellipsis')
    job_list = job_list + [(Title.text.strip(), URL.strip())]

new_row = spark.createDataFrame(job_list, schema)
df = df.union(new_row)
df.show()
print(df.count())