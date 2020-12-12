import os
import findspark
findspark.init()
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.ml.image import ImageSchema
from skein import skein256, skein512, skein1024
from functools import reduce
import time
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
print('\n-------- PROGRAM START HERE --------\n')
PATH_TO_IMAGES = './bigImage/'

os.chdir(PATH_TO_IMAGES) #URL to the images folder
os.getcwd()
imageList = os.listdir()
print(f'[INFO] {len(imageList)} files found in directory')

start = time.time()
# images = sc.binaryFiles(PATH_TO_IMAGES,10).repartition(20)
hashList = []
for i in imageList:
  try:
    with open(i,'rb') as f:
      hashList.append((skein256(f.read()).hexdigest(),[i]))
  except:
    continue
images = sc.parallelize(hashList,2)

time1 = time.time()
print(images.getNumPartitions())
print(f'[INFO] Read files time: {time1-start}')
# print('[INFO] Hashing...')
hashedImages = images
# hashedImages = images.map(lambda x: (skein256(x[1]).hexdigest(),[x[0].split('/')[-1]]))
time2 = time.time()
# print(f'[INFO] Hash time: {time2-time1}')
print(f'[INFO] Successfully read {hashedImages.count()} images as (<hash_string>,[<file_name>])')


duplicatedList = sorted(hashedImages.reduceByKey(lambda a,b:a+b).collect(),key= lambda x: len(x[1]),reverse=True)

end = time.time()
print(f'[INFO] Compare time: {end-time2}')
toDelete = reduce(lambda a,b:a+b,map(lambda x: x[1][1:],duplicatedList))
print(f'[INFO] Images is duplicated is {toDelete}')
print(f'[INFO] Done time: {end-start}')


