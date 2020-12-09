import os
import findspark
findspark.init()
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.ml.image import ImageSchema
from skein import skein256, skein512, skein1024
from functools import reduce
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
print('\n-------- PROGRAM START HERE --------\n')
PATH_TO_IMAGES = './images/'

os.chdir(PATH_TO_IMAGES) #URL to the images folder
os.getcwd()
imageList = os.listdir()
print(f'{len(imageList)} files found in directory')

images = sc.binaryFiles(PATH_TO_IMAGES)
hashedImages = images.map(lambda x: (skein256(x[1]).hexdigest(),[x[0].split('/')[-1]]))
print(f'Successfully read {len(hashedImages.collect())} images as (<hash_string>,[<file_name>])')

duplicatedList = sorted(hashedImages.reduceByKey(lambda a,b:a+b).collect(),key= lambda x: len(x[1]),reverse=True)
toDelete = reduce(lambda a,b:a+b,map(lambda x: x[1][1:],duplicatedList))
print(f'Images is duplicated is {toDelete}')
# imgRdd = sc.parallelize(hashList)
# duplicatedList = sorted(imgRdd.reduceByKey(lambda a,b:a+b).collect(),key= lambda x: len(x[1]),reverse=True)
# toDelete = reduce(lambda a,b:a+b,map(lambda x: x[1][1:],duplicatedList))
# print(f'Images is duplicated is {toDelete}')
