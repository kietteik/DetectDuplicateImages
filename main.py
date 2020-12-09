import os
import findspark
findspark.init()
from pyspark import SparkContext
from skein import skein256, skein512, skein1024
from functools import reduce
sc = SparkContext.getOrCreate()
print('\n-------- PROGRAM START HERE --------\n')
PATH_TO_IMAGES = './bigImage/'

os.chdir(PATH_TO_IMAGES) #URL to the images folder
os.getcwd()
imageList = os.listdir()
print(f'{len(imageList)} files found in directory')

hashList = []
for i in imageList:
  try:
    with open(i,'rb') as f:
      hashList.append((skein256(f.read()).hexdigest(),[i]))
  except:
    continue
print(f'Successfully read {len(hashList)} images as (<hash_string>,[<file_name>])')

imgRdd = sc.parallelize(hashList)
duplicatedList = sorted(imgRdd.reduceByKey(lambda a,b:a+b).collect(),key= lambda x: len(x[1]),reverse=True)
toDelete = reduce(lambda a,b:a+b,map(lambda x: x[1][1:],duplicatedList))
print(f'Images is duplicated is {toDelete}')
