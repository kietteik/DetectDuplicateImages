PATH_TO_IMAGES = './images/'
NUM_PAR = 4
import os
import findspark
findspark.init()
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext, Row
from skein import skein256, skein512, skein1024
from functools import reduce
import time
from PIL import Image
import numpy as np
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


def binary_array_to_hex(arr):
	bit_string = ''.join(str(b) for b in 1 * arr.flatten())
	width = int(np.ceil(len(bit_string)/4))
	return '{:0>{width}x}'.format(int(bit_string, 2), width=width)

def dhash(imagePath, hash_size=8):
    try:
        image = Image.open(imagePath)
        if hash_size < 2:
            raise ValueError("Hash size must be greater than or equal to 2")

        image = image.convert("L").resize((hash_size + 1, hash_size), Image.ANTIALIAS)
        pixels = np.asarray(image)
        diff = pixels[:, 1:] > pixels[:, :-1]
        return binary_array_to_hex(diff)
    except:
        return None


print('\n-------- PROGRAM START HERE --------\n')

os.chdir(PATH_TO_IMAGES) #URL to the images folder
os.getcwd()
imageList = os.listdir()
print(f'[INFO] {len(imageList)} files found in directory')

imageList = sc.parallelize(imageList,NUM_PAR)
start = time.time()

print(f'[INFO] Detecting...')
hashList = imageList.map(lambda x: (dhash(PATH_TO_IMAGES+x),[x]),True)
hashList = hashList.filter(lambda x: x[0]!=None)
duplicatedList = sorted(hashList.reduceByKey(lambda a,b:a+b).collect(),key= lambda x: len(x[1]),reverse=True)
duplicatedList = filter(lambda x: len(x[1])>1,duplicatedList)
print(duplicatedList)
print(f'[INFO] Successfully read {hashList.count()} images as (<hash_string>,[<file_name>])')

end = time.time()
imagesDuplicated = reduce(lambda a,b:a+[b],map(lambda x: x[1],duplicatedList),[])
print(imagesDuplicated)
print(f'[INFO] Group of images duplicated: {len(imagesDuplicated)} group found')
print(f'[INFO] Total images duplicated: {len(np.concatenate(imagesDuplicated))} found')
print(f'[INFO] Done time: {end-start}')


