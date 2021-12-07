#!/usr/bin/env python
# coding: utf-8

# In[2]:


import socket
import json
import pandas as pd
TCP_IP = "localhost"
TCP_PORT = 6100
soc=socket.socket(socket.AF_INET, socket.SOCK_STREAM)

try:
    soc.connect((TCP_IP, TCP_PORT))
    while True:
        json_recv=soc.recv(2048).decode()
        a_json=json.loads(json_recv)
        df=pd.DataFrame.from_dict(a_json,orient="index")
        prepro(df)
        break
        print('.......................................................................................................')
except Exception as e:
    print(e)

def prepro(df):
    for i in range(0,len(df['feature1'])):
        clean=list(df["feature1"][i].split(" "))
        print(clean)
        for j in clean:
            if(j.startswith("@")):
                clean.remove(j)
                continue
            if(j.startswith("#")):
                clean.remove(j)
                continue
            if(j.startswith("http://")):
                clean.remove(j)
                continue
        
            if(any(map(str.isdigit, j))):
                clean.remove(j)
                continue
        df['feature1'][i] = clean
        print(clean)




from pyspark.sql import SparkSession
spark = SparkSession.builder.appName(
  "pandas to spark").getOrCreate()


df_spark = spark.createDataFrame(df)



from pyspark.ml.feature import StopWordsRemover

without_stop = StopWordsRemover(inputCol="feature1", outputCol="filtered")
without_stop.transform(df_spark).show()


from pyspark.ml.feature import CountVectorizer
# fit a CountVectorizerModel from the corpus.
cv = CountVectorizer(inputCol="filtered", outputCol="final", vocabSize=1000, minDF=2.0)
model = cv.fit(without_stop.transform(df_spark))

result = model.transform(without_stop.transform(df_spark))



result.show()




