import socket
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml.feature import StopWordsRemover

def prepro(df):             # used for splitting string to individual words in a list and remove username,urls and numbers
    for i in range(0,len(df['feature1'])):
        clean=list(df["feature1"][i].split(" "))
        #print(clean)
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
        #print(clean)
    
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
        
        spark = SparkSession.builder.appName("pandas to spark").getOrCreate()
        df_spark = spark.createDataFrame(df)       #Convert pandas data frame to spark df
        

        without_stop = StopWordsRemover(inputCol="feature1", outputCol="filtered")
        without_stop.transform(df_spark).show(truncate=False)       #Results in the removal of stop words 
        print('.......................................................................................................')
        
except Exception as e:
    print(e)
