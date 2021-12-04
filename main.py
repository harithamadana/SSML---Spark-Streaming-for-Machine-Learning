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
        print(df)
        print('.......................................................................................................')
        
except Exception as e:
    print(e)

