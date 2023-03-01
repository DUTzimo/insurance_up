import time
from datetime import datetime
list1=[1,2,3,4,5,6,7]
print(len(list1))
def avg(lista):
    sum=0
    if len(lista)>1:
        for i in range(len(lista)-1):
            for j in range(len(lista)-i-1):
                print("i=",i,"j=", j)
                if lista[j] > lista[j+1]:
                    lista[j], lista[j+1] = lista[j+1], lista[j]
        for i in range(len(lista)-1):
            sum+=lista[i+1]-lista[i]
        return float(sum/(len(lista)-1))
    else:
        return lista[0]
print(avg(list1))
timestamp=round(time.time())
print(timestamp)    #1670480639
ts_datetime=datetime.fromtimestamp(timestamp)
print(ts_datetime)
