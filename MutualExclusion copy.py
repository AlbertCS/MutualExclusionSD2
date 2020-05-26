import pywren_ibm_cloud as pywren
import ibm_boto3
import ibm_botocore
import json
import time
import threading
import datetime
import pytz


Bucket_name= 'phyto.sd'
N_SLAVES = 50
#Temps a esperar
t = 3


def myFunc(e):
  return e['LastModified']

def master(id, x, ibm_cos):
    write_permission_list = []
    cua_pwrite = []
    monitorBucket = False

    # File with the id's of the executed proces
    serialized = json.dumps(cua_pwrite) 
    ibm_cos.put_object(Bucket=Bucket_name, Key='result.json', Body=serialized)
    # Save the initial datetime
    result = ibm_cos.get_object(Bucket=Bucket_name, Key='result.json')
    Last_Modified_Result = result["LastModified"]

    while(len(write_permission_list) < N_SLAVES):

        # 1. monitor COS bucket each X seconds
        while(monitorBucket == False):
            # Esperem
            time.sleep(t)
            # Agafem els objectes del cos
            content=ibm_cos.list_objects_v2(Bucket=Bucket_name)
            # Contem els objectes del cos
            leng = content["KeyCount"]
            # Mentre no tinguem tots els p_write, busquem els p_write al cos
            if(len(cua_pwrite) < N_SLAVES):
                for i in range(leng):
                    objecte = content["Contents"][i]
                    nom = objecte["Key"]
                    if(nom[0:8] == "p_write_"):
                        cua_pwrite.append(objecte)
            # Quan tinguem tots els p_write sortim
            else:
                monitorBucket = True 

            # 3. Order objects by time of creation
            cua_pwrite.sort(key=myFunc)

        # 4. Pop first object of the list "p_write_{id}" 
        objecte = cua_pwrite.pop()
        # 5. Write empty "write_{id}" object into COS
        ibm_cos.put_object(Bucket=Bucket_name, Key=objecte["Key"][2:])
        # 6. Delete from COS "p_write_{id}", save {id} in write_permission_list
        ibm_cos.delete_object(Bucket=Bucket_name, Key=objecte["Key"])
        # Guardem a la llista el id que hem donat permis
        write_permission_list.append(objecte["Key"][8:])

        # 7. Monitor "result.json" object each X seconds until it is updated
        fileUpdate = False
        while (fileUpdate == False):
            # Esperem
            time.sleep(t)
            # Agafem el fitxer results.json
            result = ibm_cos.get_object(Bucket=Bucket_name, Key='result.json')
            # Agafem la llista
            contingut = result["Body"].read()
            # Mirem si ha estat modificat despres
            if(Last_Modified_Result <= result["LastModified"]):
                Last_Modified_Result = result["LastModified"]
                fileUpdate = True
            # Si ja tenim la llista amb tots els slaves sortim
            elif(N_SLAVES == len(contingut)):
                fileUpdate = True
            

        # 8. Delete from COS “write_{id}”
        ibm_cos.delete_object(Bucket=Bucket_name, Key=objecte["Key"][2:])
        # 9. Back to step 1 until no "p_write_{id}" objects in the bucket
    
    return write_permission_list        

def slave(id, x, ibm_cos):
    # 1. Write empty "p_write_{id}" object into COS
    ibm_cos.put_object(Bucket=Bucket_name, Key='p_write_'+str(id))
    # 2. Monitor COS bucket each X seconds until it finds a file called "write_{id}"
    fileFound = False
    while (fileFound == False):
        # Esperem
        time.sleep(t)
        # Agafem els fitxers del cos
        content=ibm_cos.list_objects_v2(Bucket=Bucket_name)
        # Busquem el numero de fitxers
        leng = content["KeyCount"]
        for i in range(leng):
            objecte = content["Contents"][i]
            nom = objecte["Key"]
            if(nom == ('write_'+str(id))):
                fileFound = True
    
    # 3. If write_{id} is in COS: get result.txt, append {id}, and put back to COS result.txt
    result_serialized = ibm_cos.get_object(Bucket=Bucket_name, Key='result.json')["Body"].read()
    # Deserialitzem el fitxer result.txt
    result = json.loads(result_serialized)
    # Afegim la id
    result.append(id)
    # Serialitzem el fitxer
    serialized = json.dumps(result)
    # Penjem el fitxer al cos de nou
    ibm_cos.put_object(Bucket=Bucket_name, Key='result.json', Body=serialized)
    # 4. Finish# No need to return anything


if __name__ == '__main__':     
    
    pw = pywren.ibm_cf_executor()     
    pw.map(slave, range(N_SLAVES))
    pw.call_async(master, 0)
    write_permission_list = pw.get_result()    
    
    pw1 = pywren.ibm_cf_executor()
    ibm_cos = pw1.internal_storage.get_client()
    resultSerialized = ibm_cos.get_object(Bucket=Bucket_name, Key='result.json')['Body'].read()
    result = json.loads(resultSerialized)
    
    write_permission_list = list(map(int, write_permission_list))

    if(write_permission_list == result):
        print("Llistes iguals, ha funcionat correctament")
    else:
        print("Llistes diferents, no ha funcionat")
    
    print(write_permission_list)
    print(result)

    