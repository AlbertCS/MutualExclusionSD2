import pywren_ibm_cloud as pywren
import ibm_boto3
import ibm_botocore
import json
import time
import threading
import datetime
import pytz

# Variable amb el nom del fitxer de results
keyResult = 'result.json'
# Variable amb el nom del bucket
Bucket_name= 'phyto.sd'
# Numero de slaves
N_SLAVES = 5
# Temps a esperar master
t = 2
# Temps espera slave
ts = 0.5

# Funcio per ordenar per data de creació els fitxers p_write
def myFunc(e):
  return e['LastModified']

# Funció que realitza el master
def master(id, x, ibm_cos):
    # Creació de la llista amb els ids del slaves amb permis
    write_permission_list = []
    # Llista amb els noms del fitxers p_write
    cua_pwrite = []
    # Variable per controlar la monitorització del bucket
    monitorBucket = False

    # File with the id's of the executed proces
    serialized = json.dumps(cua_pwrite) 
    ibm_cos.put_object(Bucket=Bucket_name, Key=keyResult, Body=serialized)
    # Save the initial datetime
    result = ibm_cos.get_object(Bucket=Bucket_name, Key=keyResult)
    Last_Modified_Result = result["LastModified"]

    # Mentre encara quedin slaves per donar permis
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
            result = ibm_cos.get_object(Bucket=Bucket_name, Key=keyResult)
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

# Funció que realitza el slave
def slave(id, x, ibm_cos):
    # 1. Write empty "p_write_{id}" object into COS
    ibm_cos.put_object(Bucket=Bucket_name, Key='p_write_'+str(id))
    # 2. Monitor COS bucket each X seconds until it finds a file called "write_{id}"
    fileFound = False
    while (fileFound == False):
        # Esperem
        time.sleep(ts)
        # Agafem els fitxers del cos
        content=ibm_cos.list_objects_v2(Bucket=Bucket_name)
        # Busquem el numero de fitxers
        leng = content["KeyCount"]
        # Busquem si el slave te permis
        for i in range(leng):
            objecte = content["Contents"][i]
            nom = objecte["Key"]
            if(nom == ('write_'+str(id))):
                fileFound = True
    
    # 3. If write_{id} is in COS: get result.txt, append {id}, and put back to COS result.txt
    result_serialized = ibm_cos.get_object(Bucket=Bucket_name, Key=keyResult)["Body"].read()
    # Deserialitzem el fitxer result.txt
    result = json.loads(result_serialized)
    # Afegim la id
    result.append(id)
    # Serialitzem el fitxer
    serialized = json.dumps(result)
    # Penjem el fitxer al cos de nou
    ibm_cos.put_object(Bucket=Bucket_name, Key=keyResult, Body=serialized)
    # 4. Finish# No need to return anything

# Funció per esborrar el bucket
def esborra(z, ibm_cos):
    content=ibm_cos.list_objects_v2(Bucket=Bucket_name)
    leng = content["KeyCount"]
    for i in range(leng):
        try:
            ibm_cos.delete_object(Bucket=Bucket_name, Key=content["Contents"][i]["Key"])
        finally:
            pass  
        

# Funció main
if __name__ == '__main__':     
    
    # Creem el executor del cloud 
    pw = pywren.ibm_cf_executor()
    # Creem els slaves amb la funcio map 
    pw.map(slave, range(N_SLAVES))
    # Invoquem al proces master perque controli
    pw.call_async(master, 0)
    # Retorna la llista amb els id dels processos amb permis
    write_permission_list = pw.get_result()    
    
    # Creem el executor del cloud
    pw1 = pywren.ibm_cf_executor()
    # Ens descarregem del bucket el results.json
    ibm_cos = pw1.internal_storage.get_client()
    resultSerialized = ibm_cos.get_object(Bucket=Bucket_name, Key=keyResult)['Body'].read()
    # Descerialitzem el fitxer results.json
    result = json.loads(resultSerialized)
    
    # Transformem la write_permision_list de string a int
    write_permission_list = list(map(int, write_permission_list))

    # Comparem les dues llistes per saber si ha funcionat
    if(write_permission_list == result):
        print("Llistes iguals, ha funcionat correctament")
    else:
        print("Llistes diferents, no ha funcionat")
    
    # Imprimim les dues llistes per mirar el contingut
    print(write_permission_list)
    print(result)
    # Esborrem els elements del bucket
    pw1.call_async(esborra, 1)



    