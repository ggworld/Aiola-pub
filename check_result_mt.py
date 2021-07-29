import threading 
import time
import awswrangler as wa
import logging
import boto3
import json
import sys
logging.getLogger().setLevel(logging.DEBUG)

aws_path = 's3://aiola-374014573610-us-east-1-data/tier2/quotes/'

log_file = '/home/ec2-user/workspaces/g-test1/mylog/find.log'
max_p = 20
def proc_file(file_name,key):
    try:

        tier2_bucket=file_name.split('/')[2]
        tier2_prefix = 'tier2/quotes/'
        fname=file_name.split('/')[-1]
        # print(tier2_bucket,tier2_prefix,fname)
        s3 = boto3.resource('s3')
        obj = s3.Object(tier2_bucket,tier2_prefix+fname)
        body = obj.get()['Body'].read().decode('utf-8') 
        # print(body)
        df = json.loads(body)
        if df ['quote_name'][0]==key: 
            print('=============>',file_name)
            with open(log_file,'a') as mylf:
                mylf.write(file_name+'\n')
    except :
        logging.debug(f'Problems with file {file_name}')
        with open(log_file,'a') as mylf:
                mylf.write(f'Problems with file {file_name} \n')

        
    return True

if __name__ == '__main__':
    try:
        if sys.argv[1]:
            key_to_search= sys.argv[1]  
            print(key_to_search)
    except:
           key_to_search = 20301571     
    try:
        if sys.argv[2]:
            aws_path='s3://aiola-374014573610-us-east-1-data/tier2/bad/quotes/'
    except:
           key_to_search = 20301571     
        
        

    start = time.time()
    l_p=[]
    fl = wa.s3.list_objects(aws_path)
    for n,i in enumerate(fl):
        p=threading.Thread(target=proc_file,args=(i,key_to_search))
        if n%10 == 0: print(n)
        l_p.append(p)
        p.start()
        if len(l_p)>=max_p:
            print(f'waiting for {max_p}')
            while all([x.is_alive() for x in l_p]):
                time.sleep(5)
            for p_name in l_p:  
                if not p_name.is_alive():
                    l_p.remove(p_name)
    while any([x.is_alive() for x in l_p]):
        time.sleep(5)
        for p_name in l_p:
            if not p_name.is_alive():
                l_p.remove(p_name)
    print('Total time: ' ,time.time()-start)