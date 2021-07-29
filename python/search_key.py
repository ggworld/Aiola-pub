#!/usr/bin/env python3
import threading 
import time
import awswrangler as wa
import logging
import boto3
import json
import sys
logging.getLogger().setLevel(logging.ERROR)


import argparse, sys,logging
parser=argparse.ArgumentParser()

parser.add_argument('--bucket', help='S3 path',default='s3://aiola-374014573610-us-east-1-tier1/sap/')
parser.add_argument('--key', help='what to find',type=int)
parser.add_argument('--start', help='where to start')
parser.add_argument('--stop', help='where to stop')
parser.add_argument('--maxn', help='max files to search',type=int)
parser.add_argument('--method', help='Search methode can be df,file',default='file')
parser.add_argument('--max_p', help='Search methode can be df,file',type=int,default=20)
parser.add_argument('--debug', help='Debug any value to debug level',default=False)
parser.add_argument('--all', help='Debug any value to debug level',default=False)
parser.add_argument('--onday', help='Debug any value to debug level',default=False)
parser.add_argument('--log', help='Debug any value to debug level',default=False)


LOCATIONS = [{'loc':'s3://<B1>/tier2/quotes/','type':'df'},
            {'loc':'s3://<B2>tier2/bad/quotes/','type':'df'},
            {'loc':'s3://<B3>/sap/','type':'file'}]


class TestFailed(Exception):
    def __init__(self, m):
        self.message = m
    def __str__(self):
        return self.message



args=parser.parse_args()
if args.debug:
    print('ehat ?????')
    exit()
    logging.getLogger().setLevel(logging.DEBUG)
logging.debug(args)

if not args.bucket:
    aws_path = 's3://<B3>/sap/'
else:
    aws_path = args.bucket
if args.log:
    log_file=args.log   
else:
    log_file = '/home/ec2-user/workspaces/g-test1/mylog/search_log.log'
max_p = args.max_p

def find_log(file_name,key):
    print('=============>',file_name)
    with open(log_file,'a') as mylf:
        mylf.write(f'{key}->{file_name} \n')
def proc_file(file_name,key,scan_type=args.method):
    logging.debug(file_name)
    # print(body)
    # logging.debug(args.method)
    if scan_type== 'df':
        try:
            df = wa.s3.read_json(file_name)
            if key in set(df ['quote_name']): 
                find_log(file_name,key)
        except :
            logging.error(f'Problems with file {file_name}')
            with open(log_file,'a') as mylf:
                    mylf.write(f'Problems with file {file_name} \n')
            raise

    elif scan_type =='file':
        file_name = file_name.replace('s3://','')
        bucket=file_name.split('/')[0]
        # tier2_prefix = 'tier2/quotes/'
        fname=file_name.replace(f'{bucket}/','')
        # thread safe connection 
        session = boto3.session.Session()
        s3 = session.resource('s3')
        obj = s3.Object(bucket,fname)
        body=obj.get()['Body'].read().decode('utf-8')
        if str(key) in body:
            find_log(file_name,key)
    else:
        raise TestFailed('No such search method')
        
    return True



def scan_location(fl,key_to_search,scan_type=args.method):
    for n,i in enumerate(fl):
        p=threading.Thread(target=proc_file,args=(i,key_to_search,scan_type))
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
    
if __name__ == '__main__':
    if args.all:
        for file_location in LOCATIONS:
            key_to_search=args.key
            start = time.time()
            l_p=[]
            if args.onday:
                file_location['loc']+=args.onday
            fl = wa.s3.list_objects(file_location['loc'])
            fl=list(set(fl))
            if args.maxn:
                fl=fl[:args.maxn]
            logging.debug(fl)
            print(file_location['loc'],len(fl))
            # continue
            scan_location(fl,key_to_search,file_location['type'])
    else:
        key_to_search=args.key
        start = time.time()
        l_p=[]
        fl = wa.s3.list_objects(args.bucket)
        fl=list(set(fl))
        print(args.maxn)
        if args.maxn:
            fl=fl[:args.maxn]
        logging.debug(fl)
        scan_location(fl,key_to_search,args.method)



    
    print('Total time: ' ,time.time()-start)

