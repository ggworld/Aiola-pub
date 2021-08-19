import awswrangler as wr
import boto3
boto3.setup_default_session(profile_name='DataScientist')
def worker(x):
    return (x,wr.s3.read_json(x)['items'][0]['n0:uniqueNumber'])
