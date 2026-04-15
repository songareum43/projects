'''
- Amazon Data Firehose(ADF)에게 direct로 데이터를 put 샘플
'''
# apache-airflow-providers-amazon 설치하여 자동으로 awd sdk(boto3)가 자동 설치됨
import boto3
import json
import time