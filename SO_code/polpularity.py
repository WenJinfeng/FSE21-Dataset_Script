from internetarchive import download
from collections import defaultdict
import sys
import os
import random

import time 
import traceback
from bs4 import BeautifulSoup
import multiprocessing
import xml.etree.cElementTree as et

# users and questions of serverless computing per year
OUTPUT_user_by_year = 'USER_ID_year.txt'
OUTPUT_ques_by_year = 'QUES_ID_year.txt'

# candidate tags related to serverless computing
EXTRACT_TAGS2 = ['serverless', 'faas','serverless-framework','aws-serverless','serverless-architecture', 'serverless-offline','openwhisk', 'vercel','serverless-plugins','localstack', 'aws-lambda', 'aws-sam', 'aws-sam-cli']

# users and questions of SO dataset per year
OUTPUT_total_user_year = 'Total_USER_year.txt'
OUTPUT_total_ques_year = 'Total_QUES_year.txt'


def calculate_user_question_count(input_file):
    user_cnt = defaultdict(int)
    ques_cnt = defaultdict(int)
    
    cnt = 0
    filecnt = 0
    with open(input_file, 'r') as f:
        for line in f:
            try:
                line = line.strip()
                if '<row' not in line:
                    continue
                    
                cnt += 1
                if cnt % 10000 == 0:
                    print('process {} lines'.format(cnt))
                # parse xml
                tree = et.fromstring(line)
                content = tree.attrib
          #      print(content)

                if int(content['PostTypeId']) != 1:
                    # question post
                    continue
                
                # filter by tag
                tags = content['Tags'].split('><')
                # print(content)
                contain_tag = False
                for tag in tags:
                    tag = tag.strip().replace('<', '').replace('>', '')
                    if tag in EXTRACT_TAGS2:
                        contain_tag = True
                        break

                
                if contain_tag:
                    datastr = content['CreationDate']
                    if 'OwnerUserId' in content:
                        userstr = datastr[0:4]+',user-'+content['OwnerUserId']
                    if 'OwnerDisplayName' in content:
                        userstr = datastr[0:4]+',user-'+content['OwnerDisplayName']
                    quesstr=datastr[0:4]+',ques-'+content['Id']
                    user_cnt[userstr]+=1
                    ques_cnt[quesstr]+=1

            except:
                print(content)
     

    with open(OUTPUT_user_by_year, 'w') as f:
        for userid, cnt in user_cnt.items():
            f.write('{}\t{}\n'.format(userid, cnt))
    
    with open(OUTPUT_ques_by_year, 'w') as f:
        for quesid, cnt in ques_cnt.items():
            f.write('{}\t{}\n'.format(quesid, cnt))
    



def calculate_total_questionbyYear(input_file):
    ques_cnt = defaultdict(int)
    cnt = 0
    with open(input_file, 'r') as f:
        for line in f:
            try:
                line = line.strip()
                if '<row' not in line:
                    continue
                    
                cnt += 1
                if cnt % 10000 == 0:
                    print('process {} lines'.format(cnt))
                # parse xml
                tree = et.fromstring(line)
                content = tree.attrib
          #      print(content)

                if int(content['PostTypeId']) != 1:
                    # question post
                    continue


                datastr = content['CreationDate']
                quesstr=datastr[0:4]
                ques_cnt[quesstr]+=1

            except:
                print(content)
 
    
    with open(OUTPUT_total_ques_year, 'w') as f:
        for quesid, cnt in ques_cnt.items():
            f.write('{}\t{}\n'.format(quesid, cnt))
   

def calculate_total_userbyYear(input_file):
    user_cnt = defaultdict(int)
    userpool = set()
    
    cnt = 0

    with open(input_file, 'r') as f:
        for line in f:
            try:
                line = line.strip()
                if '<row' not in line:
                    continue
                    
                cnt += 1
                if cnt % 10000 == 0:
                    print('process {} lines'.format(cnt))
                # parse xml
                tree = et.fromstring(line)
                content = tree.attrib
          #      print(content)

                if int(content['PostTypeId']) != 1:
                    # question post
                    continue
                
                if 'OwnerUserId' in content:
                    userid=content['OwnerUserId']
                    if userid not in userpool:
                        datastr = content['CreationDate']
                        userstr=datastr[0:4]
                        user_cnt[userstr]+=1
                        userpool.add(userid)
                
                if 'OwnerDisplayName' in content:
                    userid=content['OwnerDisplayName']
                    if userid not in userpool:
                        datastr = content['CreationDate']
                        userstr=datastr[0:4]
                        user_cnt[userstr]+=1
                        userpool.add(userid)
            
            except:
                print(content)

    
    with open(OUTPUT_total_user_year, 'w') as f:
        for userid, cnt in user_cnt.items():
            f.write('{}\t{}\n'.format(userid, cnt))
  

def main_():
    print('Start to extract posts....')
    start = time.time()
    filename = 'archive/stackoverflow.com-Posts.7z/Posts.xml'
    
    # users and questions of serverless computing per year
    calculate_user_question_count(filename)

    # users and questions of SO dataset per year
    calculate_total_questionbyYear(filename)
    calculate_total_userbyYear(filename)

    print('Done in {} seconds.'.format(time.time() - start))
    

if __name__ == '__main__':
    main_()