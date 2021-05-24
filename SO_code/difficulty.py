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
from datetime import datetime


# candidate tags related to serverless computing
EXTRACT_TAGS2 = ['serverless', 'faas','serverless-framework','aws-serverless','serverless-architecture', 'serverless-offline','openwhisk', 'vercel','serverless-plugins','localstack', 'aws-lambda', 'aws-sam', 'aws-sam-cli']

# the creation date of serverless questions
Ques_time = 'Ques_time.txt'
# the corresponding answer id
AnswerIdSet = 'AnswerID.txt'


# the creation date of serverless answer
Answer_time = 'Answer_time.txt'

# calculate response time of serverless questions
Respond_time_file='Respond_time_total.txt'


#-----------------------
# the creation date of SO questions
Dataset_Ques_time = 'Dataset_Ques_time.txt'
# the corresponding answer id
Dataset_AnswerIdSet = 'Dataset_AnswerID.txt'


# the creation date of SO answer
Dataset_Answer_time = 'Dataset_Answer_time.txt'

# calculate response time of SO questions
Dataset_Respond_time_file='Dataset_Respond_time_total.txt'




# get the creation date of serverless questions
def get_questiontimestamp(input_file):

    AnswerSet=set()
    f_res = open(Ques_time, 'a')
    f_res_id = open(AnswerIdSet, 'a')
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
                
                #1 is question type
                if int(content['PostTypeId']) != 1:
                    # question post
                    continue

                # exclude posts without accept answers
                if 'AcceptedAnswerId' not in content:
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
                    f_res.write('{},{}\n'.format(content['Id'], content['CreationDate']))
                    AnswerSet.add(content['AcceptedAnswerId'])

            except:
                print(content)

    for i in AnswerSet:
        f_res_id.write('{}\n'.format(i))
    
  

# get the creation date of serverless answer
def get_answertimestamp(input_file, answerid_file):
    
    AnswerSet=set()
  
    with open(answerid_file, 'r') as f:
        for line in f:
            #print(line)
            line=line.replace('\n', '')
            AnswerSet.add(line)
    

    f_res = open(Answer_time, 'a')
    
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
                
                #1 is question type
                if int(content['PostTypeId']) != 2:
                    # question post
                    continue

                if content['Id'] in AnswerSet:
                    f_res.write('{},{}\n'.format(content['Id'], content['CreationDate']))
                
            except:
                print(content)




    
# calculate response time through the creation dates of question and the corresponding answer for serverless computing
def get_response_time(input_file, input_file_ques, input_file_answer):
    
    ques_time = defaultdict()
# read question data
    with open(input_file_ques, 'r') as f:
        for line1 in f:
            line1=line1.replace('\n', '')
            q1,t1=line1.split(',')
            ques_time[q1]=t1
    
    answer_time = defaultdict()
# read answer date
    with open(input_file_answer, 'r') as f:
        for line2 in f:
            line2=line2.replace('\n', '')
            q2,t2=line2.split(',')
            answer_time[q2]=t2  
    
    
    
    f_res = open(Respond_time_file, 'a')
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
                
                #1 is question type
                if int(content['PostTypeId']) != 1:
                    # question post
                    continue

                 # exclude posts without accept answers
                if 'AcceptedAnswerId' not in content:
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
                    present_time=ques_time[content['Id']]
                    accept_time=answer_time[content['AcceptedAnswerId']]
                    present_time1=datetime.strptime(present_time,"%Y-%m-%dT%H:%M:%S.%f")
                    accept_time1=datetime.strptime(accept_time,"%Y-%m-%dT%H:%M:%S.%f")
                    delta_time=(accept_time1-present_time1).seconds
                    
                    print('{},{},{},{},{}\n'.format(content['Id'], ques_time[content['Id']], content['AcceptedAnswerId'], answer_time[content['AcceptedAnswerId']], delta_time/60))
                    
                    f_res.write('{},{},{},{},{}\n'.format(content['Id'], ques_time[content['Id']], content['AcceptedAnswerId'], answer_time[content['AcceptedAnswerId']], delta_time/60))

            except:
                print(content)
                break


#------------------------
# calculate %no.acc.
def cal_dateset_noacc(input_file):

    cnt = 0
    filecnt = 0
    answercnt = 0
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

                filecnt += 1

                # accept answers
                if 'AcceptedAnswerId' in content:
                    answercnt += 1
                

            except:
                print(content)
    
    print('The total number of questions is {}'.format(filecnt))
    print('The total number of questions with the accepted answers is {}'.format(answercnt))


# get the creation date of SO questions
def get_Dataset_questiontimestamp(input_file):
    
    AnswerSet=set()
    
    f_res = open(Dataset_Ques_time, 'a')
    f_res_id = open(Dataset_AnswerIdSet, 'a')
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
                
                #1 is question type
                if int(content['PostTypeId']) != 1:
                    # question post
                    continue

            #    key = content['Id']
                filecnt += 1
  
                # exclude posts without accept answers
                if 'AcceptedAnswerId' not in content:
                    continue
            
                f_res.write('{},{}\n'.format(content['Id'], content['CreationDate']))
                AnswerSet.add(content['AcceptedAnswerId'])
                    
            except:
                print(content)
   

    for i in AnswerSet:
        f_res_id.write('{}\n'.format(i))
    
    print('The total number of questions is {}'.format(filecnt))

# get the creation date of SO answers
def get_Dataset_answertimestamp(input_file, answerid_file):
    
    AnswerSet=set()
   
    with open(Dataset_answerid_file, 'r') as f:
        for line in f:
            #print(line)
            line=line.replace('\n', '')
            AnswerSet.add(line)
    

    f_res = open(Answer_time, 'a')
   
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
                
                #1 is question type
                if int(content['PostTypeId']) != 2:
                    # question post
                    continue

            #    key = content['Id']
                filecnt += 1
                
                if content['Id'] in AnswerSet:
                    f_res.write('{},{}\n'.format(content['Id'], content['CreationDate']))
                       break

            except:
                print(content)
            
    print('The total number of questions is {}'.format(filecnt))





    
# calculate response time through the creation dates of question and the corresponding answer for SO dataset
def get_Dataset_response_time(input_file, input_file_ques, input_file_answer):
    ResponseTimeSet=[]
    ques_time = defaultdict()
    with open(input_file_ques, 'r') as f:
        for line1 in f:
            line1=line1.replace('\n', '')
            q1,t1=line1.split(',')
            ques_time[q1]=t1

    print('ques_time finish!')

    answer_time = defaultdict()
    with open(input_file_answer, 'r') as f:
        for line2 in f:
            line2=line2.replace('\n', '')
            q2,t2=line2.split(',')
            answer_time[q2]=t2  
    
    print('answer_time finish!')
    
    f_res = open(Dataset_Respond_time_file, 'a')
    
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
                
                #1 is question type
                if int(content['PostTypeId']) != 1:
                    # question post
                    continue

            #    key = content['Id']
                filecnt += 1
                
                # exclude posts without accept answers
                if 'AcceptedAnswerId' not in content:
                    continue
               
                present_time=ques_time[content['Id']]
                accept_time=answer_time[content['AcceptedAnswerId']]
                present_time1=datetime.strptime(present_time,"%Y-%m-%dT%H:%M:%S.%f")
                accept_time1=datetime.strptime(accept_time,"%Y-%m-%dT%H:%M:%S.%f")
                delta_time=(accept_time1-present_time1).seconds
                ResponseTimeSet.append(delta_time/60)
                print('{},{},{},{},{}\n'.format(content['Id'], ques_time[content['Id']], content['AcceptedAnswerId'], answer_time[content['AcceptedAnswerId']], delta_time/60))
                f_res.write('{},{},{},{},{}\n'.format(content['Id'], ques_time[content['Id']], content['AcceptedAnswerId'], answer_time[content['AcceptedAnswerId']], delta_time/60))
                    
            except:
                pass

            continue

    valueResponse = np.median(ResponseTimeSet)
    print('The median response time is {}'.format(valueResponse))
    print('The total number of questions is {}'.format(filecnt))






def main_():
    print('Start to extract posts....')
    start = time.time()
    filename = 'archive/stackoverflow.com-Posts.7z/Posts.xml'
    
    # calculate respinse time of serverless computing questions
    answeridfile='AnswerID.txt'
    input_file_ques='Ques_time.txt'
    input_file_answer='Answer_time.txt'

    get_questiontimestamp(filename)
    get_answertimestamp(filename, answeridfile)
    get_response_time(filename, input_file_ques, input_file_answer)


    # calculate %no.acc.
    cal_dateset_noacc(filename)
    
    # calculate response time of SO questions
    Dataset_answeridfile='Dataset_AnswerID.txt'
    Dataset_input_file_ques='Dataset_Ques_time.txt'
    Dataset_input_file_answer='Dataset_Answer_time.txt'

    get_Dataset_questiontimestamp(filename)
    get_Dataset_answertimestamp(filename, Dataset_answeridfile)
    get_Dataset_response_time(filename, Dataset_input_file_ques, Dataset_input_file_answer) 

    print('Done in {} seconds.'.format(time.time() - start))
    

if __name__ == '__main__':
    main_()