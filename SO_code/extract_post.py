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




PROCESS_NUM = 10
#seed tags
EXTRACT_TAGS = ['serverless', 'faas']
#using seed tags to get post urls, id set of posts, tag set of posts
OUTPUT_FILE = '1_serverless_url.txt'
OUTPUT_ID = '1_serverless_id.txt'
OUTPUT_TAG = '1_serverless_tag.txt'


# tag information about the tatal SO posts
OUTPUT_ALL_TAG = 'all_data_tag.txt'

# candidate tags related to serverless computing
EXTRACT_TAGS2 = ['serverless', 'faas','serverless-framework','aws-serverless','serverless-architecture', 'serverless-offline','openwhisk', 'vercel','serverless-plugins','localstack', 'aws-lambda', 'aws-sam', 'aws-sam-cli']
#using candidate tags to get serverless-related posts
OUTPUT_FILE2 = '2_serverless_url.txt'
#using candidate tags to get serverless-related posts with the accepted answers
OUTPUT_FILE2_WITH_ANSWER = '2_serverless_url_with_answer.txt'


#extract posts using seed tags
def worker_byseed(input_file):
    coapper_tags = defaultdict(int)
    f_res = open(OUTPUT_FILE, 'a')
    f_res_id = open(OUTPUT_ID, 'a')
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
                    # extract question post
                    continue

                # count the number of total questions
                filecnt += 1

                # filter by tag
                tags = content['Tags'].split('><')
                # print(content)
                contain_tag = False
                for tag in tags:
                    tag = tag.strip().replace('<', '').replace('>', '')
                    if tag in EXTRACT_TAGS:
                        contain_tag = True
                        # get tag set
                        for t in tags:
                            t = t.strip().replace('<', '').replace('>', '')
                            coapper_tags[t] += 1
                        break
                    
                if contain_tag:
                    f_res.write('https://stackoverflow.com/questions/{}\t{}\n'.format(content['Id'], content['Tags']))
                    f_res_id.write('{}\n'.format(content['Id']))
            except:
                print(content)


# write statistics about tag 
    with open(OUTPUT_TAG, 'w') as f:
        for tag, cnt in coapper_tags.items():
            f.write('{}\t{}\n'.format(tag, cnt))
    
    print('The total number of questions is {}'.format(filecnt))

# statistic tag information about total SO posts
def worker_all(input_file):
    all_coapper_tags = defaultdict(int)
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
                
                # filter by tag
                tags = content['Tags'].split('><')
                # count tag
                for tag in tags:
                    tag = tag.strip().replace('<', '').replace('>', '')
                    all_coapper_tags[tag] += 1
                
            except:
                print(content)
            #    break
           #     traceback.print_exc()


    with open(OUTPUT_ALL_TAG, 'w') as f:
        for tag, cnt in all_coapper_tags.items():
            f.write('{}\t{}\n'.format(tag, cnt))



# using candidate tags to get serverless-related posts
def worker_bycnd(input_file):

    f_res = open(OUTPUT_FILE2, 'a')
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
                    f_res.write('URL=https://stackoverflow.com/questions/{}\tTAG={}\n'.format(content['Id'], content['Tags']))
                    
            except:
                print(content)
 


# using candidate tags to get serverless-related posts with the accepted answers
def worker_bycnd_answer(input_file):
    f_res = open(OUTPUT_FILE2_WITH_ANSWER, 'a')
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
                    f_res.write('URL=https://stackoverflow.com/questions/{}\tTAG={}\n'.format(content['Id'], content['Tags']))
                    
            except:
                print(content)




def main():

    
    print('Start to extract posts....')
    
    start = time.time()
    
    filename = 'archive/stackoverflow.com-Posts.7z/Posts.xml'
    # using seed tags to extract
    worker_byseed(filename)

    # get total SO tag information
    worker_all(filename)

    # using candidate tags to get serverless-related posts
    worker_bycnd(filename)
    # using candidate tags to get serverless-related posts with the accepted answers
    worker_bycnd_answer(filename)

    
    print('Done in {} seconds.'.format(time.time() - start))
    

if __name__ == '__main__':
    main()