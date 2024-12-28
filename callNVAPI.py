# 231214 네이버 오픈 API
# for nv shopping SEO
# inspi@hanmail.net

# 모듈 독립화

import urllib.request
import json
import re
import ssl
import callGetKey as getkey

# Naver Search API id, secret key
CLIENT_ID = getkey.get_apikey('CLIENT_ID', 'secrets.json')
CLIENT_SECRET = getkey.get_apikey('CLIENT_SECRET', 'secrets.json')

ssl._create_default_https_context = ssl._create_unverified_context

# query UTF-8 인코딩 필요
# display 10~100
# start 1~1000

# 네이버 쇼핑 JSON 함수
def callNvAPI(query, display=100, start=1, serviceid='shop', sort='sim', filter='all'):
    encText = urllib.parse.quote(query)

    url = (f'https://openapi.naver.com/v1/search/shop?start={start}&display={display}&query={encText}')

    request = urllib.request.Request(url)
    request.add_header('X-Naver-Client-Id', CLIENT_ID)
    request.add_header('X-Naver-Client-Secret', CLIENT_SECRET)
    response = urllib.request.urlopen(request)
    rescode = response.getcode()
    if rescode == 200:
        response_body = response.read()
    else:
        print('API Call - Error Code:' + rescode)
        return False
    
    data = json.loads(response_body.decode('utf-8'))  # JSON 형태의 문자열 읽기

    if len(data) < 1:
        print('No Result')
        return -1
    
    if data['total'] == 0:
        print('No Product')
        return 0
    
    return data

def getNVTotal(query):
    result = callNvAPI(query, display=40)

    if result == False: return [False for i in range(6)]    # False (Error)
    elif result == -1: return [-1 for i in range(6)]        # -1 (No result)
    elif result == 0: return [0 for i in range(6)]          # 0 (No Data)

    rank = 0
    catalog6 = 0
    catalog20 = 0
    catalog40 = 0
    limit_catalog6 = 6
    limit_catalog20 = 20

    category = []

    for item in result['items']:
        rank += 1
        if item['mallName'] == '네이버':
            catalog40 += 1
            if limit_catalog20 >= rank: 
                catalog20 += 1
                if limit_catalog6 >= rank: 
                    catalog6 += 1
        cate_name = item['category1'] + '>' + item['category2'] + '>' + item['category3']
        if item['category4'] != '': cate_name = cate_name + '>' + item['category4']
        category.append(cate_name)

    # 카테고리 카운트
    category_set = list(set(category))
    category_count = [0 for i in range(len(category_set))]
    for i in range(len(category_set)):
        category_count[i] = category.count(category_set[i])

    return [result['total'], catalog6, catalog20, catalog40, category_set, category_count] # result['total']

def getNVRank(mid, query, start=1):
    while start <= 1000:
        display = 99 if start == 1 else 100

        result = callNvAPI(query, display=display, start=start)
        
        # print('{}|'.format(start),end='',flush=True)
        if (start % 500) == 0:
            print('.', end='', flush=True)
        #     time.sleep(0.05)
        
        if result == False: return False
        elif result == -1: return -1
        elif result == 0: return 0
        else:
            rank = start
            for item in result['items']:
                if mid == item['productId']: 
                    # item.update({'rank':rank})
                    # return item
                    return rank
                rank += 1

        start += display

    return 9999999

def getNVRankInfo(mid, query, start=1):
    while start <= 1000:
        display = 99 if start == 1 else 100

        result = callNvAPI(query, display=display, start=start)
        
        # print('{}|'.format(start),end='',flush=True)
        if (start % 500) == 0:
            print('.', end='', flush=True)
        #     time.sleep(0.05)
        
        if result == False: return False
        elif result == -1: return -1
        elif result == 0: return 0
        else:
            rank = start
            for item in result['items']:
                if mid == item['productId']: 
                    # item.update({'rank':rank})
                    # return item
                    return [rank, re.sub(re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});'), '', item['title'])]
                rank += 1

        start += display

    return [9999999, '']

def getNVProduct(storename, keyword):
    query = storename + ' ' + keyword
    result = callNvAPI(query, sort='date')

    if result == False: return [False, False]    # Error
    elif result == -1: return [-1, -1]        # 결과값이 없을 때
    elif result == 0: return [0, 0]          # 검색량이 0일 때
    else:
        for item in result['items']:
            if (storename in item['mallName']) and (keyword in item['title']):
                return [item['productId'], re.sub(re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});'), '', item['title'])]
    
    return [False, False]