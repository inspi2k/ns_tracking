# 231214 네이버 광고 API
# for nv shopping SEO
# inspi@hanmail.net
# 출처: https://dataanalytics.tistory.com/entry/Python-네이버-광고-API-키워드-검색 [퍼포먼스 마케팅 데이터 분석:티스토리]

# 모듈 독립화

import requests
import time
import hmac
import hashlib
import base64
import pandas as pd
import callGetKey as getkey

# API_KEY #api 키
# SECRET_KEY #api 시크릿키
# CUSTOMER_ID #customer ID 키 입력
API_KEY = getkey.get_apikey('API_KEY', 'secrets.json')
SECRET_KEY = getkey.get_apikey('SECRET_KEY', 'secrets.json')
CUSTOMER_ID = getkey.get_apikey('CUSTOMER_ID', 'secrets.json')

def generate(timestamp, method, uri, secret_key):
    message = "{}.{}.{}".format(timestamp, method, uri)
    #hash = hmac.new(bytes(secret_key, "utf-8"), bytes(message, "utf-8"), hashlib.sha256)
    hash = hmac.new(secret_key.encode("utf-8"), message.encode("utf-8"), hashlib.sha256)
    hash.hexdigest()
    return base64.b64encode(hash.digest())

def get_header(method, uri, api_key, secret_key, customer_id):
    timestamp = str(int(time.time() * 1000))
    signature = generate(timestamp, method, uri, SECRET_KEY)
    return {'Content-Type': 'application/json; charset=UTF-8', 'X-Timestamp': timestamp, 'X-API-KEY': API_KEY, 'X-Customer': str(CUSTOMER_ID), 'X-Signature': signature}

def callNvAd(keyword):
    BASE_URL = 'https://api.naver.com'
    dic_return_kwd = {}
    naver_ad_url = '/keywordstool'
    #_kwds_string = '원피스' #1개일경우
    #_kwds_string = ['나이키', '원피스', '운동화'] #키워드 여러개일경우
    method = 'GET'
    prm = {'hintKeywords' : keyword , 'showDetail' : 1}
    #    ManageCustomerLink Usage Sample
    r = requests.get(BASE_URL + naver_ad_url, params=prm, headers=get_header(method, naver_ad_url, API_KEY, SECRET_KEY, CUSTOMER_ID))

    r_data = r.json()
    naver_ad_summary = pd.DataFrame(r_data['keywordList'])  

    return naver_ad_summary[:1]   #[:1]

def getTotalQcCnt(keyword):
    result = callNvAd(keyword)

    # numpy.int64 가 int type이 아니라서 isinstance(,int)에서 오류발생
    #   ('< 10' str type 넘어올 때 다른 값(예. 10)은 numpy.int64 type)
    # print(type(result.iloc[0]["monthlyPcQcCnt"]), type(result.iloc[0]["monthlyMobileQcCnt"]))
    # pcQc = result.iloc[0]["monthlyPcQcCnt"] if isinstance(result.iloc[0]["monthlyPcQcCnt"], int) == True else 0 
    # mobileQc = result.iloc[0]["monthlyMobileQcCnt"] if isinstance(result.iloc[0]["monthlyMobileQcCnt"], int) == True else 0 

    pcQc = result.iloc[0]["monthlyPcQcCnt"] if str(result.iloc[0]["monthlyPcQcCnt"]).isdecimal() == True else 1
    mobileQc = result.iloc[0]["monthlyMobileQcCnt"] if str(result.iloc[0]["monthlyMobileQcCnt"]).isdecimal() == True else 1
    # print("{}/{}=>{}/{}_".format(result.iloc[0]["monthlyPcQcCnt"], result.iloc[0]["monthlyMobileQcCnt"], pcQc, mobileQc))
    return pcQc + mobileQc
