import requests
from urllib.parse import quote
import time
import random
from typing import Dict, List
import gspread
from google.oauth2.service_account import Credentials
import json
import pandas as pd
import callGetKey as getKey
import os

def get_naver_shopping_results(query: str, mid: int, max_pages: int = 1, cursor: int = 1, page_size: int = 50) -> dict:
    base_url = 'https://search.shopping.naver.com/ns/v1/search/paged-composite-cards'
    encoded_query = quote(query)
    all_results = {'data': [], 'pageSize': page_size}
    
    session = requests.Session()
    # 먼저 검색 페이지 방문
    session.get(f'https://search.shopping.naver.com/ns/search?query={encoded_query}')
    
    headers = {
        'accept': '*/*',
        'accept-language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        'referer': f'https://search.shopping.naver.com/ns/search?query={encoded_query}',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
    }

    for page in range(1, max_pages + 1):
        try:
            # 랜덤 딜레이 추가
            time.sleep(random.uniform(1, 3))

            params = {
                'cursor': cursor,
                'pageSize': page_size,
                'query': query,
                'sort': 'RECOMMEND',
                'searchMethod': 'all.basic',
                'isFreshCategory': 'false',
                'isOriginalQuerySearch': 'false'
            }
            
            response = session.get(
                base_url,
                params=params,
                headers=headers,  # headers는 기존과 동일
                timeout=10
            )
            response.raise_for_status()
            
            result = response.json()
            
            print(f"페이지 {page} 검색 완료", flush=True)
            
            # 결과가 없으면 중단
            if not result['data']['data']:
                break

            cursor = result['data']['cursor']

            # 결과 누적
            for item in result['data']['data']:
                all_results['data'].append(item)
                if item['card']['product']['nvMid'] == mid:
                    print(f"찾는 상품(mid: {mid})을 발견했습니다.", flush=True)
                    return all_results

        except requests.exceptions.RequestException as e:
            print(f"페이지 {page} 요청 중 오류 발생: {e}", flush=True)
            break
        except json.JSONDecodeError as e:  # JSON 파싱 오류 처리 추가
            print(f"JSON 파싱 중 오류 발생: {e}", flush=True)
            break
    
    print(f"{max_pages} 페이지내에서 찾는 상품(mid: {mid})을 발견하지 못했습니다.", flush=True)
    return all_results

# 재시도 로직을 포함한 래퍼 함수
def get_naver_shopping_results_with_retry(query, max_retries=3, delay_between_retries=5):
    for attempt in range(max_retries):
        result = get_naver_shopping_results(query)
        if result is not None:
            return result
        
        if attempt < max_retries - 1:  # 마지막 시도가 아니라면
            time.sleep(delay_between_retries)
            print(f"재시도 중... ({attempt + 1}/{max_retries})")
    
    return None

def parse_shopping_results(json_response, page_size=50, keyword=None) -> List[Dict]:
    """
    네이버 쇼핑 검색 결과를 파싱하여 필요한 정보만 추출
    
    Args:
        json_response: API 응답 JSON
        page_size: 페이지당 결과 수
        keyword: 검색 키워드
        
    Returns:
        List[Dict]: 파싱된 상품 정보 리스트
    """
    products = []
    rank = page_size * (json_response['data'][0]['page'] - 1)
    
    for idx, item in enumerate(json_response['data'], 1):
        product = item['card']['product']
        
        if 'cardType' not in product:
            rank += 1
            
        parsed_item = {
            'keyword': keyword,  # 검색 키워드 추가
            'no': idx + page_size * (product['page'] - 1),
            'rank': rank if 'cardType' not in product else product['cardType'],
            'nvMid': product['nvMid'],
            'mallName': product['mallName'],
            'productName': product['productName'],
        }
        
        products.append(parsed_item)
        
    return products

def get_sheet_data(sheet_url, sheet_name=None, sheet_client=None):
    """구글 시트에서 데이터를 가져오는 함수"""
    # 시트 열기
    workbook = sheet_client.open_by_url(sheet_url)
    if sheet_name:
        try:
            worksheet = workbook.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            print(f"시트 '{sheet_name}'을(를) 찾을 수 없습니다. 첫 번째 시트를 사용합니다.")
            worksheet = workbook.get_worksheet(0)
    else:
        worksheet = workbook.get_worksheet(0)
    
    # 데이터 가져오기
    data = worksheet.get_all_records()
    
    # 필요한 형식으로 데이터 변환
    processed_data = []
    for row in data:
        if row['TRACKING'] == 1:  # tracking이 yes인 항목만 처리
            processed_data.append({
                'mid': int(row['MID']),
                'keyword': row['KEYWORD'],
                'tracking_yes_or_no': row['TRACKING']
            })
    
    return processed_data

def update_rank_sheet_batch(sheet_url: str, rank_data: List[Dict], sheet_client: gspread.Client = None, sheet_name: str = "rank") -> None:
    """
    검색된 상품들의 순위를 구글 시트에 한 번에 저장하는 함수
    
    Args:
        sheet_url (str): 구글 시트 URL
        rank_data (list): [{mid, keyword, rank}, ...] 형식의 순위 데이터 리스트
        sheet_client: 구글 시트 클라이언트
        sheet_name (str): 저장할 시트 이름
    """
    workbook = sheet_client.open_by_url(sheet_url)
    
    # rank 시트가 없으면 생성
    try:
        worksheet = workbook.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = workbook.add_worksheet(sheet_name, 1000, 5)
        # 헤더 추가
        # worksheet.update('A1:E1', [['Timestamp', 'MID', 'Keyword', 'Rank', 'Date']])
        worksheet.update('A1:K1', [['Date', 'Time', 'MID', 'Keyword', 'Store', 'Item', 'Rank', 'Channel', 'Name_Prd', 'Amt_Search', 'Amt_Prds']])
    
    # 현재 시간과 날짜
    date = time.strftime("%y. %m. %d")
    times = time.strftime("%H:%M:%S")
    
    # 마지막 행 번호 가져오기
    last_row = len(worksheet.get_all_values())
    next_row = last_row + 1
    
    # 모든 데이터를 한 번에 업데이트할 행 준비
    update_rows = []
    for data in rank_data:
        new_row = [date, times, data['mid'], data['keyword'], data['store'], data['item'], data['rank'], data['channel'], data['title']]
        update_rows.append(new_row)
    
    # 한 번에 업데이트
    if update_rows:
        cell_range = f'A{next_row}:I{next_row + len(update_rows) - 1}'
        worksheet.update(values=update_rows, range_name=cell_range)
        print(f"총 {len(update_rows)}개의 순위 정보가 {sheet_name} 시트에 저장되었습니다.", flush=True)

# 메인 실행 코드 수정
if __name__ == "__main__":
    # 설정 파일 로드
    config = {}

    try:
        config['credentials_file'] = getKey.get_apikey('GS_JSON', 'config.json')  # getKey를 사용하여 설정 가져오기
        config['sheet_url'] = getKey.get_apikey('GS_URL', 'config.json')
        config['sheet_name'] = getKey.get_apikey('GSHEET_KEYWORDS', 'config.json')
        config['rank_updates'] = getKey.get_apikey('GSHEET_RANK_PRE', 'config.json')
        config['limit_page'] = getKey.get_apikey('LIMIT_PAGE', 'config.json')

    except Exception as e:
        print(f"설정을 불러오는 중 오류가 발생했습니다: {e}")
        exit(1)

    start_time = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"작업 시작 시간: {start_time}")

    # 구글 시트 인증 정보 초기화
    SCOPE = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    try:
        CREDENTIALS = Credentials.from_service_account_file(
            config['credentials_file'], 
            scopes=SCOPE
        )
        SHEET_CLIENT = gspread.authorize(CREDENTIALS)
    except FileNotFoundError:
        print(f"인증 파일({config['credentials_file']})을 찾을 수 없습니다.")
        exit(1)
    
    tracking_items = get_sheet_data(
        sheet_url=config['sheet_url'],
        sheet_name=config['sheet_name'],
        sheet_client=SHEET_CLIENT
    )

    tracking_items = sorted(tracking_items, key=lambda x: x['mid'])  # mid 기준 오름차순 정렬
    
    all_results = []
    rank_updates = []  # 순위 업데이트를 위한 데이터 저장
    current_mid = None  # 현재 처리 중인 mid 추적용
    
    for idx, item in enumerate(tracking_items, 1):
        
        # mid가 변경되었을 때 이전 데이터 저장
        if current_mid is not None and current_mid != item['mid'] and rank_updates:
            update_rank_sheet_batch(
                sheet_url=config['sheet_url'],
                rank_data=rank_updates,
                sheet_client=SHEET_CLIENT,
                sheet_name=str(current_mid)  # mid를 시트 이름으로 사용
            )
            rank_updates = []  # 데이터 초기화
        
        print(f"\n[{idx}/{len(tracking_items)}] 검색어: {item['keyword']}, MID: {item['mid']} 검색 중...", flush=True)

        current_mid = item['mid']  # 현재 mid 업데이트
        results = get_naver_shopping_results(item['keyword'], item['mid'], max_pages=config['limit_page'])
        
        if results and results['data']:
            products = parse_shopping_results(results, keyword=item['keyword'])
            all_results.extend(products)
            
            # 해당 mid의 순위 찾기
            for product in products:
                if product['nvMid'] == item['mid']:
                    rank_updates.append({
                        'mid': item['mid'],
                        'keyword': item['keyword'],
                        'rank': product['rank'],
                        'store': product['mallName'],
                        'item': product['productName'],
                        'channel': 'newStore',
                        'title': product['productName'],
                    })
                    break
    
    # 마지막 mid의 데이터 저장
    if rank_updates:
        update_rank_sheet_batch(
            sheet_url=config['sheet_url'],
            rank_data=rank_updates,
            sheet_client=SHEET_CLIENT,
            sheet_name=str(current_mid)
        )
    
    if all_results:
        # results 폴더가 없으면 생성
        if not os.path.exists('results'):
            os.makedirs('results')

        # CSV로 저장
        df = pd.DataFrame(all_results)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        filename = f'results/naver_shopping_results_{timestamp}.csv'  # 경로 수정
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"\nCSV 파일로 저장되었습니다: {filename}")
    
    end_time = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n작업 시작 시간: {start_time}")
    print(f"작업 종료 시간: {end_time}")
