import os
from pkgutil import get_data
import re
import time
import json
import pandas as pd
import requests
import datetime

# from requests_html import HTMLSession 
from bs4 import BeautifulSoup
from lxml import etree


from util.dbutil import db_connector


def request_book_list_url(one_category, basic_url, category_url = "", page_num = 1):

    print(one_category)
    search_url =  basic_url + category_url

    # 초기 데이터 셋팅
    get_data = {"linkClass": one_category["category_mid_id"], "mallGb": "KOR", "orderClick": "JAR", "targetPage": page_num}

    response = requests.get(f"{search_url}", get_data)
    html = response.text
    soup = BeautifulSoup(html, 'html.parser')
    dom = etree.HTML(str(soup))

    return dom



if __name__=="__main__":    
    print("01 데이터 크롤링 >>>>> >>>>> >>>>> >>>>> >>>>>")    
    time.sleep(1.5)                       # SLEEP 시간 주기

    conn = db_connector()
    cur = conn.cursor()

    # 대분류 정의 >>
    """
    # 완료된 대분류
    list_link_class = ["01", "05", "08", "09", "11", "13", "17", "35", 
                       "21", "23", "38", "31", "53", "51"]
    list_link_name = ["소설", "인문", "요리", "건강", "취미/실용/스포츠", "경제/경영", "정치/사회", "잡지",
                      "종교", "예술/대중문화", "청소년", "취업/수험서", "한국소개도서", "어린이전집"]
    """

    """
    # 미완료 대분류
    list_link_class = ["03", "07", "15", "19", "32", "33", "26",  
                       "27", "41", "42", "47", "25", "39", "29",
                       "50", ]
    list_link_name = ["시/에세이", "가정/육아", "자기계발", "역사/문화", "여행", "컴퓨터/IT", "기술/공학", 
                      "외국어", "유아(0~7세)", "어린이(초등)", "만화", "중/고등참고서", "초등참고서", "과학"
                      "대학교재", ]
    """

    # 수집 대상 대분류
    list_link_class = []
    list_link_name = []
                       
    mallGb = "KOR"
    orderClick = "JAR"
    
    for big_i in range(len(list_link_class)):
        # 카테고리 저장
        info_mid_category = []
        info_full_category = []
        
        basic_url = "http://www.kyobobook.co.kr/"
        category_url = "categoryRenewal/categoryMain.laf"
        search_url = basic_url + category_url

        # get 데이터
        get_data = {"linkClass": list_link_class[big_i], "mallGb": "KOR", "orderClick": "JAR"}
        response = requests.get(f"{search_url}", get_data)
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')
        dom = etree.HTML(str(soup))

        catagory_div = dom.xpath('//*[@id="container"]/div[1]/div[4]/div/div/div/div')[0]
        
        for one in catagory_div:        
            if one.tag == 'ul':
                for li_info in one:
                    
                    li_href = li_info[0].attrib['href']
                    param_str = li_href.split('?', 1)[1]
                    param_list = param_str.split('&')

                    category_mid_id = ""

                    for tmp_str in param_list:
                        if "linkClass" in tmp_str:
                            category_mid_id = tmp_str.replace("linkClass=", "")

                    category_dict = {
                        "category_big_id": list_link_class[big_i],
                        "category_big_name": list_link_name[big_i],
                        "category_mid_id": category_mid_id,
                        "category_mid_name": li_info[0].text,
                        "category_url": li_href
                    }

                    # print(li_info[0].text)
                    # print(li_info[0].attrib['href'])

                    info_mid_category.append(category_dict)
        
        # 카테고리 수집
        for one_mid_category in info_mid_category:
            time.sleep(7.31)

            # 페이지 수 파악을 위한 URL Request
            small_category_dom = request_book_list_url(one_mid_category, basic_url, category_url, 1)

            small_category_div = small_category_dom.xpath('//*[@id="container"]/div[1]/div[5]/div/div/div/div')

            if len(small_category_div) == 0:
                one_dict = {
                    "category_big_id": one_mid_category["category_big_id"],
                    "category_big_name": one_mid_category["category_big_name"],
                    "category_mid_id": one_mid_category["category_mid_id"],
                    "category_mid_name": one_mid_category["category_mid_name"],
                    "category_small_id": one_mid_category["category_mid_id"],
                    "category_small_name": one_mid_category["category_mid_name"],
                    "category_url": one_mid_category["category_url"]
                }                        
                info_full_category.append(one_dict)             # 카테고리 전체 데이터 저장

            else:
                small_category_div = small_category_div[0]
            
                for one_s in small_category_div:
                    if one_s.tag == 'ul':
                        for li_s_info in one_s:
                            
                            li_s_href = li_s_info[0].attrib['href']
                            param_str = li_s_href.split('?', 1)[1]
                            param_list = param_str.split('&')

                            category_small_id = ""
                            for tmp_str in param_list:
                                if "linkClass" in tmp_str:
                                    category_small_id = tmp_str.replace("linkClass=", "")

                            one_dict = {
                                "category_big_id": one_mid_category["category_big_id"],
                                "category_big_name": one_mid_category["category_big_name"],
                                "category_mid_id": one_mid_category["category_mid_id"],
                                "category_mid_name": one_mid_category["category_mid_name"],
                                "category_small_id": category_small_id,
                                "category_small_name": li_s_info[0].text,
                                "category_url": li_s_href
                            }                        
                            info_full_category.append(one_dict)             # 카테고리 전체 데이터 저장
        
        file_path = './data/' + list_link_class[big_i] + '.json'

        with open(file_path, 'w', encoding='UTF-8') as save_file:
            save_file.write(json.dumps(info_full_category, ensure_ascii=False, default=str, indent='\t'))

        # Book 리스트 출력
        for one_small in info_full_category:
            print(one_small)

            book_list_search_url =  basic_url + category_url
            # perPage=20
            # mallGb=KOR
            # linkClass=330709
            # menuCode=000
            # targetPage=1

            # 페이징            
            pagging_num = 1
            
            list_random = [7.42, 7.37, 7.83, 8.25, 7.28, 7.51, 8.02, 7.77, 7.98, 8.13]
            len_random = len(list_random)

            while True:
                # print(pagging_num)
                i_sleep = pagging_num % len_random
                time.sleep(list_random[i_sleep])

                # 데이터 셋팅
                first_get_data = {"linkClass": one_small["category_small_id"], "mallGb": "KOR", "menuCode": "000", "targetPage": pagging_num}
                book_list_response = requests.get(f"{book_list_search_url}", first_get_data)
                time.sleep(6.29)

                book_list_html = book_list_response.text
                book_list_soup = BeautifulSoup(book_list_html, 'html.parser')
                book_list_dom = etree.HTML(str(book_list_soup))

                # 페이징 체크
                pagging_div = book_list_dom.xpath('//*[@id="eventPaging"]/div')[0]
                len_pagging_div = len(pagging_div)

                check_next = pagging_div[len_pagging_div-1]

                book_list_div = book_list_dom.xpath('//*[@id="prd_list_type1"]')[0]

                for i in range(0, len(book_list_div)):                    
                    if i % 6 == 4:
                        book_li = book_list_div[i]
                        book_a_tag = book_li[0][0][1][0][0]
                        
                        book_href = book_a_tag.attrib["href"]
                        book_title = book_a_tag[0].text
                        
                        
                        insert_column_sql =  """
                            INSERT INTO book_list
                            (url_href, book_title, link_class, is_collect_yn)
                            VALUES(?, ?, ?, ?)       
                        """

                        insert_column_values = (book_href, book_title, one_small["category_small_id"], 'N')
                                
                        cur.execute(insert_column_sql, insert_column_values)
                        conn.commit()

                        time.sleep(0.17)

                        # //*[@id="prd_list_type1"]/li[3]/div/div[1]/div[2]/div[1]/a/strong
                        # //*[@id="prd_list_type1"]/li[1]/div/div[1]/div[2]/div[1]/a
                        # //*[@id="prd_list_type1"]/li[3]/div/div[1]/div[2]/div[1]/a
                        
                if check_next.tag != 'a':
                    print("check_next.tag is NOT A tag >>>> ")
                    print("pagging_num = " + str(pagging_num))
                    break;

                pagging_num += 1
        
    conn.close()
    print("_-------------------------------------------------------------------------_")
