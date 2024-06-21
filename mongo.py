import requests
from pymongo import MongoClient
import mysql.connector
from datetime import datetime, date
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
import schedule
import time
import DE.crawl_data.proj_topdev.connect.connect_mongodb as connect_mongodb
import DE.crawl_data.proj_topdev.connect.connect_mysql as connect_mysql

collection =connect_mongodb.connect_db()
def extract_mongo(page):
    api_url = "https://api.topdev.vn/td/v2/jobs?fields[job]=id,slug,title,salary,company,extra_skills,skills_str,skills_arr,skills_ids,job_types_str,job_levels_str,job_levels_arr,job_levels_ids,addresses,status_display,detail_url,job_url,salary,published,refreshed,applied,candidate,requirements_arr,packages,benefits,content,features,is_free,is_basic,is_basic_plus,is_distinction&fields[company]=slug,tagline,addresses,skills_arr,industries_arr,industries_str,image_cover,image_galleries,benefits&page="+str(page)+"&locale=vi_VN&ordering=jobs_new"
    response =requests.get(api_url)
    data = response.json()
    jobs = data['data']
    for job in jobs:
        collection.insert_one(job)
    
    print(page)  
    meta = data['meta']
    current_page = meta['current_page']
    last_page = meta['last_page']
    if current_page <= last_page:
        extract_mongo(page+1)
        
def extract_mysql():
    mongo_data = collection.find()
    db_config = connect_mysql.mysql

    # Kết nối tới cơ sở dữ liệu
    mysql_conn = mysql.connector.connect(**db_config)
    ## Create cursor, used to execute commands
    mysql_cur = mysql_conn.cursor()
        
    ## Create books table if none exists
    print("Tạo bảng:")
    mysql_cur.execute("""
    CREATE TABLE IF NOT EXISTS jobs(
        id int NOT NULL auto_increment, 
        title TEXT,
        full_address TEXT,
        company_name TEXT,
        detail_url TEXT,
        job_level TEXT,
        skills TEXT,
        job_type TEXT,
        salary TEXT,
        published DATE,
        refreshed DATE,
        PRIMARY KEY (id)
    )
    """)
    for job in mongo_data:
        title = job['title']
        full_address = job['addresses']['full_addresses'][0]
        
        company = job['company']
        company_name = company['display_name']
        company_detail_url = company['detail_url']
        company_image_logo = company['image_logo']
        company_industries = company['industries_str']
        
        detail_url = job['detail_url']
        job_level = job['job_levels_str']
        skills = job['skills_str']
        job_type = job['job_types_str']
        
        salary = job['salary']
        salary_job = ""
        is_negotible = salary['is_negotiable']
        max = salary['max']
        min = salary['min']
        currency = salary['currency']
        if is_negotible ==1:
            salary_job = "Thuong Luong"
        else:
            salary_job = f"Tu {min} {currency} den {max} {currency}"
            
        published_date = job['published']['date']
        refreshed_date = job['refreshed']['date']
        
        sql="""              
            INSERT INTO jobs (
                title, 
                full_address,
                company_name,
                detail_url,
                job_level,
                skills,
                job_type,
                salary,
                published,
                refreshed 
            ) VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )"""
                       
        values = (title,full_address,company_name,detail_url,job_level,skills,job_type,salary_job,datetime.strptime(published_date, '%d-%m-%Y').date(),datetime.strptime(refreshed_date, '%d-%m-%Y').date())
    # them log 
        mysql_cur.execute(sql, values)
    # Lưu thay đổi
    mysql_conn.commit()

    # Đóng kết nối
    mysql_cur.close()
    mysql_conn.close()
    client.close()
    
    
def sheet_extract():
    db_config = connect_mysql.mysql_config

    # Kết nối tới cơ sở dữ liệu
    cnx = mysql.connector.connect(**db_config)
    query = """SELECT 
                title as Job,
                company_name as Company,
                job_level,
                skills,
                job_type,
                salary,
                DATE_FORMAT(published, '%Y-%m-%d') AS published,
                DATE_FORMAT(refreshed, '%Y-%m-%d') AS refreshed
            FROM topdev.jobs;"""
    df = pd.read_sql(query, cnx)
    cnx.close()


    # Xác thực và ủy quyền ứng dụng
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('D:\VScode\DE\crawl_data\proj_topdev\seed\zinc-transit-427014-m5-38621fde1e12.json', scope)
    client = gspread.authorize(creds)

    # Tạo một Google Sheets mới
    spreadsheet = client.open('PY to Gsheet Test')
    # Lấy sheet đầu tiên từ Google Sheets mới tạo
    sheet = spreadsheet.sheet1
    # Ghi dữ liệu từ DataFrame vào Google Sheets
    sheet.delete_rows()
    sheet.update([df.columns.values.tolist()] + df.values.tolist())
    
    print("Dữ liệu đã được chuyển thành công sang Google Sheets.")
    
def job():
    extract_mongo(1)
    extract_mysql()
    sheet_extract()
job()
# schedule.every().day.at("22:00").do(job)
# while True:
#     schedule.run_pending()
#     time.sleep(1)
