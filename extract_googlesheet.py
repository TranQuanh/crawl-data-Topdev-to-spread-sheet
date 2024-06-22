import connect.connect_mysql as connect_mysql
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
import mysql.connector

def sheet_extract():
    db_config = connect_mysql.mysql_config()

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
    sheet.clear()
    sheet.update([df.columns.values.tolist()] + df.values.tolist())
    
    print("Dữ liệu đã được chuyển thành công sang Google Sheets.")
    