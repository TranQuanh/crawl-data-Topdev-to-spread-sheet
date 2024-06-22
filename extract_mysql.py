import mysql.connector
from datetime import datetime, date
import connect.connect_mongodb as connect_mongodb
import connect.connect_mysql as connect_mysql


collection =connect_mongodb.connect_db()
def extract_mysql():
    mongo_data = collection.find()
    db_config_mysql = connect_mysql.mysql()

    # Kết nối tới cơ sở dữ liệu
    mysql_conn = mysql.connector.connect(**db_config_mysql)
    ## Create cursor, used to execute commands
    mysql_cur = mysql_conn.cursor()
        
    ## Create books table if none exists
    print("Tạo bảng:")
        # Xóa bảng nếu nó tồn tại
    
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
    mysql_cur.execute("""
                   TRUNCATE TABLE jobs
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
    
    