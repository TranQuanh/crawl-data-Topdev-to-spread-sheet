import extract_mongo as mongo
import extract_mysql as mysql
import extract_googlesheet as googlesheet
import connect.connect_mongodb as connect_mongodb


collection =connect_mongodb.connect_db()
collection.drop()
mongo.extract_mongo(1)
mysql.extract_mysql()
googlesheet.sheet_extract()