from sqlalchemy import create_engine
import pymysql
from datetime import datetime

pymysql.install_as_MySQLdb()

user_nm = "root"
user_pw = "ss019396"

host_nm = "127.0.0.1:3306"

engine = create_engine("mysql+mysqldb://"+user_nm+":"+user_pw+"@"+host_nm, encoding="utf-8")

conn = engine.connect()

date_today = datetime.today().strftime("%Y-%m-%d")