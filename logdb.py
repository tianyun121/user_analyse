from selenium import webdriver
import os
import time
from sqlalchemy import create_engine
import pymysql
import pandas as pd
import datetime
user = "etl_user"
password = "189TREhiu"
host = "172.27.2.14"
port = 3306
database_bi_report = "bi_report"
chromedriver = '/usr/local/bin/chromedriver'
os.environ["webdriver.chrome.driver"] = chromedriver

# 查询
def get_database_data(sql):
    conn = pymysql.connect(host=host, port= port, user =user, password = password, db = database_bi_report, charset="utf8")
    cursor = conn.cursor()
    cursor.execute( sql)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    df = pd.DataFrame(list(data))
    return  df
#插入
def insert_bi_report(table_name,data):
    print('mysql+pymysql://'+ user+":"+ password+"@"+str(host)+":"+str(port)+"/"+ database_bi_report+'?charset=utf8')
    engine = create_engine('mysql+pymysql://'+user+":"+password+"@"+str(host)+":"+str(port)+"/"+ database_bi_report+'?charset=utf8')
    pd.io.sql.to_sql(data, table_name, con = engine, schema=database_bi_report,if_exists='append',index=False)

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--headless") # 隐藏界面
def run_pro():
    try:
        driver = webdriver.Chrome(chromedriver,options=chrome_options) #模拟打开浏览器
        driver.get(r"http://172.27.2.8:8080/webroot/decision/view/report?viewlet=%E7%9B%91%E6%8E%A7%E6%98%8E%E7%BB%86.cpt&op=write") #打开网址
        time.sleep(30) #页面加载休眠时间
        if (len(driver.find_elements_by_xpath('//table[@id="0"]')) == 0):
            print("未访问到界面", time.strftime('%Y.%m.%d %H:%M:%S ', time.localtime(time.time())))
        else:
            element = driver.find_element_by_class_name('x-table')
            print("开始时间", time.strftime('%Y.%m.%d %H:%M:%S ', time.localtime(time.time())))
            table_tr_list = element.find_elements_by_tag_name('tr')
            list2 = []
            sql = "SELECT max(time) FROM `fine_record_execute`"
            max_time =get_database_data(sql).iloc[0,0]
            for r, tr in enumerate(table_tr_list, 1):
                list1 = [] # 存储行
                # 将表的每一行的每一列内容存在table_td_list中
                table_td_list = tr.find_elements_by_tag_name('td')
                # 写入表的内容到sheet 1中，第r行第c列
                if((table_td_list[0].text > max_time) & (table_td_list[0].text !="时间")): #判断第一行数据是否大于数据库中最大值且不是“时间”
                    for c, td in enumerate(table_td_list):
                        list1.append(td.text)
                    if(len(list1)!=0):
                        list2.append(list1)
            data_2 = pd.DataFrame(list2)
            if(len(data_2.columns) == 19):
                data_2.columns = ["time", "id", "tname", "displyName", "type", "param", "ip", "username", "userrole", "consume",
                                "sql", "sqlTime", "browser", "memory", "reportId", "source", "sessionID", "complete", "usrId"]
                data_2 = data_2.dropna(subset=["id"])
                data_2 = data_2.dropna(subset=["time"])
                insert_bi_report("fine_record_execute", data_2)
            else:
                print("异常", time.strftime('%Y.%m.%d %H:%M:%S ', time.localtime(time.time())))
    except BaseException as  e:
        print(e)
    finally:
        print("结束时间", time.strftime('%Y.%m.%d %H:%M:%S ', time.localtime(time.time())))
        driver.close()

if __name__ == '__main__':
    while True:
        time_hour = int(str(datetime.datetime.now())[11:13]) #爬取时间从6点到24点
        if(time_hour>=6):
            # print("true")
            run_pro()
            time.sleep(60)
        else :
            time.sleep(60)




