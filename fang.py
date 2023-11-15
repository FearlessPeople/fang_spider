# -*- coding: utf-8 -*-
import json
import os
import sqlite3
import sys
import textwrap
import time
from datetime import datetime

import pandas as pd
import requests
from lxml import etree
from playwright.sync_api import sync_playwright

cookies_path = "./fang_cookies.json"
housing_url = 'https://gz.esf.fang.com/housing/'
default_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}
DEBUG = False


class SQLiteDB:
    def __init__(self, db_file='fang.db'):
        self.conn = sqlite3.connect(db_file, check_same_thread=False)
        self.cursor = self.conn.cursor()

    def check_cursor(self):
        if self.cursor is None:
            self.cursor = self.conn.cursor()
        return self.cursor

    def close(self):
        if self.conn:
            self.cursor.close()
            self.conn.close()

    def execute(self, sql, params=()):
        self.check_cursor()
        self.cursor.execute(sql, params)
        self.conn.commit()

    def query(self, sql):
        self.check_cursor()
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()

        columns = [column[0] for column in self.cursor.description]

        result = []
        for row in rows:
            result.append(dict(zip(columns, row)))

        return result

    def commit(self):
        self.conn.commit()

    def insert(self, table, data, echo=False):
        fields = ', '.join(data.keys())
        placeholders = ', '.join('?' * len(data))
        sql = f'INSERT INTO {table} ({fields}) VALUES ({placeholders})'
        data_values = tuple(data.values())
        if echo:
            formatted_sql = sql
            for value in data_values:
                formatted_sql = formatted_sql.replace('?', f"'{value}'", 1)
            Print.print2(formatted_sql)
        self.execute(sql, data_values)

    def batch_insert(self, table, data):
        if len(data) == 0:
            raise Exception("Data is null")
        keys = data[0].keys()
        placeholders = ','.join(':' + key for key in keys)
        insert_statement = f'INSERT INTO {table} ({",".join(keys)}) VALUES ({placeholders})'
        self.check_cursor()
        for item in data:
            self.cursor.execute(insert_statement, item)
        self.conn.commit()

    def upsert(self, table, data):
        placeholders = ", ".join(["?"] * len(data))
        columns = ", ".join(data.keys())
        values = tuple(data.values())
        sql = f"INSERT OR REPLACE INTO {table} ({columns}) VALUES ({placeholders})"
        self.execute(sql, values)

    def update(self, table, data, condition):
        set_fields = ', '.join([f'{k}=?' for k in data.keys()])
        sql = f'UPDATE {table} SET {set_fields} WHERE {condition}'
        self.execute(sql, tuple(data.values()))

    def delete(self, table, condition):
        self.check_cursor()
        sql = f'DELETE FROM {table} WHERE {condition}'
        self.execute(sql)

    def count(self, table):
        self.check_cursor()
        self.cursor.execute(f"SELECT count(*) FROM {table};")
        return self.cursor.fetchall()[0][0]

    def select(self, table, condition=''):
        sql = f'SELECT * FROM {table}'
        if condition:
            sql += f' WHERE {condition} ;'
        return self.query(sql)


db = SQLiteDB()


class Print:
    @staticmethod
    def red(text):
        print("\033[31m" + text + "\033[0m")

    @staticmethod
    def green(text):
        print("\033[32m" + text + "\033[0m")

    @staticmethod
    def yellow(text):
        print("\033[33m" + text + "\033[0m")

    @staticmethod
    def blue(text):
        print("\033[34m" + text + "\033[0m")

    @staticmethod
    def magenta(text):
        print("\033[35m" + text + "\033[0m")

    @staticmethod
    def cyan(text):
        print("\033[36m" + text + "\033[0m")

    @staticmethod
    def print2(*args, **kwargs):
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{current_time}]", *args, **kwargs)


class Cookies:
    def __init__(self, cookie_path):
        self.cookie_path = cookie_path
        self.cookies = self._load_cookies()

    def _load_cookies(self):
        try:
            with open(self.cookie_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(e)
            Print.red("Failed to load cookies file")
            Print.red(e.args[0])

    def convert_to_http_header(self, cookies=None, filter_dict=None):
        if cookies:
            input_list = cookies
        else:
            input_list = self.cookies['cookies']
        header_string = ''
        for item in input_list:
            if filter_dict and all(item.get(key) == value for key, value in filter_dict.items()):
                if 'name' in item and 'value' in item:
                    header_string += f"{item['name']}={item['value']};"
            elif not filter_dict:
                if 'name' in item and 'value' in item:
                    header_string += f"{item['name']}={item['value']};"
        return header_string


class HttpUtils:

    @staticmethod
    def get(url, params=None, headers=None, timeout=None):
        try:
            response = requests.get(url=url, params=params, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            Print.print2(f'Error in GET request: {e}')
        except Exception as e:
            Print.print2(f'Error in GET request: {e}')

    @staticmethod
    def post(url, *args, **kwargs):
        try:
            response = requests.post(url, *args, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            Print.print2(f'Error in POST request: {e}')
        except Exception as e:
            Print.print2(f'Error in GET request: {e}')

    @staticmethod
    def get_header_cookies(cookies_path):
        """
        根据一个cookies.json文件生成Request的header中cookies数据
        :param cookies_path:
        :return:
        """
        cookie = Cookies(cookies_path)
        cookies = cookie.cookies['cookies']
        return cookie.convert_to_http_header(cookies=cookies)

    @staticmethod
    def get_by_cookies(url, params, cookies_path):
        """
        发送get请求（必须携带cookies_path）
        :param url:
        :param params:
        :param cookies_path:
        :return:
        """
        headers = {
            'Cookie': HttpUtils.get_header_cookies(cookies_path)
        }
        response = HttpUtils.get(url, headers=headers, params=params)
        return response

    @staticmethod
    def post_by_cookies(url, cookies_path, params=None, payload=None):
        """
        发送post请求（必须携带cookies_path）
        :param url:
        :param cookies_path:
        :param params:
        :param payload:
        :return:
        """
        headers = {
            'Cookie': HttpUtils.get_header_cookies(cookies_path)
        }
        response = HttpUtils.post(url=url, headers=headers, params=params, json=payload)
        return response


class FileUtil:
    @staticmethod
    def file_exists(file_path):
        return os.path.exists(file_path)


def extract_city_id(url):
    if url:
        subdomains = str(url.split('//')[1])
        aaa = subdomains.split('.')
        return aaa[0]
    else:
        return None


def create_table():
    init_sql = """
    CREATE TABLE IF NOT EXISTS `ftx_base_province`
    (
        `id`            INTEGER PRIMARY KEY AUTOINCREMENT,
        `province_name` varchar(255),
        `city_id`       varchar(255),
        `city_name`     varchar(255),
        `city_url`      varchar(255),
        `create_time`   DATETIME DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime')),
        `update_time`   DATETIME DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime'))
    );

    CREATE TABLE IF NOT EXISTS `ftx_base_areas`
    (
        `id`              INTEGER PRIMARY KEY AUTOINCREMENT,
        `city_id`         varchar(255),
        `region_id`       varchar(255),
        `region_name`     varchar(255),
        `region_url`      varchar(255),
        `sub_region_id`   varchar(255),
        `sub_region_name` varchar(255),
        `sub_region_url`  varchar(255),
        `create_time`     DATETIME DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime')),
        `update_time`     DATETIME DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime'))
    );

    CREATE TABLE IF NOT EXISTS `ftx_base_xiaoqu`
    (
        `id`            INTEGER PRIMARY KEY AUTOINCREMENT,
        `city_id`       varchar(255),
        `region_id`     varchar(255),
        `sub_region_id` varchar(255),
        `xiaoqu_id`     varchar(255),
        `xiaoqu_name`   varchar(255),
        `xiaoqu_url`    varchar(255),
        `create_time`   DATETIME DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime')),
        `update_time`   DATETIME DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime'))
    );

    CREATE TABLE IF NOT EXISTS `ftx_xiaoqu_detail`
    (
        `id`          INTEGER PRIMARY KEY AUTOINCREMENT,
        `xiaoqu_id`   varchar(255),
        `fwzs`        varchar(255), -- 房屋总数
        `ldzs`        varchar(255), -- 楼栋总数
        `xqdz`        varchar(255), -- 小区地址
        `create_time` DATETIME DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime')),
        `update_time` DATETIME DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime'))
    );
    """
    for sql in init_sql.split(";"):
        db.execute(sql=sql)


def print_disclaimer():
    message = """
    ######################################################################################################################
                                                   免责声明                                                               
    此工具仅限于学习研究，用户需自己承担因使用此工具而导致的所有法律和相关责任！作者不承担任何法律责任！                 
    ######################################################################################################################
    """
    print(textwrap.dedent(message))
    while True:
        user_input = input("如果您同意本协议, 请输入Y继续: (y/n) ")
        if user_input.lower() == "y":
            return True
        elif user_input.lower() == "n":
            sys.exit(0)


def get_base_province():
    final_result = []
    url = 'https://esf.fang.com/newsecond/esfcities.aspx'
    response = requests.get(url=url, headers=default_headers)
    if response.status_code == 200:
        tree = etree.HTML(response.text)
        province_elements = tree.xpath('//*[@id="c02"]/ul/li')
        for province in province_elements:
            province_name = province.xpath('./strong/text()')
            city_lists = province.xpath('./a')
            for city in city_lists:
                city_name = city.xpath('./text()')[0]
                city_url = city.xpath('./@href')[0]
                city_id = extract_city_id(city_url)
                insertdata = {
                    "province_name": province_name[0],
                    "city_id": str(city_id),
                    "city_name": str(city_name),
                    "city_url": str(city_url)[6:] if province_name[0] == '直辖市' else str(city_url)
                }
                final_result.append(insertdata)
    else:
        Print.print2("获取所有省市区信息失败")
        Print.red(response.text)
    return final_result


def get_base_xiaoqu_list(response_text):
    final_result = []
    tree = etree.HTML(response_text)
    xiaoqu_list = tree.xpath('//*[@class="houseList"]/div')
    if len(xiaoqu_list) > 1:
        del xiaoqu_list[-1]
        for xiaoqu in xiaoqu_list:
            xiaoqu_name = xiaoqu.xpath('./dl/dd/p[1]/a[1]/text()')[0]
            xiaoqu_url = xiaoqu.xpath('./dl/dd/p[1]/a[1]/@href')[0]
            final_result.append({
                "xiaoqu_id": str(xiaoqu_url).split("/")[2][:-4],
                "xiaoqu_name": str(xiaoqu_name),
                "xiaoqu_url": str(xiaoqu_url)
            })
    return final_result


def get_base_xiaoqu(page, url):
    final_result = []
    page.goto(url)
    while True:
        response_text = page.content()
        temp_xiaoqu_list = get_base_xiaoqu_list(response_text)
        final_result.extend(temp_xiaoqu_list)
        next_button = page.locator('text=下一页')
        if not next_button.is_visible():
            break
        next_button.click()
        page.wait_for_load_state("load")
    return final_result


def get_sub_region(page, url):
    final_result = []
    page.goto(url)
    response_text = page.content()
    tree = etree.HTML(response_text)
    a_elements = tree.xpath('//*[@id="shangQuancontain"]/a')
    for a in a_elements:
        region_name = a.xpath('./text()')[0]
        if region_name != '不限':
            sub_region_name = a.xpath('./text()')[0]
            sub_region_url = a.xpath('./@href')[0]
            sub_region_id = sub_region_url
            final_result.append({
                "sub_region_id": sub_region_id,
                "sub_region_name": sub_region_name,
                "sub_region_url": url.split("housing")[0] + sub_region_url[1:]
            })
    return final_result


def get_base_areas(page, url):
    final_result = []
    page.goto(url)
    response_text = page.content()
    tree = etree.HTML(response_text)
    a_elements = tree.xpath('//*[@class="qxName"]/a')
    for a in a_elements:
        region_name = a.xpath('./text()')[0]
        if region_name != '不限':
            region_url = a.xpath('./@href')[0]
            region_id = region_url
            region = {
                "region_id": region_id,
                "region_name": region_name,
                "region_url": url + region_url[9:]
            }
            sub_regions = get_sub_region(page=page, url=region['region_url'])
            if sub_regions:
                for sub_region in sub_regions:
                    final_result.append({**region, **sub_region})
            else:
                region['sub_region_id'] = region['region_id']
                region['sub_region_name'] = region['region_name']
                region['sub_region_url'] = region['region_url']
                final_result.append(region)
    return final_result


def to_excel(province_name, city, area):
    current_timestamp = time.time()
    current_timestamp = int(current_timestamp)
    file_path = f'{province_name}数据_{current_timestamp}.xlsx'
    lj_base_areas_sql = f"ftx_base_areas"
    if city:
        lj_base_province_sql = f"(select * from ftx_base_province where province_name='{province_name}' and city_name='{city}' )"
        file_path = f'{province_name}-{city}数据_{current_timestamp}.xlsx'
        if area:
            lj_base_areas_sql = f"(select * from ftx_base_areas t where region_name='{area}')"
            file_path = f'{province_name}-{city}-{area}数据_{current_timestamp}.xlsx'
    else:
        lj_base_province_sql = f"(select * from ftx_base_province where province_name='{province_name}' )"

    sql = f'''
    select 
       lbp.province_name   as `省份`
     , lbp.city_name       as `城市`
     , lba.city_id         as `城市ID`
     , lba.region_id       as `区域ID`
     , lba.region_name     as `区域名称`
     , lba.sub_region_id   as `子区域ID`
     , lba.sub_region_name as `子区域名称`
     , "`" || t.xiaoqu_id  as `小区ID`
     , t.xiaoqu_name       as `小区名称`
     , t.xiaoqu_url        as `小区URL`
     , lxd.xqdz            as `小区地址`
     , lxd.fwzs            as `房屋总数`
     , lxd.ldzs            as `楼栋总数`
from ftx_base_xiaoqu t
left join {lj_base_areas_sql} lba on t.city_id = lba.city_id and t.region_id = lba.region_id and t.sub_region_id = lba.sub_region_id
left join {lj_base_province_sql} lbp on lba.city_id = lbp.city_id
left join ftx_xiaoqu_detail lxd on t.xiaoqu_id = lxd.xiaoqu_id
where lbp.province_name = '{province_name}'
group by 
       lbp.province_name 
     , lbp.city_name     
     , lba.city_id      
     , lba.region_id      
     , lba.region_name     
     , lba.sub_region_id   
     , lba.sub_region_name 
     , "`" || t.xiaoqu_id  
     , t.xiaoqu_name      
     , t.xiaoqu_url      
     , lxd.fwzs        
     , lxd.ldzs            
;'''
    query_list = db.query(sql)
    result_df = pd.DataFrame(query_list)
    result_df.to_excel(file_path, index=False)
    Print.print2(f"导出成功:{file_path}")


def db_init(page=None, province_name=None, city_name=None):
    if not province_name:
        raise Exception("未传递省份参数province_name")
    Print.print2(f"开始初始化[{province_name}]省份基础数据...")
    province_list = get_base_province()
    db.delete(table='ftx_base_province', condition=f" province_name='{province_name}' ")
    for province in province_list:
        if province['province_name'] == province_name:
            db.insert(table='ftx_base_province', data=province, echo=DEBUG)

    # 获取省份-城市下所有区域信息
    if city_name:
        condition = f" province_name='{province_name}' and city_name='{city_name}' "
    else:
        condition = f" province_name='{province_name}' "
    city_list = db.select(table='ftx_base_province', condition=condition)
    for city in city_list:
        city_url = city['city_url']
        city_id = city['city_id']
        db.delete(table='ftx_base_areas', condition=f" city_id='{city_id}'")
        url = f"https:{city_url}/housing/"
        areas_list = get_base_areas(page=page, url=url)
        for area in areas_list:
            area['city_id'] = city_id
            region_id = area['region_id']

            db.insert(table='ftx_base_areas', data=area, echo=DEBUG)

            # 获取区域下所有小区信息
            sub_region_id = area['sub_region_id']
            sub_region_url = area['sub_region_url']
            xiaoqu_list = get_base_xiaoqu(page=page, url=sub_region_url)
            if xiaoqu_list:
                for xiaoqu in xiaoqu_list:
                    xiaoqu_id = xiaoqu['xiaoqu_id']
                    xiaoqu['city_id'] = city_id
                    xiaoqu['region_id'] = region_id
                    xiaoqu['xiaoqu_url'] = "https:" + city_url + xiaoqu['xiaoqu_url']
                    xiaoqu['sub_region_id'] = sub_region_id
                    db.delete(table='ftx_base_xiaoqu',
                              condition=f" city_id='{city_id}' and xiaoqu_id='{xiaoqu_id}'")
                    db.insert(table='ftx_base_xiaoqu', data=xiaoqu, echo=DEBUG)
            else:
                Print.print2(f"{sub_region_url}下无小区信息")
    Print.print2(f"[{province_name}]省份下所有城市、区域、子区域、小区信息初始化完成......")


def get_xiaoqu_detail(url):
    # TODO 这里get请求可以使用ip代理
    response = requests.get(url)
    if response.status_code == 200:
        final_result = []
        tree = etree.HTML(response.text)
        label_list = tree.xpath('//*[@id="baseinfo"]/div/ul/li')
        for label in label_list:
            label_key = label.xpath('./span/text()')
            label_value = ''
            if len(label.xpath('./div')) > 0 and label.xpath('./div')[0] is not None:
                if len(label.xpath('./div/p')) and label.xpath('./div/p')[0] is not None:
                    if len(label.xpath('./div/p/b')) > 0 and label.xpath('./div/p/b')[0] is not None:
                        label_value = label.xpath('./div/p/b/text()')
                    else:
                        label_value = label.xpath('./div/p/b/text()')
            if len(label.xpath('./p')) > 0 and label.xpath('./p')[0] is not None:
                if len(label.xpath('./p/span')) > 0 and label.xpath('./p/span')[0] is not None:
                    label_value = label.xpath('./p/span/text()')
                else:
                    label_value = label.xpath('./p/text()')

            if isinstance(label_key, list):
                if len(label_key) > 0:
                    label_key = label_key[0]
                    label_key = "".join(label_key.split())
                else:
                    label_key = ''

            if isinstance(label_value, list):
                if len(label_value) > 0:
                    label_value = label_value[0]
                    label_value = "".join(label_value.split())
                else:
                    label_value = ''
            final_result.append({
                "label": label_key,
                "value": label_value
            })
        return final_result


def get_specific_value(xiaoqu_detail, label):
    for xiaoqu in xiaoqu_detail:
        if xiaoqu['label'] == label:
            return xiaoqu['value']
    return None


def process_list(all_xiaoqu_list):
    list_size = len(all_xiaoqu_list)
    for index, xiaoqu in enumerate(all_xiaoqu_list):
        xiaoqu_id = xiaoqu['xiaoqu_id']
        db.delete(table='ftx_xiaoqu_detail', condition=f" xiaoqu_id = '{xiaoqu_id}'")
        xiaoqu_url = xiaoqu['xiaoqu_url'][:-4] + '/housedetail.htm'
        xiaoqu_detail = get_xiaoqu_detail(url=xiaoqu_url)
        Print.print2(f"({index}/{list_size}) {xiaoqu_url}")
        if xiaoqu_detail:
            insert_detail = {
                'xiaoqu_id': xiaoqu_id,
                'fwzs': get_specific_value(xiaoqu_detail, '房屋总数'),
                'ldzs': get_specific_value(xiaoqu_detail, '楼栋总数'),
                'xqdz': get_specific_value(xiaoqu_detail, '小区地址')
            }
            db.insert(table='ftx_xiaoqu_detail', data=insert_detail)


def spider_by_condition(province, city=None, area=None):
    area_msg = f"{province}"
    ftx_base_areas_sql = f"ftx_base_areas"
    if city:
        area_msg += f"-{city}"
        ftx_base_province_sql = f"(select * from ftx_base_province where province_name='{province}' and city_name='{city}' )"
        if area:
            area_msg += f"-{area}"
            ftx_base_areas_sql = f"(select * from ftx_base_areas t where region_name='{area}')"
    else:
        ftx_base_province_sql = f"(select * from ftx_base_province where province_name='{province}' )"

    sql = f"""
    select
    lbp.province_name
    ,lbp.city_name
    ,lba.city_id
    ,lba.sub_region_id
    ,lba.sub_region_name
    ,t.xiaoqu_id
    ,t.xiaoqu_name
    ,t.xiaoqu_url
    from ftx_base_xiaoqu t
    inner join {ftx_base_areas_sql} lba on t.city_id = lba.city_id and t.region_id=lba.region_id and t.sub_region_id=lba.sub_region_id
    inner join {ftx_base_province_sql} lbp on lba.city_id = lbp.city_id
    left join ftx_xiaoqu_detail lxd on t.xiaoqu_id = lxd.xiaoqu_id
    where 1=1
      and lxd.xiaoqu_id is null
    group by
    lbp.province_name
    ,lbp.city_name
    ,lba.city_id
    ,lba.sub_region_id
    ,lba.sub_region_name
    ,t.xiaoqu_id
    ,t.xiaoqu_name
    ,t.xiaoqu_url
    ;
    """
    Print.print2(sql)
    all_xiaoqu = db.query(sql)
    if all_xiaoqu:
        Print.green(f"开始采集[{area_msg}]区域下数据...")
        process_list(all_xiaoqu)
    else:
        # Print.red(f"[{area_msg}]区域下无小区信息，请先进行区域信息初始化.")
        raise Exception(f"[{area_msg}]区域下无小区信息，请先进行区域信息初始化.")


def main():
    try:
        disclaimer_accepted = print_disclaimer()
        if not disclaimer_accepted:
            exit()
        create_table()
        print("功能选项：\n1. 按区域采集并导出\n2. 区域信息初始化")
        function_choice = input("请输入功能序号: ")
        province = input("请输入省份名称(必填): ")
        city = input("请输入省份下城市名称(可选): ")
        area = input("请输入省份下城市下区域名称(可选): ")
        if province:
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(
                    headless=False,
                    slow_mo=1000,
                    args=['--start-maximized']
                )

                context = browser.new_context(
                    no_viewport=True,
                    accept_downloads=True
                )
                page = context.new_page()
                page.set_default_timeout(200000)
                page.goto(housing_url)
                Print.red("请在20s内滑动验证码......")
                time.sleep(20)

                context.storage_state(path=cookies_path)

                if function_choice == '1':
                    spider_by_condition(province=province, city=city, area=area)
                    to_excel(province, city, area)
                elif function_choice == '2':
                    db_init(page=page, province_name=province, city_name=city)
                context.close()
                browser.close()
        else:
            Print.red("省份名称未输入！")
    except Exception as e:
        print(e)
        exit(1)


if __name__ == "__main__":
    main()
    Print.green("程序运行完成...")
