<p align="center">
    <a target="_blank" href="https://www.python.org/downloads/release/python-3810/"><img src="https://img.shields.io/badge/Python-3.x-blue.svg" /></a>
    <a target="_blank" href='https://github.com/fangzheng0518/lianjia_spider'><img src="https://img.shields.io/github/stars/fangzheng0518/lianjia_spider.svg?style=social"/></a>
    <a target="_blank" href="LICENSE"><img src="https://img.shields.io/:license-GPLv3-blue.svg"></a>
</p>

# 房天下小区信息爬取

[简介](#简介) | [特性](#特性) | [技术架构](#技术架构) | [快速开始](#快速开始) | [使用说明](#使用说明) | [免责声明](#免责声明) | [附录](#附录)

# 简介

一个基于进程池的链家网快速爬虫项目，严禁将所得数据商用！

# 特性

- [x] 支持Python3.6+版本
- [x] 支持数据存储sqliteDB中
- [x] 支持导出Excel
- [x] 支持自定义区域（省、市、区）采集

# 技术架构

- Python3.6+
- request
- lxml的xpath解析
- sqlite

## 实现原理

1. 打开一个需要滑块验证码的URL，手工进行滑块验证
2. 验证通过后，通过playwright保存cookie.json
3. 再次使用cookie.json打开网站，requests请求时携带刚才的cookie进行请求
4. 根据请求结果进行解析HTML源码


# 快速开始

1. clone本项目代码
2. 在项目根目录创建Python虚拟环境venv
    1. `cd lianjia_spider`
    2. `pip install virtualenv`
    3. `virtualenv venv`
3. 安装依赖库`pip install -r requirements.txt`
4. 运行`python lianjia.py`
5. 根据提示输入对应信息采集

# 使用说明

## 数据存储

- 程序运行时会使用sqlite数据库存储当前job运行历史，数据库文件`lianjia.db`

## 采集更多信息

目前程序只测试采集每个小区的楼栋数，小区数，可根据需要修改代码采集更多字段
可修改`get_community_detail`函数中`xiaoqu_info.xpath`获取的部分代码

```python
def get_community_detail(url):
    response = requests.get(url)
    if response.status_code == 200:
        final_result = []
        tree = etree.HTML(response.text)
        xiaoquInfoItems = tree.xpath('//div[@class="xiaoquInfoItem"]')
        for xiaoqu_info in xiaoquInfoItems:
            label = xiaoqu_info.xpath('.//span[@class="xiaoquInfoLabel"]/text()')[0]
            value = xiaoqu_info.xpath('.//span[@class="xiaoquInfoContent"]/text()')[0]
            final_result.append({
                "label": label,
                "value": value
            })
        return final_result
```

## 运行截图

- 运行截图
  ![整体截图](example/run1.png "运行截图")
- 运行结果
  ![整体截图](example/result.png "运行结果")
- 统计信息
  ![整体截图](example/tongji.png "统计信息")



# 免责声明
请勿将`lianjia_spider`应用到任何可能会违反法律规定和道德约束的工作中，请友善使用`lianjia_spider`，遵守蜘蛛协议，不要将`lianjia_spider`用于任何非法用途。如您选择使用`lianjia_spider`即代表您遵守此协议，作者不承担任何由于您违反此协议带来任何的法律风险和损失，一切后果由您承担。
