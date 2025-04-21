# _*_ coding : utf-8 _*_
# @Time : 2025/3/31 19:15
# @Author : 张振哲
# @File : spider_lianjia_每爬取10页存档一次
# @Project : spider_project


import asyncio
import aiohttp
from lxml import etree
import pandas as pd
import logging
import datetime
import openpyxl
import random

wb = openpyxl.Workbook()
sheet = wb.active
sheet.append(['房源', '房子信息', '所在区域', '单价', '关注人数和发布时间', '标签'])
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
start = datetime.datetime.now()


class Spider(object):
    def __init__(self):
        self.semaphore = asyncio.Semaphore(1)  # 信号量，控制协程数，防止爬得过快被反爬
        self.delay_range = (0.5, 3)  # 随机延时的范围
        self.header = {
            "Host": "bj.lianjia.com",
            "Referer": "https://bj.lianjia.com/ershoufang/",
            'Cookie': 'lianjia_uuid=0a245c01-0249-4548-a9de-66e6367fdd82; _ga=GA1.2.1314715887.1743086781; _ga_EYZV9X59TQ=GS1.2.1743086782.1.1.1743086797.0.0.0; _ga_DX18CJBZRT=GS1.2.1743086782.1.1.1743086797.0.0.0; _ga_BKB2RJCQXZ=GS1.2.1743088349.1.0.1743088349.0.0.0; Qs_lvt_200116=1743090960; Qs_pv_200116=3209232641914898400%2C2610558853758770700%2C2860350900287569000%2C2313803583990493000; _ga_E91JCCJY3Z=GS1.2.1743090925.1.1.1743091086.0.0.0; _ga_MFYNHLJT0H=GS1.2.1743090925.1.1.1743091086.0.0.0; Hm_lvt_46bf127ac9b856df503ec2dbf942b67e=1743086769,1743327950; HMACCOUNT=37989191F901F592; _jzqc=1; _qzjc=1; _gid=GA1.2.1869468630.1743327970; crosSdkDT2019DeviceId=-2gp41z-o3lzyu-63pkn8jey4ausm7-e52x58qto; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%22195d81156b0581-08cc2e949a02e-4c657b58-1821369-195d81156b125a3%22%2C%22%24device_id%22%3A%22195d81156b0581-08cc2e949a02e-4c657b58-1821369-195d81156b125a3%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E8%87%AA%E7%84%B6%E6%90%9C%E7%B4%A2%E6%B5%81%E9%87%8F%22%2C%22%24latest_referrer%22%3A%22https%3A%2F%2Fcn.bing.com%2F%22%2C%22%24latest_referrer_host%22%3A%22cn.bing.com%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC%22%7D%7D; select_city=110000; _jzqckmp=1; lfrc_=7ac40290-a05f-4af6-a343-5cf68223992f; lianjia_ssid=cef6d138-2a67-4902-825c-acca81cd9b18; hip=ZqJNHesc-q8M-cLKVXS1DczLesgAfOukX5k-TW536DddyCxdjzFacxTuoT6OFIL9zPCvPoLBTCWdqch3wo14TobVAlrneXVkD5LWjb5-nAG_Okvr3CEe251SMxD8pkD4Vk97E8DHYOWUUedxHu-_FUOV3EytI2WFqr0wcAcr75y_KMth0Pay2WVBDA%3D%3D; _jzqa=1.2936646379323121700.1743086770.1743413095.1743417476.5; _jzqx=1.1743086770.1743417476.3.jzqsr=hip%2Elianjia%2Ecom|jzqct=/.jzqsr=bj%2Elianjia%2Ecom|jzqct=/; login_ucid=2000000475594965; lianjia_token=2.00143e639042ce0ea905934aa172e74f88; lianjia_token_secure=2.00143e639042ce0ea905934aa172e74f88; security_ticket=kJTW0O78ZiEbLJdC52BMmhOVWkmk+JRY2VbTz+6M2fz1Vedzx57t0Cl+jZLbTTOORFvFDMaoSp6PYHT3+SQQLgTcNO41eMPcXvJsnkTzR9Hqr2n5WPsbYyuvEOcmJosQmPuEdxqV3I/kaNM5vhlwEd9fjJFTlx5FHUZkfBZVb6E=; ftkrc_=49b169b',
            'User-Agent': '"User - Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"'

        }

    async def scrape(self, url):
        async with self.semaphore:
            delay = random.uniform(*self.delay_range)
            await asyncio.sleep(delay)  # 延时
            timeout = aiohttp.ClientTimeout(total=60)  # 设置超时时间为60秒
            async with aiohttp.ClientSession(headers=self.header, timeout=timeout) as session:
                try:
                    response = await session.get(url)
                    response.raise_for_status()  # 检查请求是否成功
                    return await response.text()
                except asyncio.TimeoutError:
                    logging.error(f"Timeout occurred for {url}")
                except aiohttp.ClientError as e:
                    logging.error(f"Request failed for {url} with error: {e}")
                return None

    async def scrape_index(self, page):
        url = f'https://bj.lianjia.com/ershoufang/pg{page}/'
        text = await self.scrape(url)
        if text:
            await self.parse(text)

    async def parse(self, text):
        html = etree.HTML(text)
        lis = html.xpath('//*[@id="content"]/div[1]/ul/li')
        for li in lis:
            try:
                house_data = li.xpath('.//div[@class="title"]/a/text()')[0]  # 房源
                house_info = li.xpath('.//div[@class="houseInfo"]/text()')[0]  # 房子信息
                address = ' '.join(li.xpath('.//div[@class="positionInfo"]/a/text()'))  # 位置信息
                price = li.xpath('.//div[@class="priceInfo"]/div[2]/span/text()')[0]  # 单价 元/平米
                attention_num = li.xpath('.//div[@class="followInfo"]/text()')[0]  # 关注人数和发布时间
                tag = ' '.join(li.xpath('.//div[@class="tag"]/span/text()'))  # 标签
                sheet.append([house_data, house_info, address, price, attention_num, tag])
                logging.info([house_data, house_info, address, price, attention_num, tag])

                # 每次爬取完一页后保存文件
                file_name = 'house_data_8.xlsx'
                wb.save(file_name)
                logging.info('Data saved to ' + file_name)

            except IndexError:
                continue  # 忽略空白或错误的房源数据

    async def main(self, start_page, stop_page):
        scrape_index_tasks = []
        for page in range(start_page, stop_page + 1):
            scrape_index_tasks.append(asyncio.ensure_future(self.scrape_index(page)))

        # 执行所有页面爬取任务
        loop = asyncio.get_event_loop()
        tasks = asyncio.gather(*scrape_index_tasks)
        await tasks


if __name__ == '__main__':
    spider = Spider()
    asyncio.run(spider.main(start_page=0, stop_page=50))  # 设置爬取页数为1到500
    end = datetime.datetime.now()
    logging.info(f"爬取结束, 总耗时: {end - start}")


