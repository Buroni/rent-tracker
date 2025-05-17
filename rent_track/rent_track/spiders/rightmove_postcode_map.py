import scrapy
import sqlite3
import os
import re

os.chdir(os.path.dirname(os.path.abspath(__file__)))


class RightmovePostcodeMapSpider(scrapy.Spider):
    name = "rightmove-postcode-map"
    download_delay = 0.5
    user_agent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"

    def __init__(self):
        self.con = sqlite3.connect("../../../db/uk-rent.db")
        self.cur = self.con.cursor()

    def start_requests(self):
        postcodes = [pc[0].lower() for pc in self.cur.execute("SELECT postcode FROM postcodes").fetchall()]
        urls = [f"https://www.rightmove.co.uk/house-prices/{postcode}.html" for postcode in postcodes]
        for idx, url in enumerate(urls):
            yield scrapy.Request(url=url, callback=self.parse, meta=dict(postcode=postcodes[idx]))

    def parse(self, response):
        state_obj = response.xpath("//script[contains(text(), 'window.PAGE_MODEL')]").getall()[0]
        print(state_obj)
        location_id = re.search("OUTCODE\^([0-9]+)", state_obj).group(1)

        self.cur.execute(f"""
            INSERT INTO rightmove_postcode_map VALUES
                ('{response.meta["postcode"]}', '{location_id}')
        """)

        yield dict(postcode=response.meta["postcode"], location_id=location_id)

    def closed(self, *a):
        self.con.commit()
