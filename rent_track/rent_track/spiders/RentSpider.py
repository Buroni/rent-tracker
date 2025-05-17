import scrapy
import sqlite3


class RentSpider(scrapy.Spider):
    download_delay = 2
    user_agent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"
    custom_settings = {
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    }

    def __init__(self):
        self.con = sqlite3.connect("../../../db/uk-rent.db")
        self.cur = self.con.cursor()

    def closed(self, *a):
        self.con.commit()