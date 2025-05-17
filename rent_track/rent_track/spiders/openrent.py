import os
import scrapy
import logging
import re
from datetime import datetime
from .RentSpider import RentSpider

logging.getLogger("scrapy-playwright").setLevel(logging.INFO)

os.chdir(os.path.dirname(os.path.abspath(__file__)))

NON_POSTCODE_THRESHOLD = 10


class OpenrentSpider(RentSpider):
    name = "openrent"
    download_delay = 3

    def start_requests(self):
        property_rows = [row for row in self.cur.execute("SELECT postcode FROM postcodes").fetchall()]
        for idx, (postcode,) in enumerate(property_rows):
            yield self._gen_request(postcode)

    async def parse(self, response):
        page = response.meta["playwright_page"]
        num_non_postcode = 0

        card_count = 0
        end_reached = False
        while not end_reached:
            await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
            card_count += 20
            try:
                await page.wait_for_selector(f"#property-data a:nth-child({card_count + 1})", timeout=2000)
            except:
                end_reached = True

        s = scrapy.Selector(text=await page.content())
        await page.close()

        for card in s.css(".lpcc"):
            address = card.css(".listing-title").xpath("text()").get().replace("'", "''")
            if num_non_postcode > NON_POSTCODE_THRESHOLD or len(card.css(".let-agreed")):
                break
            elif response.meta["postcode"] not in address:
                num_non_postcode += 1
                continue

            location_detail = card.css(".location-detail")
            num_bedrooms = re.sub("[^0-9]", "", location_detail.xpath(".//li[contains(text(),'Bed')]/text()").get() or "1")
            num_bathrooms = re.sub("[^0-9]", "", location_detail.xpath(".//li[contains(text(),'Bath')]/text()").get())
            price = re.sub("[^0-9.]", "", card.css(".price-location").xpath(".//h2/text()").get())
            property_id = card.xpath("../@id").get()
            property_href = card.xpath("../@href").get()
            url = f"https://www.openrent.co.uk{property_href}"
            now = datetime.now()

            self.cur.execute(f"""
                INSERT INTO timeline VALUES
                    ('{address}', '{response.meta["postcode"]}', {price}, {num_bedrooms}, {num_bathrooms}, NULL, '{now.isoformat()}', {now.timestamp()}, '{url}', '{property_id}')
            """)

            yield dict(
                address=address,
                num_bedrooms=num_bedrooms,
                num_bathrooms=num_bathrooms,
                price_pcm=price,
                id=id,
                url=url,
            )

    def _gen_request(self, postcode):
        url = f"https://www.openrent.co.uk/properties-to-rent/{postcode}?prices_max=20000&bedrooms_min=0"
        return scrapy.Request(
            url=url,
            callback=self.parse,
            meta=dict(
                postcode=postcode,
                playwright=True,
                playwright_include_page=True,
            ),
            errback=self.close_page,
        )

    async def close_page(self, failure):
        page = failure.request.meta["playwright_page"]
        await page.close()


