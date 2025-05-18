import scrapy
import os
import re
import logging
from urllib.parse import urlparse, parse_qs
from scrapy_playwright.page import PageMethod
from datetime import datetime
from .RentSpider import RentSpider

logging.getLogger("scrapy-playwright").setLevel(logging.INFO)

os.chdir(os.path.dirname(os.path.abspath(__file__)))

MAX_BATCH_SIZE = 100

class RightmoveSpider(RentSpider):
    name = "rightmove"
    ids = []
    num_missing_info = 0
    batch = []

    def start_requests(self):
        property_rows = [row for row in self.cur.execute("SELECT location_id, postcode FROM rightmove_postcode_map").fetchall()]
        for idx, (location_id, postcode) in enumerate(property_rows):
            yield self._gen_request(postcode, location_id)

    def parse(self, response):
        self._process_batch(response)

        property_cards = response.css(".PropertyCard_propertyCardContainerWrapper__mcK1Z")

        if property_cards is None or len(property_cards) == 0:
            return

        for card in property_cards:
            property_href = card.xpath(".//a[contains(concat(' ', normalize-space(@class), ' '), ' propertyCard-img-link ')]/@href").get()

            if property_href is None:
                continue

            property_id = re.search("/properties/([0-9]+)", property_href).group(1)
            if property_id in self.ids:
                # Sometimes playwright iterates over the same card twice, not sure why
                continue
            self.ids.append(property_id)

            address = card.xpath(".//address/text()").get().replace("'", "''")
            price = re.sub("[^0-9.]", "", card.xpath(".//*[@class='PropertyPrice_price__VL65t']/text()").get())
            property_info = card.xpath(".//*[@data-test='property-details']")
            property_type = property_info.xpath(".//*[@class='PropertyInformation_propertyType__u8e76']/text()").get()
            full_url = f"https://www.rightmove.co.uk{property_href}"

            if property_type == "Flat Share" or "OpenRent" in card.get():
                # Not interested in rooms in a shared flat
                continue

            try:
                num_bedrooms = "1" if property_type == "Studio" else property_info.xpath(".//*[@class='PropertyInformation_bedroomsCount___2b5R']/text()").get()
            except:
                # Not lived accommodation
                continue

            try:
                num_bathrooms = property_info.xpath(".//*[@class='PropertyInformation_bathContainer__ut8VY']/span/text()").get()
            except:
                # Sometimes Rightmove doesn't list the bathroom number if there's only 1
                num_bathrooms = 1

            if num_bedrooms is None or num_bathrooms is None:
                self.num_missing_info += 1
                continue

            entry = dict(
                address=address,
                price_pcm=price,
                num_bedrooms=num_bedrooms,
                num_bathrooms=num_bathrooms,
                property_type=property_type,
                id=property_id,
                url=full_url,
            )

            self.batch.append(entry)

            yield entry

        next_button = response.xpath("//button[contains(concat(' ', normalize-space(@class), ' '), ' Pagination_button__5gDab ')]").getall()[1]
        if "disabled" not in next_button:
            page_index = parse_qs(urlparse(response.url).query)["index"][0]
            yield self._gen_request(response.meta["postcode"], response.meta["location_id"], int(page_index) + 24)

    def _gen_request(self, postcode, location_id, index=0):
        url = f"https://www.rightmove.co.uk/property-to-rent/find.html?searchType=RENT&locationIdentifier=OUTCODE%5E{location_id}&insId=1&radius=0.0&index={index}"
        return scrapy.Request(
            url=url,
            callback=self.parse,
            meta=dict(postcode=postcode, location_id=location_id, playwright=True, playwright_page_methods=[PageMethod("goto", url, timeout=60000)]) # `playwright_page_methods` used for setting timeout
        )
    
    def _process_batch(self, response):
        if len(self.batch) > MAX_BATCH_SIZE:
            for entry in self.batch:
                address = entry["address"]
                postcode = response.meta["postcode"]
                price = entry["price_pcm"]
                num_bedrooms = entry["num_bedrooms"]
                num_bathrooms = entry["num_bathrooms"]
                property_type = entry["property_type"]
                full_url = entry["url"]
                property_id = entry["id"]
                now = datetime.now()

                self.cur.execute(f"""
                    INSERT INTO timeline VALUES
                        ('{address}', '{postcode}', {price}, {num_bedrooms}, {num_bathrooms}, '{property_type}', '{now.isoformat()}', {now.timestamp()}, '{full_url}', '{property_id}')
                """)
            self.con.commit()
            self.batch = []

            print('Flushed batch')


    def closed(self, *a):
        print(f"Num missing information: {self.num_missing_info}")






