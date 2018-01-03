# scrapes completed listings

import scrapy


class CarMaxSpider(scrapy.Spider):
    name = 'carmax'

    custom_settings = {
        "DOWNLOAD_DELAY": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "HTTPCACHE_ENABLED": True
    }

# Used cars, 'Buy it Now', no zip code specified

    start_urls = ['https://www.carmax.com/search#Distance=all&ExposedCategories=249+250+1001+1000+265+999+772&ExposedDimensions=249+250+1001+1000+265+999+772&Page=1&PerPage=50&SortKey=0&Zip=10022']


    def parse(self, response):
        # Extract links to each car listing
        for href in response.xpath(
                '//h3[@class="lvtitle"]/a/@href'
        ).extract():
            # For each car link, call 'parse_car' (defined later)
            yield scrapy.Request(
                url=href,
                callback=self.parse_car,
                meta={'url': href}
            )
        # Follow pagination links and repeat
        next_url = response.xpath(
            '//td[@class="pagn-next"]/a/@href'
        ).extract()[0]

        yield scrapy.Request(
            url=next_url,
            callback=self.parse
        )

    def parse_car(self, response):

        title = response.xpath('//h1/text()').extract()

        subtitle = [subtl.strip() for subtl in response.xpath('//h2[@class="it-sttl"]/text()').extract()]

        winningbid = response.xpath('//div[@class="u-flL w29 vi-price-np"]/span[@id]/text()').extract()
        buy_it_now_price = [bin.strip() for bin in response.xpath('//div[@class="u-flL w29 vi-price"]/span[@id]/text()').extract()]

        num_bids = response.xpath('//a[@class="vi-bidC"]/span/text()').extract_first()

        url = response.request.meta['url']

        #cats = [cat.strip() for cat in response.xpath('//td[@class="attrLabels"]/text()').extract()]
        #vals = [val.strip() for val in response.xpath('//td[@class="attrLabels"]/following-sibling::td/descendant::*/text()').extract()]
        catsvals = [catval.strip() for catval in response.xpath('//div[@class="section"]//tr//text()').extract()]

        yield {
            'title': title,
            'subtitle' : subtitle,
            'winningbid' : winningbid,
            'buy_it_now_price':buy_it_now_price,
            'num_bids' : num_bids,
            'url': url,
            #'cats': cats,
            #'vals': vals,
            'catsvals': catsvals
        }

    











