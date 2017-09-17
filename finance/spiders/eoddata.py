from datetime import datetime
import re
import scrapy
import logging

import requests
from scrapy.http import HtmlResponse
from finance.items import EODQuote


logger = logging.getLogger(__name__)

def sanitize(s):
    return s.strip().replace(',', '')

class EodDataSpider(scrapy.Spider):
    name = "eoddata"
    matcher = re.compile(r".*\[(.+)\].*")

    def start_requests(self):
        urls = [
            'http://eoddata.com/stocklist/AMEX/A.htm',
            'http://eoddata.com/stocklist/ASX/A.htm',
            'http://eoddata.com/stocklist/AMS/A.htm',
            'http://eoddata.com/stocklist/BRU/A.htm',
            'http://eoddata.com/stocklist/CBOT/A.htm',
            'http://eoddata.com/stocklist/CFE/Q.htm',
            'http://eoddata.com/stocklist/CME/A.htm',
            'http://eoddata.com/stocklist/COMEX/A.htm',
            'http://eoddata.com/stocklist/EUREX/A.htm',
            'http://eoddata.com/stocklist/FOREX/A.htm',
            'http://eoddata.com/stocklist/HKEX/A.htm',
            'http://eoddata.com/stocklist/INDEX/A.htm',
            'http://eoddata.com/stocklist/KCBT/A.htm',
            'http://eoddata.com/stocklist/LIFFE/A.htm',
            'http://eoddata.com/stocklist/LIS/A.htm',
            'http://eoddata.com/stocklist/LSE/A.htm',
            'http://eoddata.com/stocklist/MGEX/A.htm',
            'http://eoddata.com/stocklist/MLSE/A.htm',
            'http://eoddata.com/stocklist/MSE/A.htm',
            'http://eoddata.com/stocklist/NASDAQ/A.htm',
            'http://eoddata.com/stocklist/NYBOT/A.htm',
            'http://eoddata.com/stocklist/NYMEX/A.htm',
            'http://eoddata.com/stocklist/NYSE/A.htm',
            'http://eoddata.com/stocklist/NZX/A.htm',
            'http://eoddata.com/stocklist/OTCBB/A.htm',
            'http://eoddata.com/stocklist/PAR/A.htm',
            'http://eoddata.com/stocklist/SGX/A.htm',
            'http://eoddata.com/stocklist/TSX/A.htm',
            'http://eoddata.com/stocklist/TSXV/A.htm',
            'http://eoddata.com/stocklist/USMF/A.htm',
            'http://eoddata.com/stocklist/WCE/A.htm',
        ]

        # Load pages
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse_date(self, response):
        if not 'data_dates' in dir(self):
            self.data_dates = {}

        exchange = response.url.split('/')[-2]
        dt = response.xpath("//table[contains(@class, 'rc_t')]/tr/td[@nowrap][@style]/text()").extract_first()
        self.data_dates[exchange] = dt
        
    def parse(self, response):
        """
        Parse data
        """
        # Get the exchange
        exchange = response.xpath("//table[contains(@class, 'rc_t')]/tr/td/div/text()").extract_first()
        exchange = self.matcher.match(exchange).group(1)

        # Get all quote rows
        table = response.xpath("//table[contains(@class, 'quotes')]")[0]
        rows = table.xpath('tr')

        # Extract the header
        header = rows[0].xpath('th/text()').extract()

        # Get individual values
        for row in rows[1:]:
            try:
                cols = row.xpath('td')

                if not 'data_dates' in dir(self) \
                   or not exchange in self.data_dates:
                    url = response.urljoin(cols[0].xpath('a/@href').extract_first())
                    resp = requests.get(url)
                    self.parse_date(HtmlResponse(url, status=resp.status_code,
                                                 headers=resp.headers, body=resp.content))

                ticker = cols[0].xpath('a/text()').extract_first()
                name = cols[1].xpath('text()').extract_first()
                high = cols[2].xpath('text()').extract_first()
                low = cols[3].xpath('text()').extract_first()
                close = cols[4].xpath('text()').extract_first()
                volume = cols[5].xpath('text()').extract_first()

                # format raw data
                res = {'ticker': ticker,
                       'date': self.data_dates[exchange],
                       'exchange': exchange,
                       'name': name,
                       'high': float(sanitize(high)),
                       'low': float(sanitize(low)),
                       'close': float(sanitize(close)),
                       'volume': int(sanitize(volume)),
                       'source': 'eoddata.com'
                }
                res = EODQuote(res)
                yield res
            except Exception as e:
                logger.warning("Error parsing row: " + str(e) + " - " + row.extract())

        # get links to other pages
        pages = response.xpath("//table[contains(@class, 'lett')]/tr/td[contains(@class, 'ld')]/a/@href").extract()
        for page in pages:
            yield scrapy.Request(response.urljoin(page), callback=self.parse)
