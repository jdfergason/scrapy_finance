# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import logging

logger = logging.getLogger(__name__)

class DatePipeline(object):
    def process_item(self, item, spider):
        from dateutil.parser import parse as date_parse
        if item['date']:
            item['date'] = date_parse(item['date'])
        return item

class EODPipeline(object):
    alias = {'AMEX': ['NYSE Arca',]}

    def __init__(self, host, keyspace, enum, create_new_securities=False):
        import toml
        self.host = host
        self.keyspace = keyspace
        with open(enum) as fh:
            self.enum_map = toml.load(fh)
        self.create_new_securities = create_new_securities

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            host=crawler.settings.get('CASSANDRA_HOST', None),
            keyspace=crawler.settings.get('CASSANDRA_KEYSPACE', 'pennyvault'),
            enum=crawler.settings.get('ENUM_MAP', '/data/01/pv/apps/database/cassandra/enums.toml'),
            create_new_securities=crawler.settings.get('CREATE_NEW_SECURITIES', False)
        )

    def open_spider(self, spider):
        from cassandra.cluster import Cluster
        if self.host:
            self.cluster = Cluster([self.hsot])
        else:
            self.cluster = Cluster()

        self.session = self.cluster.connect(self.keyspace)

        # get a list of all securities
        res = self.session.execute("SELECT ticker, id, exchange FROM security")
        res = list(res)
        self.security_list = {}
        for security in res:
            self.security_list[(security.ticker, security.exchange)] = security
        
    def close_spider(self, spider):
        self.cluster.shutdown()
    
    def process_item(self, item, spider):
        from finance.items import EODQuote
        if isinstance(item, EODQuote):
            # Get the security id
            symbol = (item['ticker'], item['exchange'])
            result = dict(item)
            if symbol in self.security_list:
                result['security'] = self.security_list[symbol].id
            else:
                # check alias' first
                if item['exchange'] in self.alias:
                    for exchange_alias in self.alias[item['exchange']]:
                        s2 = (item['ticker'], exchange_alias)
                        if s2 in self.security_list:
                            symbol = s2
                            result['security'] = self.security_list[s2].id

                if not ('security' in result and result['security'] > 0):
                    # Security not currently in the database, create it
                    if self.create_new_securities:
                        res = self.session.execute("SELECT max(id) as id FROM security")
                        res = list(res)
                        next_id = res[0].id + 1
                        name = item['name']
                        if name is not None:
                            name = name.replace("'", "''")
                            sql = "INSERT INTO security (ticker, id, exchange, active, name) VALUES ('{ticker}', {id}, '{exchange}', true, '{name}')".format(ticker=item['ticker'], id=next_id, exchange=item['exchange'], name=name)
                            self.session.execute(sql)
                        else:
                            sql = "INSERT INTO security (ticker, id, exchange, active) VALUES ('{ticker}', {id}, '{exchange}', true)".format(ticker=item['ticker'], id=next_id, exchange=item['exchange'])
                            self.session.execute(sql)
                        result['security'] = next_id                    
                    else:
                        logger.warn("Not saving security: '{}:{}' because it does not exist in database".format(item['exchange'], item['ticker']))
                        return item

            if not 'open' in result:
                result['open'] = None

            for k, v in result.items():
                if v is None:
                    result[k] = 'null'

            # map source to unique id
            try:
                result['source'] = self.enum_map['sources'][result['source']]
            except:
                result['source'] = -1
                
            # If there's not already an entry then store in cassandra
            sql = ("INSERT INTO eod (security, date, open, high, low, close, volume, source) "
                   "VALUES ({security}, '{date:%Y-%m-%d}', {open}, {high}, {low}, {close}, {volume}, {source}) "
                   "IF NOT EXISTS".format(**result))
            self.session.execute(sql)
        return item

class FinancePipeline(object):
    def process_item(self, item, spider):
        return item
