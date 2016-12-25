# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch
from yahoo_finance import Share, YQLResponseMalformedError
from datetime import timedelta, datetime
import math
import config
import logging

logger = logging.getLogger(__name__)

es = Elasticsearch(config.conf['elasticsearch']['hosts'])


class Stocks(object):
    """
    Manage stocks information
    """

    def getLatestClose(self, symbol, date_begin, date_end):
        """
        Get the latest date which has a stock value in elasticsearch storage

        symbol -- symbol of the stock
        date_begin -- date from which stock values are selected
        date_end -- date after which stock values are not selected

        Return the date of the latest stock value
        """
        res = es.search(index='stocks-hist', doc_type='stock_day', body={
            'query': {
                'query_string': {
                    'query': 'symbol = %(symbol)s AND date: [%(date_begin)s TO %(date_end)s]' %
                        {'symbol': symbol, 'date_begin': date_begin, 'date_end': date_end}
                        }
            }
        }, sort='date:desc', size=1)
        if res['hits']['total'] > 0:
            return datetime.strptime(res['hits']['hits'][0]['_source']['date'], '%Y-%m-%dT%H:%M:%S').date()
        return date_begin

    def indexStocksHist(self, symbol, date_begin, date_end):
        """
        Index stock values into elasticsearch storage

        symbol -- symbol of the stock
        date_begin -- date from which stock values should be inserted
        date_end -- date to which stock values shouldn't be insert
        """
        try:
            hist = Share(symbol).get_historical(date_begin.strftime('%Y-%m-%d'), date_end.strftime('%Y-%m-%d'))
            for day in hist:
                es.index(index='stocks-hist', doc_type='stock_day', id=day['Symbol'] + '_' + day['Date'], body={
                    'date': datetime.strptime(day['Date'], '%Y-%m-%d'),
                    'close': float(day['Close']),
                    'open': float(day['Open']),
                    'high': float(day['High']),
                    'low': float(day['Low']),
                    'symbol': day['Symbol'],
                    'volume': int(day['Volume'])
                })
        except YQLResponseMalformedError:
            logger.exception("unable to retrieve stock values from yahoo api")

    def getHistPrice(self, symbol, period, mvavg_window, date_end, days):
        """
        Get an history of stock closing prices

        symbol -- symbol of the stock
        period -- interval (days) between stock values
        mvavg_window -- moving average window in days
        date_end -- date after which stock values will not be selected
        days -- number of days of stock values to get

        Returns the history of stock closes price an its moving average
        """
        mvavg_nb_buckets = math.ceil(mvavg_window / period)
        date_begin = date_end - timedelta(days=days)
        timestamp_begin = int(datetime.fromordinal(date_begin.toordinal()).timestamp()) * 1000
        date_begin_window = date_begin - timedelta(days=mvavg_window)
        latest_close = self.getLatestClose(symbol, date_begin_window, date_end)
        if latest_close < date_end:
            self.indexStocksHist(symbol, latest_close, date_end)
        res = es.search(index='stocks-hist', doc_type='stock_day', body={
            'query': {
                'query_string': {
                    'query': 'symbol = %(symbol)s AND date: [%(date_begin)s TO %(date_end)s]' %
                        {'symbol': symbol, 'date_begin': date_begin_window, 'date_end': date_end}
                        }
            },
            'aggs': {
                'time_agg': {
                    'date_histogram': {
                        'field': 'date',
                        'interval': '%(period)dd' % {'period': period}
                    },
                    'aggs': {
                        'avg_close': {
                            'avg': {
                                'field': 'close'
                            }
                        },
                        'mv_avg': {
                            'moving_avg': {
                                'buckets_path': 'avg_close',
                                'window': mvavg_nb_buckets,
                                'model': 'linear'
                            }
                        }
                    }
                }
            },
            'sort': 'date'
        }, sort=['date:asc'], size=0)
        hist = []
        for agg in res['aggregations']['time_agg']['buckets']:
            if agg['key'] > timestamp_begin and agg['avg_close']['value'] is not None:
                hist.append({
                    'symbol': symbol,
                    'mstime': agg['key'],
                    'close': agg['avg_close']['value'],
                    'mv_close': agg['mv_avg']['value'],
                })
        return hist
