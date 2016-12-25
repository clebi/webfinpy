#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import falcon
from falcon_cors import CORS
from datetime import date, timedelta
from fin import Stocks
from middle import JsonTranslator


class Graph(object):
    """
    Handles graph request
    """

    def on_get(self, req, resp, stock_id, days):
        if req.params['window'] is None:
            raise Exception("parameter window is missing")
        if req.params['step'] is None:
            raise Exception("step window is missing")
        today = date.today() - timedelta(days=1)
        stocks = Stocks()
        hist = stocks.getHistPrice(stock_id, int(req.params['step']), int(req.params['window']), today, int(days))
        resp.status = falcon.HTTP_200
        req.context['result'] = hist


cors = CORS(allow_all_origins=True)

app = falcon.API(middleware=[
    JsonTranslator(),
    cors.middleware
])

graph = Graph()
app.add_route('/{stock_id}/graph/{days}', graph)
