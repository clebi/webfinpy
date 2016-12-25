#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import falcon
from falcon_cors import CORS
from datetime import date, timedelta
from fin import Stocks
from middle import JsonTranslator
import logging

logger = logging.getLogger(__name__)


class ValidationException(Exception):

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return 'parameter %(name)s failed validation' % {'name': self.name}


class Graph(object):
    """
    Handles graph request
    """

    def on_get(self, req, resp, stock_id, days):
        try:
            if 'window' not in req.params:
                raise ValidationException('window')
            if 'step' not in req.params:
                raise ValidationException('step')
            stocks = Stocks()
            today = date.today() - timedelta(days=1)
            hist = stocks.getHistPrice(stock_id, int(req.params['step']), int(req.params['window']), today, int(days))
            resp.status = falcon.HTTP_200
            req.context['result'] = hist
        except ValidationException as exc:
            logger.exception("parameter validation")
            raise falcon.HTTPBadRequest('validation_error', 'Parameter %(parameter)s is not valid' %
                                        {'parameter': exc.name})
        except Exception:
            logger.exception("Error getting graph data")
            raise falcon.HTTPInternalServerError('unknown_error', '')


cors = CORS(allow_all_origins=True)

app = falcon.API(middleware=[
    JsonTranslator(),
    cors.middleware
])

graph = Graph()
app.add_route('/{stock_id}/graph/{days}', graph)
