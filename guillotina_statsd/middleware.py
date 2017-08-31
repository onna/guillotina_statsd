from guillotina import app_settings
from guillotina.utils import get_dotted_name

import logging


logger = logging.getLogger('guillotina_statsd')


class Middleware:

    def __init__(self, app, handler):
        self._client = app_settings['statsd_client']
        self._app = app
        self._handler = handler
        self._prefix = app_settings['statsd'].get(
            'key_prefix', 'guillotina_request')

    async def __call__(self, request):
        try:
            return await self.instrument(request)
        except:
            logger.warn('Error instrumenting code for statsd...')
            return await self._handler(request)

    async def instrument(self, request):
        timer_key = f'{self._prefix}.processing'
        with self._client.timer(timer_key):
            resp = await self._handler(request)

        try:
            try:
                view_name = get_dotted_name(request.found_view.view_func)
            except AttributeError:
                view_name = get_dotted_name(request.found_view)
        except AttributeError:
            view_name = 'unknown'

        key = f"{self._prefix}.{view_name}"
        self._client.incr(f".{key}.request")
        self._client.incr(f".{key}.{request.method}")
        self._client.incr(f".{key}.{resp.status}")
        return resp


async def middleware_factory(app, handler):

    if 'statsd_client' not in app_settings:
        return handler

    return Middleware(app, handler)
