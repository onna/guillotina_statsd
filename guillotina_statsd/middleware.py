from guillotina import app_settings
from guillotina.utils import get_dotted_name

import contextlib
import logging
import time


logger = logging.getLogger('guillotina_statsd')


class StatsdRequestTimer:
    done = False

    def __init__(self, name, client):
        self.name = name
        self.client = client
        self.start_time = time.time()
        self.done = False

    def record(self, name):
        if self.done:
            return
        self.send_data(name)

    def send_data(self, sub_name=None):
        name = self.name
        if sub_name is not None:
            name = name + '.' + sub_name
        duration_sec = time.time() - self.start_time
        # time.time() returns seconds; we need msec
        duration_msec = int(round(duration_sec * 1000))
        self.client.send_timer(name, duration_msec)

    @contextlib.contextmanager
    def __call__(self):
        try:
            yield
        finally:
            self.send_data()
            self.done = True


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
        timer = StatsdRequestTimer(timer_key, self._client)
        request._timer = timer
        with timer:
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
