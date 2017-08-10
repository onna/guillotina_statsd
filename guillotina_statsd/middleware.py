from guillotina import app_settings
from guillotina.utils import get_dotted_name

async def middleware_factory(app, handler):

    if 'statsd_client' not in app_settings:
        return handler

    client = app_settings['statsd_client']

    async def middleware_handler(request):
        with client.timer('guillotina_request_processing'):
            resp = await handler(request)

        try:
            try:
                view_name = get_dotted_name(request.found_view.view_func)
            except AttributeError:
                view_name = get_dotted_name(request.found_view)
        except AttributeError:
            view_name = 'unknown'

        key = f"guillotina_request.{view_name}"
        client.incr(f".{key}.request")
        client.incr(f".{key}.{request.method}")
        client.incr(f".{key}.{resp.status}")
        return resp
    return middleware_handler
