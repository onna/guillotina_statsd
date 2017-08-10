from guillotina import configure
from guillotina_statsd.middleware import middleware_factory  # noqa

app_settings = {
    "statsd": {
        "host": "localhost",
        "port": 8015
    }
}


def includeme(root):
    """
    custom application initialization here
    """
    configure.scan('guillotina_statsd.utility')
