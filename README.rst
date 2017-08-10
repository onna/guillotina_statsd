guillotina_statsd Docs
======================


Integrates statsd into guillotina.


Configuration
-------------


Example docs:

    {
        "statsd": {
	    "host": "localhost",
	    "port": 8015
	    }
    }

    
Dependencies
------------

Python >= 3.6


Installation
------------

This example will use virtualenv::

  virtualenv .
  ./bin/python setup.py develop


Running
-------

Most simple way to get running::

  ./bin/guillotina
