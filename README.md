# A light-weight JSON-RPC2 implementation.

### Features:
1. Load balancing.

### Usage:

server.py

```python
import time
from zjsonrpc2 import RPCServer

server = RPCServer('tcp://*:9999')

@server.procedure('test')
def test_func():
    return 'test ok'

@server.procedure('test_exception')
def test_exc():
    raise Exception, 'test exception'

@server.procedure()
def echo(*args, **kwargs):
    return 'echoed: %s, %s' % (repr(args), repr(kwargs))

try:
    server.start()
    while True:
        time.sleep(0.5)
except KeyboardInterrupt as e:
    print 'end.'
```

client.py

```python
from zjsonrpc2 import RPCClient, Error

with RPCClient('tcp://localhost:9999') as client:
    # list all procedures
    print 'all methods:', client.list_methods()
    print 'test:', client.test()
    print 'echo "a":', client.echo("a")

    try:
        client.test_exception()
    except Error as e:
        print 'RPC Error:', str(e)
```

