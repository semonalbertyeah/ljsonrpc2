# -*- coding:utf-8 -*- 

import zmq
import random
import time

def randchr():
    return chr(random.randint(97, 122))

def pressure_test(endpoint, num=10, sleep=None, timeout=3000):
    # print 'pressure_test'
    socks = []
    ctx = zmq.Context.instance()
    for i in xrange(num):
        sock = ctx.socket(zmq.REQ)
        sock.identity = 'client-%d' % i
        sock.connect(endpoint)
        if sleep:
            sock.send(str(int(sleep)))
        else:
            sock.send(randchr())
        socks.append(sock)
        time.sleep(0.01)

    for sock in socks:
        if sock.poll(int(timeout), zmq.POLLIN) == zmq.POLLIN:
            r = sock.recv()
            print '%s: %s' % (sock.identity, r)
        else:
            print '%s: timeout' % sock.identity
        sock.close()


if __name__ == '__main__':
    pressure_test()


