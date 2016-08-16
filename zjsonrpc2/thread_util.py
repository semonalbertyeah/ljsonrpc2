# -*- coding:utf-8 -*-

import threading

def threaded(**options):
    """
        make calling of func in a new thread.
        options:
            name -> default : decorated function name
            daemon -> default : False
            start -> default : True
        Example:
            @threaded(name='test_function', start=False, daemon=True)
            def test():
                print 'test'

            t = test()
            t.start()
            t.join()
    """
    def decorator(func):
        name = options.get('name', func.__name__)
        daemon = bool(options.get('daemon', False))
        start = bool(options.get('start', True))

        assert callable(func)

        def new_func(*args, **kwargs):

            t = threading.Thread(
                target=func, 
                name=name,
                args=args, 
                kwargs=kwargs
            )
            t.setDaemon(daemon)
            if start:
                t.start()
                t.setName('%s-%d' % (t.name, t.ident))
            return t

        return new_func

    return decorator

