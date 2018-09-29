# -*- coding:utf-8 -*-

import signal

class SignalContext(object):
    """
        import signal
        sig_context = SignalContext()

        class Termination(BaseException):
            pass

        @sig_context.on(signal.SIGTERM)
        def termination(signum, stack):
            raise Termination, 'terminated'

        def main():
            import time
            try:
                with sig_context:
                    while 1:
                        print 'heartbeat'
                        time.sleep(0.5)
            except Termination as e:
                print 'end'

        if __name__ == '__main__':
            main()  # keep print heartbeat until SIGTERM
    """
    def __init__(self, signal_actions={}):
        self.sig_actions = signal_actions
        self.original_actions = None

    @property
    def active(self):
        return self.original_actions is not None

    def register(self, signum, action):
        self.sig_actions[signum] = action

    def on(self, signum):
        def decorator(func):
            self.register(signum, func)
            return func
        return decorator

    def activate(self):
        assert not self.active, 'already active'

        self.original_actions = {}
        for signum in self.sig_actions.iterkeys():
            self.original_actions[signum] = signal.getsignal(signum)

        for signum, action in self.sig_actions.iteritems():
            signal.signal(signum, action)


    def deactivate(self):
        if not self.active:
            return
        for signum, action in self.original_actions.iteritems():
            signal.signal(signum, action)

        self.original_actions = None

    def __enter__(self):
        self.activate()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.deactivate()


class Flag(object):
    """
        A flag indicate true or false.
    """
    def __init__(self, val=True):
        self.__val = bool(val)

    def __bool__(self):
        return bool(self.__val)

    __nonzero__ = __bool__

    # @mthread_safe()
    def set_true(self):
        self.__val = True

    # @mthread_safe()
    def set_false(self):
        self.__val = False




