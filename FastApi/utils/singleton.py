class _Singleton(type):
    """ A metaclass that creates a Singleton base class when called. """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if (cls, args) not in cls._instances:
            cls._instances[(cls, args)] = super(_Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[(cls, args)]
