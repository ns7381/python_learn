class Dict(dict):
    def __init__(self, **kwargs):
        super.__init__(**kwargs)

    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError:
            raise AttributeError(r"'Dict' obj has not attr %s" % item)

    def __setattr__(self, key, value):
        self[key] = value