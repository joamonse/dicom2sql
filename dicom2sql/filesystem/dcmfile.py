#Must need to be processed in order to work
class DcmFile:
    def __init__(self, config, path):
        self.path = path
        self.config = config

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_type:
            self.config.set_last_file(self.path)

    def __repr__(self):
        return self.path.__repr__()