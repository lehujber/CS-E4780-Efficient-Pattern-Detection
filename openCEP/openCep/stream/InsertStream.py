from stream.Stream import InputStream

class InsertStream(InputStream):
    """
    A stream that allows inserting items.
    """
    def __init__(self):
        super().__init__()

    def add_item(self, item):
        self._stream.put(item)