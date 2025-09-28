from stream.Stream import OutputStream

class StdOutStream(OutputStream):
    def __init__(self):
        super().__init__()


    def add_item(self, data):
        print(data)

    def close(self):
        print("Closing StdOutStream...", flush=True)