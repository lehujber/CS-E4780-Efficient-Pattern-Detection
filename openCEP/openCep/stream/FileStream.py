import os
import time

from stream.Stream import InputStream, OutputStream
from base.PatternMatch import PatternMatch


class FileInputStream(InputStream):
    """
    Reads the objects from a predefined input file.
    """
    def __init__(self, file_path: str):
        super().__init__()
        # TODO: reading the entire content of the input file here is very inefficient
        with open(file_path, "r") as f:
            for line in f.readlines():
                self._stream.put(line)
        self.close()


class FileOutputStream(OutputStream):

    """
    Writes the objects into a predefined output file.
    """
    def __init__(self, base_path: str, file_name: str, is_async: bool = True, console_output=True):
        super().__init__()
        if not os.path.exists(base_path):
            os.makedirs(base_path, exist_ok=True)
        self.__is_async = is_async
        self.__output_path = os.path.join(base_path, file_name)
        self.__base_path = base_path
        if self.__is_async:
            self.__output_file = open(self.__output_path, 'w')
        else:
            self.__output_file = None

        self.console_output = console_output
        self.printed = 0
        self.latencies = []

    def add_item(self, item: object):
        """
        Depending on the settings, either writes the item to the file immediately or buffers it for future write.
        """
        # Calculate latency if item is a PatternMatch
        if isinstance(item, PatternMatch):
            arrival_time = item.get_last_event_arrival_timestamp()
            emission_time = time.perf_counter()
            latency = (emission_time - arrival_time) * 1000  # Convert to milliseconds
            self.latencies.append(latency)
        
        if self.console_output:
            print(item)
            self.printed += 1
            print("Printed: ",self.printed, flush=True)
        if self.__is_async:
            self.__output_file.write(str(item))
        else:
            super().add_item(item)

    def close(self):
        """
        If asynchronous write is disabled, writes everything to the output file before closing it.
        """
        super().close()
        if not self.__is_async:
            self.__output_file = open(self.__output_path, 'w')
            for item in self:
                self.__output_file.write(str(item))
        self.__output_file.close()
        
        # Print latency statistics
        if self.latencies:
            import statistics
            avg_latency = statistics.mean(self.latencies)
            median_latency = statistics.median(self.latencies)
            min_latency = min(self.latencies)
            max_latency = max(self.latencies)
            
            print("\n=== Latency Statistics ===")
            print(f"Total matches: {len(self.latencies)}")
            print(f"Average latency: {avg_latency:.2f} ms")
            print(f"Median latency: {median_latency:.2f} ms")
            print(f"Min latency: {min_latency:.2f} ms")
            print(f"Max latency: {max_latency:.2f} ms")
            print("==========================\n")
            
            # Write latencies to file
            latencies_path = os.path.join(self.__base_path, "latencies.txt")
            with open(latencies_path, 'w') as f:
                for latency in self.latencies:
                    f.write(f"{latency:.4f}\n")
