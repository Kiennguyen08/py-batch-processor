import time
from typing import List
from main import BatchProcessor


def do_stuff(batch):
    for item in batch:
        print(item)
    time.sleep(1)


if __name__ == "__main__":
    processor = BatchProcessor(max_batch_delay=5, batch_size=5)
    processor.register_callback(do_stuff)
    processor.add_work("1")
    processor.add_work("2")
    processor.start()
