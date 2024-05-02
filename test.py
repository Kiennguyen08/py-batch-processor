import time
from typing import List

import ray
from main import BatchProcessor, start_batch_processor


if __name__ == "__main__":
    max_batch_delay = 2  # Maximum batch delay in seconds
    batch_size = 10  # Batch size
    ray.init()
    batch_processor = BatchProcessor(max_batch_delay, batch_size)
    callback_function = lambda batch: [item * 2 for item in batch]

    # Register callback function
    batch_processor.register_callback(callback_function)

    # Start processor in parallel
    start_task = start_batch_processor.remote(batch_processor)

    batch_processor.add_work("1")
    batch_processor.add_work("2")
    batch_processor.add_work("3")
    time.sleep(2)

    batch_processor.add_work("4")
    time.sleep(2)

    batch_processor.add_work("5")

    start_result = ray.get(start_task)

    print("Start Result:", start_result)
