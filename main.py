import time
from typing import Callable, List
import ray
from ray.util.queue import Queue


@ray.remote
def process_data_batch(batch: List[object], callback: Callable):
    res = callback(batch)
    return res


class BatchProcessor:
    def __init__(self, max_batch_delay: int, batch_size: int) -> None:
        ray.init()
        self.max_batch_delay = max_batch_delay
        self.internal_queue = Queue(maxsize=500)
        self.batch_size = batch_size
        self.result_ids = []
        self.callback = None

    def fire(self):
        start_time = time.time()
        while True:
            time_diff = time.time() - start_time
            size_queue = self.internal_queue.size()
            if size_queue == self.batch_size or time_diff >= self.max_batch_delay:
                batch = self.internal_queue.get_nowait_batch(size_queue)
                self.result_ids.append(
                    process_data_batch.remote(batch=batch, callback=self.callback)
                )
                break
            time.sleep(0.1)
        result = self.collect()
        self.result_ids = []
        print(result)

    def collect(self):
        results = ray.get(self.result_ids)
        return results

    def start(self):
        if not self.callback:
            raise Exception("Callback function has not been declared")
        while True:
            self.fire()

    def stop(self):
        size_queue = self.internal_queue.size()
        batch = self.internal_queue.get_nowait_batch(size_queue)
        self.result_ids.append(self.process_data_batch.remote(batch, self.callback))
        result = self.collect()
        print(result)

    def register_callback(self, callback: Callable):
        self.callback = callback

    def add_work(self, work: object):
        self.internal_queue.put(work)
