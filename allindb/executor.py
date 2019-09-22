import concurrent.futures


class CurrentThreadExecutor(concurrent.futures.Executor):
    def submit(self, fn, *args, **kwargs):
        result = fn(*args, **kwargs)
        future = concurrent.futures.Future()
        future.set_result(result)
        return future

    def map(self, func, *iterables, timeout=None, chunksize=1):
        return list(map(func, *iterables))

    def shutdown(wait=True):
        return
