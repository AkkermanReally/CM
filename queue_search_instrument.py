import asyncio


class QueueSearch:

    @staticmethod
    async def find_queue(queue, required_mark: str) -> dict:  #

        while True:
            # noinspection PyBroadException
            try:
                return [obj for mark, obj in queue._queue if mark == required_mark][0]
            except:
                await asyncio.sleep(0.1)
