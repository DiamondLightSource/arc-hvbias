import asyncio
import sys


class AsyncExitBag:
    def __init__(self, *cms):
        self.cms = cms
        self.tg = asyncio.TaskGroup()

    async def __aenter__(self):
        def task_finished(task: asyncio.Task):
            return task.done() and (not task.cancelled()) and (task.exception() is None)

        await self.tg.__aenter__()
        aenter_task_dict = {self.tg.create_task(cm.__aenter__()): cm for cm in self.cms}
        try:
            await self.tg.__aexit__(*sys.exc_info())
        except:
            # __aexit__ those successfully completed
            await asyncio.gather(
                *[
                    cm.__aexit__(*sys.exc_info())
                    for task, cm in aenter_task_dict.items()
                    if task_finished(task)
                ]
            )
            raise

        return (task.result() for task in aenter_task_dict.keys())

    async def __aexit__(self, *exc_details):
        await asyncio.gather(*[cm.__aexit__(*exc_details) for cm in self.cms])
