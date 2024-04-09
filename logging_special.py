import loguru
import inspect


class Log:

    def __init__(self):
        filename = 'all_log.log'
        self.filename = filename

    @staticmethod
    async def info(message):
        filename = 'all_log.log'
        loguru.logger.info(f"{message}")
        # loguru.logger.add(f'LOGS/{filename}', format="{time} | {level} | {message}")

    @staticmethod
    async def info_positive(message):
        filename = 'all_log.log'
        loguru.logger.opt(ansi=True).info(f"<green>{message}</green>")
        # loguru.logger.add(f'LOGS/{filename}', format="{time} | {level} | {message}")

    async def debug(self, message):
        loguru.logger.debug(f"{self.get_caller()}: {message}")
        # loguru.logger.add(f'LOGS/{self.filename}', format="{time} | {level} | {message}")

    async def warning(self, message):
        loguru.logger.warning(f"{self.get_caller()}: {message}")
        # loguru.logger.add(f'LOGS/{self.filename}', format="{time} | {level} | {message}")

    async def error(self, message):
        loguru.logger.error(f"{self.get_caller()}: {message}")
        # loguru.logger.add(f'LOGS/{self.filename}', format="{time} | {level} | {message}")

        # async def error(self, message):
    #     loguru.logger.error(f"{self.get_caller()}: {message}")

    @staticmethod
    def get_caller():
        caller_stack = inspect.stack()
        filename = caller_stack[2].filename.split('/')[-2] + '/' + caller_stack[2].filename.split('/')[-1]
        funcname = caller_stack[2].function
        linevo = caller_stack[2].lineno
        return f'FROM {filename}:{funcname}, {linevo}'

