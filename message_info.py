from logging_special import Log
import asyncio


class MessageInfo:

    def __init__(self, event=None, test_mess=None):
        if event:
            self.event = event
            self.channel_id = self.event.message.to_dict()['peer_id']['channel_id']

            try:
                self.message = event.message.to_dict()['message'][0:25]
            except:
                self.message = event.message.to_dict()['message']

        if test_mess:
            self.message = test_mess

    async def message_aloud(self, client, queue_main):  # Log information about chat, user and message
        chat = (await client.get_entity(self.channel_id)).to_dict()['title']
        await queue_main.put(('chat_name', chat))

        await Log().info(f'MESSAGE WAS CATCHED Chat: {chat}, Mess: {self.message}')

    async def is_mess_short(self):  # return message if message 8 symbols, upper case and includes numbers
        try:
            return self.message if await basic_rules(self.message) else None
        except:
            pass

    async def is_mess_hybrid(self):  # like is_mess_short also include space and \n
        for paragraphed_mess in self.message.split('\n'):
            for split_mess in paragraphed_mess.split(' '):
                if await basic_rules(split_mess):
                    return split_mess
        return None

    async def is_special_message(self):  # some groups have special key for finding codes

        async def from_parser(message):  # messages from Cryptobox Parser
            try:
                return message.split(' ')[-1] if len(message) == 10 and await basic_rules(
                    message.split(' ')[-1]) else None
            except:
                pass

        async def from_agnelia(message):  # messages from safe crypto box | agnelia sehwary
            try:
                return message.split(' ')[1][1:9] if len(message.split(' ')[1]) == 10 and await basic_rules(
                    message.split(' ')[1][1:9]) else None
            except:
                pass

        async def from_BCBC(message):
            try:
                return message.split(' ') if await basic_rules(message.split(' ')) else None
            except:
                pass

        async def from_CBC(message):
            try:
                return message[1:9] if len(message) == 10 and await basic_rules(message[1:9]) else None
            except:
                pass

        tasks = [
            asyncio.create_task(from_parser(self.message)),
            asyncio.create_task(from_agnelia(self.message)),
            asyncio.create_task(from_BCBC(self.message)),
            asyncio.create_task(from_CBC(self.message)),
        ]

        codes = await asyncio.gather(*tasks)

        try:
            return [code for code in codes if code is not None][0]
        except:
            pass

    async def process_initiator(self, queue_main, test_mess=None):  # message_aloud need client!

        if test_mess:
            try:
                test_mess = test_mess[0:25]
            except:
                zxc = 1000 - 7  # :)

            tasks = [
                asyncio.create_task(MessageInfo(test_mess=test_mess).is_mess_short()),
                asyncio.create_task(MessageInfo(test_mess=test_mess).is_special_message()),
                asyncio.create_task(MessageInfo(test_mess=test_mess).is_mess_hybrid()),
            ]
        else:
            if self.message[0:2] == 'BP' and len(self.message) == 10:
                raise Exception('Message starts with BP, it is RED PACKET!')
            tasks = [
                asyncio.create_task(MessageInfo(self.event).is_mess_short()),
                asyncio.create_task(MessageInfo(self.event).is_special_message()),
                asyncio.create_task(MessageInfo(self.event).is_mess_hybrid()),
            ]

        codes = await asyncio.gather(*tasks)

        try:
            code = [code for code in codes if code is not None][0]

            await queue_main.put(('code', code))

            await Log().info_positive(
                f'Code was found: {code}')

            # return [code for code in codes if code is not None][0]
        except:
            await Log().info('Message has no code')
            raise Exception('Message has no code')


async def basic_rules(mess):  # func for short code
    if len(mess) == 8:
        if mess.isalnum():
            if mess.isupper():
                return True
    return False
