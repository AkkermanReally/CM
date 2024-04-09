from message_info import MessageInfo
from telethon import TelegramClient, events
from database import DbCode
from database import CodeStatistic
from CM_config import Config
from logging_special import Log
import asyncio
from paste_box_code import PasteBoxCode

tg_data = Config().config_tg()
phone = tg_data['phone']
api_id = tg_data['api_id']
api_hash = tg_data['api_hash']


async def binance_main():
    tg_group_name = []
    client = TelegramClient(phone, api_id, api_hash)
    async with (client):
        async for dialog in client.iter_dialogs():
            if dialog.is_channel:
                await Log().info(f'Channel: {dialog.title} is OK')
                tg_group_name.append(dialog.id)

        await Log().info('Bot is active!.\n')

        @client.on(events.NewMessage(chats=tg_group_name))
        async def normal_handler(event):

            restart = asyncio.create_task(PasteBoxCode.restart_offline_accounts())
            await restart

            queue_main = asyncio.Queue()

            tasks = [
                asyncio.create_task(MessageInfo(event=event).message_aloud(client=client, queue_main=queue_main)),  # message log
                asyncio.create_task(MessageInfo(event=event).process_initiator(queue_main)),  # search for the code in the message
                asyncio.create_task(DbCode().code_check(queue_main)),  # check code for include in list
                asyncio.create_task(DbCode().code_update(queue_main)),  # if code not include, code added in list
                asyncio.create_task(PasteBoxCode.extract_data(queue_main)),
                asyncio.create_task(PasteBoxCode.do_headers_cookies(queue_main)),
                asyncio.create_task(PasteBoxCode().send_code_main(queue_main)),
                asyncio.create_task(CodeStatistic().insert_code_data(queue_main))
            ]
            try:
                done, trash = await asyncio.wait([*tasks], return_when=asyncio.FIRST_EXCEPTION)
            except Exception as ex:
                await Log().error(f'{ex}, cycle canceled')

            if [d.exception() for d in done]:
                [task.cancel() for task in tasks]
            return

        await client.start()
        await client.run_until_disconnected()

