import ast
import aiohttp
import asyncio
from database import DbAccounts, DbError
from logging_special import Log
from queue_search_instrument import QueueSearch
from binance_authentication.auth_main import auto_auth_main


class PasteBoxCode:

    @staticmethod
    def custom_sort(item):  # sort datas set status online first in the list
        return item['status'] == 'banned', item['status'] == 'online'

    @staticmethod
    async def restart_offline_accounts():

        accounts = await DbAccounts().return_data()

        offline_accounts = [account['login'] for account in accounts if account['status'] == 'offline']

        await Log().info(f'offline_accounts: {offline_accounts}')

        if offline_accounts:
            await auto_auth_main(logins=offline_accounts)

    @staticmethod
    async def extract_data(queue_main=None) -> list[dict] | None:

        accounts = await DbAccounts().return_data()

        unsorted_accounts = [{'login': account['login'],  # getting datas if account not offline
                              'headers': account['headers'],
                              'cookies': ast.literal_eval(account['cookies']),
                              'status': account['status']} for account in accounts if account['status'] == 'online' or
                             account['status'] == 'banned']

        if not unsorted_accounts:  # if list is empty raise error
            await Log().error(f'all accounts are offline!, unsorted_accounts: {unsorted_accounts}')
            raise Exception('all accounts are offline!')

        error_count = await DbError().return_data()

        if int(error_count) > len(unsorted_accounts):
            match = int(error_count) / 5
            error_count = int(error_count) - 5 * int(match)

        a = 0

        while a != int(error_count):
            unsorted_accounts.insert(0, unsorted_accounts[len(unsorted_accounts) - 1])
            unsorted_accounts.pop()
            a = a + 1

        sorted_accounts = sorted(unsorted_accounts, key=PasteBoxCode().custom_sort)

        await Log.info(f'active accounts: {[[account["login"], account["status"]] for account in sorted_accounts]}')

        if queue_main:
            await queue_main.put(('accounts', sorted_accounts))
            return

        return sorted_accounts

    @staticmethod
    async def do_headers_cookies(queue_main):

        headcooks = []

        accounts = await QueueSearch.find_queue(queue=queue_main, required_mark='accounts')
        for account in accounts:
            headers = account['headers']
            cookies_dict = account['cookies']
            login = account['login']

            cookies = {
                'theme': 'dark',
                'source': 'crm',
                'campaign': 'transactional',
                'BNC-Location': 'PL',
                'userPreferredCurrency': 'USD_USD',
                'lang': 'en',
                's9r1': cookies_dict['s9r1'],
                'cr00': cookies_dict['cr00'],
                'd1og': cookies_dict['d1og'],
                'r2o1': cookies_dict['r2o1'],
                'f30l': cookies_dict['f30l'],
                'logined': 'y',
                'p20t': cookies_dict['p20t']
            }

            headers = ast.literal_eval(headers)

            headcooks.append({'login': login, 'headers': headers, 'cookies': cookies})

            await Log.info_positive(f'headers and cookies created for: {login}')
        await queue_main.put(('headcooks', headcooks))
        return

    @staticmethod
    async def send_code_box(queue_main, login: str, code: str, headers: dict, cookies: dict):

        response_coro = await QueueSearch.find_queue(queue=queue_main, required_mark='response_coro')

        for task in asyncio.all_tasks():
            if response_coro == task.get_coro():
                response_task = task

        await Log().info_positive(f'send_code for {login} work')

        async with aiohttp.ClientSession() as session1:

            json_data = {
                'grabCode': '{}'.format(code),
                'channel': 'DEFAULT',
            }
            async with session1.request('POST', 'https://www.binance.com/bapi/pay/v1/private/binance-pay/gift-box/code/query',
                                     # proxy=proxy,
                                     cookies=cookies,
                                     headers=headers,
                                     json=json_data,) as r:

                responce1 = await r.json()

        # await Log().info(f'responce1: {responce1}')

        if responce1['code'] != '000000':
            match responce1['code']:

                case '403803':
                    await Log().warning(f'ERROR 403803, for {login}, box code: {code}, info: {responce1["message"]}')
                    await DbError().error_update()
                    response_task.cancel()
                    await queue_main.put(('statistic_data', {'code': False}))
                    return

                case '403802':  # This Red Packet has been fully claimed
                    await Log().warning(f'ERROR 403803, for {login}, box code: {code}, info: {responce1["message"]}')
                    response_task.cancel()
                    await queue_main.put(('statistic_data', {'code': False}))
                    return

                case 'PAY4001COM000':  # Invalid code, please double check the code and try again.
                    await Log().error(f'ERROR PAY4001COM000, for {login}, box code: {code}, info: {responce1["message"]}')
                    await DbError().error_update()
                    await queue_main.put(('statistic_data', {'code': False}))
                    response_task.cancel()
                    return

                case '403804':
                    await Log().warning(f'ERROR 403804, for {login}, box code: {code}, info: {responce1["message"]}')
                    await DbError().error_update()
                    await queue_main.put(('statistic_data', {'code': False}))
                    response_task.cancel()
                    return

                case '403067':
                    await Log().warning(f'ERROR 403067, for {login}, box code: {code}, info: {responce1["message"]}')
                    await DbAccounts().update_status(login=login, status='banned')
                    await queue_main.put(('statistic_data', {'code': False}))
                    return

                case '100002001':
                    await Log().warning(f'ERROR 100002001, for {login}, box code: {code}, info: {responce1["message"]}')
                    await DbAccounts().update_status(login=login, status='offline')
                    await queue_main.put(('statistic_data', {'code': False}))
                    return

            await Log().warning(f'ERROR ?, for {login}, box code: {code}, info: {responce1["message"]} ')
            response_task.cancel()
            return

        async with aiohttp.ClientSession() as session2:
            json_data = {
                'grabCode': '{}'.format(code),
                'channel': 'DEFAULT',
                'scene': None,
            }

            async with session2.request('POST',
                                        'https://www.binance.com/bapi/pay/v1/private/binance-pay/gift-box/code/grabV2',
                                        cookies=cookies,
                                        headers=headers,
                                        json=json_data,
                                        # proxy=proxy
            ) as r:
                responce2 = await r.json()

        # await Log().info(f'responce2: {responce2}')

        match responce2['code']:
            case '000000':
                await Log().info_positive(f'{login} code: {code} Монета: {responce2["data"]["currency"]} | Kоличество: {responce2["data"]["grabAmountStr"]}')
                await DbAccounts().update_status(login=login, status='online')
                await queue_main.put(('statistic_data', {'code': code, 'coin': responce2["data"]["currency"], 'amount': responce2["data"]["grabAmountStr"]}))
                return

            case '403067':
                await Log().warning(f'ERROR 403067, for {login}, box code: {code}, info: {responce2["message"]}')
                await DbAccounts().update_status(login=login, status='banned')
                await queue_main.put(('statistic_data', {'code': False}))
                return

            case '403012':
                await Log().warning(f'ERROR 403012, for {login}, box code: {code}, info: {responce2["message"]}')
                await queue_main.put(('statistic_data', {'code': False}))
                return

            case 'PAY4001MGS001':
                await Log().error(f'ERROR PAY4001MGS001, for {login}, box code: {code}, info: {responce2["message"]}')
                await DbAccounts().update_status(login=login, status='offline')
                await queue_main.put(('statistic_data', {'code': False}))
                return

            case 'PAY4001COM000':  # This Red Packet has already been claimed.
                await Log().warning(f'ERROR PAY4001COM000, for {login}, box code: {code}, info: {responce2["message"]}')
                await queue_main.put(('statistic_data', {'code': False}))
                response_task.cancel()
                return

            case '403802':  # This Red Packet has been fully claimed. Total amount.
                await Log().warning(f'ERROR 403802, for {login}, box code: {code}, info: {responce2["message"]}')
                await queue_main.put(('statistic_data', {'code': False}))
                response_task.cancel()
                return

        await Log().warning(f'ERROR ?, for {login}, box code: {code}, info: {responce2["message"]} ')
        await queue_main.put(('statistic_data', {'code': False}))
        response_task.cancel()
        return

    @staticmethod
    async def send_code_main(queue_main, tick=2):

        await QueueSearch.find_queue(queue=queue_main, required_mark='code_check')
        code = await QueueSearch.find_queue(queue=queue_main, required_mark='code')
        headcooks = await QueueSearch.find_queue(queue=queue_main, required_mark='headcooks')

        await queue_main.put(('response_coro', asyncio.current_task(asyncio.get_running_loop()).get_coro()))

        for headcook in headcooks:

            asyncio.create_task(PasteBoxCode.send_code_box(queue_main, login=headcook['login'], headers=headcook['headers'], cookies=headcook['cookies'], code=code))
            await Log.info(f'TICK -> {tick}')

            await asyncio.sleep(tick)
            tick = tick / 2
