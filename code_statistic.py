import datetime
import calendar

from database import CodeStatistic
import aiohttp
import asyncio


async def get_coin_price(key, count):

    async with aiohttp.ClientSession() as session:

        async with session.get(f'https://api.binance.com/api/v3/ticker/price?symbol={key}USDT') as r:
            data = await r.json()
            try:
                coin = data['symbol'].split('USDT')[0]
                price = float(data['price']) * float(count)
            except:
                return None
            return {'coin': coin, 'count': count, 'price': ("%.15f" % price).rstrip('0')}


# a = asyncio.run(get_coin_price(key='LUNC', count='0.001'))
# print(a)

# async def get_statistics_day_now(coin: str = None, channel: str = None, year: str = None, month: str = None, day: str = None, hour: str = None):


async def get_coin_channel(coin: str = None, channel: str = None, year: str = None, month: str = None, day: str = None, hour: str = None):

    tasks = []
    channels_count = 0
    coin_count = 0
    count_prices = []

    coins, channels = await CodeStatistic().return_merge_statistic_by_keys(
        coin=coin, channel=channel, year=year, month=month, day=day, hour=hour
    )

    print('coins=', coins)
    print('channels=', channels)

    for coin in coins:
        if coin != 'USDT':
            tasks.append(asyncio.create_task(get_coin_price(key=coin, count=coins[coin])))
    #
    # coin_prices = await asyncio.gather(*tasks)
    # try:
    #     for coin_price in coin_prices:
    #         count_prices.append(float(coin_price['price']))
    # except:
    #     zxc = 1000 - 7
    # count_prices = sorted(count_prices, reverse=True)
    #
    # coins_price = []
    # for count_price in count_prices:
    #     for coin_price in coin_prices:
    #         try:
    #             if count_price == float(coin_price['price']):
    #                 coins_price.append(coin_price)
    #         except:
    #             zxc = 1000 - 7
    #
    # for coin in coins_price:
    #     coin_count += float(coin['price'].split(' ')[0])
    #
    # statistic = ''
    #
    # if month:
    #     statistic = f'\n\nMonth: {month}\n'
    #     if day:
    #         statistic = f'\n\nday: {day}\n'
    #         if hour:
    #             statistic = f'\n\nHour: {hour}:00\n\n'
    #
    # statistic = statistic + f"{'Name' : <10}{'Count' : <20}{'Price' : >5}"
    # for coin in coins_price:
    #         statistic = statistic + f"\n{coin['coin'] : <10}{coin['count'] : <20}{coin['price'] : <10}"
    #
    # statistic = statistic + f"'\n\nTotal price: {coin_count} \n\n"
    #
    # channels = dict(sorted(channels.items(), reverse=True, key=lambda item: item[1]))
    #
    # for channel in channels:
    #     channels_count += int(channels[channel])
    #
    # statistic = statistic + f"Boxes count: {channels_count}\n{'Channel' : <40}{'Box Count' : <10}{'Use' : >9}"
    # for channel in channels:
    #     statistic = statistic + f"\n{channel.rstrip(): <40}{channels[channel] : <5}{int(100 * channels[channel] / channels_count) : >13}%"
    # return statistic


asyncio.run(get_statistics(
    year='2024', month='3', day='6'))



async def get_statistic_definite(year: str, month: str, day: str):
    statistic = await get_statistics(
        year=year, month=month, day=day
    )
    with open('statistic_definite.txt', 'w') as file:
        file.write(statistic)
    return statistic


async def get_statistic_day_now():
    year = f'{datetime.datetime.now().year}'
    month = f'{datetime.datetime.now().month}'
    day = f'{datetime.datetime.now().day}'

    statistic = await get_statistics(year=year, month=month, day=day)
    with open('statistic_day_now.txt', 'w') as file:
        file.write(statistic)
    return statistic


async def get_statistic_per_tag_by_tag(datas: list):
    tasks = []
    day = None
    month = None
    year = None
    count = None

    tag, data = datas

    if tag == 'year':
        year = data
        count = '12'

    if tag == 'month':
        year = f'{datetime.datetime.now().year}'
        month = data
        count = f'{calendar.monthrange(2024, 2)[1]}'

    if tag == 'day':
        year = f'{datetime.datetime.now().year}'
        month = f'{datetime.datetime.now().month}'
        day = data
        count = '23'

    statistic = ''

    if count:
        for c in range(0, int(count)):
            statistic = statistic + await get_statistics(year=year,
                                 month=[month if month is not None else f'{c}'][0],
                                 day=[day if day is not None else f'{c}'][0],
                                 hour=[f'{c}' if tag != 'month' or tag != 'year' else None][0])
    else:
        return None
    # q = 'statistic.txt'
    with open('statistic_per_tag_by_tag.txt', 'w') as file:
        file.write(statistic)

    return statistic

# asyncio.run(get_statistic_per_tag_by_tag(['day', '17']))
# a = asyncio.run(get_statistic_day_now())
# print(a)