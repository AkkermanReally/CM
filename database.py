import datetime
import pymysql.cursors
from CM_config import Config
from logging_special import Log
from queue_search_instrument import QueueSearch


class Database:

    def __init__(self):
        config = Config().config_db()
        self.db_ac_name = config['db_name']['account_data_table']
        self.db_c_name = config['db_name']['code_table']
        self.db_e_name = config['db_name']['error_table']
        self.db_s_name = config['db_name']['statistic_table']
        try:
            self.con = pymysql.connect(
                host=config['host'],
                port=config['db_port'],
                password=config['password'],
                user=config['user'],
                cursorclass=pymysql.cursors.DictCursor
            )

        except Exception as ex:
            print(ex)
            self.con.close()


class DbAccounts(Database):

    def __init__(self):
        super().__init__()
        self.table = 'account_data_table'

        with self.con.cursor() as cur:
            select = f"USE {self.db_ac_name};"
            cur.execute(select)
        self.con.commit()

    async def create_table(self):  # create accounts_data table
        with self.con.cursor() as cur:
            creating_table = f"CREATE TABLE IF NOT EXISTS {self.table}" \
                             "(id SERIAL PRIMARY KEY, " \
                             "login TEXT, " \
                             "pass TEXT, " \
                             "encode_pass TEXT, " \
                             "safe_pass TEXT, " \
                             "headers TEXT, " \
                             "cookies TEXT, " \
                             "status TEXT);"

            cur.execute(creating_table)
        self.con.commit()
        await Log().info_positive(f'{self.table} table was created')

    async def register_custom(self, login: str):  # register new customer
        for db_data in await DbAccounts().return_data():
            if login == db_data['login']:
                await Log().error(f'This login: {login}, already in database!')
                return False

        with self.con.cursor() as cur:
            insert = f"INSERT INTO {self.table} (login) VALUES (%s);"
            value = login
            cur.execute(insert, value)
            self.con.commit()
        self.con.close()
        await Log().info(f'new customer was registered {login}')

    async def update_passwords(self, login: str, password: str, encode_pass: str, safe_pass: str):  # update passwords
        with self.con.cursor() as cur:
            update = f"UPDATE {self.table} SET pass = %s, encode_pass = %s, safe_pass = %s  WHERE login = %s"
            value = (password, encode_pass, safe_pass, login)
            cur.execute(update, value)
            self.con.commit()
            self.con.close()
            await Log.info(f'passwords: {password}, {encode_pass}, {safe_pass} was updated for {login}')

    async def update_tokens(self, login: str, headers, cookies):  # update headers and cookies
        with self.con.cursor() as cur:
            update = f"UPDATE account_data_table SET headers = %s, cookies = %s  WHERE login = %s"
            values = (headers, cookies, login)
            cur.execute(update, values)
            self.con.commit()
            self.con.close()
            await Log.info(f'tokens: {headers[0:25]}, {cookies[0:10]} was updated')

    async def update_status(self, login: str, status: str):  # update status to avoid errors in updating tokens
        with self.con.cursor() as cur:
            update = f"UPDATE {self.table} SET status = %s  WHERE login = %s"
            values = (status, login)
            cur.execute(update, values)
            self.con.commit()
            self.con.close()
            await Log.info(f'For account: {login} set {status} status\n')

    @staticmethod
    async def registration(login: str, password: str, encode_pass: str, safe_pass: str):

        await DbAccounts().register_custom(login=login)
        await DbAccounts().update_passwords(login=login, password=password, encode_pass=encode_pass, safe_pass=safe_pass)
        await DbAccounts().update_status(login=login, status='offline')
        await Log().info(f'{login} registered!')

    async def return_data(self) -> tuple[dict]:  # return data from table
        con = self.con
        with con.cursor() as cur:
            select = f"SELECT * FROM {self.table};"
            cur.execute(select)
            return cur.fetchall()


class DbCode(Database):  # Database for collect codes what was used

    def __init__(self):
        super().__init__()
        self.table = 'code_table'
        with self.con.cursor() as cur:
            select = f"USE {self.db_c_name};"
            cur.execute(select)
        self.con.commit()

    async def create_table(self):  # create code table
        with self.con.cursor() as cur:
            create_table = f"CREATE TABLE IF NOT EXISTS {self.table}(id SERIAL PRIMARY KEY, code TEXT);"
            cur.execute(create_table)

            insert = "INSERT INTO code_table (code) VALUES (%s);"
            values = 'ASDASD12'
            cur.execute(insert, values)
            self.con.commit()
            self.con.close()
            await Log.info(f'{self.table} created!')

    async def code_update(self, queue_main=None, code: str = None):  # update code to avoid reuse code
        with self.con.cursor() as cur:

            codes = await DbCode().return_data()

            if not code:
                await QueueSearch.find_queue(queue=queue_main, required_mark='code_check')
                code = await QueueSearch.find_queue(queue=queue_main, required_mark='code')

            if len(codes.split(',')) > 100:  # if numbers of codes more than 250 clear list
                update = f"UPDATE {self.table} SET code = %s WHERE id = 1;"
                values = ','.join(codes.split(',')[-3::]) + f',{code}'
                cur.execute(update, values)
                await Log().info_positive('list of code was cleared')
                self.con.commit()
                return

            if code not in codes:  # add new code in list of codes
                update = "UPDATE code_table SET code = %s WHERE id = 1;"
                values = codes + ',' + code
                cur.execute(update, values)
                self.con.commit()
                self.con.close()
                await Log().info_positive(f'code {code} was added in list')
                return

            # raise Exception
            await Log().error(f'code: {code} has already been in list!')
            raise Exception(f'code: {code} has already been in list!')

    @staticmethod
    async def code_check(queue_main, code: str = None):  # checks if the code is in the list
        codes = await DbCode().return_data()

        if not code:
            code = await QueueSearch.find_queue(queue=queue_main, required_mark='code')

        if code in codes:
            await Log().warning(f'code: {code} already in list!')
            raise Exception(f'code: {code} already in list!')

        await queue_main.put(('code_check', True))
        return

    async def return_data(self) -> str:  # return data from table
        con = self.con
        with con.cursor() as cur:
            select = f"SELECT * FROM {self.table};"
            cur.execute(select)
            return cur.fetchall()[0]['code']


class DbError(Database):  # Database for count amount of errors

    def __init__(self):
        super().__init__()
        self.table = 'error_table'
        with self.con.cursor() as cur:
            select = f"USE {self.db_e_name};"
            cur.execute(select)
        self.con.commit()

    async def create_table(self):  # create error table
        with self.con.cursor() as cur:
            create_table = f"CREATE TABLE if not exists {self.table}(id SERIAL PRIMARY KEY, error TEXT);"
            cur.execute(create_table)

            insert = "INSERT INTO error_table (error) VALUES (%s);"
            values = '0'
            cur.execute(insert, values)
            self.con.commit()
            self.con.close()
            await Log.info(f'{self.table} created!')

    async def error_update(self):  # update numbers of errors
        error_number = await DbError().return_data()

        if int(error_number) > 1000:
            error_number = await DbError().error_nullify()

        with self.con.cursor() as cur:

            update = f"UPDATE {self.table} SET error = %s WHERE id = 1;"
            values = int(error_number) + 1
            cur.execute(update, values)
            self.con.commit()
            self.con.close()
            await Log().info(f'number of error updated, now: {values}')

    async def error_nullify(self):  # reset number of error
        with self.con.cursor() as cur:
            update = f"UPDATE {self.table} SET error = %s WHERE id = 1;"
            values = 1
            cur.execute(update, values)
            self.con.commit()
            self.con.close()
            await Log().info('number of errors set 0')
            return '0'

    async def return_data(self) -> str:  # return data from table
        con = self.con
        with con.cursor() as cur:
            select = f"SELECT * FROM {self.table};"
            cur.execute(select)
            return cur.fetchall()[0]['error']


class CodeStatistic(Database):

    def __init__(self):
        super().__init__()
        self.table = 'statistic_table'
        with self.con.cursor() as cur:
            select = f"USE {self.db_s_name};"
            cur.execute(select)
        self.con.commit()

    async def create_table(self):  # create accounts_data table
        con = self.con
        with con.cursor() as cur:
            creating_table = f"CREATE TABLE IF NOT EXISTS {self.table}" \
                             "(id SERIAL PRIMARY KEY, " \
                             "code TEXT, " \
                             "coin TEXT, " \
                             "amount TEXT, " \
                             "year TEXT, " \
                             "month TEXT, day TEXT, " \
                             "hour TEXT, " \
                             "channel TEXT);"

            cur.execute(creating_table)
            await Log().info_positive(f'{self.table} table created')

    async def insert_code_data(self, queue_main=None, channel: str = None, statistic_data: dict = None):

        if not channel:
            channel = await QueueSearch().find_queue(queue=queue_main, required_mark='chat_name')
            statistic_data = await QueueSearch().find_queue(queue=queue_main, required_mark='statistic_data')

        if statistic_data['code'] is False:
            self.con.commit()
            self.con.close()
            return

        code = statistic_data['code']
        coin = statistic_data['coin']
        amount = statistic_data['amount']

        hour = datetime.datetime.now().hour
        day = datetime.datetime.now().day
        month = datetime.datetime.now().month
        year = datetime.datetime.now().year

        with self.con.cursor() as cur:
            insert = f"INSERT INTO {self.table} (code, coin, amount, year, month, day, hour, channel) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
            value = (code, coin, amount, year, month, day, hour, channel)
            cur.execute(insert, value)
            self.con.commit()
            self.con.close()
            await Log.info(
                f'{self.table} got new data: {code}, {coin}, {amount} in day: {day}, hour: {hour}, by {channel}')

    @staticmethod
    async def return_by_coin_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        #  statistic by coin:
        if return_data['coin']:
            return [statistic for statistic in statistics if statistic['coin'] == return_data['coin']]

        return statistics

    @staticmethod
    async def return_by_year_month_day_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year'] and return_data['month'] and return_data['day'] and return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['month'] == return_data['month'] and
                    statistic['day'] == return_data['day'] and
                    statistic['hour'] == return_data['hour']]

        return statistics

    @staticmethod
    async def return_by_coin_year_month_day_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year'] and return_data['month'] and return_data['day'] and return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['month'] == return_data['month'] and
                    statistic['day'] == return_data['day'] and
                    statistic['hour'] == return_data['hour'] and
                    statistic['coin'] == return_data['coin']]

        return statistics

    @staticmethod
    async def return_by_channel_year_month_day_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year'] and return_data['month'] and return_data['day'] and return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['month'] == return_data['month'] and
                    statistic['day'] == return_data['day'] and
                    statistic['hour'] == return_data['hour'] and
                    statistic['channel'] == return_data['channel']]

        return statistics

    @staticmethod
    async def return_by_coin_channel_year_month_day_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year'] and return_data['month'] and return_data['day'] and return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['month'] == return_data['month'] and
                    statistic['day'] == return_data['day'] and
                    statistic['hour'] == return_data['hour'] and
                    statistic['channel'] == return_data['channel'] and
                    statistic['coin'] == return_data['coin']]

        return statistics

    @staticmethod
    async def return_by_year_month_day_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['day']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['month'] == return_data['month'] and
                    statistic['day'] == return_data['day']]

        return statistics

    @staticmethod
    async def return_by_coin_year_month_day_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year'] and return_data['month'] and return_data['day']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['month'] == return_data['month'] and
                    statistic['day'] == return_data['day'] and
                    statistic['coin'] == return_data['coin']]

        return statistics

    @staticmethod
    async def return_by_channel_year_month_day_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year'] and return_data['month'] and return_data['day']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['month'] == return_data['month'] and
                    statistic['day'] == return_data['day'] and
                    statistic['channel'] == return_data['channel']]

        return statistics

    @staticmethod
    async def return_by_coin_channel_year_month_day_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year'] and return_data['month'] and return_data['day']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['month'] == return_data['month'] and
                    statistic['day'] == return_data['day'] and
                    statistic['coin'] == return_data['coin'] and
                    statistic['channel'] == return_data['channel']]

        return statistics

    @staticmethod
    async def return_by_month_day_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['month'] and return_data['day'] and return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['month'] == return_data['month'] and
                    statistic['day'] == return_data['day'] and
                    statistic['hour'] == return_data['hour']]

        return statistics

    @staticmethod
    async def return_by_coin_month_day_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['month'] and return_data['day'] and return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['month'] == return_data['month'] and
                    statistic['day'] == return_data['day'] and
                    statistic['hour'] == return_data['hour'] and
                    statistic['coin'] == return_data['coin']]

        return statistics

    @staticmethod
    async def return_by_channel_month_day_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['month'] and return_data['day'] and return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['month'] == return_data['month'] and
                    statistic['day'] == return_data['day'] and
                    statistic['hour'] == return_data['hour'] and
                    statistic['channel'] == return_data['channel']]

        return statistics

    @staticmethod
    async def return_by_coin_channel_month_day_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['month'] and return_data['day'] and return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['month'] == return_data['month'] and
                    statistic['day'] == return_data['day'] and
                    statistic['hour'] == return_data['hour'] and
                    statistic['coin'] == return_data['coin'] and
                    statistic['channel'] == return_data['channel']]

        return statistics

    @staticmethod
    async def return_by_year_day_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year'] and return_data['day'] and return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['day'] == return_data['day'] and
                    statistic['hour'] == return_data['hour']]

        return statistics

    @staticmethod
    async def return_by_coin_year_day_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year'] and return_data['day'] and return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['day'] == return_data['day'] and
                    statistic['hour'] == return_data['hour'] and
                    statistic['coin'] == return_data['coin']]

        return statistics

    @staticmethod
    async def return_by_channel_year_day_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year'] and return_data['day'] and return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['day'] == return_data['day'] and
                    statistic['hour'] == return_data['hour'] and
                    statistic['channel'] == return_data['channel']]

        return statistics

    @staticmethod
    async def return_by_coin_channel_year_day_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year'] and return_data['day'] and return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['day'] == return_data['day'] and
                    statistic['hour'] == return_data['hour'] and
                    statistic['coin'] == return_data['coin'] and
                    statistic['channel'] == return_data['channel']]

        return statistics

    @staticmethod
    async def return_by_year_month_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year'] and return_data['month'] and return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['month'] == return_data['month'] and
                    statistic['hour'] == return_data['hour']]

        return statistics

    @staticmethod
    async def return_by_coin_year_month_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year'] and return_data['month'] and return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['month'] == return_data['month'] and
                    statistic['hour'] == return_data['hour'] and
                    statistic['coin'] == return_data['coin']]

        return statistics

    @staticmethod
    async def return_by_channel_year_month_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year'] and return_data['month'] and return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['month'] == return_data['month'] and
                    statistic['hour'] == return_data['hour'] and
                    statistic['channel'] == return_data['channel']]

        return statistics

    @staticmethod
    async def return_by_coin_channel_year_month_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year'] and return_data['month'] and return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['month'] == return_data['month'] and
                    statistic['hour'] == return_data['hour'] and
                    statistic['coin'] == return_data['coin'] and
                    statistic['channel'] == return_data['channel']]

        return statistics

    @staticmethod
    async def return_by_year_month_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year'] and return_data['month']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['month'] == return_data['month']]

        return statistics

    @staticmethod
    async def return_by_coin_year_month_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year'] and return_data['month']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['month'] == return_data['month'] and
                    statistic['coin'] == return_data['coin']]

        return statistics

    @staticmethod
    async def return_by_channel_year_month_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year'] and return_data['month']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['month'] == return_data['month'] and
                    statistic['channel'] == return_data['channel']]

        return statistics

    @staticmethod
    async def return_by_coin_channel_year_month_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year'] and return_data['month']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['month'] == return_data['month'] and
                    statistic['coin'] == return_data['coin'] and
                    statistic['channel'] == return_data['channel']]

        return statistics

    @staticmethod
    async def return_by_year_day_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year'] and return_data['day']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['day'] == return_data['day']]

        return statistics

    @staticmethod
    async def return_by_coin_year_day_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year'] and return_data['day']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['day'] == return_data['day'] and
                    statistic['coin'] == return_data['coin']]

        return statistics

    @staticmethod
    async def return_by_channel_year_day_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year'] and return_data['day']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['day'] == return_data['day'] and
                    statistic['channel'] == return_data['channel']]

        return statistics

    @staticmethod
    async def return_by_coin_channel_year_day_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year'] and return_data['day']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['day'] == return_data['day'] and
                    statistic['coin'] == return_data['coin'] and
                    statistic['channel'] == return_data['channel']]

        return statistics

    @staticmethod
    async def return_by_year_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year'] and return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['hour'] == return_data['hour']]

        return statistics

    @staticmethod
    async def return_by_coin_year_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year'] and return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['hour'] == return_data['hour'] and
                    statistic['coin'] == return_data['coin']]

        return statistics

    @staticmethod
    async def return_by_channel_year_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year'] and return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['hour'] == return_data['hour'] and
                    statistic['channel'] == return_data['channel']]

        return statistics

    @staticmethod
    async def return_by_coin_channel_year_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year'] and return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['hour'] == return_data['hour'] and
                    statistic['coin'] == return_data['coin'] and
                    statistic['channel'] == return_data['channel']]

        return statistics

    @staticmethod
    async def return_by_month_day_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['month'] and return_data['day']:
            return [statistic for statistic in statistics if
                    statistic['month'] == return_data['month'] and
                    statistic['day'] == return_data['day']]

        return statistics

    @staticmethod
    async def return_by_coin_month_day_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['month'] and return_data['day']:
            return [statistic for statistic in statistics if
                    statistic['month'] == return_data['month'] and
                    statistic['day'] == return_data['day'] and
                    statistic['coin'] == return_data['coin']]

        return statistics

    @staticmethod
    async def return_by_channel_month_day_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['month'] and return_data['day']:
            return [statistic for statistic in statistics if
                    statistic['month'] == return_data['month'] and
                    statistic['day'] == return_data['day'] and
                    statistic['channel'] == return_data['channel']]

        return statistics

    @staticmethod
    async def return_by_coin_channel_month_day_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['month'] and return_data['day']:
            return [statistic for statistic in statistics if
                    statistic['month'] == return_data['month'] and
                    statistic['day'] == return_data['day'] and
                    statistic['coin'] == return_data['coin'] and
                    statistic['channel'] == return_data['channel']]

        return statistics

    @staticmethod
    async def return_by_month_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['month'] and return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['month'] == return_data['month'] and
                    statistic['hour'] == return_data['hour']]

        return statistics

    @staticmethod
    async def return_by_coin_month_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['month'] and return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['month'] == return_data['month'] and
                    statistic['hour'] == return_data['hour'] and
                    statistic['coin'] == return_data['coin']]

        return statistics

    @staticmethod
    async def return_by_channel_month_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['month'] and return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['month'] == return_data['month'] and
                    statistic['hour'] == return_data['hour'] and
                    statistic['channel'] == return_data['channel']]

        return statistics

    @staticmethod
    async def return_by_coin_channel_month_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['month'] and return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['month'] == return_data['month'] and
                    statistic['hour'] == return_data['hour'] and
                    statistic['coin'] == return_data['coin'] and
                    statistic['channel'] == return_data['channel']]

        return statistics

    @staticmethod
    async def return_by_day_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['day'] and return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['day'] == return_data['day'] and
                    statistic['hour'] == return_data['hour']]

        return statistics

    @staticmethod
    async def return_by_coin_day_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['day'] and return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['day'] == return_data['day'] and
                    statistic['hour'] == return_data['hour'] and
                    statistic['coin'] == return_data['coin']]

        return statistics

    @staticmethod
    async def return_by_channel_day_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['day'] and return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['day'] == return_data['day'] and
                    statistic['hour'] == return_data['hour'] and
                    statistic['channel'] == return_data['channel']]

        return statistics

    @staticmethod
    async def return_by_coin_channel_day_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['day'] and return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['day'] == return_data['day'] and
                    statistic['hour'] == return_data['hour'] and
                    statistic['coin'] == return_data['coin'] and
                    statistic['channel'] == return_data['channel']]

        return statistics

    @staticmethod
    async def return_by_year_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year']]

        return statistics

    @staticmethod
    async def return_by_coin_year_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['coin'] == return_data['coin']]

        return statistics

    @staticmethod
    async def return_by_channel_year_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['channel'] == return_data['channel']]

        return statistics

    @staticmethod
    async def return_by_coin_channel_year_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['year']:
            return [statistic for statistic in statistics if
                    statistic['year'] == return_data['year'] and
                    statistic['coin'] == return_data['coin'] and
                    statistic['channel'] == return_data['channel']]

        return statistics

    @staticmethod
    async def return_by_month_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['month']:
            return [statistic for statistic in statistics if
                    statistic['month'] == return_data['month']]

        return statistics

    @staticmethod
    async def return_by_coin_month_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['month']:
            return [statistic for statistic in statistics if
                    statistic['coin'] == return_data['coin'] and
                    statistic['month'] == return_data['month']]

        return statistics

    @staticmethod
    async def return_by_channel_month_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['month']:
            return [statistic for statistic in statistics if
                    statistic['month'] == return_data['month'] and
                    statistic['channel'] == return_data['channel']]

        return statistics

    @staticmethod
    async def return_by_coin_channel_month_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['month']:
            return [statistic for statistic in statistics if
                    statistic['coin'] == return_data['coin'] and
                    statistic['channel'] == return_data['channel'] and
                    statistic['month'] == return_data['month']]

        return statistics

    @staticmethod
    async def return_by_day_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['day']:
            return [statistic for statistic in statistics if
                    statistic['day'] == return_data['day']]

        return statistics

    @staticmethod
    async def return_by_coin_day_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['day']:
            return [statistic for statistic in statistics if
                    statistic['day'] == return_data['day'] and
                    statistic['coin'] == return_data['coin']]

        return statistics

    @staticmethod
    async def return_by_channel_day_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['day']:
            return [statistic for statistic in statistics if
                    statistic['day'] == return_data['day'] and
                    statistic['channel'] == return_data['channel']]

        return statistics

    @staticmethod
    async def return_by_coin_channel_day_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['day']:
            return [statistic for statistic in statistics if
                    statistic['day'] == return_data['day'] and
                    statistic['coin'] == return_data['coin'] and
                    statistic['channel'] == return_data['channel']]

        return statistics

    @staticmethod
    async def return_by_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['hour'] == return_data['hour']]

        return statistics

    @staticmethod
    async def return_by_coin_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['hour'] == return_data['hour'] and
                    statistic['coin'] == return_data['coin']]

        return statistics

    @staticmethod
    async def return_by_channel_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['hour'] == return_data['hour'] and
                    statistic['channel'] == return_data['channel']]

        return statistics

    @staticmethod
    async def return_by_coin_channel_hour_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['hour']:
            return [statistic for statistic in statistics if
                    statistic['hour'] == return_data['hour'] and
                    statistic['coin'] == return_data['coin'] and
                    statistic['channel'] == return_data['channel']]

        return statistics

    @staticmethod
    async def return_by_channel_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['channel']:
            return [statistic for statistic in statistics if
                    statistic['channel'] == return_data['channel']]

        return statistics

    @staticmethod
    async def return_by_coin_channel_statistic(return_data: dict):
        statistics = await CodeStatistic().return_statistic_data()

        if return_data['coin'] and return_data['channel']:
            return [statistic for statistic in statistics if
                    statistic['channel'] == return_data['channel'] and
                    statistic['coin'] == return_data['coin']]

        return statistics

    async def return_statistic_data(self) -> tuple[dict]:
        with self.con.cursor() as cur:
            select = f"SELECT * FROM {self.table};"
            cur.execute(select)
        self.con.close()
        return cur.fetchall()

    async def merge_amount_coin(self, statistics: list):

        coins = set([statistic['coin'] for statistic in statistics])
        channels = set([statistic['channel'] for statistic in statistics])

        merge_coins = {}
        merge_channels = {}

        for coin in coins:
            amount = 0
            for statistic in statistics:
                if coin == statistic['coin']:
                    amount = float(amount) + float(statistic['amount'])
            merge_coins.update({coin: (f"%.15f" % amount).rstrip('0')})

        for channel in channels:
            count = 0
            for statistic in statistics:
                if channel == statistic['channel']:
                    count += 1
            merge_channels.update({channel: count})
        return [merge_coins, merge_channels]

    @staticmethod
    async def return_merge_statistic_by_keys(coin: str = None, channel: str = None, year: str = None, month: str = None, day: str = None, hour: str = None):
        function_name = 'return_by'
        return_data = {}

        if coin:
            function_name = function_name + '_coin'
            return_data.update({'coin': coin})

        if channel:
            function_name = function_name + '_channel'
            return_data.update({'channel': channel})

        if year:
            function_name = function_name + '_year'
            return_data.update({'year': year})

        if month:
            function_name = function_name + '_month'
            return_data.update({'month': month})

        if day:
            function_name = function_name + '_day'
            return_data.update({'day': day})

        if hour:
            function_name = function_name + '_hour'
            return_data.update({'hour': hour})

        function_name = function_name + '_statistic'

        function = return_name_function[function_name]

        statistics = await function(return_data=return_data)

        return await CodeStatistic().merge_amount_coin(statistics=statistics)


return_name_function = {
    'return_by_coin_statistic': CodeStatistic.return_by_coin_statistic,
    'return_by_channel_statistic': CodeStatistic.return_by_channel_statistic,
    'return_by_coin_channel_statistic': CodeStatistic.return_by_coin_channel_statistic,

    'return_by_year_month_day_hour_statistic': CodeStatistic.return_by_year_month_day_hour_statistic,
    'return_by_year_month_day_statistic': CodeStatistic.return_by_year_month_day_statistic,
    'return_by_year_month_hour_statistic': CodeStatistic.return_by_year_month_hour_statistic,
    'return_by_year_month_statistic': CodeStatistic.return_by_year_month_statistic,
    'return_by_year_day_hour_statistic': CodeStatistic.return_by_year_day_hour_statistic,
    'return_by_year_day_statistic': CodeStatistic.return_by_year_day_statistic,
    'return_by_year_hour_statistic': CodeStatistic.return_by_year_hour_statistic,
    'return_by_year_statistic': CodeStatistic.return_by_year_statistic,
    'return_by_month_day_hour_statistic': CodeStatistic.return_by_month_day_hour_statistic,
    'return_by_month_day_statistic': CodeStatistic.return_by_month_day_statistic,
    'return_by_month_hour_statistic': CodeStatistic.return_by_month_hour_statistic,
    'return_by_day_hour_statistic': CodeStatistic.return_by_day_hour_statistic,
    'return_by_month_statistic': CodeStatistic.return_by_month_statistic,
    'return_by_day_statistic': CodeStatistic.return_by_day_statistic,
    'return_by_hour_statistic': CodeStatistic.return_by_hour_statistic,

    'return_by_coin_year_month_day_hour_statistic': CodeStatistic.return_by_coin_year_month_day_hour_statistic,
    'return_by_coin_year_month_hour_statistic': CodeStatistic.return_by_coin_year_month_hour_statistic,
    'return_by_coin_year_month_statistic': CodeStatistic.return_by_coin_year_month_statistic,
    'return_by_coin_year_day_statistic': CodeStatistic.return_by_coin_year_day_statistic,
    'return_by_coin_year_day_hour_statistic': CodeStatistic.return_by_coin_year_day_hour_statistic,
    'return_by_coin_year_hour_statistic': CodeStatistic.return_by_coin_year_hour_statistic,
    'return_by_coin_year_statistic': CodeStatistic.return_by_coin_year_statistic,
    'return_by_coin_month_day_hour_statistic': CodeStatistic.return_by_coin_month_day_hour_statistic,
    'return_by_coin_month_day_statistic': CodeStatistic.return_by_coin_month_day_statistic,
    'return_by_coin_month_hour_statistic': CodeStatistic.return_by_coin_month_hour_statistic,
    'return_by_coin_month_statistic': CodeStatistic.return_by_coin_month_statistic,
    'return_by_coin_day_hour_statistic': CodeStatistic.return_by_coin_day_hour_statistic,
    'return_by_coin_day_statistic': CodeStatistic.return_by_coin_day_statistic,
    'return_by_coin_hour_statistic': CodeStatistic.return_by_coin_hour_statistic,

    'return_by_channel_year_month_day_hour_statistic': CodeStatistic.return_by_channel_year_month_day_hour_statistic,
    'return_by_channel_year_month_day_statistic': CodeStatistic.return_by_channel_year_month_day_statistic,
    'return_by_channel_year_month_hour_statistic': CodeStatistic.return_by_channel_year_month_hour_statistic,
    'return_by_channel_year_month_statistic': CodeStatistic.return_by_channel_year_month_statistic,
    'return_by_channel_year_day_hour_statistic': CodeStatistic.return_by_channel_year_day_hour_statistic,
    'return_by_channel_year_day_statistic': CodeStatistic.return_by_channel_year_day_statistic,
    'return_by_channel_year_hour_statistic': CodeStatistic.return_by_channel_year_hour_statistic,
    'return_by_channel_year_statistic': CodeStatistic.return_by_channel_year_statistic,
    'return_by_channel_month_day_hour_statistic': CodeStatistic.return_by_channel_month_day_hour_statistic,
    'return_by_channel_month_day_statistic': CodeStatistic.return_by_channel_month_day_statistic,
    'return_by_channel_month_hour_statistic': CodeStatistic.return_by_channel_month_hour_statistic,
    'return_by_channel_month_statistic': CodeStatistic.return_by_channel_month_statistic,
    'return_by_channel_day_hour_statistic': CodeStatistic.return_by_channel_day_hour_statistic,
    'return_by_channel_day_statistic': CodeStatistic.return_by_channel_day_statistic,
    'return_by_channel_hour_statistic': CodeStatistic.return_by_channel_hour_statistic,

    'return_by_coin_channel_year_month_day_hour_statistic': CodeStatistic.return_by_coin_channel_year_month_day_hour_statistic,
    'return_by_coin_channel_year_month_day_statistic': CodeStatistic.return_by_coin_channel_year_month_day_statistic,
    'return_by_coin_channel_year_month_hour_statistic': CodeStatistic.return_by_coin_channel_year_month_hour_statistic,
    'return_by_coin_channel_year_month_statistic': CodeStatistic.return_by_coin_channel_year_month_statistic,
    'return_by_coin_channel_year_day_statistic': CodeStatistic.return_by_coin_channel_year_day_statistic,
    'return_by_coin_channel_year_day_hour_statistic': CodeStatistic.return_by_coin_channel_year_day_hour_statistic,
    'return_by_coin_channel_year_hour_statistic': CodeStatistic.return_by_coin_channel_year_hour_statistic,
    'return_by_coin_channel_year_statistic': CodeStatistic.return_by_coin_channel_year_statistic,
    'return_by_coin_channel_month_day_hour_statistic': CodeStatistic.return_by_coin_channel_month_day_hour_statistic,
    'return_by_coin_channel_month_day_statistic': CodeStatistic.return_by_coin_channel_month_day_statistic,
    'return_by_coin_channel_month_hour_statistic': CodeStatistic.return_by_coin_channel_month_hour_statistic,
    'return_by_coin_channel_month_statistic': CodeStatistic.return_by_coin_channel_month_statistic,
    'return_by_coin_channel_day_hour_statistic': CodeStatistic.return_by_coin_channel_day_hour_statistic,
    'return_by_coin_channel_day_statistic': CodeStatistic.return_by_coin_channel_day_statistic,
    'return_by_coin_channel_hour_statistic': CodeStatistic.return_by_coin_channel_hour_statistic,
}

# a = asyncio.run(CodeStatistic().return_merge_statistic_by_keys(
#     day='19'
# ))
# print(a)

# asyncio.run(DbCode().create_table())
# asyncio.run(CodeStatistic().insert_code_data(
#     statistic_data={'coin': 'test', 'code': 'test', 'amount': '0.1'}, channel='TEST'
# ))


# asyncio.run(DbAccounts().update_status(login='cupleniuakk1cruptomillioner4@gmail.com', status='online'))
# asyncio.run(DbAccounts().update_status(login='cupleniuakk1cruptomillioner6@gmail.com', status='offline'))
# asyncio.run(DbAccounts().update_status(login='kentcriptomillioner4@gmail.com', status=`'offline'))
# asyncio.run(DbAccounts().update_status(login='deniscriptomillioner2@gmail.com', status='closed'))

# asyncio.run(DbAccounts().registration(
#     login='cupleniuakk1cruptomillioner6@gmail.com',
#     password='Zzzzzzz123',
#     encode_pass='e643c96a12581099a15eff147d173656',
#     safe_pass='21f079437782a042e9545ff375d45659ed4ee27fb5f75842e887c98f5a57e07671b5fe43a2c73998ec14b3722d3ae3323f5d05ec8168df49220003f84ec7ab96'
# ))