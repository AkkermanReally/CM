import os
from environs import Env
import ast


class Config:
    env = Env()
    path = os.path.abspath("CM.env")
    env.read_env(f'{path}'.replace('PA', 'CM'))

    def config_db(self) -> dict:
        return {
            'host': self.env.str('DB_HOST'),
            'password': self.env.str('MYSQL_PASSWORD'),
            'user': self.env.str('MYSQL_USER'),
            'db_port': self.env.int('DB_PORT'),
            'db_name': ast.literal_eval(self.env.str('DATABASE_NAME'))
        }

    def config_tg(self) -> dict:
        return {
            'api_id': self.env.int("API_ID"),
            'api_hash': self.env.str('API_HASH'),
            'phone': self.env.str('PHONE'),
        }
