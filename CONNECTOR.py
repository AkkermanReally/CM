import sys
sys.path.append('../')

from CM.database import DbAccounts, DbCode, DbError, CodeStatistic


class Database:
    accounts_db = DbAccounts
    code_db = DbCode
    error_db = DbError
    statistic_db = CodeStatistic

