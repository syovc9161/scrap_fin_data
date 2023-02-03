from dataclasses import dataclass

@dataclass
class Dbconfig:
    SHARE_TABLE_NAME = 'DlsElsShare'
    SCRAP_DATA_PATH = '.\\output'
    LOGFILE_NAME = '230203_daily_log.log'
    GET_DAY_SQL = '''SELECT
            min(date(date)) AS min_day
            , max(date(date)) AS min_day
        FROM DlsElsShare'''
