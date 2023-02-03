import numpy as np
import pandas as pd

import requests
from datetime import date, timedelta, datetime
import xmltodict
from dateutil.parser import parse

from db_config import Dbconfig
import sqlite3
import os
from data_config import Colconfig

import logging
logger = logging.getLogger('scrap_dls_els_data_from_public_portal')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logfile_path = os.path.join(Dbconfig.SCRAP_DATA_PATH, Dbconfig.LOGFILE_NAME)
file_handler = logging.FileHandler(logfile_path)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def check_dup_date():
    dt1 = parse('20230101')
    dt2 = datetime.fromordinal(date.today().toordinal()) - timedelta(1)

    db_path = os.path.join(Dbconfig.SCRAP_DATA_PATH, Dbconfig.SHARE_TABLE_NAME)

    conn = sqlite3.connect(db_path)
    st_en_date = pd.read_sql_query(Dbconfig.GET_DAY_SQL, conn)
    conn.close()

    st_en_date = st_en_date.values.tolist()[0]

    before_date = None
    after_date = None
    if dt1 < parse(st_en_date[0]):
        before_date = (dt1, parse(st_en_date[0]) - timedelta(1))

    if dt2 > parse(st_en_date[1]):
        after_date = (parse(st_en_date[1]) + timedelta(1), dt2)

    return before_date, after_date


def api_dls_els_share_data(dt1, dt2):
    url = 'http://api.seibro.or.kr/openapi/service/DerivesSvc/getElsDlsIssucoBalanceStatusN1'
    service_key = ''
    delta = dt2 - dt1
    raw_array = list()

    for i in range(delta.days + 1):
        day = dt1 + timedelta(days=i)
        day = day.strftime('%Y%m%d')

        params = {'serviceKey': service_key, 'numOfRows': '100', 'pageNo': '1', 'stdDt': day}

        response = requests.get(url, params=params)
        dict_type = xmltodict.parse(response.text)

        if dict_type['response']['body']['items'] is not None:
            rows = dict_type['response']['body']['items']['item']
            rows = list(np.append(np.fromiter(i.values(), dtype=np.object_, count=-1), day) for i in rows)
            raw_array += rows

    return raw_array


def insert_scrap_data(insert_data):
    insert_data = insert_data.astype('str')

    db_path = os.path.join(Dbconfig.SCRAP_DATA_PATH, Dbconfig.SHARE_TABLE_NAME)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    for tmp_row in insert_data.values:
        tmp_nums = np.delete(tmp_row, [-3, -5]).tolist()
        tmp_nums = ', '.join(tmp_nums)
        tmp_date = tmp_row[-3]
        tmp_company = tmp_row[-5]
        insert_dls_els_share_sql = f'''INSERT INTO DlsElsShare (balanceSum, dlsNsubsGuarBalance, dlsNsubsGuarSecncnt
                , dlsNsubsNguarBalance, dlsNsubsNguarSecnCnt, dlsSubsGuarBalance, dlsSubsGuarSecncnt
                , dlsSubsNguarBalance, dlsSubsNguarSecncnt, elsNsubsGuarBalance, elsNsubsGuarSecncnt
                , elsNsubsNguarBalance, elsNsubsNguarSecncnt, elsSubsGuarBalance, elsSubsGuarSecncnt
                , elsSubsNguarBalance, elsSubsNguarSecncnt, secncntSum, balanceSumDls, balanceSumEls, date, repSecnNm)
            SELECT {tmp_nums}, "{tmp_date}", "{tmp_company}"
            WHERE NOT EXISTS (SELECT * FROM DlsElsShare
            WHERE date = "{tmp_date}" AND repSecnNm = "{tmp_company}")'''
        cur.execute(insert_dls_els_share_sql)

    conn.commit()
    cur.close()
    conn.close()


def scrap_dls_els_share_data():
    raw_array = list()
    before_date, after_date = check_dup_date()

    if (before_date is None) & (after_date is None):
        logger.info("scrap data empty")
    else:
        logger.info(before_date, after_date)

    if before_date is not None:
        raw_array += api_dls_els_share_data(*before_date)
    if after_date is not None:
        raw_array += api_dls_els_share_data(*after_date)

    raw_data = None
    if len(raw_array) > 0:
        raw_data = pd.DataFrame(raw_array, columns=Colconfig.total_api_cols)

        ''' preprocessing '''
        raw_data.loc[:, Colconfig.numeric_cols] = raw_data.loc[:, Colconfig.numeric_cols].astype('float64')
        raw_data.loc[:, Colconfig.date_cols] = raw_data.loc[:, Colconfig.date_cols].apply(pd.to_datetime)
        raw_data.sort_values(by='date', ignore_index=True, inplace=True)
        raw_data['balanceSumDls'] = raw_data.loc[:, Colconfig.init_dls_bal].apply(lambda x: x.sum(), axis=1)
        raw_data['balanceSumEls'] = raw_data.loc[:, Colconfig.init_els_bal].apply(lambda x: x.sum(), axis=1)

        insert_scrap_data(raw_data)
        logger.info(raw_data.shape)


if __name__ == '__main__':
    scrap_dls_els_share_data()

    # filename_head = 'dls_els_share'
    # output_filename = '_'.join([filename_head, input_start_dt, input_end_dt]) + '.parquet'
    # output_path = os.path.join(Dbconfig.SCRAP_DATA_PATH, output_filename)
    # result_data.to_parquet(output_path, index=False)
