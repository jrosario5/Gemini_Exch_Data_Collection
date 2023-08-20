from __future__ import annotations

import json
from textwrap import dedent

import pendulum

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pandas as pd
from pprint import pprint
import subprocess
#subprocess.check_call([sys.executable, "-m", "pip", "install", "robin-stocks==2.1.0"])
import robin_stocks.gemini as g
import pandas as pd
from datetime import date, datetime
from airflow.hooks.postgres_hook import PostgresHook


with DAG(
    "gemini_exchange_data_collection",
    schedule_interval="*/15 * * * *",
    default_args={"retries": 2},
    description="DAG gemini exchange",
    start_date=pendulum.datetime(2023, 8, 1, tz="UTC"),
    catchup=False,
    tags=["gemini"],
) as dag:
    dag.doc_md = __doc__
    def extract(**kwargs):
        gemini_key = Variable.get("gkey")
        gemini_secret = Variable.get("gsecret")
        coins = ["LTCUSD","BTCUSD","ETHUSD","RENUSD","MANAUSD","PEPEUSD","SHIBUSD","CRVUSD","ZECUSD","BATUSD","AMPUSD","YFIUSD","STORJUSD"]
        g.authentication.login(gemini_key, gemini_secret)
        
        data = {"Date":[], "Coin":[], "Gemini_buy_Price":[], "Gemini_sell_Price":[],"Gemini_USD_Volume":[], "Gemini_LTC_Volume":[],"Gemini_Bid":[], "Gemini_Ask":[], "Gemini_Last":[]}
        
        for coin in coins:
            coin_pub = g.crypto.get_pubticker(coin, jsonify=True)[0]
            data['Coin'].append(coin)
            data['Gemini_buy_Price'].append(float(g.crypto.get_price(coin, 'buy')))
            data['Gemini_sell_Price'].append(float(g.crypto.get_price(coin, 'sell')))
            data['Gemini_USD_Volume'].append(round(float(coin_pub['volume']['USD']), 2))
            data['Gemini_LTC_Volume'].append(round(float(coin_pub['volume'][coin.split('USD')[0]]), 5))
            data['Gemini_Bid'].append(float(coin_pub['bid']))
            data['Gemini_Ask'].append(float(coin_pub['ask']))
            data['Gemini_Last'].append(float(coin_pub['last']))
            data['Date'].append(str(datetime.now()))
        
        ti = kwargs["ti"]
        ti.xcom_push("order_data", data)

    def transform(**kwargs):
        ti = kwargs["ti"]
        extract_data_string = ti.xcom_pull(task_ids="extract", key="order_data")
        
        ti.xcom_push("order_data_trans", extract_data_string)

    def load(**kwargs):
        ti = kwargs["ti"]
        value_string = ti.xcom_pull(task_ids="transform", key="order_data_trans")
        df = pd.DataFrame(value_string)
        df['Date'] = pd.to_datetime(df['Date'], format=None)
        df.set_index('Date', inplace=True)

        engine = PostgresHook.get_hook("srv-captain--postgres").get_sqlalchemy_engine()
        
        df.to_sql("CryptoMarketWatchV2", con=engine, index=True, if_exists="append")

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )
    extract_task.doc_md = dedent(
        """\
    #### Extract task
    A simple Extract task to get data ready for the rest of the data pipeline.
    In this case, getting data is simulated by reading from a hardcoded JSON string.
    This data is then put into xcom, so that it can be processed by the next task.
    """
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )
    transform_task.doc_md = dedent(
        """\
    #### Transform task
    A simple Transform task which takes in the collection of order data from xcom
    and computes the total order value.
    This computed value is then put into xcom, so that it can be processed by the next task.
    """
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )
    load_task.doc_md = dedent(
        """\
    #### Load task
    A simple Load task which takes in the result of the Transform task, by reading it
    from xcom and instead of saving it to end user review, just prints it out.
    """
    )

    extract_task >> transform_task >> load_task
