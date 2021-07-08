import pandas as pd

from connections import get_bq_client, get_bq_storage

def get_run_status():

    client = get_bq_client()
    bqstorageclient = get_bq_storage()

    sql = "SELECT * from rooscrape.foodhoover_store.scrape_log order by scrape_time desc limit 100"

    data = (
        client.query(sql)
        .result()
        .to_dataframe(bqstorage_client=bqstorageclient)
    )

    data = data.groupby(['run_id','step']).first().stack().unstack('step')

    return data

def get_ref_stats():
    client = get_bq_client()
    bqstorageclient = get_bq_storage()

    sql = "select vendor, count(1) as rx_total, sum(case when rx_name is null then 1 else 0 end) as no_name,sum(case when rx_postcode is null then 1 else 0 end) as no_postcode, sum(case when rx_sector is null then 1 else 0 end) as no_sector from rooscrape.foodhoover_store.rx_ref group by vendor"

    data = (
        client.query(sql)
        .result()
        .to_dataframe(bqstorage_client=bqstorageclient)
    )

    data.set_index('vendor', inplace=True)

    return data

def get_country_stats():
    client = get_bq_client()
    bqstorageclient = get_bq_storage()

    sql = "select run_id, roo, je, ue, fh from rooscrape.foodhoover_store.agg_country_run"

    data = (
        client.query(sql)
        .result()
        .to_dataframe(bqstorage_client=bqstorageclient)
    )

    data.set_index('run_id', inplace=True)

    return data