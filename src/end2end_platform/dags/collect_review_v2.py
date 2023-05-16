import logging
import re
import requests
import pandas as pd
from datetime import datetime
from utils import processing, save_to_postgres
from predict import predict_aspect_sentiment
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import DAG
from airflow.operators.python import PythonOperator


def main(**kwargs):
    link_list = [
        'https://shopee.vn/Son-Kem-Perfect-Diary-M%C3%A0u-L%C3%AC-T%C3%B4ng-M%C3%A0u-C%E1%BB%95-%C4%90i%E1'
        '%BB%83n-L%C3%A2u-Tr%C3%B4i-2.5g-i.277411443.7254565873',
        'https://shopee.vn/Son-Kem-L%C3%AC-Bbia-Last-Velvet-Lip-Tint-Version-5-('
        '5-m%C3%A0u)-5g-Bbia-Official-Store-i.9277307.2288984704',
        'https://shopee.vn/Son-m%C3%B4i-FOCALLURE-24g-m%C3%A0u-nhung-m%E1%BB%8Bn-l%C3%AAn-m%C3%A0u-%C4%91%E1'
        '%BA%B9p-i.182631756.9648515133',
        'https://shopee.vn/Son-kem-Focallure-M%C3%A0u-Nhung-S%C6%B0%C6%A1ng-L%C3%AC-M%E1%BB%8Bn-16g-i'
        '.182631756.7362120693',
        'https://shopee.vn/-M%C3%A3-COSDAY-gi%E1%BA%A3m-8-%C4%91%C6%A1n-150K-Ver-Noir-Son-Kem-L%C3%AC-Merzy'
        '-The-First-Velvet-Tint-4.5g-i.193207976.5012657584',
        'https://shopee.vn/-Full-25-m%C3%A0u-Son-kem-l%C3%AC-l%C3%AAn-m%C3%A0u-chu%E1%BA%A9n-H%C3%A0n-Qu%E1'
        '%BB%91c-Romand-Zero-Velvet-Tint-5.5g-('
        '05-%C4%91%E1%BB%8F-g%E1%BA%A1ch-06-%C4%91%E1%BB%8F-n%C3%A2u-20-%C4%91%E1%BB%8F-tr%E1%BA%A7m)-i'
        '.157995825.4402977583',
        'https://shopee.vn/Son-Kem-L%C3%AC-Hero-Orange-Thi%E1%BA%BFt-K%E1%BA%BF-V%E1%BB%8F-Trong-Su%E1%BB'
        '%91t-MMS-i.74804504.4170825878',
        'https://shopee.vn/Son-kem-l%C3%AC-tr%C3%A0-s%E1%BB%AFa-l%C3%A2u-tr%C3%B4i-m%E1%BB%8Bn-m%C3%B4i-H%C3'
        '%A0n-Qu%E1%BB%91c-Romand-Milk-Tea-Velvet-Tint-4.4g-('
        '01-%C4%91%E1%BB%8F-th%E1%BA%ABm-02-%C4%91%E1%BB%8F-n%C3%A2u-04-cam-g%E1%BA%A1ch)-i.157995825'
        '.4130437107'
    ]

    product_name_list = [
        'Son Kem Perfect Diary Màu Lì Tông Màu Lì Siêu Mịn Lâu Trôi 2.5g',
        'Son Kem Lì Bbia Last Velvet Lip Tint Version 5 (5 màu) 5g',
        'Son môi FOCALLURE 24g màu nhung mịn lên màu đẹp',
        'Son kem Focallure Màu Nhung Sương Lì Mịn 16g',
        'Son Kem Lì Merzy The First Velvet Tint 4.5g',
        'Son kem lì, lên màu chuẩn Hàn Quốc Romand Zero Velvet Tint 5.5g',
        'Son Kem Lì Hero Orange Thiết Kế Vỏ Trong Suốt MMS',
        'Son kem lì trà sữa, lâu trôi, Hàn Quốc Romand Milk Tea Velvet Tint 4.4g'
    ]
    df_full = pd.DataFrame()
    s = "https://shopee.vn/api/v2/item/get_ratings?filter=0&flag=1&itemid={item_id}&limit=20&offset={offset}&shopid={" \
        "shop_id}&type=0"
    for link_index in range(len(link_list)):
        r = re.search(r"i\.(\d+)\.(\d+)", link_list[link_index])
        shop_id, item_id = r[1], r[2]
        offset = 0
        ratings_url = s
        d = {"username": [], "rating": [], "comment": [], 'cmt_date': [],
             "shop_id": [], "item_id": [], "product_name": [],
             "variation": []}
        logging.info('collect reviews for product'+str(product_name_list[link_index]))
        while True:
            data = requests.get(
                ratings_url.format(shop_id=shop_id, item_id=item_id, offset=offset)
            ).json()
            try:
                if data["data"] is not None:
                    logging.info(data["data"])
                else:
                    break
            except:
                break

            i = 1
            for i, rating in enumerate(data["data"]["ratings"], 1):
                if rating["comment"] == '':
                    continue
                else:
                    d["username"].append(rating["author_username"])
                    d["rating"].append(rating["rating_star"])
                    d["comment"].append(rating["comment"])
                    d["cmt_date"].append(datetime.fromtimestamp(rating["ctime"]))
                    d["shop_id"].append(rating["shopid"])
                    d["item_id"].append(rating["itemid"])
                    try:
                        d["product_name"].append(product_name_list[link_index])
                        d["variation"].append(rating["product_items"][0]["model_name"])
                    except:
                        d["product_name"].append("NULL")
                        d["variation"].append("NULL")

            if offset == 500:
                break
            offset += 20
        df_full = pd.concat([df_full, pd.DataFrame(d)])

    df_full.reset_index(drop=True, inplace=True)
    df_processed = processing(df_full)
    df_predicted = predict_aspect_sentiment(df_processed)
    save_to_postgres(df_predicted, 'son_review_v2')


log = logging.getLogger(__name__)

dag = DAG(
    dag_id='collect_shoppe_review_v2',
    schedule_interval="30 22 * * *",
    start_date=datetime(2023, 5, 1),
    catchup=False,
    tags=['collect_review_shopee'], )

collect_review_v2_data = PythonOperator(task_id="collect_review_v2",
                                        python_callable=main,
                                        dag=dag)

select_distinct_data = PostgresOperator(
    task_id="select_postgres_data_v2",
    postgres_conn_id="postgres_localhost",
    sql="""CREATE TABLE IF NOT EXISTS son_review_dedupe_v2 AS 
                 SELECT DISTINCT * FROM son_review_v2;
            DROP TABLE son_review_v2;
        ALTER TABLE son_review_dedupe_v2 RENAME TO son_review_v2;""",
    dag=dag, )

collect_review_v2_data >> select_distinct_data
