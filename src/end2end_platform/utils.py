import re
from viet_text_tools import normalize_diacritics
from underthesea import word_tokenize
import emoji
from datetime import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine


def standardize_word(text):
    # thichhhhhh -> thich
    new_text = []
    for i in range(len(text)):
        if i == 0 or (text[i - 1] != text[i]):
            new_text.append(text[i])
    new_text = "".join(new_text)
    return new_text


def remove_too_long_word(text):
    splited = text.split(' ')
    copy_splited = splited.copy()
    for i in splited:
        if len(i) > 8:
            copy_splited.remove(i)
    text = ' '.join(copy_splited)
    return text


def preprocessing(text):
    # remove end line \n
    text = re.sub(r'\n', ' ', text)
    # lower case
    text = text.lower()
    # remove icon o cuoi cau ex: thichshhhhhh-> thich
    text = standardize_word(text)
    # check từ dài thì bỏ đi ex: 'scfkevgdjce' -> ''
    text = remove_too_long_word(text)
    # emoji xử lí
    text = emoji.demojize(str(text))
    # html remove
    text = re.sub(r'<.*?.>', '', text)
    # remove dot, comma, ...
    text = re.sub(r'[^\w\s]', ' ', text)
    # normalize diacritics
    text = normalize_diacritics(text)
    # tach tu
    text = word_tokenize(text, format='text')

    return text


def process_variation(datetime_in: list):
    datetime_out = []
    variation = []
    for i in range(len(datetime_in)):
        temp1, temp2 = datetime_in[i].split('|')
        temp1 = datetime.strptime(temp1[:-1], '%Y-%m-%d %H:%M')
        datetime_out.append(temp1)

        temp2 = re.sub(' Phân loại hàng: ', '', temp2)
        variation.append(temp2)

    return datetime_out, variation


def save_to_postgres(df, dest):
    # Saving the dataframe to local postgres
    engine = create_engine('postgresql+psycopg2://airflow_user:airflow_pass@localhost:5432/airflow_result')
    df.to_sql(dest, engine, if_exists='append', index=False)


def processing(df):
    # input: Dataframe with comment
    # output: Dataframe with additional fields: processed_comment
    df['processed_comment'] = np.nan
    cmt = df['comment'].values.tolist()
    for i in range(df.shape[0]):
        df.loc[i, 'processed_comment'] = preprocessing(cmt[i])
        if df.loc[i, 'processed_comment'] == '':
            df.loc[i, 'processed_comment'] = 'NAN'
    return df


def process_product_name(df):
    product_list = ['Son Kem Lì Hero Orange Thiết Kế Vỏ Trong Suốt MMS',
                    'Son kem Focallure Màu Nhung Sương Lì Mịn 16g',
                    'Son môi FOCALLURE 24g màu nhung mịn lên màu đẹp',
                    'Son Kem Lì Bbia Last Velvet Lip Tint Version 5 (5 màu) 5g - Bbia Official Store',
                    'Son Kem Lì Merzy The First Velvet Tint 4.5g',
                    'Son Kem Perfect Diary Màu Lì Tông Màu Lì Siêu Mịn Lâu Trôi 2.5g',
                    'Son Kem Lì Perfect Diary Tông Màu Đỏ Quyến Rũ 2.5g',
                    'Son kem lì, lên màu chuẩn Hàn Quốc Romand Zero Velvet Tint 5.5g',
                    'Son kem lì trà sữa, lâu trôi, Hàn Quốc Romand Milk Tea Velvet Tint 4.4g']

    short_name = ['Son Kem Lì Hero Orange',
                  'Son kem Focallure Màu Nhung',
                  'Son môi FOCALLURE',
                  'Son Kem Lì Bbia Last Velvet',
                  'Son Kem Lì Merzy',
                  'Son Kem Perfect Diary Màu Lì',
                  'Son Kem Lì Perfect Diary',
                  'Son kem lì, lên màu chuẩn',
                  'Son kem lì trà sữa']

    product_name = df['product_name'].values.tolist()
    final_name = []
    for i in product_name:
        a = re.search(r'\b(Son)\b', i)
        for index in range(len(short_name)):
            if short_name[index] in i[a.start():]:
                final_name.append(product_name[index])
    df['product_name'] = final_name
    return df
