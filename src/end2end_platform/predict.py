import logging
import os
import numpy as np
import pandas as pd
import pickle
from tensorflow import keras
from keras.preprocessing.sequence import pad_sequences
root_path =  os.path.abspath(os.path.join(os.getcwd(), os.pardir, os.pardir))

def get_sentiment(pred_test):
    pred_smell_test = np.where(np.argmax(pred_test[1], axis=1) == 3, -1, np.argmax(pred_test[1], axis=1))
    pred_texture_test = np.where(np.argmax(pred_test[2], axis=1) == 3, -1, np.argmax(pred_test[2], axis=1))
    pred_colour_test = np.where(np.argmax(pred_test[3], axis=1) == 3, -1, np.argmax(pred_test[3], axis=1))
    pred_price_test = np.where(np.argmax(pred_test[4], axis=1) == 3, -1, np.argmax(pred_test[4], axis=1))
    pred_shipping_test = np.where(np.argmax(pred_test[5], axis=1) == 3, -1, np.argmax(pred_test[5], axis=1))
    pred_packing_test = np.where(np.argmax(pred_test[6], axis=1) == 3, -1, np.argmax(pred_test[6], axis=1))
    pred_stayingpower_test = np.where(np.argmax(pred_test[7], axis=1) == 3, -1, np.argmax(pred_test[7], axis=1))
    predict_dict_test = {'smell': pred_smell_test, 'texture': pred_texture_test, 'colour': pred_colour_test,
                         'price': pred_price_test, 'shipping': pred_shipping_test, 'packing': pred_packing_test,
                         'stayingpower': pred_stayingpower_test}
    df = pd.DataFrame(predict_dict_test)
    df.replace({-1: np.nan, 0: 'Negative', 1: 'Neutral', 2: 'Positive'}, inplace=True)
    return df


def predict_aspect_sentiment(df):
    tok = pickle.load(open(f'{root_path}/models/tokenizer.pkl', 'rb'))
    model = keras.models.load_model(f'{root_path}/models/multi_task_bigru_conv.h5')
    logging.info("Load model and tok successfully")
    max_len = 115

    cmt = df['processed_comment'].astype('str').values
    tokenized_cmt = tok.texts_to_sequences(cmt)
    padded_cmt = pad_sequences(tokenized_cmt, padding='post', maxlen=max_len)
    logging.info("Padded and tokenize successfully")
    pred = model.predict(padded_cmt)
    pred_aspect = pred[0]
    logging.info("predict successfully")
    df_aspect = round(pd.DataFrame(pred_aspect), 0)
    df_aspect.columns = ['smell', 'texture', 'colour', 'price', 'shipping', 'packing', 'stayingpower', 'others']
    df_aspect['others'] = df_aspect['others'].replace({0: np.nan, 1: 'Neutral'})
    df_sentiment = get_sentiment(pred)
    df_sentiment['others'] = df_aspect['others']
    return pd.concat([df, df_sentiment], axis=1)
