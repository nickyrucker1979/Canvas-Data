# scrub html from discussion messages and count words

import pandas as pd
import config_Exasol as ec
import exasol as e
import re

exaconnect = e.connect(
            dsn=ec.dsn,
            DRIVER=ec.DRIVER,
            EXAHOST=ec.EXAHOST,
            EXAUID=ec.EXAUID,
            EXAPWD=ec.EXAPWD,
            autocommit=True
            )

exasol_lookup_db = 'CANVAS_DATA_NIGHTLY_IMPORTS.CANVAS_COURSE_DISCUSSIONS_VIEW'
exasol_import_db = 'CANVAS_DATA_NIGHTLY_IMPORTS.CU_CANVAS_COURSE_DISCUSSIONS_METRICS'

def word_count(text):
    try:
        for char in '-.,\n':
            text=text.replace(char,' ')
        text = text.lower()
        word_list = text.split()
        if len(word_list) > 0:
            x = str(len(word_list))
        else:
            x = '0'
    except:
        x = 'NULL'
    return x

def check_media(raw_html):
    try:
        if 'class="instructure_inline_media_comment' in raw_html:
            media_content = 'Canvas Media Comment'
        elif 'ucdenver.techsmithrelay.com' in raw_html:
            media_content = 'Techsmith Relay Comment'
        else:
            media_content = 'NULL'
    except:
        media_content = 'NULL'
    return media_content

def cleanhtml(raw_html):
    try:
        raw_html = raw_html.replace('\\n','')
        cleanr = re.compile('<.*?>')
        cleantext = re.sub(cleanr, '', raw_html)
    except:
        cleantext = ''
    return cleantext

def scrub_markup(dataframe, media_field, html_field):
    dataframe[media_field] = dataframe[html_field].apply(lambda x: check_media(x))
    dataframe[html_field] = dataframe[html_field].apply(lambda x: cleanhtml(x))
    return

def count_words(dataframe, field, countwordfield):
    dataframe[field] = dataframe[countwordfield].apply(lambda x: word_count(x))
    return

if __name__ == '__main__':

    df = exaconnect.readData('Select * from ' + exasol_lookup_db + ' limit 500')
    df['DISCUSSION_TOPIC_WORD_COUNT'] = ''
    df['RESPONSE_MESSAGE_WORD_COUNT'] = ''
    df['DISCUSSION_TOPIC_MEDIA_TYPE'] = ''
    df['RESPONSE_MEDIA_TYPE'] = ''

    scrub_markup(df, 'DISCUSSION_TOPIC_MEDIA_TYPE', 'DISCUSSION_TOPIC_MESSAGE')
    count_words(df, 'DISCUSSION_TOPIC_WORD_COUNT', 'DISCUSSION_TOPIC_MESSAGE')

    scrub_markup(df, 'DISCUSSION_TOPIC_MEDIA_TYPE', 'DISCUSSION_RESPONSE_MESSAGE')
    count_words(df, 'RESPONSE_MESSAGE_WORD_COUNT', 'DISCUSSION_RESPONSE_MESSAGE')

    print(df)
