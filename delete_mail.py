from confluent_kafka import Producer
import json, os, pandas as pd
from snowflake_credentials import snowflake_connection
from googleapiclient.discovery import build
from google.oauth2 import service_account


def upload_data(conn):
    cursor = conn.cursor()
    print("snowflake connected")
    cursor.execute("delete from gmail")
    cursor.execute(r""" SELECT id, SOURCE_MAIL,
                        CASE
        WHEN domain_name IN (
            SELECT REGEXP_REPLACE(ATTRIBUTE_VALUE, '((https?|ftp)://)?(www\\.)?([a-zA-Z0-9\\-.]+\\.[a-zA-Z]{2,}).*', '\\4', 1, 1, 'i')
            FROM FINAL_MISP_BOTVRIJ_DATA
            WHERE ATTRIBUTE_TYPE IN ('url', 'link', 'domain') AND TAG_NAME LIKE CONCAT('%', 'hishing', '%')
            UNION
            SELECT REGEXP_REPLACE(ATTRIBUTE_VALUE, '((https?|ftp)://)?(www\\.)?([a-zA-Z0-9\\-.]+\\.[a-zA-Z]{2,}).*', '\\4', 1, 1, 'i')
            FROM FINAL_MISP_CIRCL_DATA
            WHERE ATTRIBUTE_TYPE IN ('url', 'link', 'domain') AND TAG_NAME LIKE CONCAT('%', 'hishing', '%')
            UNION
            SELECT CASE WHEN SPLIT_PART((ATTRIBUTE_VALUE), '/', -1) LIKE '%.com' or SPLIT_PART((ATTRIBUTE_VALUE), '/', -1) LIKE '%.org' or SPLIT_PART((ATTRIBUTE_VALUE), '/', -1) LIKE '%.co' or SPLIT_PART((ATTRIBUTE_VALUE), '/', -1) LIKE '%.in' THEN SPLIT_PART((ATTRIBUTE_VALUE), '/', -1) ELSE FALSE END AS DOMAIN_1
            FROM FINAL_MISP_BOTVRIJ_DATA
            WHERE ATTRIBUTE_TYPE IN ('url', 'link') AND TAG_NAME LIKE CONCAT('%', 'hishing', '%') AND DOMAIN_1 != 'false'
            UNION
            SELECT CASE WHEN SPLIT_PART((ATTRIBUTE_VALUE), '/', -1) LIKE '%.com' or SPLIT_PART((ATTRIBUTE_VALUE), '/', -1) LIKE '%.org' or SPLIT_PART((ATTRIBUTE_VALUE), '/', -1) LIKE '%.co' or SPLIT_PART((ATTRIBUTE_VALUE), '/', -1) LIKE '%.in' THEN SPLIT_PART((ATTRIBUTE_VALUE), '/', -1) ELSE FALSE END AS DOMAIN_2
            FROM FINAL_MISP_CIRCL_DATA
            WHERE ATTRIBUTE_TYPE IN ('url', 'link') AND TAG_NAME LIKE CONCAT('%', 'hishing', '%') AND DOMAIN_2 != 'false'
        )
        THEN case
                when domain_name in (select GOOD_DOMAIN_NAMES from good_domains)
              then '' ELSE domain_name
        end
END AS domain_name_exists FROM gmail_stream where domain_name_exists != '';""")
    domain_name_result = cursor.fetchall()
    columns = ['id', 'SOURCE_MAIL', 'domain_name']
    domain_name_df = pd.DataFrame(domain_name_result, columns=columns)
    print(domain_name_df)
    cursor.execute(r"""SELECT id, SOURCE_MAIL,
                        CASE
        WHEN url_domains IN (
            SELECT REGEXP_REPLACE(ATTRIBUTE_VALUE, '((https?|ftp)://)?(www\\.)?([a-zA-Z0-9\\-.]+\\.[a-zA-Z]{2,}).*', '\\4', 1, 1, 'i')
            FROM FINAL_MISP_BOTVRIJ_DATA
            WHERE ATTRIBUTE_TYPE IN ('url', 'link', 'domain') AND TAG_NAME LIKE CONCAT('%', 'hishing', '%')
            UNION
            SELECT REGEXP_REPLACE(ATTRIBUTE_VALUE, '((https?|ftp)://)?(www\\.)?([a-zA-Z0-9\\-.]+\\.[a-zA-Z]{2,}).*', '\\4', 1, 1, 'i')
            FROM FINAL_MISP_CIRCL_DATA
            WHERE ATTRIBUTE_TYPE IN ('url', 'link', 'domain') AND TAG_NAME LIKE CONCAT('%', 'hishing', '%')
            UNION
            SELECT CASE WHEN SPLIT_PART((ATTRIBUTE_VALUE), '/', -1) LIKE '%.com' or SPLIT_PART((ATTRIBUTE_VALUE), '/', -1) LIKE '%.org' or SPLIT_PART((ATTRIBUTE_VALUE), '/', -1) LIKE '%.co' or SPLIT_PART((ATTRIBUTE_VALUE), '/', -1) LIKE '%.in' THEN SPLIT_PART((ATTRIBUTE_VALUE), '/', -1) ELSE FALSE END AS DOMAIN_1
            FROM FINAL_MISP_BOTVRIJ_DATA
            WHERE ATTRIBUTE_TYPE IN ('url', 'link') AND TAG_NAME LIKE CONCAT('%', 'hishing', '%') AND DOMAIN_1 != 'false'
            UNION
            SELECT CASE WHEN SPLIT_PART((ATTRIBUTE_VALUE), '/', -1) LIKE '%.com' or SPLIT_PART((ATTRIBUTE_VALUE), '/', -1) LIKE '%.org' or SPLIT_PART((ATTRIBUTE_VALUE), '/', -1) LIKE '%.co' or SPLIT_PART((ATTRIBUTE_VALUE), '/', -1) LIKE '%.in' THEN SPLIT_PART((ATTRIBUTE_VALUE), '/', -1) ELSE FALSE END AS DOMAIN_2
            FROM FINAL_MISP_CIRCL_DATA
            WHERE ATTRIBUTE_TYPE IN ('url', 'link') AND TAG_NAME LIKE CONCAT('%', 'hishing', '%') AND DOMAIN_2 != 'false'
        )
        THEN case
                when url_domains in (select GOOD_DOMAIN_NAMES from good_domains)
              then '' ELSE url_domains
        end
END AS domain_name_exists FROM gmail_stream where domain_name_exists != ''; """)
    url_domain_result = cursor.fetchall()
    columns = ['id', 'SOURCE_MAIL', 'url_domains']
    url_domain_df = pd.DataFrame(url_domain_result, columns= columns)
    merged_df = pd.merge(domain_name_df, url_domain_df, on=['id', 'SOURCE_MAIL'], how='outer')
    print(merged_df)
    merged_df.fillna(" ", inplace=True)
    data = [tuple(x) for x in merged_df.to_records(index=False)]
    cursor.execute("insert into gmail(id, source_mail, domain_name, url_domains) select  id, source_mail, domain_name, url_domains from gmail_stream")
    for i in data:
        query = "insert into final_status(id, source_mail, domain_name, url_domains)  VALUES "+ str(i)
        cursor.execute(query)
    return data


def get_credentials(source_mail):
    '''
    getting credentials from stored in the database
    and storing them in token.json file so that we can
    use it access gmail api for mail deleteion
    '''
    if os.path.exists('service_account.json'):
        SERVICE_ACCOUNT_FILE = 'service_account.json'
        credentials = service_account.Credentials.from_service_account_file(
            filename=SERVICE_ACCOUNT_FILE,
            scopes=['https://mail.google.com/'],
            subject=source_mail)

    return credentials


def write_data_to_kafka(conn, data):
    '''
    pushing deleted data to kafka
    by specifying the respective topic
    and using JSON serialization to deploy data
    '''
    topic = 'status_topic'
    conf = {
        'bootstrap.servers': 'pkc-n00kk.us-east-1.aws.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': '6Q3LVYUSY4QMANZU',
        'compression.type': 'gzip',
        'message.max.bytes': '104857699',
        'sasl.password': 'M7fcPsAuB9RzhRYlF4X0Z7jVJjaT1gFHd5XN7RLm+YzreJheloKAhC9en/FWGwcw'
    }
    producer = Producer(conf)
    for email in data:
        value_bytes = json.dumps(email)
        producer.produce(topic, value=value_bytes)
    producer.flush()
    conn.close()
    print("loaded to topic")


def delete_mail(data):
    '''
    looping through the matched data to delete them from the mail logs
    '''
    for i in data:
        id = i[0]
        print("id is", id)
        source_mail = i[1]
        print("source_mail", source_mail)

        '''
        creating a cursor object for query execution
        fetching credentials of matched data
        for mail deletion
        '''
        # cursor = conn.cursor()
        # cursor.execute("select service_account_credentials from SERVICE_ACCOUNT_CREDS_FORMATTED where source_mail = '"+source_mail+"' order by service_account_credentials desc")
        # print("select service_account_credentials from SERVICE_ACCOUNT_CREDS_FORMATTED where source_mail = '"+source_mail+"' order by service_account_credentials desc")
        # tokens = cursor.fetchone()

        '''
        storing the retrieved credentials into a token
        '''
        # for token in tokens:
        #     token_value = json.dumps(token)

        '''
        accessing the mail api using the tokens
        and storing the message id of matched mail
        '''
        creds = get_credentials(source_mail)
        gmail_service = build('gmail', 'v1', credentials=creds)
        email_id = id

        '''
        deleting the matched mail
        '''
        try:
            gmail_service.users().messages().delete(
                userId='me', id=email_id).execute()
            print(f"Email with ID {email_id} has been deleted successfully.")

        except:
            pass


if __name__ == "__main__":
    conn = snowflake_connection()
    data = upload_data(conn)
    delete_mail(data)
    write_data_to_kafka(conn, data)
