import json
import os
import time
import pytz
import re
import base64
from urllib.parse import urlparse
from datetime import datetime
from confluent_kafka import Producer
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials


def get_credentials():
    '''
    getting credentials from google api and returning it
    '''
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)

    '''
    checking for credentials in the re-run and it's validity
    '''
    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file(
            'credentials.json', SCOPES)
        creds = flow.run_local_server(port=0)

        '''
        storing the credentials to avoid regular autentication
        '''
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    return creds


def get_data_from_api(service):
    '''
    loading data from the gmail api
    querying only the inbox for the messages
    printing the total numbers of messages in the inbox
    '''
    data = []
    try:
        query = 'in:inbox'
        messages = service.users().messages().list(
            userId='me',
            q=query,
            maxResults=500).execute()
        print(f"Total Messages in Inbox: {messages['resultSizeEstimate']}")

        '''
        looping through each data fetched by the api
        '''
        if 'messages' in messages:
            for message in messages['messages']:
                msg_id = message['id']

                try:
                    msg = service.users().messages().get(
                        userId='me',
                        id=message['id']).execute()
                    headers = msg['payload']['headers']

                    for header in headers:
                        if header['name'] == 'From':
                            value = header['value']

                            '''
                            spliting the data from the api
                            as name, mail address of the sender
                            '''
                            if '<' in value:
                                name, email_address = value.split('<')
                                email_address = email_address.replace('>', '').strip()

                            else:
                                name = value.strip()
                                email_address = ''

                            '''
                            formatting the data from api to different columns
                            converting epoch time format to normal format
                            '''
                            internal_date = int(msg['internalDate'])
                            utc_timezone = pytz.timezone('UTC')
                            ist_timezone = pytz.timezone('Asia/Kolkata')
                            received_datetime = datetime.utcfromtimestamp(internal_date / 1000)
                            received_datetime = utc_timezone.localize(received_datetime)
                            received_datetime = received_datetime.astimezone(ist_timezone)
                            received_date = received_datetime.strftime('%Y-%m-%d %H:%M:%S')

                            '''
                            spliting the mail address from the api
                            to get domain name and name of sender
                            '''
                            if '@' in email_address:
                                username, domain_name = email_address.split('@')

                            else:
                                username, domain_name = '', ''
                            body_data = ''

                            '''
                            formatting the body of the mail
                            '''
                            if 'parts' in msg['payload']:
                                parts = msg['payload']['parts']

                                for part in parts:
                                    if 'parts' in part:
                                        sub_parts = part['parts']

                                        for sub_part in sub_parts:
                                            if sub_part['mimeType'] == 'text/plain':
                                                body_data += sub_part['body']['data']

                                            elif sub_part['mimeType'] == 'text/html':
                                                body_data += sub_part['body']['data']

                                    elif part['mimeType'] == 'text/plain':
                                        body_data += part['body']['data']

                                    elif part['mimeType'] == 'text/html':
                                        body_data += part['body']['data']
                            else:
                                body_data = msg['payload'].get('body', {}).get('data', '')

                            '''
                            decoding the data into utf-8 format
                            fetching any URLs within the data
                            '''
                            decoded_data = base64.urlsafe_b64decode(body_data).decode('utf-8')
                            url_pattern = re.compile(r'(https?://[^\s<>"]+|www\.[^\s<>"]+)')
                            urls = url_pattern.findall(decoded_data)
                            urls = list(set([url for url in urls]))
                            url_domains = [urlparse(url).hostname for url in urls]
                            url_domains = list(set([url for url in url_domains]))

                            '''
                            appending all the formatted data
                            '''
                            data.append({
                                'id': msg_id,
                                'name': name,
                                'email_address': email_address,
                                'username': username,
                                'domain_name': domain_name,
                                'email_recieved_on': received_date,
                                'urls': urls,
                                'url_domains': url_domains
                            })

                except HttpError as error:
                    print(f"Failed to fetch message {message['id']}: {error}")

    except HttpError as error:
        print(f"An error occurred while fetching messages: {error}")

    return data


def generate_data():
    '''
    using the creds returned by the get_credentials function
    using the data returned from the get_data_from_api function
    to generate data from gmail api
    '''
    creds = get_credentials()
    service = build('gmail', 'v1', credentials=creds)
    data = get_data_from_api(service)
    return data


def write_data_to_kafka(data):
    '''
    deploying the data to kafka by configuring the kafka producer
    using JSON serialization to deploy data to kafka
    '''
    topic = 'gmail_automated'
    conf = {
        'bootstrap.servers': 'pkc-n00kk.us-east-1.aws.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': 'QTAJ2Y5WXNHMY7FM',
        'sasl.password': 'NaV2zCGy7f1dkS4JqTh1wTS4DkxC4BbRZmWxz3ljsnuSKMXp8aIqNSBmT1NeZqRr'
    }
    producer = Producer(conf)

    '''
    looping through all the formatted data that was appended
    and feeding them to kafka producer
    '''
    for email in data:
        value_bytes = json.dumps(email).encode('utf-8')
        producer.produce(topic, value=value_bytes)
    producer.flush()


if __name__ == '__main__':
    last_id = None
    '''
    running the script every 60 seconds
    if new data is not generated
    '''
    while True:
        data = generate_data()
        if data:
            write_data_to_kafka(data)
        else:
            time.sleep(60)
