import json
import os
import pytz
import re
import base64
import threading
import requests
import tldextract
from urllib.parse import urlparse
from datetime import datetime
from confluent_kafka import Producer
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from oauthlib.oauth2.rfc6749.errors import InvalidGrantError, TokenExpiredError
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2 import service_account


def get_credentials(email):
    '''
    getting tokens from google api and returning it
    checking if the tokens are valid or expired
    else fetch refreshed tokens
    '''
    SCOPES = ['https://mail.google.com/']
    creds = None

    # checking if the credentials exist
    if os.path.exists(f'{email}_token.json'):
        creds = Credentials.from_authorized_user_file(
            f'{email}_token.json', SCOPES)

    # checking for credentials in the re-run and it's validity
    if not creds or not creds.valid:

        # checking if the tokens are expired and needs to be refreshed
        if creds and creds.expired and creds.refresh_token:

            # try to fetch the refreshed token from the api
            try:
                creds.refresh(Request())

            except (InvalidGrantError, TokenExpiredError, RefreshError):
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)

        # fetch the fresh tokens from the api
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)

    # storing the credentials to avoid regular autentication
    with open(f'{email}_token.json', 'w') as token:
        token.write(creds.to_json())

    return creds


def get_data_from_api(service, email):
    '''
    querying only the inbox for the messages
    printing the total numbers of messages in the inbox
    fetcing and storing all the data retrieved
    '''
    data = []

    try:
        query = 'in:inbox'
        page_token = None
        msg_count = 0

        # checking for messages in next page and fetching them
        while True:
            messages = service.users().messages().list(
                userId='me',
                q=query,
                maxResults=1000,
                pageToken=page_token).execute()

            # calculating the total number of messages
            messages.update(messages)
            page_token = messages.get('nextPageToken')
            msg_count += int(messages['resultSizeEstimate'])

            if not page_token:
                break

        print(f"Total Messages in Inbox of {email} is : {msg_count}")

        # checking for messages in the inbox
        if 'messages' in messages:

            for message in messages['messages']:
                msg_id = message['id']

                try:
                    msg = service.users().messages().get(
                        userId='me',
                        id=message['id']).execute()
                    headers = msg['payload']['headers']

                    # formatting the user name and email address from user-id
                    for header in headers:

                        if header['name'] == 'From':
                            value = header['value']

                            if '<' in value:
                                name, email_address = value.split('<')
                                email_address = email_address.replace('>', '').strip()

                            else:
                                name = value.strip()
                                email_address = ''

                            # converting email recieved time (epoch - standard)
                            internal_date = int(msg['internalDate'])
                            utc_timezone = pytz.timezone('UTC')
                            ist_timezone = pytz.timezone('Asia/Kolkata')
                            received_datetime = datetime.utcfromtimestamp(internal_date / 1000)
                            received_datetime = utc_timezone.localize(received_datetime)
                            received_datetime = received_datetime.astimezone(ist_timezone)
                            received_date = received_datetime.strftime('%Y-%m-%d %H:%M:%S')

                            # formatting the email address into domain and name
                            if '@' in email_address:
                                username, domain_name = email_address.split('@')

                            else:
                                username, domain_name = '', ''
                            body_data = ''

                            # checking the messgae body for any urls and domain
                            if 'parts' in msg['payload']:
                                parts = msg['payload']['parts']

                                for part in parts:

                                    # segregating the body of the message
                                    if 'data' in part['body']:

                                        if part['mimeType'] == 'text/plain':
                                            body_data += part['body']['data']

                                        elif part['mimeType'] == 'text/html':
                                            body_data += part['body']['data']

                            else:
                                body_data = msg['payload'].get('body', {}).get('data', '')

                            # decoding the urls found in the non-text format
                            decoded_data = base64.urlsafe_b64decode(body_data).decode('utf-8')
                            url_pattern = re.compile(r'(https?://[^\s<>,"]+|www\.[^\s<>,"]+)')
                            urls = url_pattern.findall(decoded_data)
                            urls = [item for index, item in enumerate(urls) if urls.index(item) == index]

                            # checking if any shortened urls present
                            for i, url in enumerate(urls):
                                try:
                                    # unshortening the urls and
                                    if 'bit.ly' in url or 'goo.gl' in url or 't.co' in url or 'amzn.to' in url or 'tinyurl.com' in url or 'youtu.be' in url or 'fkrt.it' in url:
                                        response = requests.get(url, allow_redirects=True)
                                        urls[i] = response.url

                                except requests.exceptions.RequestException:
                                    continue

                            # formatting the domain names from the urls
                            url_domains = [urlparse(url).hostname for url in urls if urlparse(url).hostname is not None]
                            url_domains = [item for index, item in enumerate(url_domains) if url_domains.index(item) == index]
                            base_domains = []

                            # segregating the base domain name in domain name
                            for domain in url_domains:
                                ext = tldextract.extract(domain)
                                base_domains.append(ext.domain)
                            base_domains = [item for index, item in enumerate(base_domains) if base_domains.index(item) == index]

                            # un-listing of the urls and domain names
                            url_str = ", ".join(urls)
                            url_domains_str = ", ".join(url_domains)
                            base_domains_str = ", ".join(base_domains)

                            # appeding of mail data for data loading
                            data.append({
                                'id': msg_id,
                                'name': name,
                                'email_address': email_address,
                                'username': username,
                                'domain_name': domain_name,
                                'email_recieved_on': received_date,
                                'urls': url_str,
                                'url_domains': url_domains_str,
                                'base_domains': base_domains_str,
                                'source_mail': email,
                                'list_urls': urls,
                                'list_url_domains': url_domains,
                                'list_base_domains': base_domains
                            })

                except HttpError as error:
                    print(f"Failed to fetch message {message['id']}: {error}")

    except HttpError as error:
        print(f"An error occurred while fetching messages: {error}")

    return data


def process_email(email):
    '''
    configuring the kafka topic for data loading
    and checking for credentials
    '''
    conf = {
            'bootstrap.servers': 'pkc-n00kk.us-east-1.aws.confluent.cloud:9092',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': 'ZYY3RRWPHLI7PRNB',
            'sasl.password': 'ahW9o2kaN82gbYImYFWnJhGyuua1AXl4LcQu+pn0H0d9wy7b9M2xuX0v6m9iJ+oR'
        }

    # checing if we already have service account credentials
    if os.path.exists('service_account.json'):
        SERVICE_ACCOUNT_FILE = 'service_account.json'
        credentials = service_account.Credentials.from_service_account_file(
            filename=SERVICE_ACCOUNT_FILE,
            scopes=['https://mail.google.com/'],
            subject=email)
        service = build('gmail', 'v1', credentials=credentials)
        data = get_data_from_api(service, email)
        service_acc_creds = get_service_account_creds(email)
        write_service_account_creds(service_acc_creds, conf)

    # fetching credentials using manual authentication
    else:
        creds = get_credentials(email)
        service = build('gmail', 'v1', credentials=creds)
        data = get_data_from_api(service, email)
        creds_token = get_creds_token(email)
        write_creds_tokens(creds_token, conf)

    # loading the data fetched from gmail logs to kafka
    write_data_to_kafka(data, conf)


def get_creds_token(email):
    '''
    storing the user credentials for further use
    pushing the user credentials to kafka topic
    '''
    creds_token = []

    try:
        # storing the given credentials into a json file
        with open('credentials.json') as f:
            credentials = json.load(f)

        # storing the newly generated tokens into a json file
        with open(f'{email}_token.json') as f:
            token = json.load(f)

    except:
        pass

    # loading the credentials and tokens to push to kafka
    creds_token.append({
        'credentials': credentials,
        'token': token,
        'source_mail': email
    })

    return creds_token


def get_service_account_creds(email):
    '''
    storing the service account credentials for further use
    pushing the service account credentials to kafka topic
    '''
    service_creds_token = []

    try:
        # storing the given credentials into a json file
        with open('service_account.json') as f:
            service_account_credentials = json.load(f)

    except:
        pass

    # loading the service account credentials to push to kafka
    service_creds_token.append({
        'service_account_credentials': service_account_credentials,
        'source_mail': email
    })

    return service_creds_token


def generate_data(mail_ids):
    '''
    starting multiple threads to generate data for each email
    to reduce wait time and improve execution time
    and using join condition to avoid asynchronous processing
    '''
    threads = []

    for email in mail_ids:
        thread = threading.Thread(target=process_email, args=(email,))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()


def write_data_to_kafka(data, conf):
    '''
    pushing gmail data to kafka
    by specifying the respective topic
    and using JSON serialization to deploy data
    '''
    topic = 'gmail_automated'
    producer = Producer(conf)

    for email in data:
        value_bytes = json.dumps(email).encode('utf-8')
        producer.produce(topic, value=value_bytes)

    producer.flush()


def write_creds_tokens(creds_token, conf):
    '''
    pushing credentials and tokens to kafka
    by specifying the respective topic
    and using JSON serialization to deploy data
    '''
    topic = 'creds_token'
    producer = Producer(conf)

    for entity in creds_token:
        value_bytes = json.dumps(entity).encode('utf-8')
        producer.produce(topic, value=value_bytes)

    producer.flush()


def write_service_account_creds(service_creds, conf):
    '''
    pushing service account credentails to kafka
    by specifying the respective topic
    and using JSON serialization to deploy data
    '''
    topic = 'service_account_creds'
    producer = Producer(conf)

    for entity in service_creds:
        value_bytes = json.dumps(entity).encode('utf-8')
        producer.produce(topic, value=value_bytes)

    producer.flush()


if __name__ == '__main__':
    '''
    listing all the mail-ids for data mapping
    calling the generate function continuously
    to fetch real-time gmail data from the server
    '''
    while True:
        mail_ids = ['rahul@de-haze.com', 'munikumar@de-haze.com']
        generate_data(mail_ids)
