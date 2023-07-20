# this code scrapes wedding data from sites listed in google sheets
# this is executable from any computer or pythonanywhere
# install requirements first, pip install requirements.txt
# enter in terminal -> python scraper.py
# same for pythonanywhere

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
import hashlib
import requests
from bs4 import BeautifulSoup
import re
import openai
import json
import mysql.connector as mysql
import time
import datetime
import re

# authentication first
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

regex_extract_domain = r'^(https?://)?([a-zA-Z0-9.-]+)'


googlesheet_id = '1S7wA4R-YTfUUHBi8TxFWOEXriR2ZTk-XnKAKxVPz_lI'

# get credential of service account from console.google.com
credentials = service_account.Credentials.from_service_account_file('./credentials.json')

conn = mysql.connect(
            user='weddingrfp_bibek', 
            password='[~5]VI]btD0#',
            host='192.155.107.194',
            database='weddingrfp_wp941'
            )

# get an openai api key, get a paid subscription 
openAI_api  = ''

openai.api_key = openAI_api

def chatgpt_get_answer_from_messages(message):
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[
            {
                "role": "system",
                "content": message
            },
            {
                "role": "user",
                "content": message
            }
        ]
    )

    if response['object'] == 'chat.completion' and 'choices' in response and len(response['choices']) > 0:
        answer = response['choices'][0]['message']['content'].strip()
        return answer

    return None

def google_sheet_get_data(googlesheetid):
    try:
        
        service = build('sheets', 'v4', credentials=credentials)

        # Call the Sheets API
        
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=googlesheetid,range='r2c2:r1100c12').execute()
        values = result.get('values', [])

        if not values:
            print('No data found.')

    except HttpError as err:
        print(err)

    # extract important field form sheets, url and selectors
    sites = []
    for value in values:
        sites.append({
            'url': value[0],
            'list_selector': value[6],
            'selector': value[-1]
        })
    return sites


# below code extract all the announcement from all given page, last time i checked it has more than 165563 line and data is not even seperated by line break(166kb)
def extract_site(sites):
    announcements = []
    for site in sites:
        domain = re.search(regex_extract_domain, site['url'])
        site_domain = domain.group()
        response = requests.get(site['url'])

        soup = BeautifulSoup(response.text, "html.parser")

        a_selector = soup.select(site['list_selector'])
        if len(a_selector):
            ass = a_selector[0].select('a')
            b = []
            for a in ass:
                if a.has_key('href'):
                    if not re.search(r'^http',a['href']):
                        b.append(site_domain + a['href'])
                    else:
                        b.append(a['href'])

            b = list(set(b))
            for newb in b:
                if re.search('/.*engagement.*/', newb):
                    announcements.append({
                        'urls': newb,
                        'selector': site['selector'],
                    })
                    return announcements
                
    return announcements


def chatgpt_build_query(contenttext):

    sampleJson = {
    "engagement": {
        "bride": {
        "name": "",
        "parents": "",
        "hometown": ""
        },
        "groom": {
        "name": "",
        "parents": "",
        "hometown": ""
        },
        "date": "",
    },
    "education": {
        "bride": {
        "university": "",
        "degree": "",
        "major": "",
        "minor": ""
        },
        "groom": {
        "university": "",
        "degree": ""
        }
    },
    "employment": {
        "bride": {
        "company": "",
        "position": ""
        },
        "groom": {
        "company": "",
        "position": ""
        }
    },
    "wedding": {
        "date": "",
        "location": "",
        "city": "",
        "state": "",
        "country": "",
        "postal_code": ""
    }
    }

    questionheader = 'here is a json format(leave the field empty if not present and guess the country):'

    question = "ChatGPT, please extract the important parameters about wedding from following text in a json format: "

    text = contenttext

    messages = f"{questionheader} {json.dumps(sampleJson)} , {question} {text}"

    return messages

# extract relevent data to insert into fc_company
def database_extract_data_for_fc_company(data, sourcs_url):

    engagement = data['engagement']

    education = data['education']

    employment = data['employment']

    wedding = data['wedding']

    bride = engagement['bride']

    bride_education = education['bride']

    groom = engagement['groom']

    groom_education = education['groom']

    groom_job = employment['groom']

    bride_job = employment['bride']


    main_data = {
        "hash": hashlib.md5(sourcs_url.encode()).hexdigest(),
        "google_id": '',
        "name": groom['name'] + ' weds ' + bride['name'],
        "category": 'wedding event',
        "email_1": '',
        "time_zone": '',
        "full_address": wedding['location'],
        # "location_link": wedding['location'],
        "postal_code": wedding['postal_code'],
        "city": wedding['city'],
        "state": wedding['state'],
        "country": wedding['country'],
        "description": str(data),
        "phone": '',
        "type": 'wedding',
        "logo": '',
        "website": sourcs_url,
        "linkedin": groom['name'] + ' ' + groom_job['company'] + ' ' + groom_education['university'] + ', ' + bride['name'] + ' ' + bride_job['company'] + ' ' + bride_education['university'],
        "facebook": '',
        "twitter": '',
        "date": wedding['date']
    }

    return main_data

# insert operation
def database_insert_into_fc_company(main_data):

    # Check if the result already exists in the database
    cur = conn.cursor()
    cur.execute("SELECT id FROM wpit_fc_companies WHERE hash = %s", (main_data.get('hash'),))
    company_id = cur.fetchone()
    cur.close()

    if company_id is not None:
        print(f"data for {main_data.get('name')} already exists in the database with id {company_id[0]}.")
        return company_id[0]
    
    timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    
    cur = conn.cursor()
    cur.execute("INSERT INTO wpit_fc_companies (id, hash, owner_id, google_id, name, industry, email, timezone, address_line_1, address_line_2, postal_code, city, state, country, employees_number, description, phone, type, logo, website, linkedin_url, facebook_url, twitter_url, date_of_start, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (
        0,
        main_data.get('hash'),
        0,
        '',
        main_data.get('name'),
        main_data.get('category'),
        main_data.get('email_1'),
        main_data.get('time_zone'),
        main_data.get('full_address'),
        main_data.get('location_link'),
        main_data.get('postal_code'),
        main_data.get('city'),
        main_data.get('state'),
        main_data.get('country'),
        0,
        main_data.get('description'),
        main_data.get('phone'),
        main_data.get('type'),
        main_data.get('logo'),
        main_data.get('website'),
        main_data.get('linkedin'),
        main_data.get('facebook'),
        main_data.get('twitter'),
        # result.get('company_year_started')
        0,
        timestamp,
        timestamp
    ))
    conn.commit()
    cur.close()
    return cur.lastrowid


def scrape():
    # get data from google sheet
    sites = google_sheet_get_data(googlesheet_id)

    # extract announcements list from all sites
    announcements = extract_site(sites)

    # now lets visit each announcement
    for announcement in announcements:

        # extract relevent text data from announcement
        response = requests.get(announcement['urls'])

        soup = BeautifulSoup(response.text, "html.parser")

        articlebody = soup.select_one(announcement['selector'])

        contenttext = articlebody.text

        contenttext = contenttext.split('\r\n')[0].replace('\n', ' ')

        # build query for chat gpt
        message = chatgpt_build_query(contenttext)

        # chat gpt responses json
        response = chatgpt_get_answer_from_messages(message)

        # load json into code
        data = json.loads(response)

        # insert into database
        database_insert_into_fc_company(data, announcement['urls'])

        # inserted into database

    # check the database or fluentcrm->company for data 
    return

scrape()