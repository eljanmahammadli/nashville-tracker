"""
    author: @eljanmahammdli
    Python script to track new data and auto tweet on Twitter from
        https://data.nashville.gov
"""
import requests
import tweepy
import gspread

from os import environ
from time import sleep
from datetime import datetime as dt


from dotenv import load_dotenv

load_dotenv()


NASHVILLE_TOKEN = environ["NASHVILLE_TOKEN"]
CONSUMER_KEY = environ["CONSUMER_KEY"]
CONSUMER_SECRET = environ["CONSUMER_SECRET"]
ACCESS_TOKEN = environ["ACCESS_TOKEN"]
ACCESS_SECRET = environ["ACCESS_SECRET"]
SHEET_KEY = environ["SHEET_KEY"]


def fetchData():
    """Fetch the data from Nashville using API

    Returns:
        `data`: if succeed, list of dicts with following key
            incident_type_code,
            incident_type
            call_received,
            last_updated,
            address, city
        `None`: if fails to fetch
    """
    url = f"https://data.nashville.gov/resource/qywv-8sc2.json?$$app_token={NASHVILLE_TOKEN}"
    response = requests.get(url)
    if response.status_code != 200:
        print(f'[{response.status_code}] error code occured')
        return None
    
    data = response.json()

    # change datetime format
    old_format = "%Y-%m-%dT%H:%M:%S.000"
    new_format = "%m/%d/%Y %H:%M:%S %p"
    for i in range(len(data)):
        dt1 = data[i]["call_received"]
        dt2 = data[i]["last_updated"]
        # change to datetime
        dt1 = dt.strptime(dt1, old_format)
        dt2 = dt.strptime(dt2, old_format)
        # change to string
        data[i]["call_received"] = dt1.strftime(new_format)
        data[i]["last_updated"] = dt2.strftime(new_format)
    return data


def authTwitter():
    """Auth Twitter API"""
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    api = tweepy.API(auth, wait_on_rate_limit=True)
    api.verify_credentials()
    return api


def authGspread():
    """Auth Google Spreadsheet"""
    gc = gspread.service_account(filename="gspread_credentials.json")
    sh = gc.open_by_key(SHEET_KEY)

    # if sheet is empty
    worksheet = sh.get_worksheet(0)
    if len(worksheet.get_all_records()) == 0:
        initSpread()
    return sh


def initSpread(worksheet):
    """Fill in the Spreadsheet if it is empty"""
    data = fetchData()
    for row in data:
        worksheet.append_row(list(row.values()))


def notinDb(all_records, call, address):
    """Check if fetched data exists in database

    Args:
        all_records: list of `call_received` and `address` columns
        call: fetched call received
        address: fetched address

    Returns:
        True: if it does not exist
        False: if it does exist
    """
    if call not in all_records[0] and address not in all_records[1]:
        return True
    return False


def tweet(api, row):
    """Tweet new incident calls

    Args:
        api: Twitter API
        row: fetched data row from Nashville
    """
    text = (
        f'#NEW: {row["incident_type_code"]} {row["incident_type"]} '
        f'IS REPORTED AT {row["address"]} / {row["city"]}. '
        f'CALL RECEIVED AT {row["call_received"]}'
    )
    api.update_status(text)
    print(text)


def sleepFunc(mins=5):
    print(f"sleeping for {mins} mins...")
    sleep(mins*60)


def main():

    sh = authGspread()
    api = authTwitter()

    while True:
        data = fetchData()
        if data is None:
            sleepFunc(15)
            continue

        worksheet = sh.get_worksheet(0)
        records = [worksheet.col_values(3)[1:], worksheet.col_values(5)[1:]]

        for row in data:
            check = notinDb(records, row["call_received"], row["address"])
            if check is True:
                worksheet.append_row(list(row.values()))
                tweet(api, row)

        sleepFunc()


if __name__ == "__main__":
    main()
