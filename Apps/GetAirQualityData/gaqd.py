import sys
from datetime import timedelta, date, datetime, timedelta
import requests
from pandas .io.json import json_normalize
import pandas as pd 

url = 'https://openapi.izmir.bel.tr/api/ibb/cevre/havadegerleri/{}'
dataFile = '../../Data/AirQuality.csv'

class App:
    def getURL(self, date):    
        try:
            return (url.format(date.strftime('%Y-%m-%d')))
        except Exception as e:
            print (str(e))
        return None

    def getValues(self, endpoint):
        df = pd.read_json(endpoint)    
        return (df)

    def run(self, startDate, endDate):
        data = pd.DataFrame()
        delta = timedelta(days=1)
        currDate = startDate
        print('Getting data...')
        while currDate <= endDate:
            url = self.getURL(currDate)
            if url:
                df = self.getValues(url)
                if data.empty:
                    data = df
                else:
                    data = data.append(df)

                print('Request: {} ({})'.format(
                    url,
                    'OK' if df['OlcumTarihi'].iloc[0] == currDate else 'Warning: {}'.format(df['OlcumTarihi'].iloc[0])
                ))
    
            currDate += delta

        print('Creating the index of data...')
        data['OlcumTarihi'] = pd.to_datetime(data['OlcumTarihi'])
        data = data.set_index('OlcumTarihi')
        print('Dropping the duplicated values...')
        data.drop_duplicates(inplace=True)
        print('Writing cleaned data to the CSV file...')
        data.to_csv (dataFile, header=True)
    
if __name__ == '__main__':
    ready = True
    try:
        if (len(sys.argv) != 3):
            print('Usage: gawd <startdate> <enddate>')
            print('Date format must be in YYYY-MM-DD pattern')
            ready = False
        else:
            startDate = datetime.strptime(sys.argv[1], '%Y-%m-%d')
            endDate = datetime.strptime(sys.argv[2], '%Y-%m-%d')
            if startDate > endDate:
                print('Start date can not be greater than end date')
                ready=False
    except Exception as e:
        print ('Given date is not formatted correctly (YYYY-MM-DD)')
        print (str(e))
        ready = False

    if ready:
        print ('Data scraping started...')    
        app = App()
        app.run(startDate, endDate)
