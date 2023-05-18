from requests import get
import sys
import os

if __name__ == '__main__':   
    #Add system arguments
    country = sys.argv[1]
    year = sys.argv[2]
    month = int(sys.argv[3])
    day = int(sys.argv[4])
    print(country, year, month, day)
    
    key = os.environ['API_KEY']
    
    r = get(f"https://holidays.abstractapi.com/v1/?api_key={key}&country={country}&year={year}&month={month}&day={day}")   
    #print(r.status_code)
    data = r.json()
    print(data[0]['name'])
