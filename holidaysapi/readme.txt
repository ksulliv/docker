The Public Holiday app returns the name of any public, local, religious, and other holidays when the user specify the country and the date.
See documentation here: https://app.abstractapi.com/api/holidays/documentation 

It's simple to use: you only need to submit your API key (api_key) and a two letter country code (country), the 4 digit year, the month and the day:

Format:

        api_key holidays_app:1.0 Input 1 = Country Input 2 = Year Input 3 = Month Input 4 = Day 
        

⚠️ Note that under the free plan you can only query the current year

API key = get your own

Example:

    To query the 25th of Decemeber, 2022 you'll use:
 
    docker run -e API_KEY=API key holidays_app:1.0 US 2022 12 25

    This will return: your input and the name of the holiday (if one exist)
    
    US 2022 12 25
    
    Christmas Day
