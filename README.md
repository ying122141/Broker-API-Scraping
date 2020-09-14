# Broker-API-Scraping

**Futu API software Download**: https://www.futunn.com/download/OpenAPI

**Futu API Documentation**: https://futunnopen.github.io/futu-api-doc/intro/intro.html

This script aims to collect the minutely capital flow of stocks from Futu API.

Since the broker API does not provide the historical capital flow, this script would retrieve the data from the live stream and insert the data into MongoDB.

For the list of target stocks, users can input up to 30 stock numbers in StockList.txt with the format of HK.xxxxx, for example, HK.00001, HK.00002, HK.00388. Due to the API output limitation, only can request 30 stock data every 30 seconds, the user might change the sampling rate and modify the script based on different needs.

To connect the broker API, the user needs to install Futu API software. 

