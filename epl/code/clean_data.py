import csv
from urllib.request import urlopen
import requests
import pandas as pd

url_18 = "https://raw.githubusercontent.com/mahawkins3/football_betting/main/epl/data/EPL_Set.csv"
url_19 = "https://www.football-data.co.uk/mmz4281/1819/E0.csv"
url_20 = "https://www.football-data.co.uk/mmz4281/1920/E0.csv"
url_21 = "https://www.football-data.co.uk/mmz4281/2021/E0.csv"

res = requests.get(url_18, allow_redirects=True)
with open('EPL_Set.csv', 'wb') as file:
    file.write(res.content)

res_18 = pd.read_csv('EPL_Set.csv')
res_19 = csv.reader(urlopen(url_19))
res_20 = csv.reader(urlopen(url_20))
res_21 = csv.reader(urlopen(url_21))

for row in res_18:
    print(row)

print(res_18.shape)
