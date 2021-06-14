import pandas as pd
import requests
import time
import os
from urllib.parse import urlparse
import datetime
from lxml import html
import numpy as np
import random
from pandas.io.json import json_normalize
import json
from sqlalchemy import create_engine
import pymysql




def developers_0():
    cities_github_df = pd.read_csv('googleCity_sheet.csv')
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                        .format(user="root", pw="SG@1234", db="sample_check"))
    conn = engine.connect()
    conn.execute("CREATE TABLE IF NOT EXISTS github_developers_0 (Ecosystem varchar(50),\
                   city varchar(100), developers MEDIUMTEXT, min_followers varchar(5),timestamp timestamp);")

    timestamp = datetime.datetime.now()

    min_followers = 0
    cities_github_df = cities_github_df.drop_duplicates()
    # city_no_developer_df = pd.DataFrame(city_developers_df[city_developers_df["developers"]==0].city.unique(),
    #                                     columns=["GoogleCity"])


    # df = cities_github_df.merge(city_no_developer_df, on=['GoogleCity'], how='left', indicator=True)
    # df.drop(df.loc[df['_merge']=="both"].index, inplace=True)
    # cities_github_flagged_df = df.copy()
    # for ecosyst in cities_github_df.Ecosystem.unique():
    #     print(ecosyst)

    print("Shape:{}".format(cities_github_df.shape))
    count = 0

    for ecosyst in cities_github_df.Ecosystem.unique():
        cities_in_ecosyst_df = cities_github_df[cities_github_df.Ecosystem == ecosyst]

        city_name = []
        developers = []
        ecosystem = []

        total_cities = len(cities_in_ecosyst_df.GoogleCity)
        print("Ecosystem:{}, Total Cities Parsed:{}".format(ecosyst, total_cities))

        for city, ecosys, country in zip(cities_in_ecosyst_df.GoogleCity, cities_in_ecosyst_df.Ecosystem,
                                         cities_in_ecosyst_df.GoogleCountry):
            location_city_country = city.lower() + ',' + country.lower()

            api = "https://api.github.com/search/users?q=location%3A%22{}%22+followers%3A%3E{}&type=Users" \
                .format(location_city_country.lower().replace(" ", "%20"), min_followers)

            #          api = "https://api.github.com/search/users?q=location%3A%22{}%22&type=Users" \
            #         .format(location_city_country.replace(" ","%20"))
            #         .format(city.lower().replace(" ","%20"))

            print(api)

            try:
                response = requests.get(api)
                js = json.loads(response.text)
            except:
                pass
            sec = random.randint(5, 20)
            t = time.sleep(sec)

            dev_counts = list(js.items())
            dev_counts = dev_counts[0][-1]

            city_name.append(city)
            developers.append(dev_counts)
            ecosystem.append(ecosys)

            count = count + 1
            print("{}. City:{}, Accounts:{}, Time:{} seconds".format(count, city, dev_counts, sec))

        df = pd.DataFrame(list(zip(ecosystem, city_name, developers)), columns=["Ecosystem", "city", "developers"])
        #     df_gp = df.groupby(by="Ecosystem")[['developers']].sum()
        #     df_gp["Ecosystem"] = df_gp.index
        #     df_gp["min_followers"] = min_followers
        #     df_gp["timestamp"] = timestamp

        df = df.astype(str)
        df["min_followers"] = min_followers
        df["timestamp"] = timestamp

        print("Ecosystem Name: {}".format(ecosyst))
        df.to_sql('github_developers_0', con=engine, if_exists='append', chunksize=1000, index=False)
        conn.close()

def developers_20():
    timestamp = datetime.datetime.now()
    min_followers = 20
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                           .format(user="root", pw="SG@1234", db="sample_check"))
    conn = engine.connect()
    conn.execute("CREATE TABLE IF NOT EXISTS github_developers_20 (Ecosystem varchar(50),\
                       city varchar(100), developers MEDIUMTEXT, min_followers varchar(5),timestamp timestamp);")

    cities_github_df = pd.read_csv('googleCity_sheet.csv')

    # city_no_developer_df = pd.DataFrame(city_developers_df[city_developers_df["developers"]==0].city.unique(),
    #                                     columns=["GoogleCity"])
    # df = cities_github_df.merge(city_no_developer_df, on=['GoogleCity'], how='left', indicator=True)
    # df.drop(df.loc[df['_merge']=="both"].index, inplace=True)
    # cities_github_flagged_df = df.copy()
    # for ecosyst in cities_github_flagged_df.Ecosystem.unique():
    #     print(ecosyst)
    # print("Shape:{}".format(cities_github_flagged_df.shape))
    count = 0

    for ecosyst in cities_github_df.Ecosystem.unique():
        cities_in_ecosyst_df = cities_github_df[cities_github_df.Ecosystem == ecosyst]

        city_name = []
        developers = []
        ecosystem = []

        total_cities = len(cities_in_ecosyst_df.GoogleCity)
        print("Ecosystem:{}, Total Cities Parsed:{}".format(ecosyst, total_cities))

        for city, ecosys, country in zip(cities_in_ecosyst_df.GoogleCity, cities_in_ecosyst_df.Ecosystem, cities_in_ecosyst_df.GoogleCountry):
            location_city_country = city.lower() + ',' + country.lower()
            api = "https://api.github.com/search/users?q=location%3A%22{}%22+followers%3A%3E{}&type=Users" \
                .format(location_city_country.lower().replace(" ", "%20"), min_followers)
            print(api)
            response = requests.get(api)
            sec = random.randint(5, 20)
            t = time.sleep(sec)

            js = json.loads(response.text)

            dev_counts = list(js.items())
            dev_counts = dev_counts[0][-1]

            city_name.append(city)
            developers.append(dev_counts)
            ecosystem.append(ecosys)

            count = count + 1
            print("{}. City:{}, Accounts:{}, Time:{} seconds".format(count, city, dev_counts, sec))

        df = pd.DataFrame(list(zip(ecosystem, city_name, developers)), columns=["Ecosystem", "city", "developers"])
        #     df_gp = df.groupby(by="Ecosystem")[['developers']].sum()
        #     df_gp["Ecosystem"] = df_gp.index
        #     df_gp["min_followers"] = min_followers
        #     df_gp["timestamp"] = timestamp

        df = df.astype(str)
        df["min_followers"] = min_followers
        df["timestamp"] = timestamp

        print("Ecosystem Name: {}".format(ecosyst))
        df.to_sql('github_developers_20', con=engine, if_exists='append', chunksize=1000, index=False)
        conn.close()

        
if __name__ == '__main__':
    developers_0()
    developers_20()



