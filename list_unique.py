# gathers all unique values of anime titles and genres (used for the dropdown menu in front end)

import pandas as pd
import json

df = pd.read_csv('data/uniques/genre_uniques.csv', usecols=['genre'])

all_genres = (
    df['genre']
      .dropna()
      .str.split(',')
      .explode()
      .str.strip() 
      .unique() 
)
unique_genres = sorted(all_genres)


dfB = pd.read_csv("data/uniques/title_uniques.csv")

listB = dfB["title"].dropna().tolist()

uniques_dict = {
    "genre": unique_genres,
    "title": listB
}

with open("data/uniques/uniques.json", "w") as f:
    json.dump(uniques_dict, f, indent=2)

