import pandas as pd
from typing import List

df = pd.read_csv("./PhDBotnetsDB/User_Tweet/_outputs/user_tweets_full_enriched_english_only.csv")

def get_tweets_from_user(user_id: int) -> List[str]:
    return df.loc[(df["user_id"] == user_id) & (df["is_retweet"] == False), "tweet_text"].dropna().astype(str).tolist()

#print(df['carpeta_origen'].value_counts())

#seleccionar 10 cuentas de manera aleatoria 10 cuentas de cada carpeta, despues crear un diccionario con user_id-caroeta: get_tweets_from_user(user_id)
""" diccionario = {}
for carpeta in df['carpeta_origen'].unique():
    cuentas = df[df['carpeta_origen'] == carpeta]['user_id'].sample(10).tolist()
    for cuenta in cuentas:
        cuenta_nombre = f"{carpeta} - {cuenta}"
        diccionario[cuenta_nombre] = get_tweets_from_user(cuenta) """

#do the same just for starwars botnet
starwars_diccionario = {}
for cuenta in df[df['carpeta_origen'] == 'StarWarsBotnet']['user_id'].tolist():
    starwars_diccionario[cuenta] = get_tweets_from_user(cuenta)

print(starwars_diccionario.keys())