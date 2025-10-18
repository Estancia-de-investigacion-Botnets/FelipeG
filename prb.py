import pandas as pd
df = pd.read_csv("C:/Users/felip/OneDrive/Escritorio/BotNetsCode/PhDBotnetsDB/User_Tweet/_outputs/user_tweets_full_enriched.csv")

print(len(df["user_id"].unique()))