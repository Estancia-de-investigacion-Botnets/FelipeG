import pandas as pd
df = pd.read_csv("C:/Users/felip/OneDrive/Escritorio/BotNetsCode/PhDBotnetsDB/User_Tweet/_outputs/user_tweets_full_enriched.csv")
df2 = pd.read_csv("C:/Users/felip/OneDrive/Escritorio/BotNetsCode/PhDBotnetsDB/User_Tweet/_outputs/user_tweets_full_enriched_english_only.csv")
print(df.shape)
print(df2.shape)
