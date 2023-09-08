import pandas as pd

df = pd.read_excel("./data/results_2023-09-08.xlsx")

print(len(df))

print(len(df.drop_duplicates()))