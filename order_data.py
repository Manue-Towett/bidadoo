import pandas as pd

df = pd.read_excel("./cleaned_data_2023-09-10.xlsx")


df_wrong = df[(df["YEAR"] < 1940) | (df["YEAR"] > 2023)]

print(df_wrong)

df.fillna("", inplace=True)

df_wrong["YEAR"] = ""

df.update(df_wrong)

# defined = df[~(df["YEAR"] == None) & ~(df["YEAR"] == "")].astype("int")

# df.update(defined)

df["YEAR"] = df["YEAR"].astype("str")

df["YEAR"] = [value.replace(".0", "") for value in df["YEAR"]]

df.to_excel("./cleaned/cleaned_2023-09-10.xlsx", index=False)