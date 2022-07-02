import pandas as pd

df1 = pd.read_csv('github_repositories.csv', names=["name", "blah", "blah2"], header=None)
print(df1.head())
df1 = df1[["sql" in x for x in df1["blah2"]]]
# print(df1.head())
print(df1['blah2'])