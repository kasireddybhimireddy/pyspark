import pandas as pd

s = pd.Series(list("abcdef"), index=[49, 48, 47, 0, 1, 2])
print(s)

print(s.loc[0]) # value at index label 0

print(s.iloc[0]) # value at index location 0

print(s.loc[0:1])  # rows at index labels between 0 and 1 (inclusive)
print(s.iloc[0:1]) # rows at index location between 0 and 1 (exclusive)