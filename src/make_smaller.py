import pandas as pd 
import os

PATH = os.path.join("..", "data", "from_darby", "MSCallGraph_0.csv")
SMALL_SIZE = 100

SMALLER = PATH[:-4] + "_size" + str(SMALL_SIZE) + PATH[-4:]

df = pd.read_csv(PATH)
print(df.head())

df = df[:SMALL_SIZE]
df.to_csv(SMALLER, index=False)

print("=" * 10)
df = pd.read_csv(SMALLER)
print(df.head())
