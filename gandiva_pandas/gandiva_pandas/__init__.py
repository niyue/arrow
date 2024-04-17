import pandas as pd

def create_df(size : int = 10000):
    df = pd.DataFrame({"c1": [1.0 * i for i in range(size)], "c2": [1.0 * i for i in range(size)]})
    return df