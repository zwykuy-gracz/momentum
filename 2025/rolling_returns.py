import pandas as pd
import numpy as np


def get_rolling_returns(df, n):
    return df.rolling(n).apply(np.prod)
