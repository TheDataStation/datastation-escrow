import pickle
from io import BytesIO

import pandas as pd

class Aggregator:
    def __init__(self, schema = None):
        self.schema = schema

    def check_compatible(self, data_to_upload):
        df = pd.read_csv(BytesIO(data_to_upload))
        return self.schema == df.columns

    def aggregate(self, old_data, data_to_upload):
        old_df = pd.read_csv(BytesIO(old_data))
        df_to_combine = pd.read_csv(BytesIO(data_to_upload))
        new_df = pd.concat([old_df, df_to_combine])
        return pickle.dump(new_df)