import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import OneHotEncoder


class PricePrediction:

    columns_to_encode = ['ChClKey', 'fProdFlag', 'fProprietary', 'markKey']
    columns_to_model = ['fSize', 'Price_Kg']
    columns_price_expect = ['fPriceExpect', 'newPrice']

    def __init__(self, data: pd.DataFrame, **kwargs):
        self.df = data
        lost_columns = [c for c in self.columns_to_encode + self.columns_to_model if c not in data.columns]
        assert len(lost_columns) == 0, f"Mandatory columns {lost_columns} are not in data!"
        self.encoding = {}
        self.matrix = None
        self.y = None
        self.regression = None
        self.build_encoding()

    def build_encoding(self):
        for col in self.columns_to_encode:
            enc = OneHotEncoder(sparse=False)
            data = self.df[[col]].fillna(0).to_numpy()
            enc_data = enc.fit_transform(data)
            self.encoding[col] = {'enc': enc, 'data': data, 'enc_data': enc_data, 'length': enc_data.shape[1],
                                  'categories': enc.categories_}
            if self.matrix is None:
                self.matrix = enc_data
            else:
                self.matrix = np.hstack((self.matrix, enc_data))
        self.matrix = np.hstack((self.matrix, self.df[self.columns_to_model]))
        self.matrix = np.nan_to_num(self.matrix)
        self.y = self.df[self.columns_price_expect].max(axis=1).fillna(0).to_numpy()
        self.regression = LinearRegression().fit(self.matrix, self.y)
