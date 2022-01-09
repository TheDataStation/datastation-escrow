import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn import linear_model
from sklearn.impute import SimpleImputer
from os import listdir, path

def FilePreprocess(input_X_file):
  import_pd = pd.read_csv(input_X_file)
  import_X = import_pd.to_numpy()
  import_X = np.delete(import_X, 2, 1)
  import_X = np.delete(import_X, 6, 1)
  import_X = np.delete(import_X, 7, 1)
  import_X = np.delete(import_X, 7, 1)
  import_X = np.delete(import_X, 0, 1)
  for i in range(len(import_X)):
    if import_X[i][1] == "male":
      import_X[i][1] = 1
    else:
      import_X[i][1] = 0
  import_X.astype(float)

  imputer = SimpleImputer(missing_values=np.nan, strategy='mean')

  imputer = imputer.fit(import_X)
  import_X = imputer.transform(import_X)
  return import_X

def DataPreprocess(data_dir):
  all_df = []

  for file in listdir(data_dir):
    with open(path.join(data_dir, file)) as f:
      cur_df = pd.read_csv(f)
      all_df.append(cur_df)

  import_df = pd.concat(all_df)

  # Data cleaning

  import_np = import_df.to_numpy()
  import_X = np.delete(import_np, 1, 1)
  import_y = import_np[:, [1]]

  import_X = np.delete(import_X, 2, 1)
  import_X = np.delete(import_X, 6, 1)
  import_X = np.delete(import_X, 7, 1)
  import_X = np.delete(import_X, 7, 1)
  import_X = np.delete(import_X, 0, 1)

  # Convert gender column

  for i in range(len(import_X)):
    if import_X[i][1] == "male":
      import_X[i][1] = 1
    else:
      import_X[i][1] = 0

  # Convert to float
  import_X.astype(float)

  imputer = SimpleImputer(missing_values=np.nan, strategy='mean')

  imputer = imputer.fit(import_X)
  import_X = imputer.transform(import_X)

  return import_X, import_y

def ModelTrain(data_dir):
  import_X, import_y = DataPreprocess(data_dir)
  train_X, test_X, train_y, test_y = train_test_split(import_X, import_y, test_size=0.33, random_state=1)

  reg = linear_model.LinearRegression()
  reg.fit(train_X, train_y)
  return reg

def Predict(data_dir, input_X_file):
  reg = ModelTrain(data_dir)
  input_X = FilePreprocess(input_X_file)
  predictions = reg.predict(input_X).flatten()
  for i in range(len(predictions)):
    if predictions[i] <= 0.5:
      predictions[i] = 0
    else:
      predictions[i] = 1
  return predictions
