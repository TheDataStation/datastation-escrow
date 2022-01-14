# These are the "actual APIs" that would be exposed to the users.

from titanic import data_preprocess, model_train, predict

# train_dir = "./testV1/titanicML/sample_data"
# input_file = "./testV1/titanicML/test.csv"

train_dir = "./sample_data"
input_file = "./test.csv"

# Calling Datapreprocess
res_x, res_y = data_preprocess(train_dir)
# print(res_x)
# print(res_y)

# Calling ModelTrain
reg = model_train((train_dir))
# print(reg.coef_)

# Calling Predict
pred = predict(train_dir, input_file)
