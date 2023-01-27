# Logistic regression model for the income dataset, no trust version.

import pathlib
import time
import pickle
# from torch.utils.data import ConcatDataset, DataLoader
# from integration_tests.neural_network.model import Net, train, test

from dsapplicationregistration.dsar_core import register
from common import ds_utils

@register()
def train_cifar_model(epochs, testloader):
    """train a neural network model on cifar data"""
    # print("starting cifar model")
    # files = ds_utils.get_all_files()
    # f_name_in_order = []
    # for file in set(files):
    #     if pathlib.Path(file).is_file():
    #         f_name_in_order.append(file)
    # f_name_in_order.sort(key=lambda x: int(x.split('/')[-2]))
    # # print(f_name_in_order)
    # train_data_array = []
    # for file in f_name_in_order:
    #     f = open(file, "rb")
    #     cur_file_bytes = f.read()
    #     f.close()
    #     # print(cur_file_bytes)
    #     # These bytes should be decrpted already, we convert it to object
    #     cur_torch_ele = pickle.loads(cur_file_bytes)
    #     # print(type(cur_torch_ele))
    #     train_data_array.append(cur_torch_ele)
    # train_data = ConcatDataset(train_data_array)
    # trainloader = DataLoader(train_data, batch_size=32, shuffle=True)
    #
    # net = Net()
    # i = train(net, trainloader, epochs=epochs)
    # end = time.time()
    # cross, acc = test(net, testloader)
    #
    # return acc
    return 1
