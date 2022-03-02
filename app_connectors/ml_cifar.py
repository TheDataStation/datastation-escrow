import os.path
import pathlib
import numpy as np
import pickle
import time
import torch
from torch.utils.data import ConcatDataset, DataLoader
from jinjin.model import *

from dsapplicationregistration import register
import glob
from common import utils

@register()
def train_cifar_model(epochs, testloader):
    """train a neural network model on cifar data"""
    print("starting cifar model")
    prev_time = time.time()
    ds_path = str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parent)
    ds_config = utils.parse_config(os.path.join(ds_path, "data_station_config.yaml"))
    mount_path = pathlib.Path(ds_config["mount_path"]).absolute()
    files = glob.glob(os.path.join(str(mount_path), "**/**/**/*"), recursive=True)
    f_name_in_order = []
    for file in set(files):
        if pathlib.Path(file).is_file():
            # print(file)
            f_name_in_order.append(file)
    f_name_in_order.sort(key=lambda x: int(x.split('/')[-2]))
    # print(f_name_in_order)
    train_data_array = []
    for file in f_name_in_order:
        f = open(file, "rb")
        cur_file_bytes = f.read()
        f.close()
        # print(cur_file_bytes)
        # These bytes should be decrpted already, we convert it to object
        cur_torch_ele = pickle.loads(cur_file_bytes)
        # print(type(cur_torch_ele))
        train_data_array.append(cur_torch_ele)
    train_data = ConcatDataset(train_data_array)
    trainloader = DataLoader(train_data, batch_size=32, shuffle=True)

    print("data station overhead is: ")
    print(time.time() - prev_time)
    prev_time = time.time()

    net = Net()
    i = train(net, trainloader, epochs=epochs)
    end = time.time()
    cross, acc = test(net, testloader)

    print('time: {}, accuracy {}'.format(end - prev_time, acc))
