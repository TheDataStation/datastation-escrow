# Logistic regression model for the income dataset, no trust version.

import pathlib
import time
import pickle
from torch.utils.data import ConcatDataset, DataLoader
import torch.nn as nn
import torch
import torch.nn.functional as F
from collections import OrderedDict

from dsapplicationregistration.dsar_core import register
from common import escrow_api

DEVICE = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

class Net(nn.Module):
    def __init__(self) -> None:
        super(Net, self).__init__()
        self.conv1 = nn.Conv2d(3, 6, 5)
        self.pool = nn.MaxPool2d(2, 2)
        self.conv2 = nn.Conv2d(6, 16, 5)
        self.fc1 = nn.Linear(16 * 5 * 5, 120)
        self.fc2 = nn.Linear(120, 84)
        self.fc3 = nn.Linear(84, 10)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        x = self.pool(F.relu(self.conv1(x)))
        x = self.pool(F.relu(self.conv2(x)))
        x = x.view(-1, 16 * 5 * 5)
        x = F.relu(self.fc1(x))
        x = F.relu(self.fc2(x))
        x = self.fc3(x)
        return x

def test(net, testloader):
    """Validate the network on the entire test set."""
    criterion = torch.nn.CrossEntropyLoss()
    correct, total, loss = 0, 0, 0.0
    with torch.no_grad():
        for data in testloader:
            images, labels = data[0].to(DEVICE), data[1].to(DEVICE)
            outputs = net(images)
            loss += criterion(outputs, labels).item()
            _, predicted = torch.max(outputs.data, 1)
            total += labels.size(0)
            correct += (predicted == labels).sum().item()
    accuracy = correct / total
    return loss, accuracy

def train(net, trainloader, epochs, val = None):
    """Train the network on the training set."""
    criterion = torch.nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(net.parameters(), lr=0.001, momentum=0.9)
    cross = float('inf')
    for i in range(epochs):
        for images, labels in trainloader:
            images, labels = images.to(DEVICE), labels.to(DEVICE)
            optimizer.zero_grad()
            loss = criterion(net(images), labels)
            loss.backward()
            optimizer.step()

def get_model_parameters(net):
    return [val.cpu().numpy() for _, val in net.state_dict().items()]

def set_model_parameters(net, params):
    params_dict = zip(net.state_dict().keys(), params)
    state_dict = OrderedDict({k: torch.tensor(v) for k, v in params_dict})
    net.load_state_dict(state_dict, strict=True)

@register()
def train_cifar_model(epochs, testloader):
    """train a neural network model on cifar data"""
    print("starting cifar model")
    files = ds_utils.get_all_files()
    f_name_in_order = []
    for file in set(files):
        if pathlib.Path(file).is_file():
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

    net = Net()
    i = train(net, trainloader, epochs=epochs)
    end = time.time()
    cross, acc = test(net, testloader)

    return acc
