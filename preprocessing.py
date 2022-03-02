from cgi import test
from json import load
import torchvision.transforms as transforms
from torch.utils.data import DataLoader
from torchvision.datasets import CIFAR10
from torch.utils.data import random_split
import torch
import os

class TorchArray():
    def __init__(self, X, y) -> None:
        self.X = X
        self.y = y

def save_data(data, dir, file_name):
    if not os.path.isdir(dir):
        os.mkdir(dir)
    if isinstance(data, list):
        for i, d in enumerate(data):
            file = os.path.join(dir, file_name + str(i) + ".pt")
            torch.save(d, file)
            # data = torch.load(file)
            # trainloader = DataLoader(data, batch_size=32, shuffle=True)
            # print("success?")
    else:
        file = os.path.join(dir, file_name + ".pt")
        torch.save(data, file)


def load_data(num=4, size=12500):
    """Load CIFAR-10 (training and test set)."""
    transform = transforms.Compose(
    [transforms.ToTensor(), transforms.Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5))]
    )
    trainset = CIFAR10(".", train=True, download=True, transform=transform)
    trainsets = random_split(trainset, [size]*num, generator=torch.Generator().manual_seed(42))
    testset = CIFAR10(".", train=False, download=True, transform=transform)
    # trainloader = DataLoader(trainset, batch_size=32, shuffle=True)
    # print(type(testset))
    # testloader = DataLoader(testset, batch_size=32)
    # print(type(trainloader))
    # num_examples = {"trainset" : len(trainset), "testset" : len(testset)}
    # print(num_examples)
    return trainsets, testset

if __name__ == "__main__":
    trainsets, testset = load_data()
    save_data(trainsets, "training", 'train')
    save_data(testset, "testing", 'test')
