import os.path
import pathlib
import pickle
import time

from dsapplicationregistration import register
import glob
from common import utils

@register()
def train_covid_model():
    print("starting covid model")
