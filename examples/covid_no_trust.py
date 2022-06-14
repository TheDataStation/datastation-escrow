import os
import pathlib
import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow import keras
import pickle
import time
from sklearn.model_selection import train_test_split
from keras.preprocessing.image import ImageDataGenerator
from csv import writer


from dsapplicationregistration import register
from common import general_utils

@register()
def train_covid_model(test_data, test_label):
    print("starting covid model")

    start_time = time.time()
    ds_path = str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parent)
    ds_config = utils.parse_config(os.path.join(ds_path, "data_station_config.yaml"))
    mount_path = pathlib.Path(ds_config["mount_path"]).absolute()
    files = []
    DE_dir_name = os.listdir(mount_path)
    for i in range(len(DE_dir_name)):
        DE_dir_name[i] = os.path.join(str(mount_path), str(DE_dir_name[i]))
        files.append(os.path.join(str(DE_dir_name[i]), str(os.listdir(DE_dir_name[i])[0])))

    print(files)

    # Read in these files, and build a large <f_name, np_array> dictionary
    name_np_dict = {}
    for file in set(files):
        f = open(file, "rb")
        cur_file_bytes = f.read()
        # These bytes should be decrpted already, we convert it to object
        cur_file_dict = pickle.loads(cur_file_bytes)
        f.close()
        name_np_dict.update(cur_file_dict)

    # # Check that name_np_dict looks correct
    # for key, value in name_np_dict.items():
    #     print(key)
    #     print(np.shape(value))

    # Start the ML code

    # Read the metadata from train.txt (an open file that everyone can read)

    train_size = 0.9

    train_df = pd.read_csv('integration_tests/test_metadata/train.txt', sep="\s+", header=None)
    train_df.columns = ['patient id', 'filename', 'class', 'data source']
    train_df = train_df.drop(['patient id', 'data source'], axis=1)

    train_df, valid_df = train_test_split(train_df, train_size=train_size, random_state=0)

    # Now create the train_data and train_label that will be used for ImageDataGenerator.flow
    train_data = list()
    train_label = list()

    valid_data = list()
    valid_label = list()

    for _, row in train_df.iterrows():
        train_data.append(name_np_dict[row['filename']])
        if row["class"] == "positive":
            train_label.append(1)
        else:
            train_label.append(0)

    for _, row in valid_df.iterrows():
        valid_data.append(name_np_dict[row['filename']])
        if row["class"] == "positive":
            valid_label.append(1)
        else:
            valid_label.append(0)

    train_data = np.asarray(train_data).reshape(len(train_data), 200, 200, 3)
    valid_data = np.asarray(valid_data).reshape(len(valid_data), 200, 200, 3)
    test_data = np.asarray(test_data).reshape(len(test_data), 200, 200, 3)

    print(np.shape(train_data))
    print(np.shape(valid_data))
    print(np.shape(test_data))
    # print(train_label)
    # print(valid_label)
    # print(test_label)

    # Start the actual model training process
    train_datagen = ImageDataGenerator(rescale=1. / 255.,
                                       rotation_range=40,
                                       width_shift_range=0.2,
                                       height_shift_range=0.2,
                                       shear_range=0.2,
                                       zoom_range=0.2,
                                       horizontal_flip=True,
                                       vertical_flip=True)
    test_datagen = ImageDataGenerator(rescale=1.0 / 255.)

    train_gen = train_datagen.flow(train_data, train_label, batch_size=64)
    valid_gen = test_datagen.flow(valid_data, valid_label, batch_size=64)
    test_gen = test_datagen.flow(test_data, test_label, batch_size=64)

    base_model = tf.keras.applications.ResNet50V2(weights='imagenet',
                                                  input_shape=(200, 200, 3),
                                                  include_top=False)

    for layer in base_model.layers:
        layer.trainable = False

    model = tf.keras.Sequential([
        base_model,
        tf.keras.layers.GlobalAveragePooling2D(),
        tf.keras.layers.Dense(128, activation='relu'),
        tf.keras.layers.BatchNormalization(),
        tf.keras.layers.Dropout(0.2),
        tf.keras.layers.Dense(1, activation='sigmoid')
    ])

    class EvaluateEpochEnd(tf.keras.callbacks.Callback):
        def __init__(self, cur_test_data):
            self.test_data = cur_test_data
            self.prev_epoch_time = time.time()

        def on_epoch_end(self, epoch, logs={}):
            scores = self.model.evaluate(self.test_data, verbose=0)
            print('\nTesting loss: {}, accuracy: {}\n'.format(scores[0], scores[1]))
            cur_epoch_time = time.time()
            with open("ml_acc.csv", 'a+') as f:
                writer_object = writer(f)
                writer_object.writerow([scores[0], scores[1], cur_epoch_time - self.prev_epoch_time])
            self.prev_epoch_time = cur_epoch_time

    callbacks = [
        # tf.keras.callbacks.ModelCheckpoint("covid_classifier_model.h5", save_best_only=True, verbose=0),
        tf.keras.callbacks.ReduceLROnPlateau(monitor='val_loss', factor=0.5, patience=2, verbose=1),
        EvaluateEpochEnd(test_gen),
    ]

    model.compile(optimizer=keras.optimizers.Adam(learning_rate=0.001),
                  loss='binary_crossentropy',
                  metrics=['accuracy'])

    preprocess_overhead = time.time() - start_time

    with open("ml_acc.csv", 'a+') as f:
        writer_object = writer(f)
        writer_object.writerow([preprocess_overhead])

    print("data preprocessing overhead is", preprocess_overhead)

    history = model.fit(train_gen,
                        validation_data=valid_gen,
                        epochs=3,
                        callbacks=[callbacks])

    total_overhead = time.time() - start_time

    with open("ml_acc.csv", 'a+') as f:
        writer_object = writer(f)
        writer_object.writerow([total_overhead])

    print("data station overhead is", total_overhead)
