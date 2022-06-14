import os
import pathlib
import pandas as pd
from tensorflow import keras
import tensorflow as tf
from keras.preprocessing.image import ImageDataGenerator
from sklearn.model_selection import train_test_split

from dsapplicationregistration import register
from common import utils

@register()
def covid_v2():
    print("Start covid model version two")

    ds_path = str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parent)
    ds_config = utils.parse_config(os.path.join(ds_path, "data_station_config.yaml"))
    mount_path = pathlib.Path(ds_config["mount_path"]).absolute()
    files = []
    DE_dir_name = os.listdir(mount_path)
    for i in range(len(DE_dir_name)):
        DE_dir_name[i] = os.path.join(str(mount_path), str(DE_dir_name[i]))
        files.append(os.path.join(str(DE_dir_name[i]), str(os.listdir(DE_dir_name[i])[0])))
    # print(files)

    # Build dictionary from file name to absolute path
    name_path_dict = {}
    for f in files:
        f_name = f.split("/")[-1]
        name_path_dict[f_name] = f

    # Start the ML code

    # Read the metadata from train.txt (an open file that everyone can read)

    train_size = 0.9

    train_df = pd.read_csv('integration_tests/test_metadata/train.txt', sep="\s+", header=None)
    train_df.columns = ['patient id', 'filename', 'class', 'data source']
    train_df = train_df.drop(['patient id', 'data source'], axis=1)

    # update the df columns
    for _, row in train_df.iterrows():
        row['filename'] = name_path_dict[row['filename']]

    train_df, valid_df = train_test_split(train_df, train_size=train_size, random_state=0)

    print(f"Negative and positive values of train: {train_df['class'].value_counts()}")
    print(f"Negative and positive values of validation: {valid_df['class'].value_counts()}")

    train_datagen = ImageDataGenerator(rescale=1. / 255.,
                                       rotation_range=40,
                                       width_shift_range=0.2,
                                       height_shift_range=0.2,
                                       shear_range=0.2,
                                       zoom_range=0.2,
                                       horizontal_flip=True,
                                       vertical_flip=True)
    test_datagen = ImageDataGenerator(rescale=1.0 / 255.)

    train_gen = train_datagen.flow_from_dataframe(dataframe=train_df,
                                                  x_col='filename',
                                                  y_col='class',
                                                  target_size=(200, 200),
                                                  batch_size=64,
                                                  class_mode='binary')
    valid_gen = test_datagen.flow_from_dataframe(dataframe=valid_df,
                                                 x_col='filename',
                                                 y_col='class',
                                                 target_size=(200, 200),
                                                 batch_size=64,
                                                 class_mode='binary')

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

    callbacks = [
        tf.keras.callbacks.ModelCheckpoint("covid_classifier_model.h5", save_best_only=True, verbose=0),
        tf.keras.callbacks.EarlyStopping(patience=3, monitor='val_loss', verbose=1),
        tf.keras.callbacks.ReduceLROnPlateau(monitor='val_loss', factor=0.5, patience=2, verbose=1)
    ]

    model.compile(optimizer=keras.optimizers.Adam(learning_rate=0.001),
                  loss='binary_crossentropy',
                  metrics=['accuracy'])

    history = model.fit(train_gen,
                        validation_data=valid_gen, epochs=2,
                        callbacks=[callbacks])

    if os.path.exists("./covid_classifier_model.h5"):
        os.remove("./covid_classifier_model.h5")
