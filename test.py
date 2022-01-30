import main
import os
from common import utils
from models.user import *

ds_config = utils.parse_config("data_station_config.yaml")
app_config = utils.parse_config("app_connector_config.yaml")

client_api = main.initialize_system(ds_config, app_config)

# Adding new users

client_api.create_user(User(user_name="jerry", password="string"))
client_api.create_user(User(user_name="lucy", password="123456"))
client_api.create_user(User(user_name="david", password="string"))

# Log in a user to get a token

cur_token = client_api.login_user("jerry", "string")["access_token"]

# Look at all available APIs and APIDependencies

apis = client_api.get_all_apis(cur_token)
api_dependencies = client_api.get_all_api_dependencies(cur_token)
print(apis)
print(api_dependencies)

# Upload datasets



