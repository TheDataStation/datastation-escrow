import yaml

def parse_config(path_to_config):
    with open(path_to_config) as config_file:
        res_config = yaml.load(config_file, Loader=yaml.FullLoader)
    return res_config
