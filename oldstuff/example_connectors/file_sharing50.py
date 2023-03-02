import glob
import os
import pathlib

from dsapplicationregistration import register
from common import general_utils

@register()
def f1():
    ds_path = str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parent)
    ds_config = utils.parse_config(os.path.join(ds_path, "data_station_config.yaml"))
    mount_path = pathlib.Path(ds_config["mount_path"]).absolute()
    files = set(glob.glob(os.path.join(str(mount_path), "**/**/**/*"), recursive=True))

    contents = {}
    for file in files:
        # print(file)
        if pathlib.Path(file).is_file():
            with open(file, "rb") as cur_file:
                content = cur_file.read().rstrip(b'\x00')
                contents[file] = content
            # print("read ", file)
    return contents

@register()
def f2():
    return f1()

@register()
def f3():
    return f1()

@register()
def f4():
    return f1()

@register()
def f5():
    return f1()

@register()
def f6():
    return f1()

@register()
def f7():
    return f1()

@register()
def f8():
    return f1()

@register()
def f9():
    return f1()

@register()
def f10():
    return f1()

@register()
def f11():
    return f1()

@register()
def f12():
    return f1()

@register()
def f13():
    return f1()

@register()
def f14():
    return f1()

@register()
def f15():
    return f1()

@register()
def f16():
    return f1()

@register()
def f17():
    return f1()

@register()
def f18():
    return f1()

@register()
def f19():
    return f1()

@register()
def f20():
    return f1()

@register()
def f21():
    return f1()

@register()
def f22():
    return f1()

@register()
def f23():
    return f1()

@register()
def f24():
    return f1()

@register()
def f25():
    return f1()

@register()
def f26():
    return f1()

@register()
def f27():
    return f1()

@register()
def f28():
    return f1()

@register()
def f29():
    return f1()

@register()
def f30():
    return f1()

@register()
def f31():
    return f1()

@register()
def f32():
    return f1()

@register()
def f33():
    return f1()

@register()
def f34():
    return f1()

@register()
def f35():
    return f1()

@register()
def f36():
    return f1()

@register()
def f37():
    return f1()

@register()
def f38():
    return f1()

@register()
def f39():
    return f1()

@register()
def f40():
    return f1()

@register()
def f41():
    return f1()

@register()
def f42():
    return f1()

@register()
def f43():
    return f1()

@register()
def f44():
    return f1()

@register()
def f45():
    return f1()

@register()
def f46():
    return f1()

@register()
def f47():
    return f1()

@register()
def f48():
    return f1()

@register()
def f49():
    return f1()

@register()
def f50():
    return f1()