import os
from dsapplicationregistration import register


@register()
def ddprofiler():
    print("Start ddprofiler:")
    res = os.system("bash aurum/ddprofiler/run.sh --sources aurum/ddprofiler/mas_config.yml")
    return res

@register()
def nbc():
    print("Start nbc:")
    res = os.system("python aurum/nbc/networkbuildercoordinator.py --opath aurum/nbc/models/ --tpath "
                    "/Users/kos/Desktop/AurumTest/aurum/MAS_smallest/")
    return res

@register()
def keywords_search():
    print("Start kws:")
    res = os.system("python aurum/nbc/ddapi.py keywords_search")
    return res
