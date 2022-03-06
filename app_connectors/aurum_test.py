import os

from dsapplicationregistration import register

@register()
def start_es():
    print("Starting elastic search server")
    # os.system("/Users/kos/Desktop/AurumTest/elasticsearch-6.0.0/bin/elasticsearch")
