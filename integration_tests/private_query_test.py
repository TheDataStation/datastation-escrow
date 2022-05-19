# initialize system

# get keys

# create users

# one of the user upload catalog (not needed in crypte)
# without changing the code structure / databases we can just upload the catalog like a normal data element
# and save its id

# each user upload their encrypted file (csv)

# create a new aggregator component inside DS (not here)
# inside the upload function (pass in the flag aggregate = True):
# 1. the aggregator will first check if its schema is compatible with the catalog (not needed in crypte)
# 2. retrieve the existing aggregated data from storage manager
# note: there is always ONLY one data element (besides the catalog) inside storage!!! Because any uploaded dataset will get combined
# 3. combine with the existing data (csv file)
# 4. remove the old data from storage
# 5. upload the newly combined data to storage (keep the same id, or not)
# 6. return data id

# ex:
class Aggregator:
    def __init__(self, catalog = None):
        self.catalog = catalog

    def check_compatible(self, data_to_upload):
        pass

    def aggregate(self, old_data, data_to_upload):
        pass

# create policies for both the catalog and the aggregated data for data users who want to query data
# (or just upload data in open mode)

# users can call the read_catalog api

# then call the query_sql api

