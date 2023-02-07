from dsapplicationregistration.dsar_core import procedure
from common import escrow_api


@procedure
def upload_dataset(ds,
                   username,
                   data_name,
                   data_in_bytes,
                   data_type,
                   optimistic,
                   original_data_size=None):
    print("This is a customized upload data!")
    escrow_api.upload_dataset(ds, username, data_name, data_in_bytes, data_type, optimistic, original_data_size)
