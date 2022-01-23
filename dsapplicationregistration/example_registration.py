from dsapplicationregistration.dsar_core import expose


@expose
def preprocess(a: str):
    """preprocess all the data"""
    print("Hi, this is preprocess")
    return 33


@expose
def train(a: int):
    """trains the model"""
    return

