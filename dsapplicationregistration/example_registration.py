from dsapplicationregistration.dsar_core import expose


@expose
def preprocess(a: str):
    """preprocess all the data"""
    print("Hi, this is preprocess")
    return 33


@expose(depends_on=[preprocess])
def train(a: int):
    """trains the model"""
    return


@expose(depends_on=[train])
def query_inference(q: int):
    """queries a model"""
    return

