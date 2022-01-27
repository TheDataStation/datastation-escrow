from dsapplicationregistration import register


@register
def preprocess(a: str):
    """preprocess all the data"""
    print("Hi, this is preprocess")
    return 33


@register(depends_on=[preprocess])
def train(a: int):
    """trains the model"""
    return


@register(depends_on=[train])
def query_inference(q: int):
    """queries a model"""
    return

