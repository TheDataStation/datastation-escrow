from dsapplicationregistration import register


@register
def preprocess():
    """preprocess all the data"""
    print("preprocess called")


@register(depends_on=[preprocess])
def modeltrain():
    """trains the model"""
    print("modeltrain called")


@register(depends_on=[modeltrain])
def predict():
    """submits input to get predictions"""
    print("predict called")

