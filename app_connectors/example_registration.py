from dsapplicationregistration import register


@register()
def preprocess():
    """preprocess all the data"""
    print("preprocess called")
    return 0


@register(depends_on=[preprocess])
def modeltrain():
    """trains the model"""
    res = preprocess()
    print("modeltrain called")
    return 0


@register(depends_on=[modeltrain])
def predict(accuracy: int,
            num_times: int):
    """submits input to get predictions"""
    res = modeltrain()
    print("Prediction accuracy is "+str(accuracy)+" percent :(")
    print("Please try "+str(num_times)+" times more!")
    return 0