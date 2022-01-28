from dsapplicationregistration import register


@register()
def preprocess(num_files: int):
    """preprocess all the data"""
    print("preprocess called")
    print("Let's clean "+str(num_files)+" files!")


@register(depends_on=[preprocess])
def modeltrain(num_models: int):
    """trains the model"""
    print("modeltrain called")
    print("I want to train "+str(num_models)+" models!")


@register(depends_on=[modeltrain])
def predict(accuracy: int,
            num_times: int):
    """submits input to get predictions"""
    print("predict called")
    print("Prediction accuracy is "+str(accuracy)+" percent :(")
    print("Please try "+str(num_times)+" more!")
