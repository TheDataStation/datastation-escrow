from dsapplicationregistration import register

@register()
def f1():
    return 0

@register()
def f2():
    return 0

@register(depends_on=[f1])
def f3():
    return 0

@register(depends_on=[f2])
def f4():
    return 0

@register(depends_on=[f3, f4])
def f5():
    return 0
