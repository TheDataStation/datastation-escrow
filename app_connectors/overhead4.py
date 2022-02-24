from dsapplicationregistration import register

@register()
def f1():
    return 0

@register()
def f2():
    return 0

@register(depends_on=[f1, f2])
def f3():
    return 0

@register()
def f4():
    return 0
