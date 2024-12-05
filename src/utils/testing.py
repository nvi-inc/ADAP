def list_attrs():
    for attr in globals().keys():
        print('Global ', attr)

def set_value(val):
    globals()['MyVariable'] = val

def use_value():
    if 'MyVariable' not in globals():
        return 'Not set'
    return globals().get('MyVariable', None)



