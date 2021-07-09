from pyplinez_toolbox.pipe_tools import dict_sort_by_key, pick

def dict_pipe_key_sort(key):
    return dict_sort_by_key(key)


def dict_pipe_fromkeys(keys):
    return pick(keys)
