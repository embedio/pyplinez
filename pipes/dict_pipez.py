from pipe_tools import dd_dict, pick


def dict_pipe_key_sort(key):
    return dd_dict.dict_sort_by_key(key)


def dict_pipe_fromkeys(keys):
    return pick(keys)
