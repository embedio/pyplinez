
def seq_pipe_groupby_key_apply(key, *funcs):
    return eseq_group_by_dict_key(key, *funcs)


def seq_pipe_key_groupby(dict_key):
    return seq_group_by_dict_key(dict_key)


def seq_pipe_values_from_keys_get(dict_keys):
    return seq_extract_data_from_dict_keys(dict_keys)
