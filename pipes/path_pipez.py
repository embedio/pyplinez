from s_toolz import dd_path


def path_pipe_header_value_include(value):
    return dd_path.path_filter_row_one_value(value)


def path_pipe_header_value_exclude(value):
    return dd_path.xpath_filter_row_one_value(value)
