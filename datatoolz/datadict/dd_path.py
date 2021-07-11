from . import valmap, valfilter, curried


def path_filter_files(mapping):
    isfile = lambda path: path.is_file()
    return valfilter(isfile, mapping)


def path_filter_row_one_value(value):
    """filter value in first line of file"""
    has_value = lambda path: value in path.open().readline()
    return curried.valfilter(has_value)


def xpath_filter_row_one_value(value):
    """filter value in first line of file"""
    has_value = lambda path: value not in path.open().readline()
    return curried.valfilter(has_value)


def path_path_to_posix(mapping):
    as_posix = lambda path: path.as_posix()
    return valmap(as_posix, mapping)
