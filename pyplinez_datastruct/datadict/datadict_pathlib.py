from pathlib.Path import is_file, as_posix
from datadict_toolbox import valmap, valfilter, curried


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


if __name__ == "__main__":
    from pathlib import Path

    from datadict_toolbox import *
    from datadict import DataDict, Enhanced_DataDict

    data = {path.stem: path for path in Path("text_data").iterdir()}

    onet_filedata = Enhanced_DataDict(data).pipe(
        path_filter_files,
        path_filter_row_one_value("ONET_SOC_CODE"),
        path_filter_row_one_value("Element_Name"),
        path_filter_row_one_value("Scale_ID"),
        path_path_to_posix,
    )

    scaleid_filedata = Enhanced_DataDict(data).pipe(
        path_filter_files,
        path_filter_row_one_value("Scale_ID"),
        xpath_filter_row_one_value("ONET_SOC_CODE"),
        path_path_to_posix,
    )
