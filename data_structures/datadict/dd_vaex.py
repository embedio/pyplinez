from vaex import from_ascii, from_pandas
from ds_toolz import valmap, valfilter, second, curried, complement, unique
from datadict import Enhanced_DataDict


def vaex_filter_value_by_column(value, column_name):
    get_value = lambda df: df[df[column_name] == value]
    return curried.valmap(get_value)


def vaex_filter_by_column(column_name):
    get_column = lambda df: column_name in df.column_names
    return curried.valfilter(get_column)


def xvaex_filter_by_column(column_name):
    get_column = lambda df: column_name not in df.column_names
    return curried.valfilter(get_column)


def vaex_get_column_names(mapping):
    column_names = lambda df: df.column_names
    return valmap(column_names, mapping)


def vaex_unique_column_values(column_name):
    unique_column = lambda df: sorted(set(df[column_name].tolist()))
    return curried.valmap(unique_column)


def vaex_vaex_to_gen(mapping):
    """Transforms vaex dataframe rows into a generator of python dicts."""
    dframe_to_iterrows = lambda df: (second(x) for x in df.iterrows())
    return valmap(dframe_to_iterrows, mapping)


def vaex_ascii_to_vaex(mapping):
    """Transforms an ascii/text, tab seperated file into a vaex dataframe."""
    to_vaex_dframe = lambda path: from_ascii(path, seperator="\t")
    return valmap(to_vaex_dframe, mapping)


def vaex_transpose_vaex(mapping):
    transpose_vaex = lambda df: from_pandas(df.to_pandas_df().transpose())
    return valmap(transpose_vaex, mapping)


def vaex_vaex_to_dict(*funcs):
    to_dict = lambda df: Enhanced_DataDict(df.to_dict()).pipe(*funcs).data
    return curried.valmap(to_dict)


if __name__ == "__main__":
    import vaex as vx
    from generators import G

    from toolbox import *
    from datadict import *
    from data import *

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

    onet_dataframe = lambda: onet_filedata.pipe(vaex_ascii_to_vaex)

    scaleid_dataframe = lambda: scaleid_filedata.pipe(vaex_ascii_to_vaex)
