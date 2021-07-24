#from pathlib import Path
#from toolz import itertoolz
#import vaex


transform_path_to_posix = lambda path: path.as_posix()

transform_ascii_to_vaex  = lambda path: vaex.from_ascii(path, seperator="\t")

transform_vaex_to_gen = lambda df: (itertoolz.second(x) for x in df.iterrows())

transform_vaex_to_dict = lambda df: df.to_dict()
