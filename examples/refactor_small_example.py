import json
import os
from model_appliers import ModelApplier

ma = ModelApplier(mode='schema_CLASS')
ma.clean_slate()
# loading and data, copy-paste the content of the json file into arrows.app in the browser to view the example data
with open(os.path.join('examples', 'data', 'refactor_small_example.json')) as jsonfile:
    dct = json.load(jsonfile)
ma.load_arrows_dict(dct)

ma.refactor_all()
