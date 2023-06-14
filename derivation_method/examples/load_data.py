import os
import json
from model_managers import ModelManager
from data_loaders import FileDataLoader
from model_appliers import ModelApplier


def load_data():

    fdl = FileDataLoader()
    fdl.clean_slate()
    fdl.load_file(
        folder=os.path.join('derivation_method', 'examples'), 
        filename='test_data.csv'
    )
    mm = ModelManager()
    mm.create_model_from_data()
    mm.query("MATCH (c:Class{label:'Subject'}) set c.short_label = 'USUBJID'")
    mm.query("MATCH (c:Class{label:'Age'}) set c.data_type = 'int', c.short_label = 'AGE'")
    mm.query("MATCH (c:Class{label:'TestValue'}) set c.data_type = 'int', c.short_label = 'TESTV'")

    mm.create_class([{'label': 'NewValue', 'short_label': 'NEWV'}])
    mm.create_relationship([['Subject', 'NewValue', 'NewValue']])

    ma = ModelApplier(mode="schema_CLASS")
    ma.refactor_all()

if __name__ == '__main__':
    load_data()
