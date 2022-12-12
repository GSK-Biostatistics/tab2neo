import neointerface
import pandas as pd
import re
import pyreadstat       # A library that was open-sourced by Roche
import pyreadr          # Used for the RDA format
import os


class FileDataLoader(neointerface.NeoInterface):
    """
    Load data into Neo4j, with support for the following input formats:
            rda, xpt, sas7bdat, xls, xlsx
    A goal is to harmonize those formats.
    """

    def __init__(self, domain_dict = None, *args, **kwargs):
        """
        :param domain_dict: dictionary with file names as keys and domain to be assigned as values
                            (e.g. {'dm_xyz.sas7bdat': 'DM', 'ae_xyz.sas7bdat': 'AE'} )
        :param verbose: bool - to print or not to print exec details
        :param debug: bool - to print or not to print details for debugging (e.g. cypher queries to be submitted)
        :param args: other arguments
        :param kwargs: other keyword arguments
        """
        self.domain_dict = domain_dict
        super().__init__(*args, **kwargs)


    def read_file(self, folder:str, filename:str, sheet_name=0, query=None, metadataonly = False, test_run = False,
                  colcharsbl =r'[^A-Za-z0-9_]+'):
        """
        Read in an external file as a Pandas data frame.
        Can read in file types: rda, xpt, sas7bdat, xls, csv
        It can OPTIONALLY only retrieve the metadata from the source file.

        Note: nothing is actually loaded into Neo4j

        It returns a Pandas data frame, and the metadata.

        In case of error (such as an unsupported file format), an Exception is raised.

        :param folder:          Name of directory where the file to load resides
        :param filename:        Name (exclusive of path) of file to load
        :param sheet_name       Only for xls and xlsx - name of the sheet to load
        :param query            If not None apply a df.query on the dataframe being read
        :param metadataonly:    If True then only the folder, filename, and column names are imported
        :param test_run:        If True, only read the first 100 rows (Not applicable if the flag metadataonly is True)
        :param colcharsbl:      The column names of the input files are renamed in such a way that the symbols
                                    specified by the regex pattern are excluded
                                    (e.g. for cleaning special characters in the colnames of excel files)
                                    EXAMPLE: r'[^A-Za-z0-9_]+' will only keep A-Z, a-z, 0-9 and the underscore
                                    TODO: Perhaps default it to None, and if not None, then apply it
                                          regardless of the file extension (currently, only applied to Excel files)

        :return:                 The pair (Pandas data frame, metadata)
                                    EXAMPLE of the Pandas data frame:
                                              STUDYID     SITEID   ...    BIRTHDATE
                                        0    mid987650  214356.0   ...   1983-06-30
                                        1    mid987650  214356.0   ...   1956-06-30

                                    EXAMPLE of a value for meta:
                                        {'column_names': ['STUDYID', 'SITEID', 'BIRTHDATE']}
        """
        assert query is None or isinstance(query, str)

        ext = filename.split(".")[-1]               # Extract the filename extension (e.g. "rda")

        # Special processing for the various file formats we support
        # Set df to the Pandas dataframe, and meta to the dictionary {'column_names' : LIST_OF_COLUMN_NAMES_IN_DATA}
        if ext in ["sas7bdat"]:
            df, meta = pyreadstat.read_sas7bdat(os.path.join(folder, filename), metadataonly = metadataonly)
            meta = meta.__dict__
        elif ext in ["xpt"]:
            df, meta = pyreadstat.read_xport(os.path.join(folder, filename), metadataonly = metadataonly)
            meta = meta.__dict__
        elif ext in ["rda"]:
            r_result = pyreadr.read_r(os.path.join(folder, filename))
            name, df = r_result.popitem(last=False)
            meta = {'column_names': list(df.columns)}
            if metadataonly:
                df = pd.DataFrame(columns = df.columns)
        elif ext in ["xls", "xlsx"]:
            if ext == "xls":
                df = pd.read_excel(os.path.join(folder, filename), sheet_name=sheet_name)
            elif ext == "xlsx":
                df = pd.read_excel(os.path.join(folder, filename), engine='openpyxl', sheet_name=sheet_name)
            df.columns = [re.sub(colcharsbl, '', re.sub(r'\s', '_', s)) for s in df.columns] #TODO: maybe apply to ALL data types
            meta = {'column_names': list(df.columns)}
            if metadataonly:
                df = pd.DataFrame(columns = df.columns)
        elif ext in ["csv"]:
            # Todo: N.B CDISC Library provides csv files where values are quoted (i.e. "variabel 1","Another variable")
            #  Therefore option quotechar is used, which means that " will not appear in data.
            #  Have not tested or thought of scenarios what would happen if values are not quoted
            df = pd.read_csv(os.path.join(folder, filename), quotechar='"')
            meta = {'column_names': list(df.columns)}
            if metadataonly:
                df = pd.DataFrame(columns = df.columns)
        else:
            raise Exception(f"Unsupported file format - unrecognized filename extension: {ext}")

        # EXAMPLE of a value for meta:
        #   {'column_names': ['STUDYID', 'SITEID', 'AGE']}

        if test_run:
            df = df.head(100)   # Only use the first 100 rows

        # since pandas sets missing strings to float NaN we actively replace na with ''
        col_obj = [col for col in df.columns if df[col].dtype == 'object']
        df[col_obj] = df[col_obj].fillna(value='')

        if query:
            df = df.query(query)

        return (df, meta)


    def load_file(self, folder:str, filename:str, sheet_name=0, query=None, metadataonly = False, dataonly = False, test_run = False,
                        load_to_neo = True, colcharsbl =r'[^A-Za-z0-9_]+'):
        """
        Read in an external file as a Pandas data frame.

        Can read in file types: rda, xpt, sas7bdat, xls, csv
        It can OPTIONALLY only retrieve the metadata from the source file.
        It OPTIONALLY loads into Neo4j the metadata (creating 3 node labels: `Source Data Folder`, `Source Data Table`, `Source Data Column`),
                and, if imported, the data (creating nodes with the label `Source Data Row`,
                                            and also adding an extra field named `_domain_` to store the domain.)
                Whenever data is loaded to Neo4j, the following relationships are also created:
                    1) `Source Data Folder` -[:HAS_TABLE]-> `Source Data Table`
                    2) `Source Data Table` -[:HAS_COLUMN]-> to all `Source Data Column` nodes
                    3) 'Source Data Table' -[:HAS_DATA]-> to all 'Source Data Row' nodes

        "DOMAIN" - term used for the capitalized version of the basename of the datafile, for provenance information

        It returns a Pandas data frame.

        In case of error (such as an unsupported file format), an Exception is raised.

        :param folder:          Name of directory where the file to load resides
        :param filename:        Name (exclusive of path) of file to load
        :param sheet_name       Only for xls and xlsx - name of the sheet to load
        :param query:           A parameter to be passed to read_file
        :param metadataonly:    If True then only the folder, filename, and column names are imported
        :param dataonly:
        :param test_run:        If True, only read the first 100 rows (Not applicable if the flag metadataonly is True)
        :param load_to_neo:     If True then data (INCL. metadata) is loaded into Neo4j nodes with `Source Data Row` labels
        :param colcharsbl:      The column names of the input files are renamed in such a way that the symbols
                                    specified by the regex pattern are excluded
                                    (e.g. for cleaning special characters in the colnames of excel files)
                                    EXAMPLE: r'[^A-Za-z0-9_]+' will only keep A-Z, a-z, 0-9 and the underscore
                                    TODO: Perhaps default it to None, and if not None, then apply it
                                          regardless of the file extension (currently, only applied to Excel files)

        :return:                 A Pandas data frame.  EXAMPLE:
                                             STUDYID    SITEID    USUBJID  ...        TRTETM   BRTHDT      BRTHDTC
                                        0    mid987650  214356.0  987650.000001  ...  33420.0  1983-06-30  1983.0
                                        1    mid987650  214356.0  987650.000002  ...  33420.0  1956-06-30  1956.0
        """
        (df, meta) = self.read_file(folder=folder, filename=filename, sheet_name=sheet_name, query=query, metadataonly=metadataonly,
                                    test_run=test_run, colcharsbl=colcharsbl)

        fn = ".".join(filename.split(".")[:-1])     # The filename excluding the dot and the extension suffix
        ext = filename.split(".")[-1]

        if ext in ["xls", "xlsx"] and isinstance(sheet_name, str):
            domain = f"{fn}.{sheet_name}".upper()
        else:
            domain = fn.upper()
            
        if self.domain_dict:
            if filename in self.domain_dict.keys():
                domain = self.domain_dict[filename]

        # If the dataframe isn't empty, and it was requested to load the data into Neo4j,
        #       create nodes with the label `Source Data Row`
        if not df.empty and not metadataonly:
            # Timestamp data formats are not supported in neo4j as parameters to the query; therefore, changing to str
            # This does not cause trouble with SDTM data as dates are stored as str ther,e but would cause problems with
            # raw and ADaM data
            # TODO: get rid of this temporary workaround
            for i, dtype in enumerate(df.dtypes):
                if not dtype in ['O', 'float64', 'int64']:
                    df[df.columns[i]] = df[df.columns[i]].astype(str)

            # loading data
            # df = df.assign(_filename_ = filename)
            df = df.assign(_domain_=domain)  # Add new column, to keep track of the data provenance
            df = df.assign(_filename_=filename)  # Add new column, to keep track of the data provenance
            df = df.assign(_folder_=folder)  # Add new column, to keep track of the data provenance
            if load_to_neo:
                self.load_df(df=df, label="Source Data Row", merge=False)
                # self.load_df_clean_nan()

        # If the meta dictionary contains, as expected, the key 'column_names',
        #       and if the data (and metadata) is to be loaded into Neo4j,
        #       then handle the loading of the metadata
        if not dataonly:
            if 'column_names' in meta.keys() and load_to_neo:
                self.load_file_metadata(folder, filename, meta['column_names'], domain)

        # Create the 'HAS_DATA' relationships between the 'Source Data Table' node and all the 'Source Data Row' nodes
        self.link_nodes_on_matching_property_value(label1='Source Data Table', label2='Source Data Row',
                                                prop_name='_domain_', prop_value=domain,
                                                rel='HAS_DATA')
        return df

    # #TODO: to be moved to NeoInterface.load_df ?
    # def load_df_clean_nan(self, label = "Source Data Row"):
    #     q = f"""
    #     MATCH (sdrl:{label})
    #     WITH
    #     """
    #     if self.debug:
    #         print("        Query : ", q)
    #         print("        Query parameters: ", params)
    #     self.query(q)

    def load_file_metadata(self, folder:str, filename:str, columns: [str], domain = None) -> None:
        """
        It creates Neo4j nodes, to be used for metadata, with the following labels:
            `Source Data Folder`, `Source Data Table`, `Source Data Column`,
            and relationships HAS_TABLE and HAS_COLUMN among them

        :param folder:      Name of directory where the file resides
        :param filename:    Name (exclusive of path) of the file
        :param columns:     List of field names
        :param domain:      A string indicating a "data domain" (e.g. the name of the file with the original data)
        :return:            None
        """
        q = f"""
            MERGE (sdf:`Source Data Folder`{{_folder_:$folder}})                                 
            MERGE (sdf)-[:HAS_TABLE]->(sdt:`Source Data Table`{{_folder_:$folder, _domain_:$domain, _filename_:$filename}})
            WITH * UNWIND $columns as columnname
            MERGE (sdt)-[:HAS_COLUMN]->(sdc:`Source Data Column`{{_folder_:$folder, _domain_:$domain, _columnname_:columnname}})
            WITH DISTINCT sdf, sdt           
            MATCH (sdr:`Source Data Row`{{_folder_:$folder, _domain_:$domain}}), (sdt)
            MERGE (sdr)<-[:HAS_DATA]-(sdt)           
        """
        params = {'folder':folder, 'filename': filename, 'columns': columns, 'domain': domain}
        res = self.query(q, params)
        if self.debug:
            print("        Query : ", q)
            print("        Query parameters: ", params)


    def load_folder(self, folder="", only_files=None, metadataonly = False, test_run = False):
        """
        Loop over all files in the folder, and load them as detailed in load_file()

        :param folder:
        :param only_files:
        :param metadataonly:
        :param test_run:
        :return:
        """
        if not only_files:
            only_files = []
        self.load_df(df=pd.DataFrame([{'_folder_': folder}]), label="Source Data Folder", merge=True,
                     primary_key='_folder_')
        for filename in os.listdir(folder):
            # if only_files is empty list, then loads all files
            if (not only_files) or filename in only_files:
                if self.verbose:
                    print(f"Loading {filename}")
                self.load_file(folder, filename, metadataonly = metadataonly, test_run = test_run)

    def delete_source_data(self):
        """
        Deletes all (Non-reshaped) data
        :return:
        """
        q = """
        MATCH (sd)
        WHERE sd:`Source Data Folder` OR sd:`Source Data Row` OR sd:`Source Data Table` OR sd:`Source Data Column`
        DETACH DELETE sd
        """
        self.query(q)

    def delete_file_data(self, folder: str, domain: str):
        q = f"""
                MATCH (folder:`Source Data Folder`{{_folder_:$folder}}),                           
                (folder)-[:HAS_TABLE]->(file:`Source Data Table`{{_domain_:$domain}}),               
                (file)-[:HAS_COLUMN]->(column:`Source Data Column`)
                DETACH DELETE column
                WITH DISTINCT folder, file
                MATCH (file)-[:HAS_DATA]->(sdr:`Source Data Row`)
                DETACH DELETE sdr
                WITH DISTINCT folder, file
                DETACH DELETE file
                WITH DISTINCT folder
                OPTIONAL MATCH (folder)-[r:HAS_TABLE]->()
                WITH DISTINCT folder, count(r) as cnt
                CALL apoc.do.when(
                	cnt = 0,
                	'DELETE $folder',
                	'RETURN "keep"',
                	{{folder: folder}}
                ) 
                YIELD value RETURN value
            """
        self.query(q, {'folder': folder, 'domain': domain})


    def load_file_distinct_values_for_columns(self, folder: str, filename: str, column_list: list, test_run = False):
        """
        To be used when one doesn't want to load all the data,
        but just distinct items in a column (stored w/ a relationship to its property)

        This operation sits in-between the models and the data loading.

        :param folder:
        :param filename:
        :param column_list:
        :param test_run:
        :return:
        """
        df = self.load_file(folder, filename, metadataonly = False, load_to_neo= False, test_run = test_run)
        for column, property_id in column_list:
            values = [x for x in set(df[column]) if not x is None]
            q = """             
            MATCH (property) 
            WHERE id(property) = $property_id
            WITH *, $values as values UNWIND values as value
            MERGE (property)-[:HAS_PROPERTY_VALUE]->(:PropertyValue{value:value})
            """
            self.query(q, {'property_id': property_id, 'values': values})

    @staticmethod
    def convert_datetime_columns(df: pd.DataFrame, date_format: str, datetime_col_regex='^.*DTM$',
                                 date_col_regex='^.*DT$'):
        """
        Convert columns in df that match dtcol_regex from SAS integer dates to pandas Timestamps (equivalent to python
        datetimes).
        :param df: input df
        :param date_format: String of date format to convert from. E.g. 'sas', 'unix'
        :param datetime_col_regex: regex for datetime columns
        :param date_col_regex: regex for date columns
        :return: modified df
        """

        assert date_format.lower() in ['sas', 'unix'], f"Unsupported date_format {date_format}"

        # setup regex column matching for datetime/date columns
        r_datetime = re.compile(pattern=datetime_col_regex)
        date_time_cols = list(filter(r_datetime.match, df.columns))
        r_date = re.compile(pattern=date_col_regex)
        date_cols = list(filter(r_date.match, df.columns))

        if date_format.lower() == 'sas':
            # setup conversion function for sas datetimes/dates
            def f(x, unit):
                return pd.to_timedelta(x, unit=unit) + pd.Timestamp(year=1960, month=1, day=1)

        if date_format.lower() == 'unix':
            # setup conversion function for unix datetimes/dates
            def f(x, unit):
                return pd.to_datetime(x, unit=unit)

        for col in date_time_cols:
            # for all datetime columns convert using unit='s' (seconds)
            df[col] = df[col].apply(
                lambda x: f(x, 's'))

        for col in date_cols:
            # for all date columns convert using unit='D' (days)
            df[col] = df[col].apply(
                lambda x: f(x, 'D'))

        return df
