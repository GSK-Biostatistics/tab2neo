from data_loaders import FileDataLoader
import os
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential


class AzureDataLoader(FileDataLoader):
    """
    Assumes the following environmental variables to be defined with credentials to access Azure blob storage:
    AZ_CONTAINER, AZ_TENANT, AZ_CLIENT, AZ_CLIENT_SECRET;
    AZ_FILESYSTEM also needs to be set.
    Load data into Neo4j from Azure blob storage, with support for input formats: rda, xpt, sas7bdat and xls, xlsx
    All functions available in FileDataLoader are available here as well
    """

    def __init__(self, temp_folder=None, domain_dict=None, *args, **kwargs):
        """
        :param temp_folder: temporary folder where files from Azure would be downloaded to
                            (files by default deleted after operation completed)
        :param domain_dict: dictionary with file names as keys, and domain to be assigned as values
                            (e.g. {'dm_xyz.sas7bdat': 'DM', 'ae_xyz.sas7bdat': 'AE'} )
        :param verbose: bool - to print or not to print exec details
        :param debug: bool - to print or not to print details for debugging (e.g. cypher queries to be submitted)
        :param args: other arguments
        :param kwargs: other keyword arguments
        """
        super().__init__(domain_dict=domain_dict, *args, **kwargs)
        self.temp_folder = temp_folder if temp_folder is not None else 'temp'

        # Create the temp folder, if it doesn't already exist
        if not os.path.exists(self.temp_folder):
            os.makedirs(self.temp_folder)

        # Set up connection details to the Azure data lake
        self.service = DataLakeServiceClient(
            account_url=f"https://{os.environ.get('AZ_CONTAINER')}.dfs.core.windows.net/",
            credential=ClientSecretCredential(tenant_id=os.environ.get("AZ_TENANT"),
                                              client_id=os.environ.get("AZ_CLIENT"),
                                              client_secret=os.environ.get("AZ_CLIENT_SECRET"))
        )

        self.fs = self.service.get_file_system_client(file_system=os.environ.get("AZ_FILESYSTEM"))

    def listdir(self, path: str) -> [str]:
        """
        Lists all files (full path) in a directory on Azure blob storage

        :param path: A list of strings
                     # EXAMPLE of list item: "rd/space/test_compound/trial_01/testing_01/sdtm/vs.sas7bdat"
        """
        return [path.name for path in self.fs.get_paths(path=path)]

    def read_file(self, folder: str, filename: str, clean_up=True, *args, **kwargs):
        """
        Downloads file from Azure blob storage into a temporary folder {self.temp_folder},
        calls FileDataLoader.read_file against it and returns the result

        :param folder:
        :param filename:
        :param clean_up: Flag indicating whether to delete the temporary files
        :param args:
        :param kwargs:
        :return:
        """
        directory_client = self.fs.get_directory_client(folder)
        file_client = directory_client.get_file_client(filename)
        download = file_client.download_file()

        local_file = open(os.path.join(self.temp_folder, filename), 'wb')
        downloaded_bytes = download.readall()
        local_file.write(downloaded_bytes)
        local_file.close()

        res = super().read_file(self.temp_folder, filename, *args, **kwargs)
        if clean_up:
            os.remove(os.path.join(self.temp_folder, filename))

        return res
