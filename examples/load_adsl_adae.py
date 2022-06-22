from data_loaders import file_data_loader
from utils.utils import summarize_dataframe
import time


def main():
    """
    Import some data files upon first completely clearing the database
    """

    start_time = time.time()

    # Clear up the database, and load data into it
    # TODO: for now, it's just dummy data

    dl = file_data_loader.FileDataLoader()
    dl.clean_slate()        # Note: "ghost" labels may remain!


    df = dl.load_file(folder="dummy_data", filename="adsl.rda")
    summarize_dataframe(df, "adsl.rda")
    print("----------------------------------------------------------")
    df = dl.load_file(folder="dummy_data", filename="adae.rda")
    summarize_dataframe(df, "adae.rda")

    print(f"--- {(time.time() - start_time):.3f}' seconds ---")



if __name__ == "__main__":
    main()