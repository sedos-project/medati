"""
The datahelper helps to format input data files and edits metadata.
"""

import sys
import csv
import glob
import os
import json
from datetime import datetime

import pandas as pd

# avoid csv field size error
MAXINT = sys.maxsize

while True:
    # decrease the maxInt value by factor 10
    # as long as the OverflowError occurs.

    try:
        csv.field_size_limit(MAXINT)
        break
    except OverflowError:
        MAXINT = int(MAXINT / 10)

OEDATAMODEL_COL_LIST = [
    "id",
    "region",
    "year",
    "timeindex_resolution",
    "timeindex_start",
    "timeindex_stop",
    "bandwidth_type",
    "version",
    "method",
    "source",
    "comment",
]

JSON_COL_LIST = [
    "bandwidth_type",
    "version",
    "method",
    "source",
    "comment",
]


def get_files_from_directory(directory: str = None, type_of_file: str = "csv") -> list:
    """
    The function takes a path as input and returns all csv-file paths in the directory as a list.
    :rtype: object
    :param directory: csv directory path
    :return: files_path - list of csv file paths
    :return: metadata_path - list of json file paths
    """

    if type_of_file == "csv":
        return list(glob.glob(f"{directory}/*.csv"))
    if type_of_file == "json":
        return list(glob.glob(f"{directory}/*.json"))

    return None


class Datahelper:
    """
    The class helps ontologically annotating input data files and creating metadata.
    """

    def __init__(self, input_path: str = None, output_path: str = None):

        # define paths for csv and oeo_annotation folder
        self.input_dir = os.path.join(os.getcwd(), input_path)
        self.output_dir = os.path.join(os.getcwd(), output_path)

        os.makedirs(self.input_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)

        self.df_dict = self.prepare_df_dict(directory=self.input_dir)
        self.dict_filename_json = self.prepare_json_dict(directory=self.input_dir)

        self.now = datetime.now()

    def prepare_df_dict(self, directory: str = None) -> dict:
        """
        The method reads all csv's into pandas dataframes and saves them with their names in a dict.
        :param directory: Path to csv files.
        :return: df_dict: Key -> df name; Value -> df.
        """
        files: list = get_files_from_directory(directory, type_of_file="csv")
        # sep=None, engine='python' will use Python’s builtin sniffer tool to determine the delimiter.
        # This method is a rough heuristic and may produce both false positives and negatives.
        return {
            file.split("\\")[-1].split(".")[0]: pd.read_csv(
                filepath_or_buffer=file, sep=None, engine="python"
            )
            for file in files
        }

    def prepare_json_dict(self, directory: str = None) -> dict:
        """
        The method reads all metadata json into and saves them with their names in a dict.
        :param directory: Path to metadata json files.
        :return: df_dict: Key -> df name; Value -> df.
        """
        meta_files: list = get_files_from_directory(directory, type_of_file="json")
        print(meta_files)
        return {
            meta_file.split("\\")[-1].split(".")[0]: self.read_metadata_json(
                path=meta_file
            )
            for meta_file in meta_files
        }

    def return_user_defined_columns(self):
        """
        The method returns user defined columns that are neither columns
        of the oedatamodel-parameter scalar or timeseries.
        :return: dict: Key -> df name: str ; Value -> user_defined_columns: dict.
        """

        return {
            (df_name): (set(value.columns.tolist()) - set(OEDATAMODEL_COL_LIST))
            for (df_name, value) in self.df_dict.items()
        }

    def create_json_dict_from_user_defined_columns(self):
        """
        The method reads columns and returns dict with column names as keys and empty value.
        :param df_dict: Key -> df name; Value -> df.
        :return: dict: Key -> df name; Value -> json_dict_from_user_defined_columns.
        """
        user_defined_cols_dict = self.return_user_defined_columns()

        json_dict_user_col = {}
        for df_name, user_cols in user_defined_cols_dict.items():
            csv_dict = {user_col: "" for user_col in user_cols}
            json_dict_user_col[df_name] = csv_dict

        return json_dict_user_col

    def insert_user_column_dict_in_csv(self):
        """
        The method inserts each csv specific version dicts in respective csvs.
        :type columns: object
        :param columns: Specify one of: version, other, all
        :return:
        """
        json_dict_user_col = self.create_json_dict_from_user_defined_columns()

        for filename, df_data in self.df_dict.items():
            for column in JSON_COL_LIST:
                df_data[column] = f"{json_dict_user_col.get(filename)}"

            self.to_csv(df_dict=(filename, df_data))

    def postgresql_conform_columns(self):
        """
        The method corrects columns from csv files to be postgresql conform and saves in same csv.
        :return: Individual df_dict: Key -> df name; Value -> dataframe.
        """

        postgre_conform_dict = {}
        for filename, df_data in self.df_dict.items():

            # column header lowercase
            df_data.columns = df_data.columns.str.strip().str.lower()

            # remove postgresql incompatible characters from csv col-header
            postgresql_conform_to_replace = {
                "/": "_",
                "\\": "_",
                " ": "_",
                "-": "_",
                ":": "_",
            }
            for key, value in postgresql_conform_to_replace.items():
                df_data.columns = [col.replace(key, value) for col in df_data.columns]

            postgre_conform_dict[filename] = df_data

            self.to_csv(df_dict=(filename, df_data))

        return postgre_conform_dict

    def to_dataframe(self):
        """
        Return DataFrame as generator object - use one DataFrame at a the time.
        :return:DataFrame: generator object
        """

        yield from self.df_dict.values()

    def to_csv(self, df_dict=None):
        """
        The method saves a dataframe as csv.
        The df is stored as value in a dict with corresponding df name as key.
        :param df_dict: Key -> df name; Value -> df.
        """
        df_dict[1].to_csv(
            path_or_buf=f"{self.output_dir}/{df_dict[0]}.csv",
            index=False,
            encoding="utf-8",
            sep=";",
        )

    def read_metadata_json(self, path=None) -> object:
        """
        Read jsons.
        :param path: Paths to json file
        :return: JSON file
        """
        with open(path, "r", encoding="utf-8") as file:
            return json.load(file)

    def write_json(self, path: str = None, file=None):
        """
        Write json file
        :param path: Path to json file
        :param file: json file
        :return:
        """
        with open(path, "w", encoding="utf8") as json_file:
            json.dump(file, json_file, ensure_ascii=False)


if __name__ == "__main__":
    INPUT_PATH = "meta_data/input"
    OUTPUT_PATH = "meta_data/output"

    datahelper = Datahelper(input_path=INPUT_PATH, output_path=OUTPUT_PATH)
