"""
The datahelper helps to format input data files.
"""

import glob
import os
from datetime import datetime

import pandas as pd

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


class Datahelper:
    """
    The class helps ontologically annotating input data files and creating metadata.
    """

    def __init__(self):

        # define paths for csv and oeo_annotation folder
        self.csv_dir = os.path.join(os.getcwd(), "data", "input")
        self.oeo_annotation_path = os.path.join(os.getcwd(), "data", "oeo_annotation")

        os.makedirs(self.oeo_annotation_path, exist_ok=True)

        self.df_dict = self.prepare_df_dict(directory=self.csv_dir)

        self.now = datetime.now()

    def prepare_df_dict(self, directory: str = None) -> dict:
        """
        The method reads all csv's into pandas dataframes and saves them with their names in a dict.
        :param directory: Path to csv files.
        :return: df_dict: Key -> df name; Value -> df.
        """
        files: list = get_files_from_directory(directory)

        return {
            file.split("\\")[-1]: pd.read_csv(filepath_or_buffer=file, sep=";")
            for file in files
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

    def to_csv(self, df_dict=None):
        """
        The method saves a dataframe as csv.
        The df is stored as value in a dict with corresponding df name as key.
        :param df_dict: Key -> df name; Value -> df.
        :return:
        """
        df_dict[1].to_csv(
            path_or_buf=f"{self.oeo_annotation_path}/{df_dict[0]}",
            index=False,
            encoding="utf-8",
            sep=";",
        )


def get_files_from_directory(directory: str = None) -> list:
    """
    The function takes a path as input and returns all csv-file paths in the directory as a list.
    :rtype: object
    :param directory: csv directory path
    :return: files - list of csv file paths
    """

    return list(glob.glob(f"{directory}/*.csv"))


if __name__ == "__main__":

    OEO_ANNOTATION_DIR = "data/oeo_annotation"

    datahelper = Datahelper()

    print(datahelper.create_json_dict_from_user_defined_columns())

    datahelper.insert_user_column_dict_in_csv()
