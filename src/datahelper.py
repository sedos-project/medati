import pandas as pd
import glob
import os

from datetime import datetime

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

        return {file.split("\\")[-1]: pd.read_csv(filepath_or_buffer=file, sep=";") for file in files}

    def create_json_dict_from_user_defined_columns(self):
        """
        The method reads columns and returns dict with column names as keys and empty value.
        :param df_dict: Key -> df name; Value -> df.
        :return: dict: Key -> df name; Value -> json_dict_from_user_defined_columns.
        """
        user_defined_cols_dict = {
            (df_name): (set(value.columns.tolist()) - set(OEDATAMODEL_COL_LIST))
            for (df_name, value) in self.df_dict.items()
        }

        json_dict_user_col = {}
        for df_name, user_cols in user_defined_cols_dict.items():
            csv_dict = {}
            for user_col in user_cols:
                csv_dict[user_col] = ""

            json_dict_user_col[df_name] = csv_dict

        return json_dict_user_col

    def create_version_dict(self) -> dict:
        """
        The method inserts a version as value for each user-defined column. The version is the date of today.
        :rtype: object
        :return: version_dict:
        """

        json_dict_user_col = self.create_json_dict_from_user_defined_columns()

        df = pd.DataFrame.from_dict(json_dict_user_col, orient="index")

        df.where(df.isna(), (self.now.strftime("%d/%m/%Y")), inplace=True)

        return {k: v.dropna().to_dict() for k, v in df.T.items()}

    def insert_user_column_dict_in_csv(self, columns=None):
        """
        The method inserts each csv specific version dicts in respective csvs.
        :type columns: object
        :param columns: Specify one of: version, other, all
        :return:
        """
        version_dict = self.create_version_dict()
        # todo: rewrite function that it takes arguments 1. version, 2. other - to have only one function that inserts version_json or json_dict_user_col in the other columns
        for index, (filename, df) in enumerate(self.df_dict.items()):
            # get matching-file-version_dict and insert in version column
            # todo: integrate check whether each row is already filled with version or not
            df["version"] = f"{version_dict.get(filename)}"

            self.to_csv(df_dict=(filename, df))


    def to_csv(self, df_dict=None):
        """
        The method saves a dataframe as csv. The df is stored as value in a dict with corresponding df name as key.
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

    return [f for f in glob.glob(f"{directory}/*.csv")]


if __name__ == "__main__":

    OEO_ANNOTATION_DIR = "data/oeo_annotation"

    datahelper = Datahelper()

    print(datahelper.create_json_dict_from_user_defined_columns())
