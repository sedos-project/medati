"""
Medati helps to format input data files and edits metadata.
"""

import sys
import csv
import glob
import json
from datetime import datetime

from difflib import SequenceMatcher

from omi.oem_structures.oem_v15 import OEPMetadata
from omi.dialects.oep.dialect import OEP_V_1_5_Dialect

from tqdm import tqdm

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
    "method",
    "source",
    "comment",
]


class Medati:
    """
    The class helps prepare data and metadata files for upload to the OpenEnergyPlatform.


    Methods
    -------

    _return_user_defined_columns(self) -> dict
        return user defined columns that are neither columns of oedatamodel-parameter scalar or timeseries
    create_json_dict_from_user_defined_columns(self) -> dict
        read columns and return dict with column names as keys and empty value
    insert_user_column_dict_in_csv_based_on_oedatamodel_parameter(self) -> None
        insert each csv specific column dicts in respective csv
    make_csv_columns_postgresql_conform(self) -> dict
        correct columns from csv files to be postgresql-conform and save in csv
    update_oemetadata_schema_fields_name_from_csv_using_similarity(self) -> None
        update metadata information with actual csv column-header information and write into repective metadata json

    """

    def __init__(self, dataframe: pd.DataFrame = None, metadata: dict = None):
        """
        :param dataframe: Specify csv file
        :param metadata: Specify metadata json
        """

        # define paths for csv and oeo_annotation folder
        if isinstance(dataframe, pd.DataFrame):
            self.dataframe = dataframe
        else:
            raise TypeError("'dataframe' has to be type: pd.DataFrame")
        if isinstance(metadata, dict):
            self.metadata = metadata
        else:
            raise TypeError("'metadata' has to be type: dict")

        self.now = datetime.now()

    def _return_user_defined_columns(self) -> dict:
        """
        Return user-defined columns that are neither columns of oedatamodel-parameter scalar or timeseries.
        :return: dict: Key -> filename: str ; Value -> set of user_defined_columns
        """

        return {
            "custom_columns": (
                set(self.dataframe.columns.tolist()) - set(OEDATAMODEL_COL_LIST)
            )
        }

    def create_json_dict_from_user_defined_columns(self) -> dict:
        """
        Read columns and return dict with column names as keys and empty value.
        :return: dict: Key -> filename; Value -> dict of user_defined_columns
        """
        user_defined_cols_dict = self._return_user_defined_columns()

        json_dict_user_col = {}
        for df_name, user_cols in user_defined_cols_dict.items():
            csv_dict = {user_col: "" for user_col in user_cols}
            json_dict_user_col[df_name] = csv_dict

        return json_dict_user_col

    def insert_user_column_dict_in_csv_based_on_oedatamodel_parameter(self) -> None:
        """
        Insert each csv-specific column dicts in respective csv.
        :type columns: object
        :param columns: Specify one of: version, other, all
        :return:
        """
        json_dict_user_col = self.create_json_dict_from_user_defined_columns()

        for column in JSON_COL_LIST:
            self.dataframe[f"{column}"] = f"{json_dict_user_col['custom_columns']}"

    def make_csv_columns_postgresql_conform(self) -> dict:
        """
        Correct columns from csv files to be postgresql conform and save in csv.
        :return: df_dict: Key -> df name; Value -> pd.DataFrame
        """

        # column header lowercase
        self.dataframe.columns = self.dataframe.columns.str.strip().str.lower()

        # remove postgresql incompatible characters from csv col-header
        postgresql_conform_to_replace = {
            "/": "_",
            "\\": "_",
            " ": "_",
            "-": "_",
            ":": "_",
            ",": "_",
            ".": "_",
            "+": "_",
            "%": "_",
            "!": "_",
            "?": "_",
            "(": "_",
            ")": "_",
            "[": "_",
            "]": "_",
            "}": "_",
            "{": "_",
            "ß": "ss",
            "ä": "ae",
            "ö": "oe",
            "ü": "ue",
        }

        for key, value in postgresql_conform_to_replace.items():
            self.dataframe.columns = [
                col.replace(key, value) for col in self.dataframe.columns
            ]

    def update_oemetadata_schema_fields_name_from_csv_using_similarity(self) -> None:
        """
        Update metadata information with actual csv column-header information and write into respective metadata json.
        :return: None
        """

        # make column header postgresql conform
        self.make_csv_columns_postgresql_conform()

        csv_column_header = self.dataframe.columns

        metadata_user = self.metadata

        # omi
        # metadata 1.5 instance and parse metadata_user in python dict
        dialect1_5 = OEP_V_1_5_Dialect()
        # parse metadata_user in python dict
        parsed: OEPMetadata = dialect1_5._parser().parse(  # pylint: disable=W0212
            metadata_user
        )

        # similarity isn't case-agnostic. field.name.lower() -> to enable string comparison on lowercase
        for ressource in parsed.resources:
            for field in ressource.schema.fields:
                try:
                    field.name = self._similar(csv_column_header, field.name.lower())
                except Exception as exc:
                    raise Exception(
                        f"There is a problem in metadata file: {metadata_user['name']}. "
                        f"The metadata key `name` is: {field.name}"
                    ) from exc

        metadata = dialect1_5.compile_and_render(parsed)
        metadata = json.loads(metadata)

        self.metadata = metadata

        return metadata

    def _similar(self, csv_column_header: list, metadata_key: str) -> str:
        """
        Check the similarity of metadata and new postgresql-conform column headers and match them. Return the
        postgresql-conform column name.
        :param csv_column_header: list of csv column headers, after postgresql-conform correction
        :param metadata_key: metadata column key from metadata file
        :return: postgresql-conform column name
        """
        similarity_criteria = 0.8
        sim_dict = {}
        for csv_header in csv_column_header:
            sim_value = SequenceMatcher(None, csv_header, metadata_key).ratio()
            sim_dict[(csv_header, metadata_key)] = sim_value
            if sim_value == 1:
                break

        if max(sim_dict.values()) >= similarity_criteria:
            return max(sim_dict, key=sim_dict.get)[0]

        raise ValueError(
            f"Your metadata column name: {metadata_key} - has no similarity with the postgresql-conform "
            f"corrected csv column headers {csv_column_header}\n"
            f"Postgresql-conform corrected csv column headers cannot be inserted into metadata, due to missing "
            f"match, please check manually if the column is present in your metadata.\n"
            f"Similarity below {similarity_criteria}: {sim_dict}"
        )


def read_metadata_json(path: str = None) -> object:
    """
    Read json file.
    :param path: Path to json file
    :return: json file
    """
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


def write_json(path: str = None, file=None) -> None:
    """
    Write json file.
    :param path: Path to json file
    :param file: json file
    :return: None
    """
    with open(path, "w", encoding="utf8") as json_file:
        json.dump(file, json_file, ensure_ascii=False)


def get_files_from_directory(directory: str = None, type_of_file: str = "csv") -> list:
    """
    Take path as input and return all csv-file or json paths in the directory as a list.
    :param directory: Path to directory
    :param type_of_file: Specify whether csv or json file paths will be returned
    :return: file paths
    """

    if type_of_file == "csv":
        return list(glob.glob(f"{directory}/*.csv"))
    if type_of_file == "json":
        return list(glob.glob(f"{directory}/*.json"))

    return None


def combine_dict(dict_1: dict, dict_2: dict) -> dict:
    """
    This method merges two dicts even if the keys in the two dictionaries are different.
    :param dict_1, dict_2: Two input dicts
    :return: Merged dict
    """
    return {
        k: tuple(d[k] for d in (dict_1, dict_2) if k in d)
        for k in set(dict_1.keys()) | set(dict_2.keys())
    }


def prepare_df_dict(directory: str = None) -> dict:
    """
    Read all csv's into pd.DataFrame and save them with their filename in a dict.
    :param directory: Path to csv files
    :return: dict: Key -> filename; Value -> pd.DataFrame
    """
    files: list = get_files_from_directory(directory, type_of_file="csv")
    # sep=None, engine='python' will use Python’s builtin sniffer tool to determine the delimiter.
    # This method is a rough heuristic and may produce both false positives and negatives.

    return {
        file.split("\\")[-1].split(".")[0]: pd.read_csv(
            filepath_or_buffer=file, sep=None, engine="python"
        )
        for file in tqdm(files, desc="Load csv-files to DataFrames")
    }


def prepare_json_dict(directory: str = None) -> dict:
    """
    Read all metadata json and save them with their filename in a dict.
    :param directory: Path to metadata json files
    :return: dict: Key -> filename; Value -> metadata json
    """
    meta_files: list = get_files_from_directory(directory, type_of_file="json")
    return {
        meta_file.split("\\")[-1].split(".")[0]: read_metadata_json(path=meta_file)
        for meta_file in tqdm(meta_files, desc="Load Metadata")
    }


if __name__ == "__main__":

    # script to run on your local machine
    INPUT_PATH = "meta_data/input"
    OUTPUT_PATH = "meta_data/output/test"

    data_metadata_dict = combine_dict(
        prepare_df_dict(directory=INPUT_PATH), prepare_json_dict(directory=INPUT_PATH)
    )

    for table_name, tuple_data_metadata in data_metadata_dict.items():
        df = tuple_data_metadata[0]
        meta = tuple_data_metadata[1]

        medati = Medati(dataframe=df, metadata=meta)

        # update metadata
        medati.update_oemetadata_schema_fields_name_from_csv_using_similarity()
        # insert user-col dict in df
        medati.insert_user_column_dict_in_csv_based_on_oedatamodel_parameter()

        # save df to csv

        print(f"Save {table_name}.csv in:{OUTPUT_PATH}")
        medati.dataframe.to_csv(
            path_or_buf=f"{OUTPUT_PATH}/{table_name}.csv",
            index=False,
            encoding="utf-8",
            sep=";",
        )

        # save updated metadata
        print(f"Save {table_name}.json in:{OUTPUT_PATH}")
        write_json(path=f"{OUTPUT_PATH}/{table_name}.json", file=medati.metadata)
