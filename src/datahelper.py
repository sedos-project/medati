"""
The datahelper helps to format input data files and edits metadata.
"""

import sys
import csv
import glob
import os
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
    "version",
    "method",
    "source",
    "comment",
]


class Datahelper:
    """
    The class helps prepare data and metadata files for upload to the OpenEnergyPlatform.


    Methods
    -------
    prepare_df_dict(self, directory: str = None) -> dict
        read all csv's into pd.DataFrame and save them with their filename in a dict
    prepare_json_dict(self, directory: str = None, debug: str = None) -> dict
        read all metadata json and save them with their filename in a dict
    return_user_defined_columns(self) -> dict
        return user defined columns that are neither columns of oedatamodel-parameter scalar or timeseries
    create_json_dict_from_user_defined_columns(self) -> dict
        read columns and return dict with column names as keys and empty value
    insert_user_column_dict_in_csv(self) -> None
        insert each csv specific column dicts in respective csv
    postgresql_conform_columns(self) -> dict
        correct columns from csv files to be postgresql-conform and save in csv
    fill_resources_column_names_with_actual_column_header(self, number_of_datapackages: int = None) -> None
        update metadata information with actual csv column-header information and write into repective metadata json
    combine_dict(self, dict_1: dict, dict_2: dict) -> dict
        merge two dicts even if the keys in the two dictionaries are different
    similar(self, csv_column_header: list, metadata_key: str) -> str
        check the similarity of metadata and new postgresql-conform column headers and match them
    to_dataframe(self) -> object
        return single dataframe from generator
    to_csv(self, df_dict=None) -> None
        save a dataframe as csv
    read_metadata_json(self, path: str = None, debug: str = None) -> object
        read json file
    write_json(self, path: str = None, file=None) -> None
        write json file
    get_files_from_directory(self, directory: str = None, type_of_file: str = "csv") -> list
        take a path as input and return all csv-file or json paths in the directory as a list
    """

    def __init__(
        self, input_path: str = None, output_path: str = None, debug: str = None
    ):
        """
        :param input_path: Specify input path to csv files and metadata json
        :param output_path: Specify output path to csv files and metadata json
        :param debug: None or "json" - only load json files to review their format
        """

        # define paths for csv and oeo_annotation folder
        self.input_dir = os.path.join(os.getcwd(), input_path)
        self.output_dir = os.path.join(os.getcwd(), output_path)

        os.makedirs(self.input_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)

        if debug != "json":
            self.df_dict = self.prepare_df_dict(directory=self.input_dir)
        self.dict_filename_json = self.prepare_json_dict(
            directory=self.input_dir,
            debug=debug,
        )
        self.now = datetime.now()

    def prepare_df_dict(self, directory: str = None) -> dict:
        """
        Read all csv's into pd.DataFrame and save them with their filename in a dict.
        :param directory: Path to csv files
        :return: dict: Key -> filename; Value -> pd.DataFrame
        """
        files: list = self.get_files_from_directory(directory, type_of_file="csv")
        # sep=None, engine='python' will use Python’s builtin sniffer tool to determine the delimiter.
        # This method is a rough heuristic and may produce both false positives and negatives.

        return {
            file.split("\\")[-1].split(".")[0]: pd.read_csv(
                filepath_or_buffer=file, sep=None, engine="python"
            )
            for file in tqdm(files, desc="Load csv-files to DataFrames")
        }

    def prepare_json_dict(self, directory: str = None, debug: str = None) -> dict:
        """
        Read all metadata json and save them with their filename in a dict.
        :param directory: Path to metadata json files
        :param debug: If set to "json" print json in read_metadata_json()
        :return: dict: Key -> filename; Value -> metadata json
        """
        meta_files: list = self.get_files_from_directory(directory, type_of_file="json")
        return {
            meta_file.split("\\")[-1].split(".")[0]: self.read_metadata_json(
                path=meta_file, debug=debug
            )
            for meta_file in tqdm(meta_files, desc="Load Metadata")
        }

    def return_user_defined_columns(self) -> dict:
        """
        Return user-defined columns that are neither columns of oedatamodel-parameter scalar or timeseries.
        :return: dict: Key -> filename: str ; Value -> set of user_defined_columns
        """

        return {
            (df_name): (set(value.columns.tolist()) - set(OEDATAMODEL_COL_LIST))
            for (df_name, value) in self.df_dict.items()
        }

    def create_json_dict_from_user_defined_columns(self) -> dict:
        """
        Read columns and return dict with column names as keys and empty value.
        :return: dict: Key -> filename; Value -> dict of user_defined_columns
        """
        user_defined_cols_dict = self.return_user_defined_columns()

        json_dict_user_col = {}
        for df_name, user_cols in user_defined_cols_dict.items():
            csv_dict = {user_col: "" for user_col in user_cols}
            json_dict_user_col[df_name] = csv_dict

        return json_dict_user_col

    def insert_user_column_dict_in_csv(self) -> None:
        """
        Insert each csv-specific column dicts in respective csv.
        :type columns: object
        :param columns: Specify one of: version, other, all
        :return:
        """
        json_dict_user_col = self.create_json_dict_from_user_defined_columns()

        for filename, df_data in self.df_dict.items():
            for column in JSON_COL_LIST:
                df_data[column] = f"{json_dict_user_col.get(filename)}"

            self.to_csv(df_dict=(filename, df_data))

    def postgresql_conform_columns(self) -> dict:
        """
        Correct columns from csv files to be postgresql conform and save in csv.
        :return: df_dict: Key -> df name; Value -> pd.DataFrame
        """

        postgre_conform_dict = {}
        for filename, df_data in tqdm(
            self.df_dict.items(),
            desc="Make csv-file columns postgresql-conform",
            postfix=f"and save postgresql-conform tables as csv to: '{self.output_dir}'",
        ):

            # column header lowercase
            df_data.columns = df_data.columns.str.strip().str.lower()

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
                df_data.columns = [col.replace(key, value) for col in df_data.columns]

            postgre_conform_dict[filename] = df_data

            self.to_csv(df_dict=(filename, df_data))

        return postgre_conform_dict

    def fill_resources_column_names_with_actual_column_header(
        self, number_of_datapackages: int = None
    ) -> None:
        """
        Update metadata information with actual csv column-header information and write into respective metadata json.
        :return: None
        """
        postgresql_conform_dict = self.postgresql_conform_columns()

        merge_metadata_data = self.combine_dict(
            postgresql_conform_dict, self.dict_filename_json
        )

        if number_of_datapackages != len(merge_metadata_data.items()):
            raise ValueError(
                "The number of datapackages for upload does not match the number of items for internal "
                f"processing. \n At least {len(merge_metadata_data.items())-number_of_datapackages} csv files "
                f"cannot be matched with metadata. Csv and metadata filenames must be identical! \n Ensure the "
                f"parameter `number_of_datapackages` reflects the number of datapackages you want to upload."
            )

        for data_item in tqdm(
            merge_metadata_data.values(),
            desc="Fill resource column names in metadata with "
            "postgresql-conform table headers from respective csv file",
        ):

            csv_column_header = data_item[0].columns

            metadata_user = data_item[1]

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
                        field.name = self.similar(csv_column_header, field.name.lower())
                    except Exception as exc:
                        raise Exception(
                            f"There is a problem in metadata file: {metadata_user['name']}. "
                            f"The metadata key `name` is: {field.name}"
                        ) from exc

            metadata = dialect1_5.compile_and_render(parsed)
            metadata = json.loads(metadata)

            self.write_json(
                path=f"{self.output_dir}/{data_item[1]['name']}.json", file=metadata
            )

    def combine_dict(self, dict_1: dict, dict_2: dict) -> dict:
        """
        This method merges two dicts even if the keys in the two dictionaries are different.
        :param dict_1, dict_2: Two input dicts
        :return: Merged dict
        """
        return {
            k: tuple(d[k] for d in (dict_1, dict_2) if k in d)
            for k in set(dict_1.keys()) | set(dict_2.keys())
        }

    def similar(self, csv_column_header: list, metadata_key: str) -> str:
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

    def to_dataframe(self) -> object:
        """
        Return pd.DataFrame as generator object - use one pd.DataFrame at a the time.
        :return:DataFrame: generator object
        """

        yield from self.df_dict.values()

    def to_csv(self, df_dict=None) -> None:
        """
        Save a pd.DataFrame to csv.
        :param df_dict: Key -> filename; Value -> pd.DataFrame.
        :return None
        """
        df_dict[1].to_csv(
            path_or_buf=f"{self.output_dir}/{df_dict[0]}.csv",
            index=False,
            encoding="utf-8",
            sep=";",
        )

    def read_metadata_json(self, path: str = None, debug: str = None) -> object:
        """
        Read json file.
        :param path: Path to json file
        :param debug: If set to "json" print json
        :return: json file
        """
        with open(path, "r", encoding="utf-8") as file:
            if debug == "json":
                print(file)
            return json.load(file)

    def write_json(self, path: str = None, file=None) -> None:
        """
        Write json file.
        :param path: Path to json file
        :param file: json file
        :return: None
        """
        with open(path, "w", encoding="utf8") as json_file:
            json.dump(file, json_file, ensure_ascii=False)

    def get_files_from_directory(
        self, directory: str = None, type_of_file: str = "csv"
    ) -> list:
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


if __name__ == "__main__":
    INPUT_PATH = "meta_data/input"
    OUTPUT_PATH = "meta_data/output"

    datahelper = Datahelper(input_path=INPUT_PATH, output_path=OUTPUT_PATH)

    datahelper.fill_resources_column_names_with_actual_column_header(
        number_of_datapackages=1
    )
