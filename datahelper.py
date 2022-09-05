import pandas as pd
import glob
import os

from datetime import date

class Datahelper:
    """
    The class helps ontologically annotating input data files and creating metadata.
    """

    def __init__(self):

        self.oedatamodel_col_list = [
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
        self.json_col_list = [
            "bandwidth_type",
            "version",
            "method",
            "source",
            "comment",
        ]

        # define paths for csv and oeo_annotation folder
        self.csv_dir = os.path.join(os.getcwd(), "data", "input")
        self.oeo_annotation_path = os.path.join(os.getcwd(), "data", "oeo_annotation")

        os.makedirs(self.oeo_annotation_path, exist_ok=True)

        self.df_dict = self.prepare_df_dict(directory=self.csv_dir)

        def prepare_df_dict(self, directory: str = None) -> dict:
            """
            The method reads all csv's into pandas dataframes and saves them with their names in a dict.
            :param directory: Path to csv files.
            :return: df_dict: Key -> df name; Value -> df.
            """
            files: list = get_files_from_directory(directory)

            df_list = [pd.read_csv(filepath_or_buffer=file, sep=";") for file in files]

            # zip only filename from path and dataframe
            df_dict = dict(zip([file.split("\\")[-1] for file in files], df_list))

            return df_dict

        def add_empty_colums_next_to_annotable_string_columns(self):
            """
            The method creates an empty column next to each user-defined annotatable column.
            :param csv:
            :return:
            """

            # add empty columns next to columns that at least one string
            for filename, df in self.df_dict.items():
                # offset index for column insertion to right for each added column
                offset_index = 0

                for col_index, col_header in enumerate(df.columns):

                    if (
                            isinstance(df.dtypes[col_header], object)
                            and col_header not in self.oedatamodel_col_list
                    ):
                        # TODO: Currently, the if-check only checks for objects.
                        #       [1,2,3] & Europe -> object
                        #       np.nan -> float64
                        #       Implement a if-check that only creates empty cols next to cols that contain strings such as "Europe" and not [1,2,3], which is used as bandwidth and there is nothing to annotate
                        df.insert(
                            loc=col_index + 1 + offset_index,
                            column=col_header + "_isAbout",
                            value="valueReference",
                        )
                        # offset index for column insertion to right for each added column, increase by 1 for each column
                        offset_index += 1

                self.to_csv(df_dict=(filename, df))

    def to_csv(self, df_dict=None):
        """
        The method saves a dataframe as csv. The df is stored as value in a dict with corresponding df name as key.
        :param df_dict:
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
    files = [f for f in glob.glob(f"{directory}/*.csv")]

    return files