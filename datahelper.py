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

    def create_json_dict_from_user_defined_columns(self):
        # read columns and make dict with column names as keys and current timestamp as value

        user_defined_cols_dict = {
            (df_name): (set(value.columns.tolist()) - set(self.oedatamodel_col_list))
            for (df_name, value) in self.df_dict.items()
        }

        # todo: Think about working with pd.DataFrame.from_dict here
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

        df.where(df.isna(), (today.strftime("%d/%m/%Y")), inplace=True)

        version_dict = {k: v.dropna().to_dict() for k, v in df.T.items()}

        return version_dict

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

    ### metadata related-methods -> extra class

    def read_annotation_pairs(self):
        """
        The method reads user-defined columns and corresponding annotation columns with annotations.
        :return:
        """
        pass

    def save_annotation_pairs_to_dict(self):
        """
        The method saves concept and annotation pairs as dict.
        :return:
        """
        pass



def get_files_from_directory(directory: str = None) -> list:
    """
    The function takes a path as input and returns all csv-file paths in the directory as a list.
    :rtype: object
    :param directory: csv directory path
    :return: files - list of csv file paths
    """
    files = [f for f in glob.glob(f"{directory}/*.csv")]

    return files


if __name__ == "__main__":

    OEO_ANNOTATION_DIR = "data/oeo_annotation"

    datahelper = Datahelper()

    datahelper.add_empty_colums_next_to_annotable_string_columns()