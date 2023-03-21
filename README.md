# Medati (Metadata and Datamodel Utils)

The class helps prepare data and metadata files for upload to the OpenEnergyPlatform.


**Use with oedatamodel-parameter for:**

* create dict with user-defined columns
* insert dict with user-defined columns into oedatamodel-parameter columns "method", "source", "bandwidth_type", "comment"
* make column header postgresql-conform
* update OEMetadata field names, with postgresql-conform column names

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
