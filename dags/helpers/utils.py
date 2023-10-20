import os
from helpers.logging import get_logger

logger = get_logger()


def get_tmp_path_files(file_name):
    return f"{os.path.abspath(os.path.dirname(file_name))}/tmp_files/"


def save_df_file(path, filen_name, df):
    os.makedirs(path, mode=0o0777, exist_ok=True)
    file_path = f"{path}/{filen_name}"
    df.to_pickle(file_path)
    os.chmod(file_path, 0o0777)
    logger.info(f"File saved on: {file_path}")
