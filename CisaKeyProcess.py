import os
import pandas as pd

from CISAKeyDownloader import CISAKeyDownloader


class CisaKeyProcess:
    def __init__(self,
                 data_df: pd.DataFrame = None,
                 data_file_path: str = None,
                 cisa_kev_df: pd.DataFrame = None,
                 cisa_kev_file_path: str = None,
                 download_cisa_kev: bool = None):

        self.data_df = data_df
        self.data_file_path = data_file_path
        self.cisa_kev_df = cisa_kev_df
        self.cisa_kev_file_path = cisa_kev_file_path
        self.download_cisa_kev = download_cisa_kev
        self.load_data()

    def read_file(self, file_path):
        if os.path.exists(file_path):
            if file_path.lower().endswith('.xlsx'):
                return pd.read_excel(file_path)
            elif file_path.lower().endswith('.csv'):
                return pd.read_csv(file_path)
        return None

    def load_data(self):
        if not self.data_df and self.data_file_path:
            self.data_df = self.read_file(self.data_file_path)

    def load_cisa_kev(self):
        if not self.cisa_kev_df and self.cisa_kev_file_path:
            self.data_df = self.read_file(self.cisa_kev_file_path)

        elif not self.cisa_kev_df and self.download_cisa_kev:
            downloader = CISAKeyDownloader()
            status_code, message = downloader.download_and_save()
            if status_code > 0:
                self.cisa_kev_df = self.read_file(downloader.cisa_kev_file_path)

    def update_is_cisa_kev(self):
        if (isinstance(self.data_df, pd.DataFrame) and len(self.data_df) > 0 and
                isinstance(self.cisa_kev_df, pd.DataFrame) and len(self.cisa_kev_df) > 0):
            pass
        return self.data_df

    # check this function required or not at the end
    def update_cisa_kev_column_position(self):
        if (isinstance(self.data_df, pd.DataFrame) and len(self.data_df) > 0 and
                isinstance(self.cisa_kev_df, pd.DataFrame) and len(self.cisa_kev_df) > 0):
            df_columns = self.data_df.columns.tolist()
            cisa_columns = ['Vulnerability CVSS Score', 'Vulnerability CVSSv3 Severity', 'CisaKev']

            if 'Vulnerability CVSS Score' in df_columns:
                # find first column
                cvss_column_position = df_columns.index('Vulnerability CVSS Score')
                # remove columns
                for col in cisa_columns:
                    df_columns.remove(col)
                # insert columns
                for col in reversed(cisa_columns):
                    df_columns.insert(cvss_column_position, col)
                # set columns
                self.data_df = self.data_df[df_columns]
        return self.data_df

    def run(self):
        self.load_data()
        self.load_cisa_kev()
        self.update_is_cisa_kev()
        self.update_cisa_kev_column_position()
