import os
import pandas as pd
import requests
import shutil
from io import StringIO
import traceback


class CISAKeyDownloader:
    def __init__(self):
        self.cisa_key_url = 'https://www.cisa.gov/sites/default/files/csv/known_exploited_vulnerabilities.csv'
        self.cisa_key_file_path = os.path.join('static', 'cisa_key.csv')
        self.backup_path = self.cisa_key_file_path + ".bak"

    def download_and_save(self):
        """Manage the download and verification of the CSV file."""
        try:
            # Backup existing file
            if os.path.exists(self.cisa_key_file_path):
                shutil.copy2(self.cisa_key_file_path, self.backup_path)

            # download the file
            response = requests.get(self.cisa_key_url)
            response.raise_for_status()  # Check for HTTP errors

            # Validate the downloaded file
            pd.read_csv(StringIO(response.text))

            # save the downloaded file
            with open(self.cisa_key_file_path, 'wb') as file:
                file.write(response.content)

            # validate cisa key file to confirm valid
            pd.read_csv(self.cisa_key_file_path)

            # remove backup file
            if os.path.exists(self.backup_path):
                os.remove(self.backup_path)

            return 1, "File is downloaded and verified as correct."

        except Exception as e:
            print(f'error: {traceback.format_exc()}')

            # restore backup file
            if os.path.exists(self.backup_path):
                shutil.copy2(self.backup_path, self.cisa_key_file_path)
                os.remove(self.backup_path)

            # check is cisa key file exists
            if not os.path.exists(self.cisa_key_file_path):
                return 0, "File did not download, and no existing CSV exists to proceed further."

            return 2, "File did not download, but old CSV is intact to use. Check log for details."

    def update_is_cisa_key(self):
        """Compare CVE IDs between the local data file and the CISA key file."""
        try:
            data_file_path = 'datafile.csv'
            data_df = pd.read_csv(data_file_path)
            cisa_df = pd.read_csv(self.cisa_key_file_path)

            data_df['Cve Ids'] = data_df['Cve Ids'].str.upper()
            cisa_df['CveID'] = cisa_df['CveID'].str.upper()

            data_df['IsCisaKey'] = data_df['Cve Ids'].isin(cisa_df['CveID'])
            return data_df
        except Exception as e:
            print(f"An error occurred while comparing CVE IDs: {e}")
            return None

downloader = CISAKeyDownloader()
status_code, message = downloader.download_and_save()
print(f"Status Code: {status_code}, Message: {message}")
