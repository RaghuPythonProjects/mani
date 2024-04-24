import os
import sys
import math
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
from urllib3.exceptions import InsecureRequestWarning
from inventory_files_config import hi_config

# Disable warnings for insecure requests.
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)


class VulnerabilityReportProcessor:
    def __init__(self, config, download_new_reports=True):
        self.download_new_reports = download_new_reports
        self.input_path = config['paths']['input_path']
        self.output_path = config['paths']['output_path']
        self.merge_files_path = config['paths']['merge_files_path']
        self.report_dict = config['report_dict']
        self.merge_files_dict = config['merge_files_dict']

        self.MAX_SHEET_ROWS = 1048000
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json;charset=UTF-8',
            'Authorization': 'Basic bnhhZG1pbjpjMGNmYTRjNDg3NWM3N2E4NTY1MjdhOWMwZmI5YjUzZg=='
        }

    def filter_to_last_30_days(self, data):
        # Figure out the date 30 days ago
        target_date = datetime.today() - timedelta(days=30)
        # Conditionally drops rows with a test date greater than 45 days old.
        data.drop(data[data['Vulnerability Test Date'] < target_date].index, inplace=True)
        return data

    def filter_to_severity_7(self, data):
        # TODO: Consider removing hard-coded value for severity.
        # NOTE: Experimenting with a boolean value for sorting the vulnerability. We want v3 scores with a value of 0 to use for v2 score fallback.
        data.drop(data[data['Vulnerability CVSS Score'] < 7.0].index, inplace=True)
        return data

    def merge_severity_scores(self, data):
        # NOTE: Built using this strategy https://stackoverflow.com/questions/55498357/update-pandas-column-with-another-columns-values-using-loc
        data['Vulnerability CVSS Score'] = np.where(data['Vulnerability CVSSv3 Score'].ne(0),
                                                    data['Vulnerability CVSSv3 Score'],
                                                    data['Vulnerability CVSS Score'])
        # After the above step, delete the v3 row as it is no longer needed.
        data.drop('Vulnerability CVSSv3 Score', axis=1, inplace=True)
        return data

    def score_to_severity(self, x):
        # In the column immediately after (Column G?)
        # add a string label identifying the vulnerability as a "high" or
        # "critical" severity for easy human readability and metrics sorting.
        if x == 0:
            return "None"
        elif 0.1 <= x <= 3.9:
            return "Low"
        elif 4.0 <= x <= 6.9:
            return "Medium"
        elif 7.0 <= x <= 8.9:
            return "High"
        elif 9.0 <= x <= 10.0:
            return "Critical"
        return ""

    def download_report_to_dataframe(self, report_id, report_name):
        # TODO: Implement date checking.
        # Fetches the latest report for the given id.
        url = f"https://vmo7222pa005.otis.com:3780/api/3/reports/{report_id}/history/latest/output"
        response = requests.request("GET", url, headers=self.headers, verify=False)
        target_filename = os.path.join(self.input_path, report_name + ".csv")
        with open(target_filename, "wb") as file:
            file.write(response.content)
        data = pd.read_csv(target_filename, dtype={
            'Asset IP Address': 'str',
            'Asset Names': 'str',
            'Asset Location': 'str',
            'Vulnerability Title': 'str',
            'Vulnerability CVE IDs': 'str',
            'Vulnerability CVSSv3 Score': np.float64,
            'Vulnerability CVSSv2 Score': np.float64,
            'Vulnerability Risk Score': 'str',
            'Vulnerability Description': 'str',
            'Vulnerability Proof': 'str',
            'Vulnerability Solution': 'str',
            'Asset OS Version': 'str',
            'Asset OS Name': 'str',
            'Asset OS Family': 'str',
            'Vulnerability Age': 'str',
            'Vulnerable Since': 'str',
            'Vulnerability Test Date': 'str',
            'Vulnerability ID': 'str'
        })
        data.fillna('', inplace=True)

        # Convert 'Vulnerability Risk Score' removing commas and converting to float.
        data['Vulnerability Risk Score'] = data['Vulnerability Risk Score'].replace(',', '', regex=True)
        data['Vulnerability Risk Score'] = pd.to_numeric(data['Vulnerability Risk Score'], errors='ignore')

        # Convert date fields from string to datetime.
        data['Vulnerable Since'] = pd.to_datetime(data['Vulnerable Since'], errors='ignore')
        data['Vulnerability Test Date'] = pd.to_datetime(data['Vulnerability Test Date'], errors='ignore')

        return data

    # Takes a dataframe and performs all the typical process steps on it.
    def perform_standard_processing(self, data):
        # Every single file is filtered for the last 30 days
        data = self.filter_to_last_30_days(data)
        data = self.merge_severity_scores(data)
        # New addition: Add a column for severity level (critical, high, etc).
        # Every single filedis filtered to have only CVSSv3 Severity 7 or Higher.
        data = self.filter_to_severity_7(data)
        # The above method destroys the v3 column and overwrites the non-zero v3 values into a single "CVSS score" column. With that
        # created, we then assign the Criticality tags.
        data.insert(loc=6, column='Vulnerability CVSSv3 Severity', value=data['Vulnerability CVSS Score'].apply(self.score_to_severity))
        # Add a column at the end that represents the unique ID of the vulnerability.
        # In order words, a specific instance of a vulnerability on a specific asset.
        # This is a concatenation of the asset name and the vulnerabilityID
        data['Unique Vulnerability ID'] = data['Asset Names'] + ' ' + data['Vulnerability ID']
        data.fillna('', inplace=True)  # Remove any empty entries
        return data

    # Processes the list of files (workstation OS, sever applications, etc.) with the intent of
    # stitching them into a single file.
    def publish_data_into_excel_file_with_sheets(self, filename, sheet_list):
        print("Building Excel file {}...".format(filename))
        # Excel file will be created with static filename
        with pd.ExcelWriter(os.path.join(self.output_path, filename)) as writer:
            for sheet_name, data in sheet_list:
                print(f"Adding sheet {sheet_name} (Count: {len(data.index)}) to file {filename}")
                data.to_excel(writer, sheet_name=sheet_name, index=False)
        print("...finished building {}.\n".format(filename))

    def publish_data_into_excel_file(self, filename, file):
        # Processing an individual file into an excel spreadsheet.
        sheet_name = 'Data'
        print(f"Building Excel file {filename}...")
        with pd.ExcelWriter(os.path.join(self.output_path, filename)) as writer:
            file.to_excel(writer, sheet_name=sheet_name, index=False)
        print("...finished processing {}.\n".format(filename))

    # merge HI - OS and Application files into single Excel File
    def merge_split_files_to_master_excel_file(self):
        for merge_files_set in self.merge_files_dict:
            files_set = merge_files_set['files_set']
            master_file_name = merge_files_set['master_file_name']
            print(f"Creating master file {master_file_name} (with: {files_set})")
            print(merge_files_set)
            with pd.ExcelWriter(os.path.join(self.merge_files_path, master_file_name)) as writer:
                for idx, file_path in enumerate(files_set, start=1):
                    print(f"Read data from file {file_path}")

                    xls = pd.ExcelFile(os.path.join(self.output_path, file_path))
                    for sheet_number, sheet_name in enumerate(xls.sheet_names, start=1):
                        df = pd.read_excel(os.path.join(self.output_path, file_path), sheet_name=sheet_name)
                        new_sheet_name = ('OS' if idx == 1 else 'APP') + (str(sheet_number) if sheet_number > 1 else '')
                        df.to_excel(writer, sheet_name=new_sheet_name, index=False)
                        print(f"add data to sheet {new_sheet_name}")

    def split_dataframe(self, base_filename, base_df):
        num_sheets = math.ceil(len(base_df) / self.MAX_SHEET_ROWS)
        chunked_df = [base_df[i:i + self.MAX_SHEET_ROWS] for i in range(0, len(base_df), self.MAX_SHEET_ROWS)]
        print(f"...calculated {num_sheets} sheets, generating {len(chunked_df)} dataframes...")
        # FIrst sheet is simply "Data", then second sheet is Data 2, then Data 3, etc.
        sheet_list = [("Data" + str(i + 1) if i else "Data", df) for i, df in enumerate(chunked_df)]
        return sheet_list

    def manage_reports(self):
        if self.download_new_reports:
            print("Downloading and processing new reports...")
            self.download_and_process_reports()
        else:
            print("Bypassing download of new reports. Continuing the publication step.")
            # Upload to Sharepoint

    def download_and_process_reports(self):
        for report_id, filename in self.report_dict.items():
            try:
                if filename is None:
                    print(f"ERROR: Unable to determine filename given report ID: {report_id}\nExiting.")
                    sys.exit(0)

                print(f"Generating dataframe for {filename}...")
                # Download the equivalent report to a dataframe
                data = self.download_report_to_dataframe(report_id, filename)
                print(f"...done generating.")

                # Execute standard processing steps (merge severity scores, assign severity labels, etc.)
                data = self.perform_standard_processing(data)

                if data.shape[0]:
                    print(f"ERROR: Data is empty for report ID: {report_id}\nExiting.")
                    sys.exit(0)

                # continue with processing.
                print("Splitting original dataframe into sheets if needed...")
                sheets = self.split_dataframe(filename, data)
                self.publish_data_into_excel_file_with_sheets(filename, sheets)

            except Exception as e:
                print(f"Error processing report {filename}: {str(e)}")
                continue
        self.merge_split_files_to_master_excel_file()
        print("...done.")

    def run(self):
        self.manage_reports()


if __name__ == "__main__":
    download = True
    manager = VulnerabilityReportProcessor(config=hi_config, download_new_reports=download)
    manager.run()

