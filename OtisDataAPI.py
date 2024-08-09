import requests
import pandas as pd


class OtisDataAPI:
    def __init__(self, url, expected_columns, save_file_path):
        self.url = url
        self.save_file_path = save_file_path
        self.expected_columns = expected_columns
        self.headers = {
            "Authorization": "Basic cHl0aG9uLmludDpZZ19AVlFqUC0wU3o7LU9fKzEmWko7MEddMDpsbXpnTSNZRVBLbzJNPjp7aWlfLklDLnBkJmFeVCw0NDFoQXorVGgua1djWmhPdDpPKjNVRzlxTy03ZUprQnViaVNCJjsjS1ZY"
        }
        self.df = None

    def fetch_data(self):
        response = requests.get(self.url, headers=self.headers)
        response.raise_for_status()
        data = response.json()['result']
        self.df = pd.DataFrame(data)

    def filter_columns(self):
        if self.df is not None:
            expected_df_columns = [col for col in self.expected_columns if col in self.df.columns]
            if len(expected_df_columns) > 0:
                self.df = self.df[expected_df_columns]
            else:
                raise ValueError(f"Not found expected columns in data. \nDF Columns: {self.df.columns}\nExpected Columns: {self.expected_columns}")
        else:
            raise ValueError("Data not fetched yet. Call fetch_data() first.")

    def save_to_csv(self):
        if self.df is not None:
            self.df.to_csv(self.save_file_path, index=False)
            print(f"Data saved to '{self.save_file_path}'")
        else:
            raise ValueError("Data not fetched yet. Call fetch_data() first.")

    def run(self):
        self.fetch_data()
        self.filter_columns()
        self.save_to_csv()


def main():
    url = "https://otisrebuild.service-now.com/api/now/table/cmn_location?sysparm_query=u_regionISNOTEMPTY&sysparm_first_row=1&sysparm_view="

    expected_columns = [
        'Site ID', 'Name', 'Street', 'City name', 'State / Province name',
        'Zip / Postal Code', 'Core country', 'Region', 'Contact',
        'Contact Mobile phone', 'Secondary contact', 'Secondary contact.Mobile phone'
    ]
    save_file_path = 'cmn_location_data.csv'
    fetcher = OtisDataAPI(url, expected_columns, save_file_path)
    fetcher.run()


if __name__ == "__main__":
    main()
  
