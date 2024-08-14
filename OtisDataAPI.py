import requests
import pandas as pd


class OtisDataAPI:
    def __init__(self, base_url, columns_map, nested_keys, save_file_path):
        self.base_url = base_url
        self.save_file_path = save_file_path
        self.headers = {
            "Authorization": "Basic cHl0aG9uLmludDpZZ19AVlFqUC0wU3o7LU9fKzEmWko7MEddMDpsbXpnTSNZRVBLbzJNPjp7aWlfLklDLnBkJmFeVCw0NDFoQXorVGgua1djWmhPdDpPKjNVRzlxTy03ZUprQnViaVNCJjsjS1ZY"
        }
        self.df = None

        self.rename_columns = columns_map
        self.expected_columns = columns_map.values()
        self.nested_keys = nested_keys

    def fetch_data(self, url):
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()['result']

    def download_report(self):
        response = self.fetch_data(self.base_url)
        self.df = pd.DataFrame(response)
        self.df = self.df.rename(columns=self.rename_columns)

    def process_nested_keys(self):
        for idx, row in self.df.iterrows():
            for parent_key, child_keys in self.nested_keys.items():
                if isinstance(row[parent_key], dict) and 'link' in row[parent_key]:
                    additional_data = self.fetch_data(row[parent_key]['link'])
                    for key in child_keys:
                        if key == 'name':
                            self.df.at[idx, parent_key] = additional_data.get(key, None)
                        else:
                            self.df.at[idx, f"{parent_key}.{key}"] = additional_data.get(key, None)

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
        self.download_report()
        self.filter_columns()
        self.process_nested_keys()
        self.save_to_csv()


def main():
    base_url = "https://otisrebuild.service-now.com/api/now/table/cmn_location?sysparm_query=u_regionISNOTEMPTY&sysparm_first_row=1&sysparm_view="

    columns_map = {"u_client_location_code": "Site ID",
                   "name": "Name",
                   "street": "Street",
                   "city": "City name",
                   "state": "State / Province name",
                   "zip": "Zip / Postal Code",
                   "u_country": "Core country",  # call api to get name
                   "u_region": "Region",  # call api to get name
                   "contact": "Contact",  # call api to get name
                   "u_secondary_site_contact": "Secondary contact"  # call api to get name
                   }
    nested_keys = {
        'Core country': ['name'],
        'Region': ['name'],
        'Contact': ['name', 'mobile_phone'],
        'Secondary contact': ['name', 'mobile_phone']
    }
    save_file_path = 'cmn_location_data.csv'
    fetcher = OtisDataAPI(base_url, columns_map, nested_keys, save_file_path)
    fetcher.run()

if __name__ == "__main__":
    main()
