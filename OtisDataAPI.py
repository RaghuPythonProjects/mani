import requests
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed


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
        print(datetime.now(), 'fetch_data', url)
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()['result']
        except Exception as e:
            print(datetime.now(), f'URL: {url}, ERROR: {e}')
            return {}

    def download_report(self):
        print(datetime.now(), 'download_report')
        response = self.fetch_data(self.base_url)
        self.df = pd.DataFrame(response)
        self.df = self.df.rename(columns=self.rename_columns)

    def process_nested_keys(self):
        print(datetime.now(), 'process_nested_keys')

        for parent_key in self.nested_keys.keys():
            self.df[f"{parent_key}_link"] = self.df[parent_key].map(
                lambda x: x['link'] if isinstance(x, dict) and 'link' in x else None
            )

        for parent_key, child_keys in self.nested_keys.items():
            temp_data = []
            temp_df = self.df[[f"{parent_key}_link"]].dropna().drop_duplicates().reset_index(drop=True)

            urls = [(row[f"{parent_key}_link"], parent_key, child_keys) for _, row in temp_df.iterrows()]

            with ThreadPoolExecutor(max_workers=30) as executor:
                future_to_url = {executor.submit(self.fetch_data, url): (url, parent_key, child_keys) for
                                 url, parent_key, child_keys in urls}
                for future in as_completed(future_to_url):
                    url, parent_key, child_keys = future_to_url[future]
                    try:
                        additional_data = future.result()
                        temp_dict = {'link': url}
                        for i, key in enumerate(child_keys):
                            if i == 0:
                                temp_dict[parent_key] = additional_data.get(key, None)
                            else:
                                temp_dict[f"{parent_key}.{key}"] = additional_data.get(key, None)
                        temp_data.append(temp_dict)
                    except Exception as e:
                        print(f'Error fetching data for URL: {url}, ERROR: {e}')

            temp_df = pd.DataFrame(temp_data)

            if not temp_df.empty:
                self.df = self.df.merge(temp_df, left_on=f"{parent_key}_link",
                                        right_on='link', how='left', suffixes=('', '_fetched'))

                for key in child_keys:
                    fetched_col = f"{parent_key}.{key}" if key != 'name' else parent_key
                    self.df[fetched_col] = self.df[fetched_col].combine_first(self.df[f"{fetched_col}_fetched"])

                columns_to_drop = [
                                      f"{parent_key}.{key}_fetched" if key != 'name' else f"{parent_key}_fetched"
                                      for key in child_keys
                                      if
                                      f"{parent_key}.{key}_fetched" in self.df.columns or f"{parent_key}_fetched" in self.df.columns
                                  ] + ['link']

                self.df.drop(columns=columns_to_drop, inplace=True)

            link_columns_to_drop = [f"{key}_link" for key in self.nested_keys.keys() if
                                    f"{key}_link" in self.df.columns]
            self.df.drop(columns=link_columns_to_drop, inplace=True)


    def filter_columns(self):
        print(datetime.now(), 'filter_columns')
        if self.df is not None:
            expected_df_columns = [col for col in self.expected_columns if col in self.df.columns]
            if len(expected_df_columns) > 0:
                self.df = self.df[expected_df_columns]
            else:
                raise ValueError(
                    f"Not found expected columns in data. \nDF Columns: {self.df.columns}\nExpected Columns: {self.expected_columns}")
        else:
            raise ValueError("Data not fetched yet. Call fetch_data() first.")

    def save_to_csv(self):
        print(datetime.now(), 'save_to_csv')
        if self.df is not None:
            self.df.to_csv(self.save_file_path, index=False)
            print(datetime.now(), f"Data saved to '{self.save_file_path}'")
        else:
            raise ValueError("Data not fetched yet. Call fetch_data() first.")

    def run(self):
        print(datetime.now(), 'run')
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
