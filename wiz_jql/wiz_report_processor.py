import pandas as pd
from wiz_sdk import WizAPIClient
from config import WIZ_ENV, WIZ_CLIENT_ID, WIZ_CLIENT_SECRET, WIZ_API_PROXY, PROJECT_ID, OUTPUT_CSV, QUERY_PATH

class WizReportProcessor:
    def __init__(self):
        self.query = self.load_query(QUERY_PATH)
        self.client = WizAPIClient(conf={
            'wiz_env': WIZ_ENV,
            'wiz_client_id': WIZ_CLIENT_ID,
            'wiz_client_secret': WIZ_CLIENT_SECRET,
            'wiz_api_proxy': WIZ_API_PROXY
        })
        self.variables = {
            "first": 20,
            "after": None,
            "filterBy": {
                "projectId": PROJECT_ID
            }
        }
        self.all_reports = []

    def load_query(self, query_file):
        with open(query_file, 'r') as file:
            return file.read()

    def fetch_reports(self):
        results = self.client.query(self.query, self.variables)
        if 'errors' in results:
            raise Exception(f"Error querying the Wiz API: {results['errors']}")
        return results

    def flatten_dict(self, d, parent_key='', sep='_'):
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self.flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                items.append((new_key, v))
            else:
                items.append((new_key, v))
        return dict(items)

    def ensure_list_values(self, df):
        for col in df.columns:
            if any(isinstance(i, list) for i in df[col]):
                df[col] = df[col].apply(lambda x: x if isinstance(x, list) else [x])
        return df

    def explode_lists(self, df):
        list_columns = [col for col in df.columns if any(isinstance(i, list) for i in df[col])]
        for col in list_columns:
            df = df.explode(col)
        return df

    def flatten_report(self, report):
        return self.flatten_dict(report)

    def run(self):
        while True:
            try:
                results = self.fetch_reports()
                reports = results.data['reports']['nodes']
                page_info = results.data['reports']['pageInfo']
                self.all_reports.extend([self.flatten_report(report) for report in reports])

                if not page_info['hasNextPage']:
                    break

                self.variables['after'] = page_info['endCursor']
            except Exception as e:
                print(f"An error occurred: {e}")
                break

        df = pd.DataFrame(self.all_reports)
        df = self.ensure_list_values(df)
        df = self.explode_lists(df)
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"Reports saved to {OUTPUT_CSV}")


if __name__ == '__main__':
    processor = WizReportProcessor()
    processor.run()

