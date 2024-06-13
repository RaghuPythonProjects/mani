import pandas as pd

class AssetAnalyzer:
    def __init__(self, data_file_path, cisa_file_path, output_file_path, total_assets):
        self.df = pd.read_csv(data_file_path)
        self.cisa_df = pd.read_csv(cisa_file_path)
        self.output_file_path = output_file_path
        self.total_assets = total_assets

    def _filter_older_than_45_days(self):
        self.df['age_temp'] = self.df['age'].str.replace(' Days', '').astype(int)
        self.df_olderthan_45 = self.df[self.df['age_temp'] > 45].drop(columns=['age_temp'])

    def _get_unique_assets_older_than_45_days(self):
        self.df_unique_olderthan_45 = self.df_olderthan_45.drop_duplicates(subset='AssetIPAddress')

    def _get_exploitable_assets_older_than_45_days(self):
        self.exploitable_assetsdf_olderthan_45 = self.df_olderthan_45[self.df_olderthan_45['ExploitCount'] > 0]

    def _get_unique_exploitable_assets_older_than_45_days(self):
        self.unique_exploitable_assetsdf_olderthan_45 = self.exploitable_assetsdf_olderthan_45.drop_duplicates(subset='AssetIPAddress')

    def _get_cisa_assets_older_than_45_days(self):
        self.df_olderthan_45_filtered = self.df_olderthan_45.dropna(subset=['CISA ID'])
        self.cisa_df_olderthan_45 = pd.merge(self.df_olderthan_45_filtered, self.cisa_df,
                                             left_on='CISA ID', right_on='cisa_id', how='inner')

    def _get_unique_exploitable_cisa_assets_older_than_45_days(self):
        self.unique_cisa_df_olderthan_45 = self.cisa_df_olderthan_45[self.cisa_df_olderthan_45['ExploitCount'] > 0]

    def create_summary(self):
        summary_data = {
            'Summary Description': [
                'Assets older than 45 days',
                'Unique assets older than 45 days',
                'Exploitable assets older than 45 days',
                'Unique exploitable assets older than 45 days',
                'CISA assets older than 45 days',
                'Unique CISA assets older than 45 days',
                '',
                '',
                'Total assets count',
                '',
                '',
                'Unique assets older than 45 days percentage',
                'Unique exploitable assets older than 45 days percentage',
                'Unique CISA assets older than 45 days percentage'
            ],
            'Summary Values': [
                len(self.df_olderthan_45),
                len(self.df_unique_olderthan_45),
                len(self.exploitable_assetsdf_olderthan_45),
                len(self.unique_exploitable_assetsdf_olderthan_45),
                len(self.cisa_df_olderthan_45),
                len(self.unique_cisa_df_olderthan_45),
                '',
                '',
                self.total_assets,
                '',
                '',
                int((len(self.df_unique_olderthan_45) / self.total_assets) * 100),
                int((len(self.unique_exploitable_assetsdf_olderthan_45) / self.total_assets) * 100),
                int((len(self.unique_cisa_df_olderthan_45) / self.total_assets) * 100)
            ]
        }
        self.summary_df = pd.DataFrame(summary_data)

    def save_summary_workbook(self):
        with pd.ExcelWriter(self.output_file_path) as writer:
            self.df.to_excel(writer, sheet_name='df', index=False)
            self.df_olderthan_45.to_excel(writer, sheet_name='df_olderthan_45', index=False)
            self.df_unique_olderthan_45.to_excel(writer, sheet_name='df_unique_olderthan_45', index=False)
            self.exploitable_assetsdf_olderthan_45.to_excel(writer, sheet_name='exploitable_assetsdf_olderthan_45', index=False)
            self.unique_exploitable_assetsdf_olderthan_45.to_excel(writer, sheet_name='unique_exploitable_assetsdf_olderthan_45', index=False)
            self.cisa_df_olderthan_45.to_excel(writer, sheet_name='cisa_df_olderthan_45', index=False)
            self.unique_cisa_df_olderthan_45.to_excel(writer, sheet_name='unique_cisa_df_olderthan_45', index=False)
            self.summary_df.to_excel(writer, sheet_name='summary_df', index=False)

    def analyze(self):
        self._filter_older_than_45_days()
        self._get_unique_assets_older_than_45_days()
        self._get_exploitable_assets_older_than_45_days()
        self._get_unique_exploitable_assets_older_than_45_days()
        self._get_cisa_assets_older_than_45_days()
        self._get_unique_exploitable_cisa_assets_older_than_45_days()
        self.create_summary()
        self.save_summary_workbook()


def main():
    data_file_path = 'GIG.csv'
    cisa_file_path = 'CISA.csv'
    output_file_path = 'summary.xlsx'
    total_assets = 100
    analyzer = AssetAnalyzer(data_file_path, cisa_file_path, output_file_path, total_assets)
    analyzer.analyze()


main()

