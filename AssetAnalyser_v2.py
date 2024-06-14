import pandas as pd

class AssetAnalyzer:
    def __init__(self, input_file_path, output_file_path, categories):
        self.df = pd.read_csv(input_file_path)
        self.cisa_df = pd.read_csv('cisa_file_path.csv')
        self.categories = categories
        self.output_file_path = output_file_path
        self.summary_data = []
        self.overall_assets = 0

    def analyze_category(self, cat_dict=None):
        if cat_dict:
            category_name = cat_dict['category']
            category_name_s = cat_dict['shortname']
            total_assets = cat_dict['count']
            self.overall_assets += total_assets
            filtered_df = self.df[self.df['category'] == category_name]
        else:
            category_name = 'Overall'
            category_name_s = 'ALL'
            filtered_df = self.df
            total_assets = self.overall_assets

        df_olderthan_45 = filtered_df
        df_unique_olderthan_45 = df_olderthan_45.drop_duplicates(subset='AssetIPAddress')

        exploitable_assetsdf_olderthan_45 = df_olderthan_45[df_olderthan_45['IsExploit'] == True]
        unique_exploitable_assetsdf_olderthan_45 = exploitable_assetsdf_olderthan_45.drop_duplicates(subset='AssetIPAddress')

        cisa_df_olderthan_45 = df_olderthan_45[df_olderthan_45['IsCISA'] == True]
        unique_cisa_df_olderthan_45 = cisa_df_olderthan_45[cisa_df_olderthan_45['ExploitCount'] > 0]

        summary_data = {
            'Summary Description': [
                'Category',
                'Total assets',
                'Vulnerable',
                'Exploitable',
                'KEY',
                'Vulnerable%',
                'Exploitable%',
                'KEY%',
                '', ''
            ],
            'Summary Values': [
                category_name,
                total_assets,
                len(df_unique_olderthan_45),
                len(unique_exploitable_assetsdf_olderthan_45),
                len(unique_cisa_df_olderthan_45),
                int((len(df_unique_olderthan_45) / total_assets) * 100),
                int((len(unique_exploitable_assetsdf_olderthan_45) / total_assets) * 100),
                int((len(unique_cisa_df_olderthan_45) / total_assets) * 100),
                '', ''
            ]
        }

        self.summary_data.append(pd.DataFrame(summary_data))

        return {
            f'{category_name_s}_data': df_olderthan_45,
            f'{category_name_s}_vol': df_unique_olderthan_45,
            f'{category_name_s}_expl': exploitable_assetsdf_olderthan_45,
            f'{category_name_s}_expl_unq': unique_exploitable_assetsdf_olderthan_45,
            f'{category_name_s}_cisa': cisa_df_olderthan_45,
            f'{category_name_s}_cisa_unq': unique_cisa_df_olderthan_45,
        }

    def analyze_all_categories(self):
        all_dataframes = {}
        for cat_dict in self.categories:
            print(f'processing ...... {str(cat_dict)}')
            dataframes = self.analyze_category(cat_dict)
            all_dataframes.update(dataframes)
        print(f'processing ...... overall')
        overall_dataframes = self.analyze_category()
        all_dataframes.update(overall_dataframes)

        summary_df = pd.concat(self.summary_data, ignore_index=True)
        print(f'saving summary file')
        with pd.ExcelWriter(self.output_file_path, engine='openpyxl') as writer:
            for name, df in all_dataframes.items():
                df.to_excel(writer, sheet_name=name, index=False)

            summary_df.to_excel(writer, sheet_name='counts', index=False)


process_list = [
    {'input_file_path': 'path_to_main_file.csv',
     'output_file_path': 'summary_workbook.xlsx',
     'categories': [
         {'category': 'cat1', 'count': 10, 'shortname': 'C1'},
         {'category': 'cat2', 'count': 20, 'shortname': 'C2'},
         {'category': 'cat3', 'count': 30, 'shortname': 'C3'}
     ]}
]

for process_set in process_list:
    analyzer = AssetAnalyzer(**process_set)
    analyzer.analyze_all_categories()
