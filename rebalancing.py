from download_wasabi import WasabiVictorTool
from datetime import datetime
import pandas as pd
import os

def main_wasabi():
    params_init = {
        'bucket_name': 'indices-data',
        'end_point_url': 'https://s3.us-east-2.wasabisys.com',
        'aws_arn': 'iam::731234585745:user/steve.moses'
    }
    
    path = '/Users/steve/Documents/GitHub/kaiko-storage-download-tool/database_wasabi_mfa'  # Update to your path

    tickers = input("Enter a list of tickers separated by a comma (e.g. 'kk_rr_btcusd,kk_rr_ethusd'): ").split(',')
    input_dates = input("Enter a list of dates separated by a comma (e.g. '2023-01-01 00:00:00,2023-01-02 00:00:00'): ").split(',')
    dates = [datetime.strptime(date.strip(), '%Y-%m-%d %H:%M:%S') for date in input_dates]

    output_file = input("Enter the name of the output CSV file (e.g. output.csv): ")

    tool = WasabiVictorTool(**params_init)

    result_df = pd.DataFrame(columns=[''] + [date.strftime('%Y-%m-%d %H:%M:%S') for date in dates])

    for ticker in tickers:
        ticker = ticker.strip()
        if ticker.endswith(('nyc', 'ldn', 'sgp')):
            folder = f"index/v1/{ticker}/index_fixing"
            type = 'index_fixing'
        else:
            folder = f"index/v1/{ticker}/real_time"
            type = 'real_time'

        ticker_prices = [ticker]

        for date in dates:
            wasabi_folder = f"{folder}/{date.year}/{date.strftime('%m')}/"
            temp_file_name = f'all_files_in_{wasabi_folder.split("/")[-1]}.txt'
            tool.store_file_names_subfolder(wasabi_subfolder_name=wasabi_folder,
                                            download_to_file_dir=temp_file_name)

            file_to_download = f"{wasabi_folder}{ticker}_{type}_{date.strftime('%Y-%m-%d')}.csv.gz"
            params_download = {
                'download_to_dir': 'database_wasabi_mfa',
                'file_type': 'csv.gz',
            }
            tool.download_single_file(file_to_download, **params_download)

            download_target_file_dir = os.path.join(path, f"{file_to_download[:-3]}")
            temp_df = pd.read_csv(download_target_file_dir, parse_dates=['intervalStart', 'intervalEnd'])
            temp_df['intervalEnd'] = temp_df['intervalEnd'].dt.strftime('%Y-%m-%d %H:%M:%S')


            matching_row = temp_df[temp_df['intervalEnd'] == date.strftime('%Y-%m-%d %H:%M:%S')].iloc[0]
            price = matching_row['price']
            ticker_prices.append(price)



            #os.remove(download_target_file_dir)     # Uncomment to delete the downloaded CSV files and keep only the final output csv

            os.remove(temp_file_name)


        result_df = pd.concat([result_df, pd.DataFrame([ticker_prices], columns=result_df.columns)], ignore_index=True)


    result_df.to_csv(output_file, index=False)

if __name__ == '__main__':
    main_wasabi()
