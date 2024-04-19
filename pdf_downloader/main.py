import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import pandas as pd
import requests
import validators

def download_file(url: str, output_dir: str, filename: str):
    df = pd.read_csv('data/Metadata2006_2016.csv')
    filepath = os.path.join(output_dir, filename)

    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(filepath, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

        df.loc[df['BRnum'] == filename[:-4], 'pdf_downloaded'] = 'yes'
        
        print(f'{filename} downloaded')

    except Exception as e:
        print(f'{filename} failed')



def download_batch(
    df: pd.DataFrame, output_dir: str, volume: Optional[int] = None
) -> None:
    
    volume = volume or len(df)

    with ThreadPoolExecutor(max_workers=5) as executor:
        for index, row in df.head(volume).iterrows():
            if not pd.isnull(row['Pdf_URL']) or validators.url(row['Pdf_URL']):
                executor.submit(download_file, row['Pdf_URL'], output_dir, row['BRnum'] + '.pdf')
            elif not pd.isnull(row['Report Html Address']) or validators.url(row['Report Html Address']):
                executor.submit(download_file, row['Report Html Address'], output_dir, row['BRnum'] + '.pdf')
            else:
                continue


def main():
    columns = ['BRnum', 'Pdf_URL', 'Report Html Address']

    df = pd.read_csv('data/GRI_2017_2020.csv', usecols=columns)

    output_dir = 'downloads'
    volume = 10

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    t1 = time.perf_counter()

    download_batch(df, output_dir, volume)

    t2 = time.perf_counter()
    print(t2 - t1)


if __name__ == '__main__':
    main()
