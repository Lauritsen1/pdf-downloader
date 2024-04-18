import os
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import pandas as pd
import requests


def download_file(url: str, output_dir: str, filename: str) -> None:
    filepath = os.path.join(output_dir, filename)
    response = requests.get(url, stream=True)

    if response.status_code == 200:
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)


def download_batch(
    df: pd.DataFrame, output_dir: str, volume: Optional[int] = None
) -> None:
    if volume is not None:
        df = df.head(volume)

    with ThreadPoolExecutor(max_workers=5) as executor:
        for _, row in df.iterrows():
            executor.submit(
                download_file, row['Pdf_URL'], output_dir, f'{row["BRnum"]}.pdf'
            )


def main():
    columns = ['BRnum', 'Pdf_URL', 'Report Html Address']

    df = pd.read_csv('data/GRI_2017_2020.csv', usecols=columns)

    output_dir = 'downloads'
    volume = 10

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    download_batch(df, output_dir, volume)


if __name__ == '__main__':
    main()
