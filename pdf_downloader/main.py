import os

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


def main():
    columns = ['BRnum', 'Pdf_URL', 'Report Html Address']

    df = pd.read_csv('data/GRI_2017_2020.csv', usecols=columns)
    row = df.iloc[1]

    url = row['Pdf_URL']
    filename = f'{row["BRnum"]}.pdf'
    output_dir = 'downloads'

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    download_file(url, output_dir, filename)


if __name__ == '__main__':
    main()
