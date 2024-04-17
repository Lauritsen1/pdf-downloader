import shutil

import pandas as pd
import requests


def main():
    columns = ['BRnum', 'Pdf_URL', 'Report Html Address']
    df = pd.read_csv('data/GRI_2017_2020.csv', usecols=columns)

    url = df['Pdf_URL'][0]
    local_filename = df['BRnum'][0] + '.pdf'

    with requests.get(url, stream=True) as r:
        with open(local_filename, 'wb') as f:
            shutil.copyfileobj(r.raw, f)


if __name__ == '__main__':
    main()
