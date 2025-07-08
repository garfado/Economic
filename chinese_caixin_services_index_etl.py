import pandas as pd
import requests
import re
import datetime
import logging
import os
from io import StringIO

def main():
    logging.basicConfig(level=logging.INFO)
    url = 'https://br.investing.com/economic-calendar/chinese-caixin-services-pmi-596'
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers)
    logging.info(f"Status code da resposta: {response.status_code}")

    # Loga o início do HTML para debug
    logging.info(f"Primeiros 500 caracteres do HTML retornado:\n{response.text[:500]}")

    try:
        # Utiliza StringIO conforme recomendação do pandas (evita FutureWarning)
        tables = pd.read_html(StringIO(response.text))
        if not tables:
            logging.error("Nenhuma tabela encontrada no HTML!")
            return
    except ValueError as e:
        logging.error(f"Nenhuma tabela encontrada no HTML! Erro: {str(e)}")
        return

    df = tables[0].rename(columns={
        'Lançamento': 'date',
        'Atual': 'actual_state',
        'Anterior': 'close',
        'Projeção': 'forecast'
    })

    df['date'] = df['date'].astype(str)
    df['date'] = df['date'].apply(lambda x: re.search(r'(\d{2}\.\d{2}\.\d{4})', x))
    df['date'] = df['date'].apply(lambda x: x.group(1) if x else None)
    df = df.dropna(subset=['date'])
    df['date'] = pd.to_datetime(df['date'], format='%d.%m.%Y')
    start_date = datetime.datetime.strptime('01/01/2012', '%d/%m/%Y')
    df = df[df['date'] >= start_date]
    df_final = df[['date', 'actual_state', 'close', 'forecast']].sort_values('date')
    output_path = os.getenv("OUTPUT_PATH", "/opt/airflow/downloads/caixin_services_index_2012_2025.csv")
    df_final.to_csv(output_path, index=False)
    logging.info(f'Arquivo salvo como {output_path}')

if __name__ == "__main__":
    main()
