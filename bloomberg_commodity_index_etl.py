def etl():
    import cloudscraper
    import pandas as pd
    import datetime
    import time
    import logging
    from sqlalchemy import create_engine

    def parse_volume(vol):
        if isinstance(vol, str):
            vol = vol.replace(',', '').strip()
            if vol.endswith('K'):
                return float(vol[:-1]) * 1_000
            elif vol.endswith('M'):
                return float(vol[:-1]) * 1_000_000
            elif vol.endswith('B'):
                return float(vol[:-1]) * 1_000_000_000
            try:
                return float(vol)
            except:
                return None
        return vol

    def generate_date_ranges(start_date, end_date, delta_years=5):
        ranges = []
        current_start = start_date
        while current_start < end_date:
            try:
                current_end = current_start.replace(year=current_start.year + delta_years)
            except ValueError:
                current_end = current_start.replace(month=2, day=28, year=current_start.year + delta_years)
            if current_end > end_date:
                current_end = end_date
            ranges.append((current_start.strftime('%d/%m/%Y'), current_end.strftime('%d/%m/%Y')))
            current_start = current_end + datetime.timedelta(days=1)
        return ranges

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("bloomberg_commodity_index_etl")
    logger.info("Iniciando ETL Bloomberg Commodity Index")

    cookies = {
        # 'cookie_name': 'cookie_value',
    }

    curr_id = '8833'  # Bloomberg Commodity Index (BCOM)
    headers = {
        "User-Agent": "Mozilla/5.0",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://www.investing.com/indices/bloomberg-commodity-historical-data"
    }
    url = 'https://www.investing.com/instruments/HistoricalDataAjax'

    start_date = datetime.datetime.strptime('01/01/1991', '%d/%m/%Y')
    end_date = datetime.datetime.now()
    date_ranges = generate_date_ranges(start_date, end_date, delta_years=5)

    scraper = cloudscraper.create_scraper()
    dfs = []

    for st, en in date_ranges:
        payload = {
            'curr_id': curr_id,
            'st_date': st,
            'end_date': en,
            'interval': 'Daily',
            'sort_col': 'date',
            'sort_ord': 'DESC',
            'action': 'historical_data'
        }
        try:
            response = scraper.post(url, headers=headers, data=payload, cookies=cookies)
            if response.status_code == 200:
                tables = pd.read_html(response.text)
                df = tables[0]
                dfs.append(df)
                logger.info(f'Baixado: {st} até {en}')
                time.sleep(2)
            else:
                logger.error(f'Erro ao acessar o período {st} até {en}: {response.status_code}')
        except Exception as e:
            logger.error(f"Erro ao baixar dados de {st} até {en}: {e}")

    if not dfs:
        logger.error("Nenhum dado foi baixado. Encerrando ETL.")
        raise Exception("Falha no download dos dados.")

    df = pd.concat(dfs, ignore_index=True).drop_duplicates()

    df.rename(columns={
        'Date': 'date',
        'Price': 'close',
        'Open': 'open',
        'High': 'high',
        'Low': 'low',
        'Vol.': 'volume'
    }, inplace=True)

    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    df['volume'] = df['volume'].apply(parse_volume)

    df_monthly = df.groupby(df['date'].dt.to_period('M')).agg({
        'close': 'last',
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'volume': 'sum'
    }).reset_index()
    df_monthly['date'] = df_monthly['date'].dt.to_timestamp()

    logger.info(f"Linhas finais agregadas: {len(df_monthly)}")

    try:
        engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')
        df_monthly.to_sql('bloomberg_commodity_index', engine, if_exists='replace', index=False)
        logger.info("Dados salvos na tabela 'bloomberg_commodity_index' com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao salvar no banco: {e}")
        raise

    logger.info("ETL Bloomberg Commodity Index finalizado com sucesso.")
