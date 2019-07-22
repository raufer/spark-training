import random
import uuid
import string
import datetime

from pyspark.sql import HiveSession
import pyspark.sql.types as T


def deleteTableContracts(hc, json_config):
    db = json_config.get("database").get("name")
    table = json_config.get("database").get("tables").get("trades").get("name")
    if util_functions.table_exists(hc=hc, db=db, table=table):
        hc.sql("DROP TABLE IF EXISTS %s.%s" % (db, table))


def createTableContracts(hc, json_config):
    EU_COUNTRIES = [
        'AT', 'BE', 'BG', 'CY', 'CZ', 'DE', 'DK',
        'EE', 'ES', 'FI', 'FR', 'GB', 'GR', 'HR',
        'HU', 'IE', 'IT', 'LT', 'LU', 'LV', 'MT',
        'NL', 'PO', 'PT', 'RO', 'SE', 'SI', 'SK'
    ]

    MAJOR_CURRENCY_CODES = ['EUR', 'GBP', 'USD']

    OTHER_CURRENCY_CODES = ['INR', 'AUD', 'CAD', 'SGD', 'CHF', 'MYR', 'JPY', 'CNY']

    SECTORS = [
        'Health care industry', 'Hospitality industry', 'Information industry', 'Manufacturing', 'Aerospace industry',
        'Chemical industry', 'Financial services industry', 'Mass media', 'Mining', 'Telecommunications industry',
        'Transport industry', 'Water industry'
    ]

    MAJOR_PLAYERS_LIST = [
        'China Industrial and Commercial Bank of China', 'China China Construction Bank Corporation',
        'China Agricultural Bank of China', 'China Bank of China', 'United Kingdom HSBC Holdings PLC',
        'United States JPMorgan Chase & Co.', 'France BNP Paribas', 'Japan Mitsubishi UFJ Financial Group',
        'United States Bank of America', 'France Crédit Agricole', 'United States Citigroup Inc',
        'Japan Japan Post Bank', 'United States Wells Fargo & Co', 'Japan Sumitomo Mitsui Financial Group',
        'Germany Deutsche Bank', 'Spain Banco Santander', 'Japan Mizuho Financial Group',
        'United Kingdom Barclays PLC', 'France Société Générale', 'France Groupe BPCE'
    ]

    MAJOR_PLAYERS = [(name, random.choice(SECTORS), random.choice(EU_COUNTRIES)) for name in MAJOR_PLAYERS_LIST]


    def _random_string(length=4):
        """
        Generate a random string of fixed length
        """
        letters = string.ascii_lowercase
        return ''.join(random.choice(letters) for i in range(length))


    def _counterparty():
        """Generate a (name, sector, jurisdiction) pair"""
        if random.uniform(0, 1) < 0.3:
            counterparty = _random_string(4)
            counterparty_sector = random.choice(SECTORS)
            counterparty_jurisdiction = random.choice(EU_COUNTRIES)

        else:
            player = random.choice(MAJOR_PLAYERS)
            counterparty = player[0]
            counterparty_sector = player[1]
            counterparty_jurisdiction = player[2]

        return counterparty, counterparty_sector, counterparty_jurisdiction


    def _random_timestamp():
        today  = datetime.datetime.now()
        dates = [(today + datetime.timedelta(days=i))for i in range(-100, 100)]
        t = datetime.time(hour=random.randint(8, 15), minute=random.randint(0, 59), second=random.randint(0, 59))
        random_date = random.choice(dates)
        return datetime.datetime.combine(random_date, t)


    def _random_date():
        today  = datetime.datetime.now().date()
        dates = [(today + datetime.timedelta(days=i))for i in range(-100, 100)]
        return random.choice(dates)


    def generate_record():
        id = str(uuid.uuid1())
        trade_id = 'trade_{}'.format(random.randint(1e5, 1e7))

        counterparty_1, counterparty_1_sector, counterparty_1_jurisdiction = _counterparty()
        counterparty_2, counterparty_2_sector, counterparty_2_jurisdiction = _counterparty()

        if random.uniform(0, 1) < 0.1:
            notional_currency = random.choice(OTHER_CURRENCY_CODES)
        else:
            notional_currency = random.choice(MAJOR_CURRENCY_CODES)

        notional_amount = random.randint(0, 10e7) + random.uniform(0, 1)

        reporting_timestamp = _random_timestamp()
        execution_date = _random_date()

        record = (
            id, trade_id,
            counterparty_1, counterparty_1_sector, counterparty_1_jurisdiction,
            counterparty_2, counterparty_2_sector, counterparty_2_jurisdiction,
            notional_currency, notional_amount,
            reporting_timestamp, execution_date
        )

        return record


    def batch(n):
        return (generate_record() for _ in range(n))


    database = json_config.get("database").get("name")
    table = json_config.get("database").get("tables").get("trades").get("name")

    hc.sql("USE %s" % (database,))

    NUMBER_OF_BACTHES = json_config.get("database").get("tables").get("contracts").get("batches")
    BATCH_SIZE = json_config.get("database").get("tables").get("contracts").get("batch_size")

    schema = T.StructType([
        T.StructField('id', T.StringType(), True),
        T.StructField('trade_id', T.StringType(), True),
        T.StructField('counterparty_id_1', T.StringType(), True),
        T.StructField('counterparty_id_2', T.StringType(), True),
        T.StructField('counterparty_jurisdiction_1', T.StringType(), True),
        T.StructField('counterparty_jurisdiction_2', T.StringType(), True),
        T.StructField('counterparty_sector_1', T.StringType(), True),
        T.StructField('counterparty_sector_2', T.StringType(), True),
        T.StructField('notional_currency', T.StringType(), True),
        T.StructField('notional_amount', T.DecimalType(38, 12), True),
        T.StructField('reporting_timestamp', T.TimestampType(), True),
        T.StructField('execution_date', T.DateType(), True)
    ])

    for _ in range(NUMBER_OF_BACTHES):
        data = batch(BATCH_SIZE)
        df = hc.createDataFrame(data, schema)
        df.write.format("orc").mode("append").saveAsTable(database + "." + table)
