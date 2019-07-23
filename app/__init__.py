from pyspark import SparkContext
from pyspark.sql import HiveContext

from app.common import args_parser
from app.common import util_functions
from app.data import database
from app.data import contracts
from app.data import products
from app.data import trades 


def main(args=None):
    def create():
        database.create_database(hc=hc, json_config=json_config)
        trades.createTableContracts(hc=hc, json_config=json_config)
        products.createTableProducts(hc=hc, json_config=json_config)


    def delete():
        trades.deleteTableContracts(hc=hc, json_config=json_config)
        products.deleteTableProducts(hc=hc, json_config=json_config)
        database.delete_database(hc=hc, json_config=json_config)


    args = args_parser.parse_arguments()
    json_config = util_functions.load_json_config(args.json_config)

    sc = SparkContext.getOrCreate()
    hc = HiveContext(sc)
    hc.setConf("hive.exec.dynamic.partition", "true")
    hc.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    hc.setConf("spark.sql.hive.convertMetastoreOrc", "false")

    
    if args.action == "create":
        # delete()
        create()

    elif args.action == "delete":
        delete()
        

if __name__ == '__main__':
    main()