import os
import re
from argparse import ArgumentParser
from argparse import ArgumentTypeError


def parse_arguments():
    """
    Method to parse arguments, any check need is made on the type argument, each type represents a function.
    :return: Parsed arguments
    """
    parser = ArgumentParser(description="Creates and removes data from a Hadoop system to use in Spark Training.")

    parser.add_argument("-j", "--json_config", dest="json_config", help="The JSON Config File Location",
                        type=json_config_file, required=True)

    parser.add_argument("-a", "--action", dest="action", help="Create/Delete data from Hadoop", type=action, 
                        required=True)

    return parser.parse_args()


def json_config_file(value):
    """
    Method that call the json validation method and raises an Exception if not
    :param value: The json file path that was passed as argument
    :return: Value or Exception in case of error
    """
    def is_json_file_location_valid():
        return os.path.basename(value).split(".")[-1] == "json"

    if is_json_file_location_valid():
        return value
    else:
        raise ArgumentTypeError


def action(value):
    """
    Method to validate if action is valid: create | delete
    :param value: The action passed as an argument
    """
    def is_action_valid():
        return (value in ["create", "delete"])

    if is_action_valid():
        return value
    else:
        raise ArgumentTypeError
