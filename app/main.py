import argparse

from clean import process_data


def parse_arguments():
    """
    Parse command-line arguments to get paths to datasets and a list of countries for filtering.

    Returns:
    - Namespace, containing the arguments and their values.
    """
    parser = argparse.ArgumentParser(description='Process paths to two datasets and a list of countries for filtering.')

    # Argument for the first dataset path
    parser.add_argument('-cd', '--client', type=str, required=True,
                        help='Path to the client dataset')

    # Argument for the second dataset path
    parser.add_argument('-fd', '--financial', type=str, required=True,
                        help='Path to the second dataset')

    # Argument for the list of countries
    parser.add_argument('-c', '--countries', type=str, nargs='*', required=False,
                        help='List of countries to filter the client dataset by')

    args = parser.parse_args()
    return args


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    arguments = parse_arguments()
    process_data(arguments.client, arguments.financial, arguments.countries)

