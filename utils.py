import os


def base_dir():
    """
        Get the base directory
    :return: the base directory
    """
    return os.path.dirname(os.path.abspath(__file__))