import argparse

from .convert import DatabricksConvert

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Add some integers.")
    parser.add_argument("input_path", help="Input file or directory")
    parser.add_argument("output_path", help="Output directory")
    parser.add_argument("--output-type", default="dbc", help="Output format")
    args = parser.parse_args()
    DatabricksConvert(**vars(args)).convert()
