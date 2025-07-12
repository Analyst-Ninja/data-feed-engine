import argparse

parser = argparse.ArgumentParser(description="CLI for Data Feed Engine")

parser.add_argument("-c", help="feed confif path")
parser.add_argument("--config", help="feed confif path")

args = parser.parse_args()
