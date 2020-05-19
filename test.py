import argparse

if __name__ == "__main__":
    parse = argparse.ArgumentParser()
    parse.add_argument('--spark', dest='spark',nargs="*")
    args = parse.parse_args()

    if args.spark is not None:
        print("success")
    else:
        print("failed")
