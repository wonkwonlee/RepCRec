from utils.FileLoader import FileLoader
from utils.driver import run, run_interactive
import argparse
import sys


def run_file(input_file, output_file):
    
    with open(output_file, "w") as f:
        sys.stdout = f
        loader = FileLoader(input_file)
        case_id = 1
        while loader.has_next():
            print(f"Test {case_id} Result")
            c = loader.next_case()
            run(c)
            case_id += 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser("RepCRec")
    parser.add_argument("mode", type=str, help="program mode (f/d/i), 'i' represents interactive mode")
    parser.add_argument("-input", type=str, help="input source")
    parser.add_argument("-output", type=str, help="output source")
    args = parser.parse_args()

    mode, input_src, output_src = args.mode, args.input, args.output

    if args.mode == "f":
        run_file(input_src, output_src)