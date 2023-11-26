import argparse
import asyncio
import logging
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Convert DCube files to miniSEED")

    parser.add_argument(
        "-v",
        action="count",
        default=0,
        help="Increase verbosity",
    )
    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    convert = subparsers.add_parser(
        "convert",
        help="Convert DCube files to miniSEED",
    )
    convert.add_argument(
        "input_path",
        type=Path,
        help="Path to configuration file.",
    )

    subparsers.add_parser(
        "init",
        help="Initialize a new DCube convert project and print to stdout",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG
        if args.v >= 2
        else logging.INFO
        if args.v >= 1
        else logging.WARNING
    )

    if args.command == "convert":
        from dcube_conv.convert import Converter

        converter = Converter.model_validate_json(args.input_path.read_bytes())
        asyncio.run(converter.convert())

    elif args.command == "init":
        from dcube_conv.convert import Converter

        print(Converter().model_dump_json(indent=2))

    else:
        parser.print_help()
