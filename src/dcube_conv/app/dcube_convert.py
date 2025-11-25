import argparse
import asyncio
import logging
from pathlib import Path

from rich import print_json
from rich.logging import RichHandler

FORMAT = "%(message)s"
logging.basicConfig(
    level="INFO", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()]
)


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

    stationxml = subparsers.add_parser(
        "stationxml",
        help="Create a StationXML file from a DCube convert project",
    )

    xml_subparsers = stationxml.add_subparsers(
        dest="xml_command",
        help="Create a StationXML file from a DCube convert project",
    )
    xml_subparsers.add_parser(
        "init",
        help="Initialize a new DCube convert project and print to stdout",
    )
    xml_create = xml_subparsers.add_parser(
        "create",
        help="Create a StationXML file from a DCube convert project",
    )

    xml_create.add_argument(
        "--fill-elevations",
        action="store_true",
        help="Fill elevations from online data.",
        default=False,
    )

    xml_create.add_argument(
        "input_config",
        type=Path,
        help="Path to generated config file.",
    )
    xml_create.add_argument(
        "input_stations",
        type=Path,
        help="Path to <run>.station.json file.",
    )

    plot = subparsers.add_parser(
        "plot",
        help="Plot station coverage",
    )
    plot.add_argument(
        "input_path",
        type=Path,
        help="Path to <run>.station.json file.",
    )

    args = parser.parse_args()

    logging.root.setLevel(
        level=logging.DEBUG
        if args.v >= 2
        else logging.INFO
        if args.v >= 1
        else logging.WARNING
    )

    loop_debug = bool(args.v)

    if args.command == "convert":
        from dcube_conv.convert import Converter

        converter = Converter.load(args.input_path)

        file_logger = logging.FileHandler(args.input_path.with_suffix(".log"))
        logging.root.addHandler(file_logger)
        asyncio.run(converter.convert(), debug=loop_debug)

    elif args.command == "init":
        from dcube_conv.convert import Converter

        print_json(Converter().model_dump_json(indent=2))

    elif args.command == "stationxml":
        if args.xml_command == "init":
            from dcube_conv.stationxml import StationXML

            print_json(StationXML().model_dump_json(indent=2))
        elif args.xml_command == "create":
            from dcube_conv.stations import CubeSites
            from dcube_conv.stationxml import StationXML

            sites = CubeSites.load(args.input_stations)
            if args.fill_elevations:
                asyncio.run(sites.fill_elevations())
            stationxml = StationXML.model_validate_json(args.input_config.read_bytes())
            stationxml.dump_stationxml(sites, args.input_config.with_suffix(".xml"))

    elif args.command == "plot":
        from dcube_conv.plot import plot_cube_coverage
        from dcube_conv.stations import CubeSites

        sites = CubeSites.load(args.input_path)
        plot_cube_coverage(sites)

    else:
        parser.print_help()
