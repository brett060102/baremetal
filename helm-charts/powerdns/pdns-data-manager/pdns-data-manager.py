""""
Utility to manage PowerDNS Zones/Records. 
Read an yaml file with the data needed to 
create/delete zones/records.
Example of how to invoke this script:
python pdns-data-manager.py --host 127.0.0.1 --port 8081 --config dnsdata.yaml --key abcdef123456 --operation create
"""
import argparse
import json
import logging
import os
import sys
import traceback
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import (Any, Dict, List, Optional, Sequence, Tuple)

import requests
import yaml


def create_options_parser() -> argparse.ArgumentParser:
    """Create a parser to parse the arguments."""

    parser = argparse.ArgumentParser(
        description="Collect the config yaml file to manage zone and records creation/deletion in PowerDNS "
    )
    # Primary command line options
    parser.add_argument('-c', '--config', default='dnsdata.yaml',
                        help="The YAML config file to use.", required=True)
    # Output verbosity control
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument('-q', '--quiet', action='store_true',
                           help="Decrease output verbosity.")
    verbosity.add_argument('-v', '--verbose', action='store_true',
                           help="Increase output verbosity.")

    default_log_destination = f"{os.path.expanduser('.')}/" \
                              f"pdns-data-manager.log"
    parser.add_argument('-L', '--logfile', action='store',
                        type=Path,
                        default=Path(default_log_destination),
                        help="path to logfile. "
                            f"Default: {default_log_destination}")
    parser.add_argument('--key', help='PowerDNS API Key', required=True)
    parser.add_argument('--host', help='PowerDNS API Host', required=True)
    parser.add_argument('--port', help='PowerDNS API Port', required=True)
    parser.add_argument('--protocol', default='http')
    parser.add_argument(
        '--operation', help='operation to perform', choices=['create', 'delete'], required=True)

    return parser


def create_logger(level: str,
                  logfile: Path) -> logging.Logger:
    """Create a logger  """
    logger = logging.getLogger()
    logger.setLevel(level)

    fmt_str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt_str)
    loghandler: Any = None
    if logfile:
        try:
            loghandler = RotatingFileHandler(
                logfile,
                maxBytes=(0x100000 * 5),
                backupCount=5)
        except OSError as error:
            loghandler = logging.StreamHandler()
            if level != 'DEBUG':
                formatter = logging.Formatter('%(levelname)s - %(message)s')
                print("Error:", error, file=sys.stderr)
            else:
                traceback.print_exc()
    else:
        loghandler = logging.StreamHandler()
        if level != 'DEBUG':
            formatter = logging.Formatter('%(levelname)s - %(message)s')

    loghandler.setFormatter(formatter)
    logger.addHandler(loghandler)

    return logger


def setup_logging(args: argparse.Namespace) -> Tuple[logging.Logger, int]:
    """Setup the logging environment and default log_level."""
    if args.quiet:
        log_level = logging.WARN
    elif args.verbose:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    logger = create_logger(
        level=logging.getLevelName(log_level), logfile=args.logfile
    )

    return (logger, log_level)


def read_zones(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """ Return the list of zones read from the config yaml file 
    - an empty list if zones section is empty
    """
    zones: List[Dict[str, Any]] = cfg.get('zones', [])
    if zones is None:
        zones = []
    return zones.copy()


def create(args, headers, data, logger):
    err_count = 0
    for zone in read_zones(data):
        url = args.protocol + "://%s:%s/api/v1/servers/localhost/zones" % (
            args.host, args.port)
        if create_zone(url=url, headers=headers, zone=zone, logger=logger):
            create_records(url=url, headers=headers, zone=zone, logger=logger)
        else:
            err_count=err_count+1
            logger.error(
                "Zone %s Creation failed. Skipping record creation", zone['name'])
    if err_count > 0:
        raise Exception("Create operation failed")

def create_zone(url, headers, zone, logger):
    if zone['kind'] == "MASTER" or zone['kind'] == "NATIVE":
        payload = {
            "name": zone['name'],
            "kind": zone['kind'],
            "masters": [],
            "nameservers": zone['nameservers']
        }
    else:
        payload = {
            "name": zone['name'],
            "kind": zone['kind'],
            "masters": zone['masters'],
            "nameservers": []
        }
    if not zone_exists(url+'/'+zone['name'], headers, logger):
        logger.info("Request to create Zone %s", zone['name'])
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        if response.status_code == 201:
            logger.info("Successfully created zone %s", zone['name'])
            return True
        else:
            logger.info("Failed to create zone %s", zone['name'])
            return False
    else:
        return True


def zone_exists(url, headers, logger):
    logger.debug("Sending GET request to %s", url)
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except Exception as error:
        return False
    else:
        return True


def create_records(url, headers, zone, logger):
    for record in zone['records']:
        records = []
        for content in record['content']:
            records.append({
                "content": content,
                "disabled": record['disabled']
            })
        payload = {
                "rrsets": [
                    {
                        "name": record['name'], 
                        "type": record['type'],
                        "ttl": record['ttl'], 
                        "changetype": "REPLACE",
                        "records": records
                    }
                ]
        }

        try:
            logger.info(
                "Sending request to create record %s", record['name'])
            response = requests.patch(
                url+'/'+zone['name'], data=json.dumps(payload), headers=headers)
            response.raise_for_status()
        except Exception as error:
            logger.error(f'Error: {error}')
        else:
            logger.info(f'Successfully created/updated record %s', record['name'])


def delete(args, headers, data, logger):
    for zone in read_zones(data):
        url = args.protocol + "://%s:%s/api/v1/servers/localhost/zones/%s" % (
            args.host, args.port, zone['name'])
        delete_records(url, headers, zone, logger)
        delete_zone(url, headers, zone, logger)


def delete_records(url, headers, zone, logger):
    for record in zone['records']:
        name = record['name']
        payload = {
            "rrsets": [
                {
                    "name": name,
                    "type": record['type'],
                    "changetype": "DELETE",
                }
            ]
        }
        logger.info("Request to delete record  %s", name)
        if zone_exists(url, headers, logger):
            try:
                response = requests.patch(
                    url, data=json.dumps(payload), headers=headers)
                response.raise_for_status()
            except Exception as error:
                logger.error(f'Error: {error}')
            else:
                logger.info(f'Successful deleted DNS record %s', name)
        else:
            logger.info(
                f'Zone %s for the record %s requested to be deleted does not exists', zone['name'], name)

def delete_zone(url, headers, zone, logger):
    payload = {
        "name": zone['name']
    }
    logger.info("Request to delete Zone %s", zone['name'])
    if zone_exists(url, headers, logger):
        try:
            response = requests.delete(
                url, data=json.dumps(payload), headers=headers)
            response.raise_for_status()
        except Exception as error:
            logger.error(f'Error: {error}')
        else:
            logger.info(f'Successful deleted zone %s', zone['name'])
    else:
        logger.info(
            f'Zone %s requested to be deleted does not exists', zone['name'])

def validate_data(data):
    # zone data validation
    for zone in read_zones(data):
        if zone['kind'] == 'MASTER':
            if len(zone['nameservers']) < 1:
                raise Exception(
                    "A nameserver is required to create a MASTER zone")

        if zone['kind'] == 'SLAVE':
            if len(zone['masters']) < 1:
                raise Exception("A master is required to create SLAVE zone")


def main(argv: Optional[Sequence[str]] = None) -> None:
    """Manage PowerDNS Zones/Records """

    parser = create_options_parser()

    args = parser.parse_args(argv)
    logger, log_level = setup_logging(args)
    cfg_file = Path(args.config)
    data = yaml.safe_load(cfg_file.read_text(encoding="utf-8"))
    if data is None:
        raise Exception("No config settings loaded")
    validate_data(data)
    headers = {'X-API-Key': args.key}

    if args.operation == "create":
        try:
            create(args=args, headers=headers, data=data, logger=logger)
            msg = "Create operation successful"
            logger.info(msg)
            print(msg)
        except:
            msg = "Create operation failed"
            logger.error(msg)
            print(msg)

    if args.operation == "delete":
        try:
            delete(args=args, headers=headers, data=data, logger=logger)
            msg = "Delete operation successful"
            logger.info(msg)
            print(msg)
        except:
            msg = "Delete operation failed"
            logger.error(msg)
            print(msg)

if __name__ == '__main__':
    main()
