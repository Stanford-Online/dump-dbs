#!/usr/bin/env python

import sys
import os
import subprocess
from collections import OrderedDict

import smtplib
from email.mime.text import MIMEText

import json
import yaml
import ordered_yaml

myname = sys.argv[0]

sender = "sef@stanford.edu"
if len(sys.argv) > 1:
    recipient = sys.argv[1]
else:
    recipient = "sef@stanford.edu"

CONFIG_FILENAME = "config.yml"
DEFAULT_TARGET_DIR = "/data/dump"


# Worker Functions

def query_mongo(config, db):
    cmd_template = ("echo '{query}' "
                    "| mongo --quiet {host}:{port}/{db} "
                    "-u {user} -p {password}")
    cmd = [cmd_template.format(**config[db])]
    output = subprocess.check_output(cmd, shell=True)
    result_dict = json.loads(output)
    return result_dict


def email_report(results):
    info("sending mail to {}".format(recipient))

    body = "There are {} large files in GridFS.\n\n".format(len(results))
    for entry in results:
        body += "{}: {}\n".format(entry['filename'], entry['length'])
    msg = MIMEText(body)
    msg['Subject'] = "Large GridFS Files: {}".format(len(results))
    msg['From'] = sender
    msg['To'] = recipient

    s = smtplib.SMTP('localhost')
    s.sendmail(sender, [recipient], msg.as_string())
    s.quit()


# Helper Functions

def info(msg):
    sys.stderr.write(myname + " INFO: " + msg + "\n")


def error(msg):
    sys.stderr.write(myname + " ERROR: " + msg + "\n")


# Main

def main():
    config_file = open(CONFIG_FILENAME, "r")
    config = yaml.load(config_file, Loader=ordered_yaml.OrderedDictYAMLLoader)
    if 'large_files' not in config:
        error("large_file_query stanza not found in config.yml")
        sys.exit(1)
    result_dict = query_mongo(config, 'large_files')
    results = result_dict['result']
    info("found {} files".format(len(results)))
    if len(results) > 0:
        email_report(results)

if __name__ == "__main__":
    main()
