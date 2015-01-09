#!/usr/bin/env python
#
# config.yml should have a stanza that looks like this:
#
# large_files:
#    host: MONGO_FULLY_QUALIFIED_DOMAIN_NAME
#    port: MONGO_PORT
#    user: MONGO_USER_NAME
#    password: MONGO_PASSWORD
#    db: MONGO_DB_NAME
#    size: 10000000
#    sender: admin@foo.com
#

import sys
import os
import subprocess
from collections import OrderedDict
import json
import yaml
import ordered_yaml
import smtplib
from email.mime.text import MIMEText

CONFIG_FILENAME = "config.yml"
myname = sys.argv[0]
recipients = sys.argv[1:]

# Worker Functions

def query_mongo(config):
    config['query'] = ("db.fs.files.aggregate(["
                       "{$match: {length: {$gt: %s }}},"
                       "{$project: {filename:1,length:1, _id:0}},"
                       "{$sort: {length:-1}}"
                       "])") % config['size']
    cmd_template = ("echo '{query}' "
                    "| mongo --quiet {host}:{port}/{db} "
                    "-u {user} -p {password}")
    cmd = [cmd_template.format(**config)]
    output = subprocess.check_output(cmd, shell=True)
    resultlist = json.loads(output)
    return resultlist['result']


def email_report(body, result_count, sender):
    msg = MIMEText(body)
    msg['Subject'] = "Large GridFS Files: {}".format(result_count)
    msg['From'] = sender
    msg['To'] = ",".join(recipients)

    s = smtplib.SMTP('localhost')
    s.sendmail(sender, recipients, msg.as_string())
    s.quit()


# Helper Functions

def info(msg):
    sys.stderr.write(myname + " INFO: " + msg + "\n")


def error(msg):
    sys.stderr.write(myname + " ERROR: " + msg + "\n")


# Main

def main():
    with open(CONFIG_FILENAME, "r") as config_file:
        config = yaml.load(config_file,
                           Loader=ordered_yaml.OrderedDictYAMLLoader)
    if 'large_files' not in config:
        error("large_files stanza not found in configuration file")
        sys.exit(1)
    bigs = query_mongo(config['large_files'])

    sizeMB = int(config['large_files']['size']) / 1e6
    report = u"There are {} large files in GridFS over {} MB.\n\n" \
        .format(len(bigs), sizeMB)
    for big in bigs:
        report += u"{}: {}\n".format(big['filename'], big['length'])
    if len(recipients):
        info("found {} files".format(len(bigs)))
        info("sending mail to {}".format(",".join(recipients)))
        email_report(report.encode('utf-8'), len(bigs), config['large_files']['sender'])
    else:
        sys.stdout.write(report)

if __name__ == "__main__":
    main()

