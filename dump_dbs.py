#!/usr/bin/env python

import sys
import subprocess
import datetime
from collections import OrderedDict

import yaml
import ordered_yaml

myname = sys.argv[0]

CONFIG_FILENAME = "databases.yml"


## Worker Functions

def mongodump(config, db):
    """
    Drive the mongodump command from a stanza that looks like this:
        dbname:
            use: mongodump
            host: dbname.dbhoster.com
            port: 27017
            user: root
            password: redacted
            db: importantstuff
            collection: stuff
            format: tarball
            keepdays: 7
    """
    target_name = make_targetname(config, db)
    info("dumping \"" + db + "\" to \"" + target_name + "\"")
    option_mapping = OrderedDict([
            ("-h", "host"),
            ("--port", "port"),
            ("-d", "db"),
            ("-c", "collection"),
            ("-u", "user"),
            ("-p", "password"),
            ])
    cmd = ["mongodump"]
    for (option, setting_name) in option_mapping.iteritems():
        if setting_name in config[db]:
            cmd.append(option)
            setting = str(config[db][setting_name])
            if len(setting):
                cmd.append(setting)
    cmd.append("-o")
    cmd.append(target_name)
    subprocess.call(cmd)

    compress(config, db, target_name)
    rotate(config, db)


def mysqldump(config, db):
    """
    Drive the mysql command from a stanza that looks like this:
        dbname:
            use: mysqldump
            host: dbname.dbhoster.com
            port: 3306
            user: root
            password: redacted
            db: importantstuff
            format: tarball
            keepdays: 7
    """
    target_name = make_targetname(config, db)
    info("dumping \"" + db + "\" to \"" + target_name + "\"")
    option_mapping = OrderedDict([
            ("-h", "host"),
            ("-P", "port"),
            ("-u", "user"),
            ])
    cmd = ["mysqldump"]
    for (option, setting_name) in option_mapping.iteritems():
        if setting_name in config[db]:
            cmd.append(option)
            setting = str(config[db][setting_name])
            if len(setting):
                cmd.append(setting)
    cmd.append("--lock-tables=false")  # so R/O account will work
    if 'password' not in config[db]:
        error(db + ": \"password\" required, skipping")
        return
    cmd.append("-p" + config[db]['password'])  # not space separated
    if 'db' not in config[db]:         # db parameter must come last
        cmd.append(db)
    else:
        cmd.append(config[db]['db'])
    with open(target_name, "w") as outfile:
        subprocess.call(cmd, stdout=outfile)

    compress(config, db, target_name)
    rotate(config, db)


## Helper Functions

def compress(config, db, target_name):
    fmt = config[db].get("format", None)
    if fmt in ["tarball", ".tar.gz", "tar.gz"]:
        info("zipping and compressing " + target_name)
        cmd = ["tar", "zcvf", target_name + ".tar.gz", target_name]
        subprocess.call(cmd)

        info("removing " + target_name)
        cmd = ["rm", "-r", target_name]
        subprocess.call(cmd)
    elif fmt in [".gz", "gz", "compress", "compressed", "gzip", "gzipped"]:
        info("compressing " + target_name)
        cmd = ["gzip", "-r", "-q", target_name]
        subprocess.call(cmd)
    else:
        error(db + ": invalid \"compress\" setting, should be tarball or compress")
    return

def rotate(config, db):
    keep = config[db].get("keepdays", None)
    if keep is None:
        return
    keep_days = int(keep)
    if keep_days < 1 or keep_days > 100:
        error("invalid \"keepdays\" setting, skipping rotation")
        return
    # TODO: rotate
      
def make_targetname(config, db):
    templ = config[db].get("name", "%(dbname)s-%(today)s")
    name = templ % { 
            "today": datetime.datetime.now().strftime("%Y%m%d"), 
            "dbname": db, 
            "name": db, 
            }
    return name

def info(msg):
    sys.stderr.write(myname + " INFO: " + msg + "\n")

def error(msg):
    sys.stderr.write(myname + " ERROR: " + msg + "\n")


## Main

def main():
    config_file = open(CONFIG_FILENAME, "r")
    config = yaml.load(config_file, Loader=ordered_yaml.OrderedDictYAMLLoader)

    for db in config:
        try:
            # look up the local func named in method. We'll use that worker func.
            methodfunc = globals()[config[db]["use"]]
        except KeyError:
            error(db + " has bad or missing \"use\" parameter, skipping")
            continue
        methodfunc(config, db)

if __name__ == "__main__":
    main()

