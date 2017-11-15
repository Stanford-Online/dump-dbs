#!/usr/bin/env python

import sys
import os
import errno
import re
import subprocess
import datetime
from collections import OrderedDict
import MySQLdb

import yaml
import ordered_yaml

myname = sys.argv[0]

CONFIG_FILENAME = "config.yml"
DEFAULT_TARGET_DIR = "/data/dump"


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
    """
    target_name = make_target_name(config, db)
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

    compressed_name = compress(config[db], target_name)
    make_symlink(config[db], compressed_name)


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
            sed:
                - s/test.class.stanford.edu/localhost:8000/g
                - s/class.stanford.edu/localhost:8000/g
            format: tarball
    """
    target_name = make_target_name(config, db)
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
    cmd.append("--lock-tables=false")                  # for R/O account
    cmd.append("-p" + config[db].get('password', ""))  # not space separated
    cmd.append('--ignore-table=' + config[db].get('db', db) + '.courseware_studentmodulehistory') # Ignore csmh since it is now static
    cmd.append(config[db].get('db', db))               # db param is last
    with open(target_name, "w") as outfile:
        subprocess.call(cmd, stdout=outfile)

    filter_with_sed(config[db], target_name)
    compressed_name = compress(config[db], target_name)
    make_symlink(config[db], compressed_name)


def mysqlcsmhedump(config, db):
    """
    Dumps the contents of the CSMHE table in increments.

    Drive the mysqlcsmhe command from a stanza that looks like this:
        dbname:
            use mysqlcsmhedump
            host: dbname.dbhoster.com
            port: 3306
            user: root
            password: redacted
            db: importantstuff
            sed:
                - s/test.class.stanford.edu/localhost:8000/g
                - s/class.stanford.edu/localhost:8000/g
            format: tarball
            csmhe_id_file: max_id_file
    """
    # Read previous max(id) from file
    info("Reading previous max(id) from file")
    id_file = config["target_dir"] + "/csmhe/" + config[db].get("csmhe_id_file")
    old_max_id = read_id_from_file(id_file)

    # Read current max(id) from db
    info("Reading max(id) from db")
    dbConnection = MySQLdb.connect(
        str(config[db]['host']),
        str(config[db]['user']),
        str(config[db]['password']),
        str(config[db]['db']),
    )
    cursor = dbConnection.cursor()
    cursor.execute('select max(id) from coursewarehistoryextended_studentmodulehistoryextended')
    data = cursor.fetchone()
    cursor.close()
    dbConnection.close()
    new_max_id = str(data[0])

    # Dump rows from old to new ids.
    target_name = config["target_dir"] + '/csmhe/' + make_target_name(config, db)
    info("Dumping \"" + db + "\" to \"" + target_name + "\"")
    mysqldump_cmd = ["mysqldump"]
    option_mapping = OrderedDict([
        ("-h", "host"),
        ("-P", "port"),
        ("-u", "user"),
    ])
    add_options(config, db, mysqldump_cmd, option_mapping, old_max_id, new_max_id)

    with open(target_name, "w") as outfile:
        subprocess.call(mysqldump_cmd, stdout=outfile)

    filter_with_sed(config[db], target_name)
    compressed_name = compress(config[db], target_name)

    # Write new max(id) to file
    info("Writing max(id) to file")
    write_id_to_file(id_file, new_max_id)


## Helper Functions

def read_id_from_file(file):
    """
    Read the last written max(id) from the tracking file.
    """
    input = open(file, 'r')
    id = input.readline().strip()
    input.close()
    return id


def write_id_to_file(file, id):
    """
    Write the new max(id) to the tracking file.
    """
    output = open(file, 'w')
    output.write(id)
    output.close()


def add_options(config, db, cmd, option_mapping, min_id, max_id):
    """
    Add the base options to the command
    """
    for (option, setting_name) in option_mapping.iteritems():
        if setting_name in config[db]:
            cmd.append(option)
            setting = str(config[db][setting_name])
            if len(setting):
                cmd.append(setting)

    cmd.append("--where=id >" + min_id + " and id <=" + max_id)  # where clause
    cmd.append("--lock-tables=false")                            # for R/O account
    cmd.append("-p" + config[db].get('password', ""))            # not space separated
    cmd.append(config[db].get('db', db))                         # db param
    cmd.append("coursewarehistoryextended_studentmodulehistoryextended") # only export this table


def filter_with_sed(dbconfig, target_name):
    for sedcmd in dbconfig.get('sed', []):
        info("cleaning " + target_name + "with \"" + sedcmd + "\"")
        cmd = ['sed', '-i', '-e', sedcmd, target_name]
        subprocess.call(cmd)


def compress(dbconfig, target_name):
    """
    Compress the target, method depends on the "format" parameter.  Returns
    the resulting target filename, useful for symlinking to.
    """
    fmt = dbconfig.get("format", None)
    if fmt in ["tarball", ".tar.gz", "tar.gz"]:
        info("zipping and compressing " + target_name)
        output_name = target_name + ".tar.gz"
        cmd = ["tar", "zcvf", output_name, target_name]
        subprocess.call(cmd)
        info("removing " + target_name)
        cmd = ["rm", "-r", target_name]
        subprocess.call(cmd)
    elif fmt in [".gz", "gz", "compress", "compressed", "gzip", "gzipped"]:
        info("compressing " + target_name)
        cmd = ["gzip", "-r", "-q", target_name]
        output_name = target_name + ".gz"
        subprocess.call(cmd)
    else:
        error("invalid \"compress\" setting, should be tarball or compress, " + target_name)
        output_name = ""
    return output_name


def make_symlink(dbconfig, targ):
    """Create a symlink unless explicitly configured not to."""
    if "latest" in dbconfig and not dbconfig["latest"]:
        return
    link = re.sub(r'[0-9]+', 'latest', targ)
    try:
        os.symlink(targ, link)
        info("create link " + link + " --> " + targ)
    except OSError as e:
        if e.errno == errno.EEXIST:
            os.remove(link)
            os.symlink(targ, link)
            info("move link " + link + " --> " + targ)


def make_target_name(config, db):
    """
    Return a tuple: the filename that we'll want to generate, with
    today's date, and the symlink from/to that we'll want to create.
    """
    templ = config[db].get("name", "%(dbname)s-%(today)s")
    return templ % {
            "today": datetime.datetime.now().strftime("%Y%m%d"),
            "dbname": db, "name": db,
            }


def info(msg):
    sys.stderr.write(myname + " INFO: " + msg + "\n")


def error(msg):
    sys.stderr.write(myname + " ERROR: " + msg + "\n")


## Main

def main():
    config_file = open(CONFIG_FILENAME, "r")
    config = yaml.load(config_file, Loader=ordered_yaml.OrderedDictYAMLLoader)

    if "target_dir" in config:
        os.chdir(config["target_dir"])
    else:
        os.chdir(DEFAULT_TARGET_DIR)

    for db in config:
        if type(config[db]) is str:
            continue
        try:
            # look up the local func named in method. We'll use that worker func.
            methodfunc = globals()[config[db]["use"]]
        except KeyError:
            error(db + " has bad or missing \"use\" parameter, skipping")
            continue
        methodfunc(config, db)

if __name__ == "__main__":
    main()
