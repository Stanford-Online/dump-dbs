#!/usr/bin/env python
#
# Look for differences beween the LMS and Forum database user list.
# Because doing this directly against models is offensively slow,
# this just directly uses the MongoDB and MySQL Python drivers to
# hit the databases directly and compare the lists.
#
# This expects to have a local YAML file that looks like this:
#
#    ---
#    lms:
#        host: localhost
#        port: 3306
#        db: edxapp
#        user: root
#        passwd: ''
#
#    forum:
#        host: localhost
#        db: forum
#        port: 27017
#
# If it works it produces output that looks like this:
#
#    > time python forum_user_audit.py
#    forum_user_audit.py INFO: querying lms DB at <redacted>
#    forum_user_audit.py INFO: lms found 362330 users
#    forum_user_audit.py INFO: querying forum DB at <redacted>
#    forum_user_audit.py INFO: forums found 283883 users
#    forum_user_audit.py INFO: missing = 78488, written to missing.csv
#    forum_user_audit.py INFO: different = 32, written to different.csv
#
#    real0m8.646s
#    usersr0m2.559s
#    sys0m1.897s
#
# TODO's:
# - error handling
# - don't silently clobber output files
# - help messages, options for config file names
#

import sys
import os
import yaml
import MySQLdb
import pandas as pd
import pymongo


myname = sys.argv[0]
MISSING_CSV = "missing.csv"
DIFF_CSV = "different.csv"


def get_lms_users(lms_config):
    info("querying lms DB at {}".format(lms_config["host"]))
    conn = MySQLdb.connect(**lms_config)
    query = """
            select id, username
            from auth_user
            """
    df = pd.io.sql.read_sql(query, conn)
    df.set_index('id', inplace=True)
    info("lms found {} users".format(len(df)))
    return df


def get_forum_users(forum_config):
    info("querying forum DB at {}".format(forum_config["host"]))
    # import ipdb; ipdb.set_trace()
    uri="mongodb://{user}:{passwd}@{host}:{port}/{db}"\
            .format(**forum_config)
    client = pymongo.mongo_client.MongoClient(host=uri)
    db = client[forum_config['db']]
    coll = db.users
    curs = coll.find(
        fields={'_id': False, 'external_id': True, 'username': True}
    )
    # maybe this "tuple from list" business isn't strictly necessary but
    # was fussy to get working without that.
    data = [tuple([int(row['external_id']), row['username']]) for row in curs]
    df = pd.DataFrame(data, columns=('id', 'username'))
    df.set_index('id', inplace=True)
    info("forums found {} users".format(len(df)))
    return df


def config_filename(myname):
    extpos = myname.rfind('.')
    return myname[0:extpos] + "_config.yaml"


def info(msg):
    sys.stderr.write(myname + " INFO: " + msg + "\n")


def error(msg):
    sys.stderr.write(myname + " ERROR: " + msg + "\n")


# Main

def main():
    config_file = open(config_filename(myname), "r")
    config = yaml.load(config_file)

    lms_users = get_lms_users(config['lms'])
    forum_users = get_forum_users(config['forum'])

    # left outer join
    users = pd.merge(left=lms_users, left_index=True,
                     right=forum_users, right_index=True,
                     suffixes=('_lms', '_forum'), how='left')

    # queries using pandas clever/quirky query language
    missing = users[users.username_forum.isnull()]
    missing.to_csv(MISSING_CSV)
    info("missing = {}, written to {}".format(len(missing), MISSING_CSV))

    both = users[users.username_forum.notnull()]
    different = both[both.username_lms != both.username_forum]
    different.to_csv(DIFF_CSV)
    info("different = {}, written to {}".format(len(different), DIFF_CSV))

if __name__ == "__main__":
    main()
