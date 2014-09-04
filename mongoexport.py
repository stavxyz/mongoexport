"""mongoexport implemented as a python module.

'username' and 'password' for accessing target mongodb
can be stored as environment variables or in python
keyring.

Stored as environment variables:

export MONGOEXPORT_USERNAME=*****
export MONGOEXPORT_PASSWORD=*****

or in python keyring:

$ keyring set mongoexport username
Password for 'username' in 'mongoexport': *****

$ keyring set mongoexport password
Password for 'password' in 'mongoexport': *****


Usage:

    me = MongoExport('helperdb.objectrocket.com', 'help', 'advicecollection')
    me.run()
    # to move each document into its own file...
    me.file_per_document()
"""

import itertools
import mmap
import os
import shlex
import subprocess
import sys

from multiprocessing import pool

import arrow
try:
    import ujson as json
except ImportError:
    import json

import keyring

DAYFMT = '%a_%b_%d_%Y'
SERVICE = 'mongoexport'


class MongoExport(object):

    def __init__(self, host, database, collection, port=27017,
                 username=None, password=None, output=None, query=None,
                 fields=None, use_ssl=True):
        """Constructor for mongoexport job.

        :param fields:  Fields as a list which will be selected for export.
                        Each field may reference a subdocument or value using
                        dot notation.
        """

        now = arrow.now()
        todaystr = now.floor('day').strftime(DAYFMT)
        filename = "%s_%s" % (collection, now.strftime('%X'))
        output = normalized_path(output)
        if not output:
            output = "%s/%s/%s/%s" % (SERVICE.lower(), collection, todaystr, filename)
        elif os.path.isdir(output):
            output = "%s/%s" % (output, filename)
        elif os.path.isfile(output):
            pass
        output = normalized_path(output)
        self.dirs, self.filename = os.path.split(output)

        ensure_dirpath(self.dirs)
        self.docspath = os.path.join(self.dirs, 'documents')
        ensure_dirpath(self.docspath)

        if not username:
            username = get_configured_value('username')
        if not password:
            password = get_configured_value('password')

        if query:
            query = make_json(query)
            if not query.startswith("'"):
                query = "'" + query
            if not query.endswith("'"):
                query = query + "'"

        self.host = host
        self.port = port
        self.database = database
        self.collection = collection
        self.username = username
        self.password = password
        self.query = query
        self.fields = fields
        self.use_ssl = use_ssl
        self.output = output


    def get_command(self):
        command = ("mongoexport --host {host} --port {port} "
                   "--db {db} --collection {collection} --out {output}")
        command = command.format(host=self.host, port=self.port,
                                 db=self.database, collection=self.collection,
                                 output=self.output)
        if self.username:
            command += " --username %s" % self.username
        if self.password:
            command += " --password %s" % self.password
        if self.query:
            command += " --query %s" % self.query
        if self.fields:
            command += " --fields %s" % ",".join(self.fields)
        if self.use_ssl:
            command += " --ssl"
        return command

    def run(self):
        command = self.get_command()
        return execute(command)

    def file_per_document(self):
        return _file_per_document(self.output)


def _file_per_document(exportfile):
    if not os.path.exists(exportfile):
        print "%s doesn't exist!" % exportfile
        return
    dirs, _ = os.path.split(exportfile)
    docspath = os.path.join(dirs, 'documents')
    ensure_dirpath(docspath)
    expfile = open(exportfile, 'r')
    def wat(ammapobject):
        x = True
        while x:
            ablob = ammapobject.readline()
            if ablob:
                yield ablob
            else:
                x = False
    tpool = pool.ThreadPool(pool.cpu_count()*64)
    gettingweird = wat(mmap.mmap(expfile.fileno(), 0, prot=mmap.PROT_READ))
    job = tpool.imap_unordered(
        _fpd,
        itertools.izip_longest(gettingweird, (), fillvalue=docspath))
    while True:
        try:
            job.next()
        except Exception:
            return


def _fpd(jsonblobdocspath):
    jsonblob, docspath = jsonblobdocspath
    r = json.loads(jsonblob)
    realpath = os.path.join(docspath, r['_id'] + '.json')
    with open(realpath, 'w') as document:
        document.write(jsonblob)


class SubprocessError(subprocess.CalledProcessError):

    def __init__(self, returncode, cmd, output=None, stderr=None):
        super(SubprocessError, self).__init__(returncode, cmd, output=output)
        self.stderr = stderr

    def __str__(self):
        line = super(SubprocessError, self).__str__()
        if self.stderr:
            line += " | %s" % self.stderr
        return line


def get_configured_value(valuename):
    """Gets value by valuename, from environment variable or keyring.

    If the value is stored in keyring, it should be stored with
    service_name equal to the variable SERVICE (lowercased),
    defined at the top of this module.

    If the value is stored as an environment variable, it should be
    stored with the prefix SERVICE + "_".
    """
    value = keyring.get_password(SERVICE.lower(), valuename)
    if not value:
        value = os.getenv('%s_%s' % (SERVICE.upper(), valuename.upper()))
    return value


def normalized_path(value, must_exist=False):
    """Normalize and expand a shorthand or relative path."""
    if not value:
        return
    norm = os.path.normpath(value)
    norm = os.path.abspath(os.path.expanduser(norm))
    if must_exist:
        if not os.path.exists(norm):
            raise ValueError("%s is not a valid path." % norm)
    return norm


def ensure_dirpath(path):
    """Ensures that the directory exists.

    Creates the directory structure if necessary.
    """
    if not path:
        return
    try:
        os.makedirs(path)
    except OSError:
        if os.path.isdir(path):
            pass
        else:
            raise


def make_json(something):
    """Return a json-encoded string from a file, path, or standard object."""
    if isinstance(something, file):
        something = something.read()
    elif isinstance(something, (int, tuple, list, dict)):
        something = json.dumps(something)
    elif os.path.exists(something):
        with open(something, 'r') as thing:
            something = thing.read()
    return something


def execute(command):
    """Manages the subprocess and returns a dictionary with the result.

    Executes the command in the current working directory.
    If the return code is non-zero, raises a SubprocessError.
    """
    cmd = shlex.split(command)
    try:
        result = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except OSError as err:
        raise SubprocessError(err.errno, command, stderr=err.strerror)
    out, err = result.communicate()
    resultdict = {
        'exit_code': result.returncode,
        'stdout': out.strip(),
        'stderr': err.strip(),
    }
    if resultdict['exit_code'] != 0:
        raise SubprocessError(resultdict['exit_code'], command,
                              output=resultdict['stderr'],
                              stderr=resultdict['stderr'])
    return resultdict

