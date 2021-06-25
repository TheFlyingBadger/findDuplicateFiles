
import time
from dataclasses import dataclass
from version import getVersion
import sqlite3
import json
from pathlib import Path
import datetime as dt
import logging
from configFileHelper import Config

from pathlib import Path

try:
    from icecream import ic

    def ic_set(debug):
        if debug:
            ic.enable()
        else:
            ic.disable()


except ImportError:  # Graceful fallback if IceCream isn't installed.
    doDebug: bool = False

    def ic(thing):  # just print to STDOUT
        if doDebug:
            print(thing)

    def ic_set(debug):
        global doDebug
        doDebug = debug
        ic("* icecream module not imported successfully, using STDOUT")


def nowString():
    return f"{dt.datetime.now().strftime('%Y.%m.%d %T')} |> "


PATH_FOLDER_START = ['APP', 'FOLDER_START']
PATH_DB_LOCATION = ['APP', 'DATABASE_PATH']


class finderConfig (Config):

    def __init__(self, configFile):
        super().__init__(file_path=configFile)
        self.folderStart = Path(self.get(PATH_FOLDER_START))
        self.dbLocation = Path(self.get(PATH_DB_LOCATION))

    @property
    def folderStart(self):
        return Path(self.get(PATH_FOLDER_START))

    @property
    def deletePrior(self):
        return self.get_bool("APP/DELETE_PRIOR")

    @property
    def dbLocation(self):
        return Path(self.get(PATH_DB_LOCATION))

    @dbLocation.setter
    def dbLocation(self, value):
        if type(value) == str:
            value = Path(str)
        if value.is_dir():
            raise IOError(f'Folder not allowed \'{str(value)}\'')
        try:
            old = self.folderStart.resolve()
        except:
            old = 'None'
        if old == value.resolve():
            ...
        else:
            self.set(PATH_DB_LOCATION, str(value))
            self.save()
        return self

    @folderStart.setter
    def folderStart(self, value):
        if type(value) == str:
            value = Path(str)
        if not value.is_dir():
            raise NotADirectoryError(str(value))
        try:
            old = self.folderStart.resolve()
        except:
            old = 'None'
        if old == value.resolve():
            ...
        else:
            self.set(PATH_FOLDER_START, str(value))
            self.save()
        return self


def getConfig(configFile: str = "config.json"):

    config = finderConfig(configFile=Path(configFile).resolve())
    ic_set(config.get_bool("APP/DEBUG"))
    return config


CONFIG = getConfig(Path('__file__').parent.parent.joinpath('config.yaml'))


def adapt_datetime(ts):
    return time.mktime(ts.timetuple())


sqlite3.register_adapter(dt.datetime, adapt_datetime)


@dataclass
class file(object):
    path: Path
    fname: str
    size: int

    def __init__(self, pathObj):
        if type(pathObj) == str:
            pathObj = Path(pathObj)
        self.path = pathObj
        self.fname = self.path.name
        self.size = self.path.stat().st_size

    def asdict(self):
        return self.__dict__

    def __repr__(self):
        return f'file(pathObj=Path(\'{(self.path)}\'))'

    def __str__(self):
        d = self.__dict__
        d['path'] = str(d['path'])
        txt, d = json.dumps(d, indent=2), None
        return txt


def createSearch(root, con):
    cur = con.cursor()
    cur.execute('INSERT INTO search (searchroot, timestart) VALUES (?,?)',
                (str(root), dt.datetime.now()))
    id = cur.lastrowid
    cur.close()
    return id


def createTables(con):

    cur = con.cursor()
#  people (
#    person_id INTEGER PRIMARY KEY AUTOINCREMENT,
#    first_name text NOT NULL,
#    last_name text NOT NULL
    stm = ['''CREATE TABLE IF NOT EXISTS search
                    (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT
                    ,searchroot text NOT NULL
                    ,timestart datetime NOT NULL
                    ,timeend datetime
                    )''', '''CREATE TABLE IF NOT EXISTS searchresult
                    (id  INTEGER NOT NULL
                    ,result INTEGER NOT NULL
                    ,numfiles INTEGER NOT NULL
                    ,filesize INTEGER
                    ,PRIMARY KEY (id, result)
                    ,FOREIGN KEY (id) REFERENCES search (id)
                    )''', '''CREATE TABLE IF NOT EXISTS searchresultfiles
                    (id   INTEGER NOT NULL
                    ,result  INTEGER NOT NULL
                    ,file text NOT NULL
                    ,PRIMARY KEY (id, result, file)
                    ,FOREIGN KEY (id,result) REFERENCES searchresult (id, result)
                    )'''
           ]
    for s in stm:
        cur.execute(s)

    cur.close()


def deletePrevious(root, con):
    cur = con.cursor()
    cur.execute("SELECT id FROM search WHERE searchroot = ?", (str(root),))
    rows = cur.fetchall()

    if len(rows) > 0:
        logging.debug(
            f'Deleting {len(rows)} previous search{"" if len(rows) == 1 else "es"}')
        cur.executemany('DELETE FROM searchresultfiles WHERE id = ?', rows)
        cur.executemany('DELETE FROM searchresult WHERE id = ?', rows)
        cur.executemany('DELETE FROM search WHERE id = ?', rows)

    cur.close()


def connect(locn):
    if type(locn) == Path:
        locn = str(locn)
    logging.debug(f'Connecting to {locn}')
    return sqlite3.connect(locn)


def disconnect(con):
    logging.debug(f'Disconnecting')
    con.commit()
    con.close()


def getAllFiles():

    logging.info(CONFIG.folderStart)
    # category=RuntimeWarning)
    import find_duplicate_files

    con = connect(CONFIG.dbLocation)

    try:
        createTables(con)
        if CONFIG.deletePrior:
            deletePrevious(CONFIG.folderStart, con)
        id = createSearch(CONFIG.folderStart, con)
    finally:
        disconnect(con)

    '''
    Close the DB connection to do this search part, it may well be quite time consuming and there's no
     sense in leaving the DB open for all this time
    '''
    fileSets = find_duplicate_files.find_duplicate_files(
        CONFIG.folderStart, chunks=1)

    con = connect(CONFIG.dbLocation)
    try:

        cur = con.cursor()
        try:
            resultSets = [(f[0], f[1])
                          for f in enumerate(fileSets, 1)]
            cur.executemany(
                f'INSERT INTO searchresult (id,result,numfiles) VALUES ({id},?,?)', [(r[0], len(r[1])) for r in resultSets])
            cur.executemany(
                f'INSERT INTO searchresultfiles(id, result, file) VALUES ({id},?,?)', [(sublist[0], val)
                                                                                       for sublist in resultSets for val in sublist[1]])
            cur.execute('UPDATE search SET timeend = ? WHERE id = ?',
                        (dt.datetime.now(), id))
        finally:
            cur.close()
    finally:
        disconnect(con)


if __name__ == '__main__':
    getAllFiles()
