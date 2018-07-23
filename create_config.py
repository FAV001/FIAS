#создаем конфигурационный файл
import sqlite3
from sqlite3 import Error
import rarfile
from tqdm import tqdm
from dbfread import DBF
import os
 
 
def create_connection(db_file):
    """ create a database connection to a SQLite database """
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        conn.close()

def sanitised_input(prompt, type_=None, min_=None, max_=None, range_=None):
    if min_ is not None and max_ is not None and max_ < min_:
        raise ValueError("min_ must be less than or equal to max_.")
    while True:
        ui = input(prompt)
        if type_ is not None:
            try:
                ui = type_(ui)
            except ValueError:
                print("Input type must be {0}.".format(type_.__name__))
                continue
        if max_ is not None and ui > max_:
            print("Input must be less than or equal to {0}.".format(max_))
        elif min_ is not None and ui < min_:
            print("Input must be greater than or equal to {0}.".format(min_))
        elif range_ is not None and ui not in range_:
            if isinstance(range_, range):
                template = "Input must be between {0.start} and {0.stop}."
                print(template.format(range_))
            else:
                template = "Input must be {0}."
                if len(range_) == 1:
                    print(template.format(*range_))
                else:
                    print(template.format(" or ".join((", ".join(map(str,
                                                                     range_[:-1])),
                                                       str(range_[-1])))))
        else:
            return ui 
 
if __name__ == '__main__':
    #create_connection(".\\DB\\config.sqlite")
    rf = rarfile.RarFile('.\\update\\full\\fias_dbf.rar')
    print(len([elem for elem in rf.namelist() if elem[0:6] == 'ADDROB']))
    addrob = [elem for elem in rf.namelist() if elem[0:6] == 'ADDROB']
#    for i1 in tqdm(range(len(addrob)), ascii=True):
    for i1 in tqdm(addrob, leave=False):
        fileWrite = open('.\\tmp\\' + i1, 'wb')
        #tqdm.write('file -> ' + i1 + ' size : ' + str(rf.getinfo(i1).file_size))
        with tqdm(total=rf.getinfo(i1).file_size, unit='B', unit_scale=True, leave=False) as tq2:
        #tq2 = tqdm(total=rf.getinfo(i1).file_size, unit='B', unit_scale=True, leave=False)
            tq2.set_description('файл-> %s' % i1)
            with rf.open(i1) as f:
                while True:
                    chunk = f.read(10240)
                    tq2.update(len(chunk))
                    if chunk:
                        fileWrite.write(chunk)
                    else:
                        break
            fileWrite.close()
        #распаковали файл
        with DBF('.\\tmp\\' + i1) as table:
        #table = DBF('.\\tmp\\' + i1)

#        table = dbf.Table('.\\tmp\\' + i1).open()
            offname = ''
            shortname = ''
            regioncode = ''
            for row in table:
                if len(row['NEXTID']) == 0 and row['AOLEVEL'] == 1:
                    offname = row['OFFNAME'].rstrip()
                    shortname = row['SHORTNAME'].rstrip()
                    regioncode = row['REGIONCODE'].rstrip()
                    break
                #print(row)
#        table.close()
        answer = sanitised_input(' Обрабатывать ' + offname + ' ' + shortname + ' y/n ',str.lower, range_=('y', 'n'))
        print(answer)
        if os.path.isfile('.\\tmp\\' + i1):
            os.remove('.\\tmp\\' + i1)
        #tqdm.write(i1)
