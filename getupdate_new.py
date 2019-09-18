# -*- coding: UTF-8 -*-
# Получение файлов обновлений FIAS с сайта налоговой
from configobj import ConfigObj
from zeep import Client
from requests import Session
import requests
from zeep.transports import Transport
import datetime
import re
import os
import urllib3
from progressbar import AnimatedMarker, Bar, BouncingBar, Counter, ETA, \
    AdaptiveETA, FileTransferSpeed, FormatLabel, Percentage, \
    ProgressBar, ReverseBar, RotatingMarker, \
    SimpleProgress, Timer

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def main():


    def get_delta(fias_spisok, data, useproxy, proxy=None):
        # качаем все файлы дельта обновлений начиная с даты data
        if config['Update']['lastupdateid'] == '':
            lastupdateid = 0
        else:
            lastupdateid = int(config['Update']['lastupdateid'])
        maxid = lastupdateid
        for row in  reversed(fias_spisok):
            print(row)
            row_data = datetime.datetime.strptime((row.FiasCompleteDbfUrl).split('/')[-2], "%Y%m%d").date()
            if row.VersionId > maxid:
                maxid = row.VersionId
            if row_data > data:
            #     # print('Дата обновления (%s) > даты базы (%s)' % (row_data, data))
            #     # проверяем существет ли каталог по дате
                if os.path.isdir('.\\update\\delta\\' + row_data.strftime("%Y%m%d")):
                    # каталог существует, проверим наличие файлов
                    if os.path.isfile('.\\update\\delta\\' + row_data.strftime("%Y%m%d") + '\\fias_delta_dbf.rar'):
                        # файл обновления существует
                        localfilesize = os.path.getsize('.\\update\\delta\\' + row_data.strftime("%Y%m%d") + '\\fias_delta_dbf.rar')
                        remotefilesize = getRemoteFileLength(row.FiasDeltaDbfUrl, useproxy, proxy)
                        if localfilesize == remotefilesize:
                            print('Дельта обновление за %s на диске - стус - pass' % row_data)
                        else:
                            print('Дельта обновление за %s на диске - стус - fail' % row_data)
                            os.remove('.\\update\\delta\\' + row_data.strftime("%Y%m%d") + '\\fias_delta_dbf.rar')
                            getFile(row.FiasDeltaDbfUrl, useproxy, '.\\update\\delta\\' + row_data.strftime("%Y%m%d") + '\\', proxy, row_data.strftime("%Y%m%d") + '.tmp')
                    else:
                        getFile(row.FiasDeltaDbfUrl, useproxy, '.\\update\\delta\\' + row_data.strftime("%Y%m%d") + '\\', proxy, row_data.strftime("%Y%m%d") + '.tmp')
                else:
                    # каталога нет, качаем файл
                    os.makedirs('.\\update\\delta\\' + row_data.strftime("%Y%m%d"))
                    getFile(row.FiasDeltaDbfUrl, useproxy, '.\\update\\delta\\' + row_data.strftime("%Y%m%d") + '\\', proxy, row_data.strftime("%Y%m%d") + '.tmp')
        config['Update']['lastupdateid'] = maxid
        try:
            config.write()
        except Exception as identifier:
            print('error ' + identifier)
        pass


    def del_delta_update():
        for root, dirs, files in os.walk('.\\update\\delta\\', topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))


    # def getRemoteFileLength(link):
    def getRemoteFileLength(link, useproxy, proxy=None, start_pos=None):
        head = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36'}
        if start_pos is not None:
            resume_header = {'Range': 'bytes=%d-' % start_pos}
        else:
            resume_header = {'Range': 'bytes=0-'}
        if useproxy:
            r = requests.get(link, stream=True, proxies=proxy, verify=False,  headers={**head, **resume_header}, timeout=10)
        else:
            r = requests.get(link, stream=True, verify=False,  headers={**head, **resume_header}, timeout=10)
        if r.status_code == 206 or r.status_code == 200:
            return int(r.headers['Content-range'].split("/")[1])
        else:
            return -1


    def getFile(link, useproxy, dest=None, proxy=None, temp_part=None, start_pos=None):
        head = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36'}
        chunk_size = 1024 * 1024
        if start_pos is not None:
            resume_header = {'Range': 'bytes=%d-' % start_pos}
            cur_pos = start_pos
        else:
            resume_header = {'Range': 'bytes=0-'}
            cur_pos = 0
        if useproxy:
            r = requests.get(link, stream=True, proxies=proxy, verify=False,  headers={**head, **resume_header}, timeout=10)
        else:
            r = requests.get(link, stream=True, verify=False,  headers={**head, **resume_header}, timeout=10)
        if r.status_code == 206 or r.status_code == 200:
            if dest is not None:
                filename = dest + link.split('/')[-1] # + t
            else:
                filename = link.split('/')[-1] # + t
            remotefilesize = int(r.headers['Content-range'].split("/")[1])
            is_Download = True
            if cur_pos != remotefilesize:
                widgets = [
                    filename + ': ',
                    Percentage(),
                    ' ',
                    Bar(marker=RotatingMarker()),
                    ' ',
                    ETA(),
                    ' ',
                    FileTransferSpeed()
                ]
                pbar = ProgressBar(widgets=widgets, maxval=remotefilesize).start()
                try:
                    with open(filename, 'ab') as out:
                        try:
                            for data in r.iter_content(chunk_size=chunk_size):
                                out.write(data)
                                cur_pos += len(data)
                                pbar.update(cur_pos)
                        except Exception as identifier:
                            print(f' error !!!!! {identifier}')
                            is_Download = False
                finally:
                    r.close()
                    pbar.finish()
                # сравниваем сколько на диске размер файла и сколько должно быть. Если совпадает то переименовываем temp файл
                # if remotefilesize == os.path.getsize(filename):
                #     newfile = filename[:-len(t)]
                #     os.rename(filename, newfile)
                #     return newfile
                # else:
                #     print('размер исходного файла не совпадает с размером скаченного')
            return filename, cur_pos, is_Download
        else:
            # ошибка при доступе к сайту
            return '', -1, False


    # def download_fias_full(use_proxy, proxy):
    def download_fias_full():
        if os.path.isfile('.\\update\\VerDate.txt'):
            os.remove('.\\update\\VerDate.txt')
        filename, filelength, downloaded = getFile('http://fias.nalog.ru/Public/Downloads/Actual/VerDate.txt', use_proxy, '.\\update\\', proxy)
        if downloaded:
            str_lastupdatedate = open(filename, 'r').read()
        else:
            return False
        d_lastupdate = datetime.datetime.strptime(str_lastupdatedate, "%d.%m.%Y").date()
        sd = d_lastupdate.strftime("%Y%m%d")
        config['Update']['fullbase'] = sd
        try:
            config.write()
        except Exception as identifier:
            print('error ' + identifier)
        # url_fb = 'http://fias.nalog.ru/Public/Downloads/Actual/fias_delta_dbf.rar'
        url_fb = 'http://fias.nalog.ru/Public/Downloads/Actual/fias_dbf.rar'
        remotefilesize = getRemoteFileLength(url_fb, use_proxy, proxy)
        filelength = 0
        while True:
            filename, filelength, downloaded = getFile(url_fb, use_proxy, '.\\update\\full\\', proxy, start_pos=filelength)
            if downloaded:
                if filelength == remotefilesize:
                    return True
            else:
                if filelength == -1:
                    return False


    fiasfile = '.\\update\\full\\fias_dbf.rar'
    # global oldfiasfile
    # config
    # fiasfile = '.\\update\\full\\fias_dbf.rar'
#     oldfiasfile = '.\\update\\full\\fias_dbf.old.rar'
    wsdl = 'http://fias.nalog.ru/WebServices/Public/DownloadService.asmx?WSDL'
    # ссылка на сервис получения обновлений сайт Налоговой
    config = ConfigObj('fias.cfg', encoding='UTF8')
    use_proxy = config.get('Proxy').as_bool('use_proxy')
    if use_proxy:
        proxy_list = config['Proxy']['Proxy']
        # proxy = {'http': 'http://' + proxy_list}
        proxy = {'http': 'http://' + proxy_list, 'https': 'https://' + proxy_list}
        session = Session()
        session.verify = False
        session.proxies = proxy
        transport = Transport(session=session)
        client = Client(wsdl=wsdl, transport=transport)
    else:
        client = Client(wsdl=wsdl)

    # удаляем все файлы в каталоге ".\\update\\full" крому fias_dbf.rar
    for file in os.scandir(".\\update\\full"):
        try:
            if file.name != 'fias_dbf.rar':
                os.unlink(file.path)
                print(f'Удалили файл {file.name}')
        except Exception as e:
            print(f'При удалении файл {file.name} возникла ошибка {e}')
    # Проверяем полную базу
    if config['Update']['fullbase'] == '':
        # в конфиге дата отсутствует. Надо брать полную последнюю базу. Удаляем все остальное
        if os.path.isfile(fiasfile):
            try:
                os.remove(fiasfile)
            except Exception as identifier:
                print(f'При удалении файл {fiasfile} возникла ошибка {identifier}')
        if download_fias_full:
            print('Закачали полную базу FIAS за дату {}'.format(config['Update']['fullbase']))
    else:
        # дата в конфиге есть. Проверяем то что есть на диске и то что есть на сервере
        if os.path.isfile('.\\update\\full\\fias_dbf.rar'):
            # получаем размер локального файла
            localfilesize = os.path.getsize(fiasfile)
            # https://fias.nalog.ru/DataArchive.aspx
            # 'http://fias.nalog.ru/Public/Downloads/20190916/fias_dbf.rar'
                # "http://fias.nalog.ru/Public/Downloads/Actual/fias_dbf.rar",
            remotefilesize = getRemoteFileLength("http://fias.nalog.ru/Public/Downloads/" + config['Update']['fullbase'] + "/fias_dbf.rar", use_proxy, proxy)
            if remotefilesize == -1:
                for file in os.scandir(".\\update\\full"):
                    try:
                        if file.name != 'fias_dbf.rar':
                            os.unlink(file.path)
                            print(f'Удалили файл {file.name}')
                    except Exception as e:
                        print(f'При удалении файл {file.name} возникла ошибка {e}')
                if download_fias_full():
                    print('Закачали полную базу FIAS за дату {}'.format(config['Update']['fullbase']))
            else:
                if localfilesize != remotefilesize:
                    while True:
                        filename, localfilesize, downloaded = getFile(
                            "http://fias.nalog.ru/Public/Downloads/" + \
                            config['Update']['fullbase'] + \
                            "/fias_dbf.rar",
                            use_proxy,
                            '.\\update\\full\\',
                            proxy,
                            start_pos=localfilesize
                        )
                        if downloaded and localfilesize == remotefilesize:
                            break
            # http://data.nalog.ru/Public/Downloads/20190912/fias_dbf.rar
        else:
            # дата в конфиге есть, файла на диске нет
            if download_fias_full():
                print('Закачали полную базу FIAS за дату {}'.format(config['Update']['fullbase']))
    # дельта обновления
    maxdeltaupdate = int(config['Update']['maxdeltaupdate'])
    if maxdeltaupdate != 0:  # при 0 не используем любое количество delta обновлений
        currentdeltaupdate = len(os.listdir('.\\update\\delta\\'))
        if currentdeltaupdate >= maxdeltaupdate:
            for file in os.scandir(".\\update\\full"):
                try:
                    os.unlink(file.path)
                    print(f'Удалили файл {file.name}')
                except Exception as e:
                    print(f'При удалении файл {file.name} возникла ошибка {e}')

            if download_fias_full():
                print('Закачали полную базу FIAS за дату {}'.format(config['Update']['fullbase']))
                del_delta_update()
                print('Удалили все дельта обновления')
        else:
            # не достигли предела по количеству дельта обновлений
            pass
    else:
        pass
    spisok = client.service.GetAllDownloadFileInfo()
    # качаем дельты, если они есть
    full_base_update_date = datetime.datetime.strptime(config['Update']['fullbase'], '%Y%m%d').date()
    get_delta(spisok, full_base_update_date, use_proxy, proxy)
    # pass
#         currentdeltaupdate = len(os.listdir('.\\update\\delta\\'))
#         if currentdeltaupdate > maxdeltaupdate:
#             # необходимо закачать полную базу и удалить дельты
#             isRen = False
#             if len(os.listdir(".\\update\\full")) != 0:
#                 isRen = True
#                 fiasfile = ".\\update\\full\\" + os.listdir(".\\update\\full")[0]
#                 oldfiasfile = fiasfile + '.old'
#                 os.rename(fiasfile, oldfiasfile)
#             try:
#                 if download_fias_full(use_proxy, proxy):
#                     os.remove(oldfiasfile)
#                     del_delta_update()
#                 else:
#                     os.rename(oldfiasfile, fiasfile)
#             except Exception as inst:
#                 if isRen:
#                     os.rename(oldfiasfile, fiasfile)
#                 print(inst)
#             return
#     else:
#         # делаем все проверки
#         if config['Update']['fullbase'] == '':
#             # в конфиге дата отсутствует. Надо брать полную последнюю базу
#             if len(os.listdir(".\\update\\full")) != 0:
#                 # в каталоге полной базы есть файлы
#                 if os.path.isfile('.\\update\\full\\fias_dbf.rar'):
#                     # получаем размер локального файла
#                     localfilesize = os.path.getsize('.\\update\\full\\fias_dbf.rar')
#                     # на диске есть файл fias_dbf.rar - определяем за какую он дату
#                     spisok = client.service.GetAllDownloadFileInfo()
#                     # rint(spisok)
#                     check_fias = False
#                     for row in spisok:
#                         remotefilesize = getRemoteFileLength(row.FiasCompleteDbfUrl, use_proxy, proxy)
#                         if localfilesize == remotefilesize:
#                             # нашли дату
#                             check_fias = True
#                             du = datetime.datetime.strptime((row.FiasCompleteDbfUrl).split('/')[-2], "%Y%m%d").date()
#                             config['Update']['fullbase'] = du.strftime("%Y%m%d")
#                             config.write()
#                             break
#                     if check_fias:
#                         # нашли дату полной базы для файла на диске
#                         print('нашли дату полной базы для файла на диске ' + du.strftime("%Y%m%d"))
#                         get_delta(spisok, du, use_proxy, proxy)
#                     else:
#                         # не нашли дату полной базы для файла на диске
#                         print('не нашли дату полной базы для файла на диске')
#                 else:
#                     print('файл есть но он не fias_dbf.rar')
#             else:
#                 # Скачиваем файл http://fias.nalog.ru/Public/Downloads/Actual/VerDate.txt определяем дату полной базы
#                 if os.path.isfile('.\\update\\VerDate.txt'):
#                     os.remove('.\\update\\VerDate.txt')
#                 str_lastupdatedate = open(getFile('http://fias.nalog.ru/Public/Downloads/Actual/VerDate.txt', use_proxy, '.\\update\\', proxy), 'r').read()
#                 d_lastupdate = datetime.datetime.strptime(str_lastupdatedate, "%d.%m.%Y").date()
#                 sd = d_lastupdate.strftime("%Y%m%d")
#                 # в каталоге с архивом полной базы файлов нет. Берем качаем последнюю доступную
#                 url_fb = 'http://fias.nalog.ru/Public/Downloads/Actual/fias_dbf.rar'
#                 remotefilesize = getRemoteFileLength(url_fb, use_proxy, proxy)
#                 getFile(url_fb, use_proxy, '.\\update\\full\\', proxy, '.' + sd + '.tmp')
#                 # localfilesize = os.path.getsize(getFile(url_fb, use_proxy, '.\\update\\full\\', proxy, '.' + sd + '.tmp'))
#                 if os.path.isfile('.\\update\\full\\fias_dbf.rar'):
#                     print('скачали полную базу ФИАС за %s' % d_lastupdate.strftime("%d/%m/%Y"))
#                     config['Update']['fullbase'] = sd
#                     config.write()
#                 else:
#                     print('не скачали полностью полную базу ФИАС за %s' % d_lastupdate.strftime("%d/%m/%Y"))
#             # print(os.listdir(".\\update\\full"))
#         else:
#             # есть в конфиге последняя дата. Надо проверить нахождение файла на диске
#             # дальше приступаем к проверке дельта обновлений
#             full_base_update_date = datetime.datetime.strptime(config['Update']['fullbase'], '%Y%m%d').date()
#             if os.path.isfile('.\\update\\full\\fias_dbf.rar'):
#                 localfilesize = os.path.getsize('.\\update\\full\\fias_dbf.rar')
#                 # 'http://fias.nalog.ru/Public/Downloads/20180705/fias_dbf.rar'
#                 remotefilesize = getRemoteFileLength('http://fias.nalog.ru/Public/Downloads/' + full_base_update_date.strftime("%Y%m%d") + '/fias_dbf.rar', use_proxy, proxy)
#                 if localfilesize == remotefilesize:
#                     # размер локального и удаленного файла совпадают
#                     spisok = client.service.GetAllDownloadFileInfo()
#                     # качаем дельты, если они есть
#                     get_delta(spisok, full_base_update_date, use_proxy, proxy)
#                 else:
#                     # размер локального и удаленного файла не совпадают
#                     isRen = False
#                     if len(os.listdir(".\\update\\full")) != 0:
#                         isRen = True
#                         fiasfile = ".\\update\\full\\" + os.listdir(".\\update\\full")[0]
#                         oldfiasfile = fiasfile + '.old'
#                         os.rename(fiasfile, oldfiasfile)
#                     try:
#                         if download_fias_full(use_proxy, proxy):
#                             os.remove(oldfiasfile)
#                             del_delta_update()
#                         else:
#                             os.rename(oldfiasfile, fiasfile)
#                     except Exception as inst:
#                         if isRen:
#                             os.rename(oldfiasfile, fiasfile)
#                         print(inst)

if __name__ == '__main__':
    main()
