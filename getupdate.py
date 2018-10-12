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

def getFile(link, useproxy, dest=None, proxy=None, temp_part=None):
    chunk_size = 10240
    if useproxy == True:
        r = requests.get(link, stream=True, proxies=proxy)
        # pr = proxy['http']
        # auth=(pr.split('//'))[1].split('@')[0]
        # ur = (pr.split('//'))[0]+'//'+(pr.split('//'))[1].split('@')[1]
        # default_headers = urllib3.make_headers(proxy_basic_auth=auth)
        # urllib3.disable_warnings()
        # http = urllib3.ProxyManager(pr)#, headers=default_headers)
    else:
        # http = urllib3.PoolManager()
        r = requests.get(link, stream=True)
    t = (lambda t: temp_part if temp_part != None else '.tmp')(temp_part)
    if dest != None:
        filename = dest + link.split('/')[-1] + t
    else:
        filename = link.split('/')[-1] + t
    #r = http.request('GET', link, preload_content=False)
    remotefilesize = int(r.headers['Content-Length'])
    widgets = [filename + ': ', Percentage(), ' ', Bar(marker=RotatingMarker()),
               ' ', ETA(), ' ', FileTransferSpeed()]
    pbar = ProgressBar(widgets=widgets, maxval=remotefilesize).start()
    cur_pos = 0
    with open(filename, 'wb') as out:
        for data in r.iter_content(chunk_size=chunk_size):
            out.write(data)
            cur_pos += len(data)
            pbar.update(cur_pos)
        r.close()
    pbar.finish()
    # сравниваем сколько на диске размер файла и сколько должно быть. Если совпадает то переименовываем temp файл
    if remotefilesize == os.path.getsize(filename):
        newfile = filename[:-len(t)]
        os.rename(filename, newfile)
        return newfile
    else:
        print('размер исходного файла не совпадает с размером скаченного')
        return filename

def getRemoteFileLength(link, useproxy, proxy=None):
    if useproxy == True:
        return int(requests.get(link, stream=True, proxies=proxy).headers['Content-Length'])
    else:
        return int(requests.get(link, stream=True).headers['Content-Length'])

def get_delta(fias_spisok, data, useproxy, proxy=None):
    #качаем все файлы дельта обновлений начиная с даты data
    for row in fias_spisok:
        row_data = datetime.datetime.strptime((row.FiasCompleteDbfUrl).split('/')[-2], "%Y%m%d").date()
        if row_data > data:
            #print('Дата обновления (%s) > даты базы (%s)' % (row_data, data))
            #проверяем существет ли каталог по дате
            if os.path.isdir('.\\update\\delta\\' + row_data.strftime("%Y%m%d")):
                #каталог существует, проверим наличие файлов
                if os.path.isfile('.\\update\\delta\\' + row_data.strftime("%Y%m%d") + '\\fias_delta_dbf.rar'):
                    #файл обновления существует
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
                #каталога нет, качаем файл
                os.makedirs('.\\update\\delta\\' + row_data.strftime("%Y%m%d"))
                getFile(row.FiasDeltaDbfUrl, useproxy, '.\\update\\delta\\' + row_data.strftime("%Y%m%d") + '\\', proxy, row_data.strftime("%Y%m%d") + '.tmp')
    pass

def download_fias_full(use_proxy, proxy):
    global fiasfile
    global config
    if os.path.isfile('.\\update\\VerDate.txt'):
        os.remove('.\\update\\VerDate.txt')
    str_lastupdatedate = open(getFile('http://fias.nalog.ru/Public/Downloads/Actual/VerDate.txt', use_proxy, '.\\update\\', proxy),'r').read()
    d_lastupdate = datetime.datetime.strptime(str_lastupdatedate, "%d.%m.%Y").date()
    sd = d_lastupdate.strftime("%Y%m%d")
    #url_fb = 'http://fias.nalog.ru/Public/Downloads/Actual/fias_delta_dbf.rar'
    url_fb = 'http://fias.nalog.ru/Public/Downloads/Actual/fias_dbf.rar'
    remotefilesize = getRemoteFileLength(url_fb, use_proxy, proxy)
    getFile(url_fb, use_proxy, '.\\update\\full\\', proxy, sd + '.tmp')
    localfilesize = os.path.getsize(fiasfile)
    if localfilesize == remotefilesize:
        config['Update']['fullbase'] = sd
        try:
            config.write()
        except Exception as identifier:
            print('error ' + identifier)
        return True
    else:
        return False

def del_delta_update():
    for root, dirs, files in os.walk('.\\update\\delta\\', topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))
        for name in dirs:
            os.rmdir(os.path.join(root, name))

def main():
    global fiasfile
    global oldfiasfile
    global config
    fiasfile = '.\\update\\full\\fias_dbf.rar'
    oldfiasfile = '.\\update\\full\\fias_dbf.old.rar'
    wsdl = 'http://fias.nalog.ru/WebServices/Public/DownloadService.asmx?WSDL' #ссылка на сервис получения обновлений сайт Налоговой
    config = ConfigObj('fias.cfg', encoding='UTF8')
    use_proxy = config.get('Proxy').as_bool('use_proxy')
    if use_proxy == True:
        proxy_list = config['Proxy']['Proxy']
        #proxy = {'http': 'http://' + proxy_list}
        proxy = {'http': 'http://' + proxy_list, 'https': 'https://' + proxy_list,}
        session = Session()
        session.verify = False
        session.proxies = proxy
        transport = Transport(session=session)
        client = Client(wsdl=wsdl, transport=transport)
    else:
        client = Client(wsdl=wsdl)
    maxdeltaupdate = int(config['Update']['maxdeltaupdate'])
    if maxdeltaupdate != -1: # при -1 не используем любое количество delta обновлений
        currentdeltaupdate = len(os.listdir('.\\update\\delta\\'))
        if currentdeltaupdate > maxdeltaupdate:
            #необходимо закачать полную базу и удалить дельты
            isRen = False
            if len(os.listdir(".\\update\\full")) != 0:
                isRen = True
                fiasfile = ".\\update\\full\\" + os.listdir(".\\update\\full")[0]
                oldfiasfile = fiasfile + '.old'
                os.rename(fiasfile, oldfiasfile)
            try:
                if download_fias_full(use_proxy, proxy):
                    os.remove(oldfiasfile)
                    del_delta_update()
                else:
                    os.rename(oldfiasfile, fiasfile)
            except Exception as inst:
                if isRen:
                    os.rename(oldfiasfile, fiasfile)
                print(inst)
            return
    else:
        #делаем все проверки
        if config['Update']['fullbase'] =='':
            # в конфиге дата отсутствует. Надо брать полную последнюю базу
            if len(os.listdir(".\\update\\full")) != 0:
                #в каталоге полной базы есть файлы
                if os.path.isfile('.\\update\\full\\fias_dbf.rar'):
                    #получаем размер локального файла
                    localfilesize = os.path.getsize('.\\update\\full\\fias_dbf.rar')
                    #на диске есть файл fias_dbf.rar - определяем за какую он дату
                    spisok = client.service.GetAllDownloadFileInfo()
                    #print(spisok)
                    check_fias = False
                    for row in spisok:
                        remotefilesize = getRemoteFileLength(row.FiasCompleteDbfUrl, use_proxy, proxy)
                        if localfilesize == remotefilesize:
                            #нашли дату
                            check_fias = True
                            du = datetime.datetime.strptime((row.FiasCompleteDbfUrl).split('/')[-2], "%Y%m%d").date()
                            config['Update']['fullbase'] = du.strftime("%Y%m%d")
                            config.write()
                            break
                    if check_fias:
                        #нашли дату полной базы для файла на диске
                        print('нашли дату полной базы для файла на диске ' + du.strftime("%Y%m%d"))
                        get_delta(spisok, du, use_proxy, proxy)
                    else:
                        #не нашли дату полной базы для файла на диске
                        print('не нашли дату полной базы для файла на диске')
                else:
                    print('файл есть но он не fias_dbf.rar')
            else:
                #Скачиваем файл http://fias.nalog.ru/Public/Downloads/Actual/VerDate.txt определяем дату полной базы
                if os.path.isfile('.\\update\\VerDate.txt'):
                    os.remove('.\\update\\VerDate.txt')
                str_lastupdatedate = open(getFile('http://fias.nalog.ru/Public/Downloads/Actual/VerDate.txt', use_proxy, '.\\update\\', proxy),'r').read()
                d_lastupdate = datetime.datetime.strptime(str_lastupdatedate, "%d.%m.%Y").date()
                sd = d_lastupdate.strftime("%Y%m%d")
                #в каталоге с архивом полной базы файлов нет. Берем качаем последнюю доступную
                url_fb = 'http://fias.nalog.ru/Public/Downloads/Actual/fias_dbf.rar'
                remotefilesize = getRemoteFileLength(url_fb, use_proxy, proxy)
                getFile(url_fb, use_proxy, '.\\update\\full\\', proxy, '.' + sd + '.tmp')
                #localfilesize = os.path.getsize(getFile(url_fb, use_proxy, '.\\update\\full\\', proxy, '.' + sd + '.tmp'))
                if os.path.isfile('.\\update\\full\\fias_dbf.rar'):
                    print('скачали полную базу ФИАС за %s' % d_lastupdate.strftime("%d/%m/%Y"))
                    config['Update']['fullbase'] = sd
                    config.write()
                else:
                    print('не скачали полностью полную базу ФИАС за %s' % d_lastupdate.strftime("%d/%m/%Y"))
            #print(os.listdir(".\\update\\full"))
        else:
            #есть в конфиге последняя дата. Надо проверить нахождение файла на диске
            #дальше приступаем к проверке дельта обновлений
            full_base_update_date = datetime.datetime.strptime(config['Update']['fullbase'], '%Y%m%d').date()
            if os.path.isfile('.\\update\\full\\fias_dbf.rar'):
                localfilesize = os.path.getsize('.\\update\\full\\fias_dbf.rar')
                #'http://fias.nalog.ru/Public/Downloads/20180705/fias_dbf.rar'
                remotefilesize = getRemoteFileLength('http://fias.nalog.ru/Public/Downloads/' + full_base_update_date.strftime("%Y%m%d") + '/fias_dbf.rar', use_proxy, proxy)
                if localfilesize == remotefilesize:
                    #размер локального и удаленного файла совпадают
                    spisok = client.service.GetAllDownloadFileInfo()
                    #качаем дельты, если они есть
                    get_delta(spisok, full_base_update_date, use_proxy, proxy)
                else:
                    #размер локального и удаленного файла не совпадают
                    isRen = False
                    if len(os.listdir(".\\update\\full")) != 0:
                        isRen = True
                        fiasfile = ".\\update\\full\\" + os.listdir(".\\update\\full")[0]
                        oldfiasfile = fiasfile + '.old'
                        os.rename(fiasfile, oldfiasfile)
                    try:
                        if download_fias_full(use_proxy, proxy):
                            os.remove(oldfiasfile)
                            del_delta_update()
                        else:
                            os.rename(oldfiasfile, fiasfile)
                    except Exception as inst:
                        if isRen:
                            os.rename(oldfiasfile, fiasfile)
                        print(inst)

if __name__ == '__main__':
    main()
