import datetime
import os
from pathlib import Path

import requests
import urllib3
from configobj import ConfigObj
from progressbar import (
    ETA,
    AdaptiveETA,
    AnimatedMarker,
    Bar,
    BouncingBar,
    Counter,
    FileTransferSpeed,
    FormatLabel,
    Percentage,
    ProgressBar,
    ReverseBar,
    RotatingMarker,
    SimpleProgress,
    Timer,
)
from requests import Session
from zeep import Client
from zeep.transports import Transport

urllib3.disable_warnings()
PROJECT_ROOT = Path(__file__).parents[1]
FOLDER_FULLUPDATE = PROJECT_ROOT / "./update/full"
FOLDER_DELTAUPDATE = PROJECT_ROOT / "./update/delta"
# ссылка на сервис получения обновлений сайт Налоговой
wsdl = "https://fias.nalog.ru/WebServices/Public/DownloadService.asmx?WSDL"
URL_VERDATA = 'https://fias.nalog.ru/Public/Downloads/Actual/VerDate.txt'
config = ConfigObj("fias.cfg", encoding="UTF8")
USE_PROXY = config.get("Proxy").as_bool("use_proxy")


def main():
    def getRemoteFileLength(link, useproxy, proxy=None, start_pos=None):
        head = {
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36"
        }
        if start_pos is not None:
            resume_header = {"Range": "bytes=%d-" % start_pos}
        else:
            resume_header = {"Range": "bytes=0-"}
        if useproxy:
            r = requests.get(
                link,
                stream=True,
                proxies=proxy,
                verify=False,
                headers={**head, **resume_header},
                timeout=10,
            )
        else:
            r = requests.get(
                link,
                stream=True,
                verify=False,
                headers={**head, **resume_header},
                timeout=10,
            )
        if r.status_code == 206 or r.status_code == 200:
            return int(r.headers["Content-Length"])
        else:
            return -1

    def getFile(link, useproxy, dest=None, proxy=None, temp_part=None, start_pos=None):
        head = {
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36"
        }
        chunk_size = 1024 * 1024
        if start_pos is not None:
            resume_header = {"Range": "bytes=%d-" % start_pos}
            cur_pos = start_pos
        else:
            resume_header = {"Range": "bytes=0-"}
            cur_pos = 0
        if useproxy:
            r = requests.get(
                link,
                stream=True,
                proxies=proxy,
                verify=False,
                headers={**head, **resume_header},
                timeout=10,
            )
        else:
            r = requests.get(
                link,
                stream=True,
                verify=False,
                headers={**head, **resume_header},
                timeout=10,
            )
        if r.status_code == 206 or r.status_code == 200:
            if dest is not None:
                filename = Path(dest) / link.split("/")[-1]
            else:
                filename = link.split("/")[-1]  # + t
            remotefilesize = int(r.headers["Content-Length"])
            is_Download = True
            if cur_pos != remotefilesize:
                widgets = [
                    str(filename) + ": ",
                    Percentage(),
                    " ",
                    Bar(marker=RotatingMarker()),
                    " ",
                    ETA(),
                    " ",
                    FileTransferSpeed(),
                ]
                pbar = ProgressBar(widgets=widgets, maxval=remotefilesize).start()
                try:
                    with open(filename, "ab") as out:
                        try:
                            for data in r.iter_content(chunk_size=chunk_size):
                                out.write(data)
                                cur_pos += len(data)
                                pbar.update(cur_pos)
                        except Exception as identifier:
                            print(f" error !!!!! {identifier}")
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
            return "", -1, False

    def download_fias_full():
        ver_data = PROJECT_ROOT / "./update/VerDate.txt"
        if ver_data.is_file():
            ver_data.unlink()
        if USE_PROXY:
            proxy_list = config["Proxy"]["Proxy"]
            proxy = {"http": "http://" + proxy_list, "https": "https://" + proxy_list}
            filename, filelength, downloaded = getFile(
                URL_VERDATA,
                USE_PROXY,
                str(ver_data.parents[0]),
                proxy,
            )
        else:
            proxy = None
            filename, filelength, downloaded = getFile(
                URL_VERDATA,
                USE_PROXY,
                str(ver_data.parents[0]),
            )
        if downloaded:
            str_lastupdatedate = open(filename, "r").read()
        else:
            return False
        d_lastupdate = datetime.datetime.strptime(str_lastupdatedate, "%d.%m.%Y").date()
        sd = d_lastupdate.strftime("%Y.%m.%d")
        config["Update"]["fullbase"] = sd
        try:
            config.write()
        except Exception as identifier:
            print("error " + identifier)
        url_fb = "https://fias.nalog.ru/Public/Downloads/Actual/fias_dbf.zip"
        remotefilesize = getRemoteFileLength(url_fb, USE_PROXY, proxy)
        filelength = 0
        while True:
            filename, filelength, downloaded = getFile(
                url_fb,
                USE_PROXY,
                str(FOLDER_FULLUPDATE),
                proxy,
                start_pos=filelength,
            )
            if downloaded:
                if filelength == remotefilesize:
                    return True
            else:
                if filelength == -1:
                    return False

    def del_delta_update(dir):
        for root, dirs, files in os.walk(dir, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))

    def get_delta(fias_spisok, data, useproxy, proxy=None):
        # качаем все файлы дельта обновлений начиная с даты data
        try:
            if config["Update"]["lastupdateid"] == "":
                lastupdateid = 0
            else:
                lastupdateid = int(config["Update"]["lastupdateid"])
        except:
            lastupdateid = 0
        maxid = lastupdateid
        for row in reversed(fias_spisok):
            print(row)
            row_data = datetime.datetime.strptime(row.Date, "%d.%m.%Y").date()
            if row.VersionId > maxid:
                maxid = row.VersionId
            if row_data > data:
                row_sdate = row_data.strftime("%Y.%m.%d")
                delta_dir = FOLDER_DELTAUPDATE / row_sdate
                delta_file = delta_dir / "fias_delta_dbf.zip"
                remotefilesize = getRemoteFileLength(
                    row.FiasDeltaDbfUrl, useproxy, proxy
                )
                #     # print('Дата обновления (%s) > даты базы (%s)' % (row_data, data))
                #     # проверяем существет ли каталог по дате
                if (delta_dir).exists():
                    # каталог существует, проверим наличие файлов
                    if (delta_file).exists():
                        # файл обновления существует
                        localfilesize = delta_file.stat().st_size
                        if localfilesize == remotefilesize:
                            print(
                                "Дельта обновление за %s на диске - стус - pass"
                                % row_data
                            )
                            continue 
                        else:
                            print(
                                "Дельта обновление за %s на диске - стус - fail"
                                % row_data
                            )
                            try:
                                delta_file.unlink()
                                print(f"Удалили файл {str(delta_file)}")
                            except Exception as e:
                                print(
                                    f"При удалении файл {str(delta_file)} возникла ошибка {e}"
                                )
                else:
                    # каталога нет, качаем файл
                    delta_dir.mkdir(parents=True, exist_ok=True)
                filelength = 0
                while True:
                    filename, filelength, downloaded = getFile(
                        row.FiasDeltaDbfUrl,
                        USE_PROXY,
                        str(delta_dir),
                        proxy,
                        start_pos=filelength,
                    )
                    if downloaded:
                        if filelength == remotefilesize:
                            break
                            # return True
                    else:
                        if filelength == -1:
                            break
                            # return False
        config["Update"]["lastupdateid"] = maxid
        try:
            config.write()
        except Exception as identifier:
            print("error " + identifier)

    fiasfile = FOLDER_FULLUPDATE / "fias_dbf.zip"
    if USE_PROXY:
        proxy_list = config["Proxy"]["Proxy"]
        proxy = {"http": "http://" + proxy_list, "https": "https://" + proxy_list}
        session = Session()
        session.verify = False
        session.proxies = proxy
        transport = Transport(session=session)
        client = Client(wsdl=wsdl, transport=transport)
    else:
        proxy = None
        client = Client(wsdl=wsdl)
    spisok = client.service.GetAllDownloadFileInfo()

    # удаляем все файлы в каталоге ".\\update\\full" крому fias_dbf.zip
    for file in FOLDER_FULLUPDATE.iterdir():
        try:
            if file.name != "fias_dbf.zip":
                file.unlink()
                print(f"Удалили файл {file.name}")
        except Exception as e:
            print(f"При удалении файл {file.name} возникла ошибка {e}")

    # Проверяем полную базу
    if config["Update"]["fullbase"] == "":
        # в конфиге дата отсутствует. Надо брать полную последнюю базу. Удаляем все остальное
        if fiasfile.is_file():
            try:
                fiasfile.unlink()
            except Exception as identifier:
                print(f"При удалении файл {fiasfile} возникла ошибка {identifier}")
        if download_fias_full():
            print(
                "Закачали полную базу FIAS за дату {}".format(
                    config["Update"]["fullbase"]
                )
            )
    else:
        # дата в конфиге есть. Проверяем то что есть на диске и то что есть на сервере
        if fiasfile.exists():
            # получаем размер локального файла
            localfilesize = fiasfile.stat().st_size
            remotefilesize = getRemoteFileLength(
                "https://fias-file.nalog.ru/downloads/"
                + config["Update"]["fullbase"]
                + "/fias_dbf.zip",
                USE_PROXY,
                proxy,
            )
            if remotefilesize == -1:
                for file in FOLDER_FULLUPDATE.iterdir():
                    try:
                        if file.name != "fias_dbf.zip":
                            file.unlink()
                            print(f"Удалили файл {file.name}")
                    except Exception as e:
                        print(f"При удалении файл {file.name} возникла ошибка {e}")
                if download_fias_full():
                    print(
                        "Закачали полную базу FIAS за дату {}".format(
                            config["Update"]["fullbase"]
                        )
                    )
            else:
                if localfilesize != remotefilesize:
                    while True:
                        filename, localfilesize, downloaded = getFile(
                            "https://fias-file.nalog.ru/downloads/"
                            + config["Update"]["fullbase"]
                            + "/fias_dbf.zip",
                            USE_PROXY,
                            str(FOLDER_FULLUPDATE),
                            proxy,
                            start_pos=localfilesize,
                        )
                        if downloaded and localfilesize == remotefilesize:
                            print("Скачали файл {}".format(filename))
                            break
        else:
            # дата в конфиге есть, файла на диске нет
            if download_fias_full():
                print(
                    "Закачали полную базу FIAS за дату {}".format(
                        config["Update"]["fullbase"]
                    )
                )

    # дельта обновления
    maxdeltaupdate = config["Update"].as_int("maxdeltaupdate")
    if maxdeltaupdate != 0:  # при 0 не используем любое количество delta обновлений
        currentdeltaupdate = len(list(FOLDER_DELTAUPDATE.iterdir()))
        if currentdeltaupdate >= maxdeltaupdate:
            if fiasfile.is_file():
                try:
                    fiasfile.unlink()
                except Exception as identifier:
                    print(f"При удалении файл {fiasfile} возникла ошибка {identifier}")
            # del_delta_update(str(PROJECT_ROOT / './update/delta'))
            # print('Удалили все дельта обновления')
            if download_fias_full():
                print(
                    "Закачали полную базу FIAS за дату {}".format(
                        config["Update"]["fullbase"]
                    )
                )
                del_delta_update(str(FOLDER_DELTAUPDATE))
                print("Удалили все дельта обновления")
    full_base_update_date = datetime.datetime.strptime(
        config["Update"]["fullbase"], "%Y.%m.%d"
    ).date()
    get_delta(spisok, full_base_update_date, USE_PROXY, proxy)


if __name__ == "__main__":
    main()
