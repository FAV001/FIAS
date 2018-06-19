# Получение файлов обновлений FIAS с сайта налоговой
from configobj import ConfigObj
 
def main():
    config = ConfigObj('fias.cfg')
    proxy_list = config['Proxy']['Proxy']
    proxy = {'http': 'http://' + proxy_list}

if __name__ == '__main__':
    main()
