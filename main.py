import yaml # pip install pyyaml
import time
import requests # pip install requests
import pandas as pd # pip install pandas
from datetime import datetime
import threading
import tempfile
pd.options.display.max_colwidth = 150
from lxml import html # pip3 install lxml

with open ('params.yaml', 'r', encoding='UTF-8') as f:
    params = yaml.safe_load(f)

user_name = params['login']
user_password = params['password']
login_url = params['login_url']
landing_url = params['landing_url']
export_url = params['export_url']


class Export():
    """

    """
    def __init__(self, full_path_to_input_file, start_time, sleep_time, chunk, verbose, path_to_file_to_export):

        self.start_time = start_time
        self.sleep_time = sleep_time
        self.chunk = chunk
        self.full_path_to_input_file = full_path_to_input_file
        self.path_to_file_to_export = path_to_file_to_export
        self.verbose = verbose

    def test_connection(self):

        # проверим соединение с  login_url
        try:
            login_payload = {"Login": user_name, "Password": user_password, "_csrf_token": "undefined"}  #
            session = requests.Session()
            session.post(url=login_url, data=login_payload)
            if self.verbose:
                print(f'соединение с {login_url} установлено')
        except Exception as context:
            text = f'ошибка соединения с {login_url}'
            print(f'comment= {text}, ce= {context}')
            return  text, context

        # проверим соединение с landing
        try:
            session.get(url=landing_url)
            if self.verbose:
                print(f'соединение с {landing_url} установлено')
        except Exception as context:
            text = f'ошибка соединения с {landing_url}'
            #print(f'comment= {text}, ce= {context}')
            return text, context

        # проверим корректность адреса export_url
        try:
            session.get(url=export_url)
            if self.verbose:
                print(f'соединение с {export_url} установлено')
        except Exception as context:
            text = f'ошибка соединения с {export_url}'
            #print(f'comment= {text}, ce= {context}')
            return text, context
        return None, None

    def send_file(self, file_to_export):
        """
        :param self:
        :param file_to_export:
        :return:
        """
        login_payload = {"Login": user_name, "Password": user_password, "_csrf_token": "undefined"}  #
        session = requests.Session()
        session.post(url=login_url, data=login_payload)

        # Зайдем на landing для получения токена
        response  = session.get(url=landing_url) #, cookies =session.cookies
        tree = html.fromstring(response.content)
        token = tree.xpath('/html/head/meta[@name="csrf-token"]/@content')[0] # альтернативный вариант token = tree.xpath('///meta[@name="csrf-token"]/@content')[0]

        cookie_got_dict = session.cookies.get_dict()
        cookie_key = [key for key in cookie_got_dict.keys()][0]
        cookie_value = cookie_got_dict[cookie_key]

        cookie = 'LastVisiteEmail=' + user_name + '; ' + cookie_key + '=' + cookie_value # 'LastVisiteEmail=aleksandr.Stalmakov%40waveaccess.ru; '
        headers = {
          'Cookie': cookie,
          'Origin': landing_url}
        payload={'_csrf_token': token}
        files = {'uploadFile': open(file_to_export,'rb')}

        session.post(url=export_url, headers=headers, files=files, data=payload) # вместо session можно и requests


    def chunker(self):
        """
        Данная функция формирует чанки файлов
        :return:
        """
        df = pd.read_csv(self.full_path_to_input_file)
        start_slice_point = 0
        end_slice_point = self.chunk
        last_time = False
        for i in range(0, len(df), self.chunk):
            temp_df = df[start_slice_point:end_slice_point]
            if end_slice_point < len(df):
                temp_file = f'admin_export_to_GS1_chunk_{start_slice_point}-{end_slice_point}.xlsx'
            else:
                temp_file = f'admin_export_to_GS1_chunk_{start_slice_point}_{len(df)}.xlsx'
                last_time = True
            if self.path_to_file_to_export != None:
                full_path = self.path_to_file_to_export + temp_file
                temp_df.to_excel(full_path, index=False)
                self.send_file(file_to_export=full_path)
            else:
                with tempfile.TemporaryDirectory() as tempdir:
                    if self.verbose:
                        print('создана временная директория', tempdir)
                    full_path = tempdir + '\\' + temp_file
                    temp_df.to_excel(full_path, index = False)
                    self.send_file(file_to_export = full_path)
            if self.verbose:
                print(f'файл {temp_file} отправлен.')
            start_slice_point = i + self.chunk
            end_slice_point = end_slice_point + self.chunk
            if not last_time:
                time.sleep(self.sleep_time*60)
        print(f'Экспорт файлов завершен. Результаты предварительной обработки см. по ссылке {landing_url}goods-import/list')
        print('-' * 50)
    def scheduller(self):
        text, context = self.test_connection()
        if text == None:
            now = datetime.now()
            run_at = datetime.fromisoformat(self.start_time)
            delay = (run_at - now).total_seconds()
            threading.Timer(delay, self.chunker ).start()
        else:
            print(f'{text} \n context: {context}')

if __name__ == '__main__':
    start_time = '2022-03-21T20:10:50'
    sleep_time = 10
    chunk = 2
    verbose = False

    full_path_to_input_file = 'D:/CRPT/2022.02_февраль/экспорт по рассписанию/GTIN_list_1.xlsx'
    path_to_file_to_export = 'D:/CRPT/2022.02_февраль/экспорт по рассписанию/export/'

    export = Export(
                    full_path_to_input_file=full_path_to_input_file,
                    path_to_file_to_export=path_to_file_to_export,
                    start_time=start_time,
                    sleep_time=sleep_time,
                    chunk=chunk,
                    verbose = verbose)

    export.scheduller()
