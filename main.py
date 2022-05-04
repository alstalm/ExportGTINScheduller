import yaml # pip install pyyaml
import time
import requests # pip install requests
import pandas as pd # pip install pandas
from datetime import datetime
import threading
import tempfile
pd.options.display.max_colwidth = 150
from lxml import html # pip3 install lxml

with open('params.yaml', 'r', encoding='UTF-8') as f:
    params = yaml.safe_load(f)

user_name = params['login']
user_password = params['password']
login_url = params['login_url']
logout_url = params['logout_url']
landing_url = params['landing_url']
export_url = params['export_url']
xpath1 = params['xpath1']
xpath2 = params['xpath2']

class Export():
    """

    """
    def __init__(self, full_path_to_input_file, start_time, sleep_time, offset, chunk, verbose, path_to_file_to_export):

        self.start_time = start_time
        self.sleep_time = sleep_time
        self.offset = offset
        self.chunk = chunk
        self.full_path_to_input_file = full_path_to_input_file
        self.path_to_file_to_export = path_to_file_to_export
        self.verbose = verbose


    def get_token(self):

        try:
            tree = html.fromstring(self.response.content)
            list_of_possible_tokens = []
            try:
                token = tree.xpath(xpath1)
                list_of_possible_tokens.append(token)
            except:
                pass
            try:
                token = tree.xpath(xpath1)[0]
                list_of_possible_tokens.append(token)
            except:
                pass
            try:
                token = tree.xpath(xpath2)
                list_of_possible_tokens.append(token)
            except:
                pass
            try:
                token = tree.xpath(xpath2)[0]
                list_of_possible_tokens.append(token)
            except:
                token = None

            for i, token in enumerate(list_of_possible_tokens):
                if isinstance(token, str) and len(token)>0:
                    text = None
                    if self.verbose:
                        print(f'получен токен {i}: {token}')
                    break
                else:
                    token = None
                    text = 'Не удалось получить токен'

        except Exception as context:
            token = None
            text = 'ошибка получения токена'
            self.session.get(url=logout_url)

        return token, text

    def test_connection(self):
        # проверим соединение с  login_url
        try:
            login_payload = {"Login": user_name, "Password": user_password, "_csrf_token": "undefined"}  #
            self.session = requests.Session()

            self.session.post(url=login_url, data=login_payload)
            if self.verbose:
                print(f'соединение с {login_url} установлено')
        except Exception as context:
            text = f'ошибка соединения с {login_url}'
            print(f'comment= {text}, ce= {context}')
            return  text, context

        # проверим соединение с landing
        try:
            self.response = self.session.get(url=landing_url)
            #session.get(url=logout_url)
            if self.verbose and self.response.status_code == requests.codes.ok:
                print(f'соединение с {landing_url} установлено')
                #TODO возможно надо добавить else

        except Exception as context:
            text = f'ошибка соединения с {landing_url}'
            #print(f'comment= {text}, ce= {context}')
            return text, context


        # проверим корректность адреса export_url
        try:
            r = self.session.post(url=export_url)  # вместо session можно и requests ####################

            if r.status_code != 403:
                text = f'115: ошибка подключения к {export_url}'
                context = None
                if self.verbose:
                    print(f'status_code подключения к {export_url} = {r.status_code}')
                return text, context
            elif r.status_code == 403:
                text = None
                context = None
                if self.verbose:
                    print(f'status_code подключения к {export_url} = {r.status_code}')
                return text, context
            else:
                text = f'status_code подключения к {export_url} = {r.status_code}'
                context = None
                return text, context



        except Exception as context:
            text = f'ошибка соединения с {export_url}'
            #print(f'comment= {text}, ce= {context}')
            self.session.get(url=logout_url)
            return text, context

        # проверим получен ли токен
        token, text = Export.get_token(self) #
        #self.session.get(url=logout_url) #TODO Закоменчено
        ###############################################################################################################################################################################
        ################
        context = None
        return text, context

    def send_file(self,  file_to_export):
        """
        :param self:
        :param file_to_export:
        :return:
        """

        login_payload = {"Login": user_name, "Password": user_password, "_csrf_token": "undefined"}  #
        #session = requests.Session() #TODO здесь закоменчено
        #session.post(url=login_url, data=login_payload) #TODO здесь закоменчено

        # Зайдем на landing для получения токена
        #self.response  = session.get(url=landing_url) #, cookies =session.cookies #TODO здесь закоменчено

        #token, text = Export.get_token(self) #TODO здесь закоменчено

        cookie_got_dict = self.session.cookies.get_dict() #TODO здесь добавлен self
        cookie_key = [key for key in cookie_got_dict.keys()][0]
        cookie_value = cookie_got_dict[cookie_key]

        cookie = 'LastVisiteEmail=' + user_name + '; ' + cookie_key + '=' + cookie_value # 'LastVisiteEmail=aleksandr.Stalmakov%40waveaccess.ru; '
        headers = {
          'Cookie': cookie,
          'Origin': landing_url}

        token, text = Export.get_token(self) #TODO добавлена строка

        payload={'_csrf_token': token} #TODO здесь добавлен self
        files = {'uploadFile': open(file_to_export,'rb')}

        self.session.post(url=export_url, headers=headers, files=files, data=payload) #TODO здесь добавлен self
        self.session.get(url=logout_url) #TODO здесь добавлен self

    def chunker(self):
        """
        Данная функция формирует чанки файлов
        :return:
        """


        df = pd.read_csv(self.full_path_to_input_file)
        start_slice_point = self.offset
        end_slice_point = start_slice_point + self.chunk
        last_time = False
        for i in range(self.offset, len(df), self.chunk):
            error_message, context = self.test_connection()
            if error_message == None:


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
                    print(f'файл {temp_file} отправлен. {datetime.now()} \n')
                start_slice_point = i + self.chunk
                end_slice_point = end_slice_point + self.chunk
                if not last_time:
                    time.sleep(self.sleep_time*60)
            else:
                print(f'{error_message} \n context: {context}')
        print(f'Работа приложения завершена. Результаты предварительной обработки см. по ссылке {landing_url}goods-import/list')
        print('-' * 50)


    def delaystart(self):
        now = datetime.now()
        run_at = datetime.fromisoformat(self.start_time)
        delay = (run_at - now).total_seconds()
        threading.Timer(delay, self.chunker ).start()


if __name__ == '__main__':
    start_time = '2022-04-06T22:00:00'
    sleep_time = 15
    chunk = 500
    verbose = True
    offset = 0

    full_path_to_input_file = 'D:/CRPT/2022.03_март/december_export/export.csv'
    path_to_file_to_export = 'D:/CRPT/2022.03_март/december_export/result/'

    export = Export(
                    full_path_to_input_file=full_path_to_input_file,
                    path_to_file_to_export=path_to_file_to_export,
                    start_time=start_time,
                    sleep_time=sleep_time,
                    chunk=chunk,
                    verbose = verbose,
                    offset = offset)

    export.delaystart()