import pandas as pd

from main import Export
import sys
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
# ОТКЛЮЧАЕТ ВОРНИНГИ НО ВОЗМОЖНО ЗАМЕДЛЯЕТ
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import argparse
import os
from  argparse import RawDescriptionHelpFormatter
import operator
import textwrap
import textwrap as _textwrap

# ГОТОВО
general_description = '''
Утилита экспорта в 'GS1 Rus по через панель администратора. 
--------------------------------------------------------------------------
 Перед запуском приложения в файле params.yaml необходимо задать параметры подключения к GS1 Rus и Консоли администратора Нац.Каталога.
Приложение разделяет исходный файл на части в соответствии с заданным аргументом chunk и экспортирует с указанной задержкой начиная с заданного времени.
Утилита не требует инсталляции и запуск осуществляется из командной строки. 
Пример команды: .\gs1_goods_export.exe file.xlsx.
С параметрами запуска приложения можно ознакомиться указав параметр -h. Пример команды  .\gs1_goods_export.exe -h
К исходному файлу и, если указан, к директории с готовыми для экспорта файлами допустимо указывать как абсолютный путь (например D:\\file.xlsx) ,
так и относительный. Например ..\\file.xlsx (для записи в директорию выше уровнем). '''




# ГОТОВО
def check_input_file_extension(input_file_full_path):
    if input_file_full_path !=None:
        extension = str(input_file_full_path[(len(input_file_full_path) - 4):])
        if extension == '.csv':
            current_function_result = True
        else:
            current_function_result = False
    else:
        current_function_result = True

    return current_function_result

# ГОТОВО
def check_input_file_format(input_file_full_path):
    if input_file_full_path != None:
        try:
            input_df = pd.read_csv(input_file_full_path)
            column_list = input_df.columns.values.tolist()

            if  'GTIN' in column_list and 'AccountId' in column_list:
                current_function_result = True
            else:
                current_function_result = False
        except Exception as e:
            print('!!!!!!!!', e)
            current_function_result = False
    else:
        current_function_result = True # считаем, что проверка пройдена. т.к. флаг скипнуть проверку отработает в пайплайне
    return current_function_result

def check_export_path(output_files_path):
    if output_files_path != None:
        current_function_result = os.path.isdir(output_files_path)
    else:
        current_function_result = True

    return current_function_result

# ГОТОВО
def check_chunk(chunk):
    if int(chunk) > 50000:
        current_function_result = False
    else:
        current_function_result = True
    return current_function_result

def preliminary_single_check(current_check_result, negative_output_message, previous_check_passed=True, previous_output_message='', skip_this_check=False):
    positive_output_message = 'Все предварительные проверки пройдены.'
    if skip_this_check == True and previous_check_passed == False:
        go_to_next_check = False
        output_message = previous_output_message
    elif skip_this_check == True and previous_check_passed == True:
        go_to_next_check = True
        output_message = positive_output_message
    elif previous_check_passed == True and current_check_result == True:
        go_to_next_check = True
        output_message = positive_output_message
    elif previous_check_passed == False:
        go_to_next_check = False
        output_message = previous_output_message
    elif current_check_result == False:
        go_to_next_check = False
        output_message = negative_output_message
    return go_to_next_check, output_message


def preliminary_check_set(args):

    go_to_next_check, output_message = preliminary_single_check(current_check_result=check_input_file_format(str(args.file.name)), #TODO для работы через консоль - вернуть атрибут
                                                                negative_output_message=' - входной файл должен содержать столбцы \'GTIN\' и \'AccountId\'')

    # проверка что указанный для сохранения экспортируемых файлов путь существует
    go_to_next_check, output_message = preliminary_single_check(previous_check_passed=go_to_next_check,
                                                                          current_check_result=check_export_path(args.path),
                                                                          previous_output_message=output_message,
                                                                          negative_output_message='Не верно указан путь для сохранения экспортируемых файлов')

    # проверка что chunk <= 50
    go_to_next_check, output_message = preliminary_single_check(previous_check_passed=go_to_next_check,
                                                                          current_check_result=check_chunk(args.chunk),
                                                                          previous_output_message=output_message,
                                                                          negative_output_message='значение chunk не должно превышать 50000')

    # проверка расширения выходного файла.  ЭТА ПРОВЕРКА  ДОЛЖНА БЫТЬ ПОСЛЕДНЕЙ. ЕЕ НЕЛЬЗЯ СКИПАТЬ!
    go_to_next_check, output_message = preliminary_single_check(previous_check_passed=go_to_next_check,
                                                                          current_check_result=check_input_file_extension(args.file.name), #TODO для работы через консоль - вернуть атрибут
                                                                          previous_output_message=output_message,
                                                                          negative_output_message=' - формат выходного файла должен быть .csv')

    # здесь закончились провеерки
    #########################################################################################

    all_checks_passed = go_to_next_check
    return all_checks_passed, output_message

# ГОТОВО
def launch_scheduller(args):
    all_checks_passed, output_message = preliminary_check_set(args)  ###################################################################################################################
    print('', )
    if all_checks_passed:
        print(output_message)
        export = Export(full_path_to_input_file = args.file.name, #TODO для работы через консоль - вернуть атрибут .name,
                        path_to_file_to_export = args.path,
                        start_time = args.start_time,
                        sleep_time = args.sleep_time,
                        chunk = args.chunk,
                        verbose = args.verbose,
                        offset = args.offset)

        export.delaystart()

    else:
        print('В процессе предварительных проверок обнаружены ошибки:\n')
        print(output_message)

def parse_args():

    '''
    def dir_path(string):
        if os.path.isdir(string):
            print('138: os.path.isdir(string)=', os.path.isdir(string))
            return string
        else:
            raise NotADirectoryError(string)
    '''

    parser = argparse.ArgumentParser(formatter_class=RawDescriptionHelpFormatter, description=general_description)

    parser.add_argument("file", type=argparse.FileType('r'), help='''Входной файл.''')
    parser.add_argument("-pt", "--path", help='''Директория для подготовленных к отправке файлов''', default=None, type=str,)
    parser.add_argument("-st", "--start_time",
                        help="Время начала экспорта. Формат: 'YYYY-MM-DDTHH:MM:SS'. Если время указано в прошлом или не указано, то экспорт начнется сразу после старта.",
                        default='2000-01-01T00:00:00', type=str)
    parser.add_argument("-sl", "--sleep_time", help="Время задержки между отправками. (мин.). Значение по умолчанию - 10", default=10, type=int)
    parser.add_argument("-ch", "--chunk", help="Размер чанка (порции для разбивки исходного файла). Значение по умолчанию - 10000", default=10000,  type=int) # без ACTION добавляет строчными буквами
    parser.add_argument("-of", "--offset", help="Смещение от начала исходнго файла. При продолжении экспорта чанками, необходимо указывать второе число из названия последнего отгруженного файла. "
                                                "Например, если отправлено 3 чанка по 10000 из 50000, то при следующем запуске данному аргументу необходимо передать 30000", type=int, default=0,)
    parser.add_argument("-vb", "--verbose", help="Отображать текущий процесс парсинга GS1 RUS", action='store_true')

    parser.set_defaults(func=launch_scheduller)

    return parser.parse_args()

# ГОТОВО
def main():
    args = parse_args()
    args.func(args)

# ГОТОВО. ФРАГМЕНТ ДЛЯ ОТЛАДКИ
if __name__ == "__main__":
    offset = 0
    start_time = '2022-04-06T21:00:00'
    sleep_time = 11
    chunk = 20
    verbose = False
    full_path_to_input_file = 'D:/CRPT/2022.02_февраль/экспорт по рассписанию/test.csv'
    #TODO добавить проверку для директории под экспорт
    path_to_file_to_export = 'D:/CRPT/2022.02_февраль/экспорт по рассписанию/export/'

    args = argparse.Namespace(
                    file=full_path_to_input_file,
                    path=path_to_file_to_export,
                    start_time=start_time,
                    sleep_time=sleep_time,
                    chunk=chunk,
                    verbose = verbose,
                    offset=offset)
    launch_scheduller(args)
