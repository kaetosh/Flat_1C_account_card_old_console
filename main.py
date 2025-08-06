# -*- coding: utf-8 -*-
"""
Created on Wed Jul 30 17:31:15 2025

@author: a.karabedyan
"""



import os
import sys
import subprocess
from abc import ABC, abstractmethod
from typing import List
import pandas as pd
import numpy as np
import tempfile
import shlex
from pathlib import Path
from tqdm import tqdm
from colorama import init, Fore
import math

from support_functions import fix_1c_excel_case, normalize_path, validate_paths, print_instruction_color, write_df_in_chunks
from custom_errors import NoExcelFilesFoundError, RegisterProcessingError, NoRegisterFilesFoundError, IncorrectFolderOrFilesPath

init(autoreset=True)


class FileProcessor(ABC):
    """Абстрактный базовый класс для обработчиков файлов"""
    
    @abstractmethod
    def process_file(self, file_path: Path) -> pd.DataFrame:
        """Обработать файл и вернуть DataFrame"""
        pass

class UPPFileProcessor(FileProcessor):
    """Обработчик для файлов из 1С УПП"""
    
    @staticmethod
    def _fast_keep_first_unique_per_row(df):
        """
        Оставляет только первое вхождение значения в строке, остальные заменяет на NaN.
        Работает корректно для всех типов данных (str, int, float).
        """
        arr = df.values
        mask = np.ones_like(arr, dtype=bool)
        
        for i in range(arr.shape[0]):  # Идём по строкам
            seen = set()
            for j in range(arr.shape[1]):  # Идём по столбцам
                val = arr[i, j]
                if pd.isna(val):
                    continue  # Пропускаем NaN
                if val in seen:
                    mask[i, j] = False  # Помечаем как дубликат
                else:
                    seen.add(val)
        
        return pd.DataFrame(np.where(mask, arr, np.nan), columns=df.columns)
    
    @staticmethod
    def _process_dataframe_optimized(df):
        # Поиск строки с "Дата" через numpy (быстрее, чем str.contains)
        first_col = df.iloc[:, 0].astype(str).str.lower()
        mask = first_col.str.contains('дата')
        if mask.any():
            date_row_idx = mask.idxmax()
        else:
            raise RegisterProcessingError(Fore.RED + 'Файл не является карточкой счета 1с.\n')
        
        # Установка заголовков и очистка
        df.columns = df.iloc[date_row_idx].str.strip()
        df = df.iloc[date_row_idx + 1:].copy()  # .copy() избегает SettingWithCopyWarning
        
        # Преобразование даты (векторизовано)
        df['Дата'] = pd.to_datetime(df['Дата'], format='%d.%m.%Y', errors='coerce')

        
        # Добавляем порядковый номер к повторяющимся значениям
        mask = df['Документ'].notna()
        df.loc[mask, 'Документ'] = (
            df.loc[mask, 'Документ'] 
            + '_end' 
            + df.loc[mask].groupby('Документ').cumcount().add(1).astype(str)
        )
        
        # Заполнение пропусков (ffill) и удаление пустых строк

        df['Дата'] = df['Дата'].ffill()
        df['Документ'] = df['Документ'].infer_objects(copy=False)
        df['Документ'] = df['Документ'].ffill()
        
        # Переименовываем пустые или NaN заголовки
        df.columns = [
            f'NoNameCol {i+1}' if pd.isna(col) or col == '' else col 
            for i, col in enumerate(df.columns)
        ]
        df = df[df['Дата'].notna()].copy()
        df = df.dropna(how='all', axis=0).dropna(how='all', axis=1)
        
        return df
    
    def process_file(self, file_path: Path) -> pd.DataFrame:
        # Реализация обработки для УПП
        fixed_data = fix_1c_excel_case(file_path)
        df = pd.read_excel(fixed_data, header=None)
        df = df.dropna(axis=1, how='all')
        
        # Обработка DataFrame
        df = UPPFileProcessor._process_dataframe_optimized(df)

        if df.empty:
            raise RegisterProcessingError(Fore.RED + f"Карточка 1с пустая в файле {file_path.name}, обработка невозможна.\n")
        
        # Оптимизированный pivot (без медленного apply)
        
        operations_pivot = (
            df.assign(row_num=df.groupby(['Дата', 'Документ']).cumcount() + 1)
            .pivot_table(index=['Дата', 'Документ'], columns='row_num', values='Операция', aggfunc='first')
            .reset_index()
            .rename(columns=lambda x: f'Операция_{x}' if isinstance(x, int) else x)
        )

        operations_pivot = UPPFileProcessor._fast_keep_first_unique_per_row(operations_pivot)
        operations_pivot = operations_pivot.dropna(how='all', axis=0).dropna(how='all', axis=1)

        
        # Атрибуты документов (без дубликатов)
        doc_attributes = (
            df.drop_duplicates(subset=['Документ', 'Дебет', 'Кредит'])
            .set_index('Документ')
            .drop(columns=['Дата'])
        )

        # Слияние через join (быстрее merge для индексированных данных)
        result: pd.DataFrame = doc_attributes.join(operations_pivot.set_index('Документ'), how='left').reset_index()

        # Финальная очистка
        result = result.dropna(subset=['Дебет', 'Кредит'], how='all')
        result['Документ'] = result['Документ'].str.replace(r'_end\d+$', '', regex=True)
        result = result.dropna(how='all', axis=0).dropna(how='all', axis=1)

        new_columns = []
        cols = result.columns.tolist()
        for i, col in enumerate(cols):
            if str(col).startswith("NoNameCol"):
                if i == 0:
                    # Для первого столбца слева нет предыдущего, можно задать дефолтное имя
                    new_name = 'NoNameCol0'
                else:
                    left_col = cols[i - 1]
                    new_name = f'{left_col}_значение'
                new_columns.append(new_name)
            else:
                new_columns.append(col)

        result.columns = new_columns

        # updated_cols = result.columns.tolist()
        updated_cols = list(result.columns)  # Заменили .tolist() на list()
        updated_cols.insert(0, updated_cols.pop(updated_cols.index('Дата')))
        result = result[updated_cols]
        result = result.drop(columns=['Операция'], errors='ignore')
        result['Имя_файла'] = os.path.basename(file_path)
        updated_cols = ['Имя_файла'] + [col for col in result.columns if col != 'Имя_файла']
        result = result[updated_cols]
        
        return result

class NonUPPFileProcessor(FileProcessor):
    """Обработчик для файлов из 1С (не УПП)"""

    @staticmethod
    def _split_and_expand(df: pd.DataFrame, col_name: str, prefix: str) -> None:
        """Разбивает столбец с разделителем \n на несколько и добавляет их в df, удаляя исходный столбец."""
        if col_name not in df.columns:
            return
        new_cols = df[col_name].str.split('\n', expand=True)
        if new_cols.empty:
            df.drop(columns=[col_name], inplace=True)
            return
        new_cols.columns = [f'{prefix}_{i+1}' for i in range(new_cols.shape[1])]
        df[new_cols.columns] = new_cols
        df.drop(columns=[col_name], inplace=True)

    def process_file(self, file_path: Path) -> pd.DataFrame:
        fixed_data = fix_1c_excel_case(file_path)
        df = pd.read_excel(fixed_data, header=None)
        df.dropna(axis=1, how='all', inplace=True)

        # Поиск строки с заголовками
        index = df.index[df.iloc[:, 0] == 'Период'].tolist()
        if not index:
            # Можно вернуть пустой df или поднять исключение
            raise RegisterProcessingError(Fore.RED + 'Не найден заголовок Период в шапке таблицы')
        header_row = index[0]
        df.columns = df.iloc[header_row]
        df = df.iloc[header_row + 1:].reset_index(drop=True)

        # Фильтрация по дате
        dates = pd.to_datetime(df['Период'], format='%d.%m.%Y', errors='coerce')
        df = df.loc[dates.notna()].copy().reset_index(drop=True)

        # Разбиваем столбцы с \n
        self._split_and_expand(df, 'Документ', 'Документ')
        self._split_and_expand(df, 'Аналитика Дт', 'Аналитика Дт')
        self._split_and_expand(df, 'Аналитика Кт', 'Аналитика Кт')

        # Переименование пустых или NaN колонок
        new_columns = []
        cols = df.columns.tolist()
        for i, col in enumerate(cols):
            if pd.isna(col) or col == '':
                if i == 0:
                    new_columns.append('NoNameCol0')
                else:
                    new_columns.append(f'{cols[i - 1]}_значение')
            else:
                new_columns.append(col)
        df.columns = new_columns

        df.dropna(axis=0, how='all', inplace=True)
        df.dropna(axis=1, how='all', inplace=True)

        # Добавляем имя файла в начало
        file_name = os.path.basename(file_path)
        df.insert(0, 'Имя_файла', file_name)
        
        if df.empty:
            raise RegisterProcessingError(Fore.RED + f"Карточка 1с пустая в файле {file_path.name}, обработка невозможна. Файл не УПП\n")
        return df


class FileProcessorFactory:
    """Фабрика для создания обработчиков файлов"""
    
    @staticmethod
    def get_processor(file_path: Path) -> FileProcessor:
        """Определить тип файла и вернуть соответствующий обработчик"""
        # Логика определения типа файла (УПП или не УПП)
        type_register_1C = FileProcessorFactory._is_upp_or_not_upp_file(file_path)
        if type_register_1C == 'UPP':
            return UPPFileProcessor()
        elif type_register_1C == 'NonUPP':
            return NonUPPFileProcessor()
        else:
            raise RegisterProcessingError(f"Файл {file_path.name} не является корректной Карточкой счета из 1С.\n")


    @staticmethod
    def _is_upp_or_not_upp_file(file_path: Path) -> str:
        """Определить, является ли файл выгрузкой из УПП или не из УПП"""
        # Логика проверки
        
        fixed_data = fix_1c_excel_case(file_path)
        # Загружаем первые 50 строк без заголовка (чтобы не потерять структуру)
        df = pd.read_excel(fixed_data, header=None, nrows=50)
    
        # Приводим все значения к строкам и к нижнему регистру для надёжного поиска
        # Можно также убрать пробелы
        def normalize(cell):
            if pd.isna(cell):
                return ''
            return str(cell).strip().lower()
    
        # Искомые наборы ключей (в нижнем регистре)
        non_upp_keys = {'период', 'аналитика дт', 'аналитика кт'}
        upp_keys = {'дата', 'документ', 'операция'}
    
        for idx, row in df.iterrows():
            row_values = set(normalize(cell) for cell in row)
            if upp_keys.issubset(row_values):
                return "UPP"
            if non_upp_keys.issubset(row_values):
                return "NonUPP"
    
        # Если ни одна строка не содержит нужных ключей
        return "Invalid"

class ExcelValidator:
    """Валидатор Excel файлов"""
    
    @staticmethod
    def is_valid_excel(file_path: Path) -> bool:
        """Проверить, что файл является корректным Excel файлом"""
        return file_path.suffix.lower() == '.xlsx'

class FileHandler:
    """Обработчик файлов и папок"""
    
    def __init__(self, verbose: bool = True):
        self.validator = ExcelValidator()
        self.processor_factory = FileProcessorFactory()
        self.verbose = verbose
        self.not_correct_files = []
        self.storage_processed_registers = dict()
    
    def handle_input(self, input_path: Path) -> None:
        """Обработать ввод пользователя (файл или папка)"""

        if os.path.isfile(input_path):
            self._process_single_file(input_path)
        elif os.path.isdir(input_path):
            self._process_directory(input_path)
    
    def _process_single_file(self, file_path: Path) -> None:
        """Обработать одиночный файл"""
        if not self.validator.is_valid_excel(file_path):
            self.not_correct_files.append(file_path.name)
            return

        processor = self.processor_factory.get_processor(file_path)
        if self.verbose:
            print('Файл в обработке...', end='\r')
        result = processor.process_file(file_path)
        self.storage_processed_registers[file_path.name]=result
        # self._save_and_open_result(result)
    
    def _process_directory(self, dir_path: Path) -> None:
        """Обработать все файлы в папке"""
        # Сохраняем текущее значение verbose
        original_verbose = self.verbose
        self.verbose = False  # Отключаем вывод при обработке папки
        try:
            excel_files = self._get_excel_files(dir_path)
            upp_results = []
            non_upp_results = []

            # for file_path in excel_files:

            with tqdm(excel_files, leave=False) as pbar:
                for file_path in pbar:
                    pbar.set_description(f"Обработка файлов: {file_path.name}")
                    try:
                        processor = self.processor_factory.get_processor(file_path)
                        result = processor.process_file(file_path)
                    except RegisterProcessingError:
                        self.not_correct_files.append(file_path.name)
                        continue
                    except Exception:
                        self.not_correct_files.append(file_path.name)
                        continue

                    if isinstance(processor, UPPFileProcessor):
                        upp_results.append(result)
                    else:
                        non_upp_results.append(result)
                        
            if upp_results or non_upp_results:
                df_pivot_upp = pd.concat(upp_results) if upp_results else pd.DataFrame()
                df_pivot_non_upp = pd.concat(non_upp_results) if non_upp_results else pd.DataFrame()
                self._save_combined_results(df_pivot_upp, df_pivot_non_upp)
            else:
                raise NoRegisterFilesFoundError(Fore.RED + 'В папке не найдены карточки счета 1С')
        finally:
            # Восстанавливаем оригинальное значение
            self.verbose = original_verbose
    

    
    @staticmethod
    def _get_excel_files(dir_path: Path) -> List[Path]:
        """Получить список Excel файлов в директории"""
        list_excel_files = [
                file_path
                for file_path in dir_path.iterdir()
                if file_path.is_file() and file_path.suffix.lower() == '.xlsx'
            ]
        if list_excel_files:
            return list_excel_files
        else:
            raise NoExcelFilesFoundError(Fore.RED + "В папке нет файлов Excel.")



    def _save_and_open_result(self, df: pd.DataFrame) -> None:
        """Сохранить результат обработки одиночного файла и открываем его"""
        # Создаём временный файл с расширением .xlsx
        if self.verbose:
            print('\nЗаписываем обработанную карточку в файл...\n')
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            temp_filename = tmp.name

        df.to_excel(temp_filename, index=False)
        if self.verbose:
            print('\nОткрываем файл...\n')
        if sys.platform == "win32":
            os.startfile(temp_filename)
        elif sys.platform == "darwin":
            subprocess.run(["open", temp_filename])
        else:
            subprocess.run(["xdg-open", temp_filename])
            

    def _save_and_open_batch_result(self, dfs: dict[str, pd.DataFrame]) -> None:
        """Сохранить результат обработки нескольких файлов в один Excel с листами и открыть его"""
        if self.verbose:
            print('Записываем обработанные данные в Excel файл...', end='\r')
    
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            temp_filename = tmp.name
    
        # Записываем все датафреймы на отдельные листы
        with pd.ExcelWriter(temp_filename, engine='openpyxl') as writer:
            for sheet_name, df in dfs.items():
                # Ограничиваем имя листа до 31 символа — ограничение Excel
                safe_sheet_name = sheet_name[:31]
                df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
    
        if self.verbose:
            print(f'Файл записан: {temp_filename}', end='\r')
            print(f'Открываем файл {temp_filename}', end='\r')
    
        if sys.platform == "win32":
            os.startfile(temp_filename)
            if self.verbose:
                print('Обработка завершена                                                      ')
        elif sys.platform == "darwin":
            subprocess.run(["open", temp_filename])
        else:
            subprocess.run(["xdg-open", temp_filename])


    @staticmethod
    def _save_combined_results(df_upp: pd.DataFrame = pd.DataFrame(), df_non_upp: pd.DataFrame = pd.DataFrame()) -> None:
        """Сохранить объединенные результаты с разбивкой на листы, если превышен лимит строк Excel"""
    
        # Создаём временный файл с расширением .xlsx
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            temp_filename = tmp.name
    
        print('Записываем сводные данные в файл...', end='\r')
    
        # Записываем DataFrame, разбивая на листы при необходимости
        with pd.ExcelWriter(temp_filename, engine='openpyxl') as writer:
            if not df_upp.empty:
                write_df_in_chunks(writer, df_upp, 'UPP')
            if not df_non_upp.empty:
                write_df_in_chunks(writer, df_non_upp, 'Non_UPP')
    
        # Открываем файл
        print('Открываем сводный файл...           ', end='\r')
        if sys.platform == "win32":
            os.startfile(temp_filename)
        elif sys.platform == "darwin":
            subprocess.run(["open", temp_filename])
        else:
            subprocess.run(["xdg-open", temp_filename])
        print('Обработка завершена                                                      ')


class UserInterface:
    """Класс для взаимодействия с пользователем"""

    @staticmethod
    def get_input() -> list[Path]:
        """Получить список путей к файлам/папкам от пользователя"""
        print(Fore.YELLOW + "\nПеретащите файл карточки 1С (.xlsx) или папку и нажмите Enter:")
        input_str = input().strip()
        input_str = input_str.replace('\\', '/')
        # Разбиваем строку с учётом кавычек
        paths_str = shlex.split(input_str)
        paths = [Path(p) for p in paths_str]
        if validate_paths(paths):
            return paths
        else:
            raise IncorrectFolderOrFilesPath(Fore.RED + 'Неверные пути к папке или файлу/файлам.')


def main():
    print_instruction_color()
    ui = UserInterface()
    file_handler = FileHandler()

    while True:
        try:
            input_paths = ui.get_input()
            # if input_paths[0].is_file():
            #     print('\nОбработанные файлы:')
            for input_path in input_paths:
                try:
                    file_handler.handle_input(normalize_path(input_path))
                except RegisterProcessingError as e:
                    # print(f'\n{e}\n')
                    file_handler.not_correct_files.append(input_path.name)
                    continue
                except Exception as e:
                    print(f'{e}')
                    # import traceback
                    # traceback.print_exc()
                    if input_path.is_file():
                        file_handler.not_correct_files.append(input_path.name)
                    continue
            
            if file_handler.storage_processed_registers:
                file_handler._save_and_open_batch_result(file_handler.storage_processed_registers)

        except Exception as e:
            print(f'{e}')
            # import traceback
            # traceback.print_exc()
        finally:
            if file_handler.not_correct_files:
                print(Fore.RED + 'Нижеследующие файлы .xlsx не распознаны как Карточки счета 1С:                 \n', end='\r')
                for i in file_handler.not_correct_files:
                    print(Fore.RED + f'    {i}')
                file_handler.not_correct_files.clear()
            if file_handler.storage_processed_registers:
                file_handler.storage_processed_registers.clear()

if __name__ == "__main__":
    main()
