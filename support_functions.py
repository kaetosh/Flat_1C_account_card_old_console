# -*- coding: utf-8 -*-
"""
Created on Thu Jul 31 09:27:31 2025

@author: a.karabedyan
"""

import zipfile
from io import BytesIO
from pathlib import Path
from typing import List
from custom_errors import PermissionFileExcelError
import math
from colorama import init, Fore, Style
init(autoreset=True)

def write_df_in_chunks(writer, df, base_sheet_name, MAX_ROWS = 1_000_000):
    n_rows = len(df)
    if n_rows == 0:
        return
    n_chunks = math.ceil(n_rows / MAX_ROWS)
    for i in range(n_chunks):
        start_row = i * MAX_ROWS
        end_row = min((i + 1) * MAX_ROWS, n_rows)
        sheet_name = base_sheet_name if n_chunks == 1 else f"{base_sheet_name}{i + 1}"
        chunk_df = df.iloc[start_row:end_row]
        chunk_df.to_excel(writer, sheet_name=sheet_name, index=False)

def print_instruction_color():
    print(Fore.CYAN + "="*60)
    print(Fore.CYAN + "Обработчик Карточки счета 1С".center(60))
    print(Fore.CYAN + "формирует плоскую таблицу из отдельных регистров".center(60))
    print(Fore.CYAN + "или из нескольких в пакетном режиме".center(60))
    print(Fore.CYAN + "="*60 + "\n")

    # print("Функции скрипта:")
    # print(" - Формирует плоскую таблицу из отдельных регистров или из нескольких в пакетном режиме.\n")
    
    print(Fore.YELLOW + "Режимы работы:")
    print(Fore.YELLOW + " 1) Обработка регистров по отдельности:")
    print(Style.RESET_ALL + "    - Перетягивайте файл в окно программы.")
    print("    - Обработанные регистры будут открываться в отдельном Excel-файле.\n")
    
    print(Fore.YELLOW + " 2) Пакетная обработка (сводная таблица):")
    print(Style.RESET_ALL + "    - Перетягивайте папку с файлами в окно программы.")
    print("    - Результаты будут расположены на одном листе Excel-файла.\n")
    
    print(Fore.YELLOW + "Поддерживаемые версии 1С и особенности:")
    print(Style.RESET_ALL + " 1) Конфигурация \"Управление производственным предприятием\" (1С 8.3):")
    print("    - Заголовки столбцов: |Дата|Документ|Операция|\n")
    
    print(" 2) Конфигурации \"Бухгалтерия предприятия\", \"1С: ERP Агропромышленный комплекс\",")
    print("    \"1С: ERP Управление предприятием 2\":")
    print("    - Заголовки столбцов: |Период|Документ|Аналитика Дт|Аналитика Кт|\n")

    print(" 3) Прочие конфигурации, если заголовки соответствуют п.1 или п.2.\n")

    print(Fore.YELLOW + "Особенности пакетной обработки:")
    print(Style.RESET_ALL + " - Результаты сохраняются на отдельных листах Excel-файла с названиями UPP и Non_UPP.")
    print(Style.RESET_ALL + " - Результаты, превышающие 1 млн. строк будут разбиты на доп. листы с соотв. индексами в названиях.\n")
    
    print(Fore.YELLOW + "Прочее:")
    print(Style.RESET_ALL + " - Время обработки объемных карточек может быть увеличено (500 тыс. строк ~ 2 мин)).\n")

    print(Fore.CYAN + "="*60)

def validate_paths(paths: List[Path]) -> bool:
    if not isinstance(paths, list) or not paths:
        return False  # не список или пустой список — False
    
    if len(paths) == 1:
        # Один элемент — может быть либо папкой, либо файлом
        p = paths[0]
        p=normalize_path(p)
        return isinstance(p, Path) and (p.is_dir() or p.is_file())
    else:
        # Несколько элементов — все должны быть файлами
        return all(isinstance(p, Path) and p.is_file() for p in paths)


def fix_1c_excel_case(file_path: Path):
    """Исправляет регистр имен файлов в xlsx-архиве 1С"""
    try:
        with zipfile.ZipFile(file_path, 'r') as z:
            # Создаем новый архив в памяти
            new_zip_data = BytesIO()
            with zipfile.ZipFile(new_zip_data, 'w') as new_z:
                for item in z.infolist():
                    # Исправляем регистр для sharedStrings.xml
                    if item.filename == 'xl/SharedStrings.xml':
                        new_name = 'xl/sharedStrings.xml'
                    else:
                        new_name = item.filename
                    # Копируем файл с новым именем
                    new_z.writestr(new_name, z.read(item))
        
        # Возвращаем исправленные данные
        new_zip_data.seek(0)
        return new_zip_data
    except PermissionError:
        raise PermissionFileExcelError(f"Возможно <{file_path}> открыт. Закройте его.\n")

def normalize_path(file_path: Path) -> Path:
    """
    Нормализует путь, возвращая длинное тире, так как при перетаскивании в
    консоль оно меняется на короткое.
    """
    if file_path.exists():
        return file_path
    else:
        dash_variants = ['-']
        for dash in dash_variants:
            file_path = str(file_path).replace(dash, '—')
        return Path(file_path)
 
    

        
