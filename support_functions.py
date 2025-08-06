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
 
    

        