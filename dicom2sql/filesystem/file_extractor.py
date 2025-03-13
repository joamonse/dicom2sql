from pathlib import Path
from typing import Generator

from dicom2sql.config_file import ConfigFile
from .dcmfile import DcmFile


class FileExtractor:
    def __init__(self, files_path: Path, preload_files: int=30, parallel_process: int=10):
        self.config_file = ConfigFile(files_path)
        self.file_generator = self._get_files_from_list(files_path) if files_path.is_file() else self._get_files_from_path(files_path)
        self.actual_file = 0
        self.max_file = 0
        self.file_buffer = [None] * preload_files

    def files(self) -> Generator[DcmFile, None, None]:
        with self.config_file:
            for f in self.file_generator:
                yield f
            self.config_file.remove()

    def _get_files_from_list(self, save_file: Path) -> Generator[DcmFile, None, None]:
        with open(save_file) as file_list:
            last_line = self.config_file.get_last_file()
            if not last_line:
                last_line = 0

            for i in range(last_line):
                file_list.readline()

            for line in file_list:
                path = Path(line.strip())
                context = DcmFile(self.config_file, path)
                yield context

                last_line += 1

    def _get_files_from_path(self, root: Path) -> Generator[DcmFile, None, None]:

        last_file = self.config_file.get_last_file()
        if last_file:
            searched_files = _generate_directory_list(last_file, root)
        else:
            searched_files = [root]

        while len(searched_files) > 0:
            current_file = searched_files.pop(0)

            if current_file.is_file():
                context = DcmFile(self.config_file, current_file)
                yield context
                continue

            next_files = sorted(list(current_file.iterdir()), key=str, reverse=True)
            searched_files = next_files + searched_files

def _generate_directory_list(last_file: Path, root: Path):
    files = [last_file]
    while last_file != root:
        parent = last_file.parent
        next_files = sorted(list(parent.iterdir()), key=str, reverse=True)
        index = next_files.index(last_file)
        files += next_files[index + 1:]
        last_file = parent
    return files
