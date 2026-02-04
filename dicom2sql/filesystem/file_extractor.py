from __future__ import annotations

import logging
import queue
import threading
from pathlib import Path
from time import sleep
from typing import Generator

from dicom2sql.config_file import ConfigFile
from .dcmfile import DcmFile


class FileExtractor:
    def __init__(self, files_path: Path, preload_files: int=30, workers: int=10):
        self.config_file = ConfigFile(files_path)
        self.file_generator = self._get_files_from_list(files_path) if files_path.is_file() else self._get_files_from_path(files_path)
        self.max_buffer_size = preload_files
        self.file_buffer:queue.Queue[tuple[DcmFile,threading.Event]] = queue.Queue(maxsize=preload_files)
        self.max_workers = workers
        self.pipeline:queue.Queue[tuple[DcmFile,threading.Event]] = queue.Queue(maxsize=preload_files)
        self.quit_event = threading.Event()
        self.files_prepared = threading.Event()
        self.workers = [threading.Thread(target=self._files_provider, daemon=True)]+[threading.Thread(target=self._load_file, daemon=True) for _ in range(1,self.max_workers)]


    def files(self) -> Generator[DcmFile, None, None]:
        with self.config_file:
            for w in self.workers:
                w.start()

            while not self.file_buffer.empty() or not self.files_prepared.is_set():
                try:
                    f, loaded_event = self.file_buffer.get(block=False)
                except queue.Empty:
                    sleep(0.001)
                    continue

                loaded_event.wait()
                logging.getLogger("dicom2sql").info(f'Provided new DICOM. File queue:{self.file_buffer.qsize()}, paths queue: {self.pipeline.qsize()}')

                yield f

            self.quit_event.set()
            self.config_file.remove()


    def _files_provider(self):
        logging.getLogger("dicom2sql").debug(f'Starting provider thread')
        for f in self.file_generator:
            logging.getLogger("dicom2sql").debug(f'getting file {f} in provider')
            event = threading.Event()
            self.file_buffer.put((f, event))
            self.pipeline.put((f, event))
        logging.getLogger("dicom2sql").debug(f'File exploration done in provider')
        self.files_prepared.set()


    def _load_file(self):
        logging.getLogger("dicom2sql").debug(f'Starting consumer thread')
        while not self.quit_event.is_set() or not self.pipeline.empty():
            job, event = self.pipeline.get()
            logging.getLogger("dicom2sql").debug(f'Loading file {job} in consumer')
            job.load()
            event.set()
        logging.getLogger("dicom2sql").debug(f'Consumer done')


    def _get_files_from_list(self, save_file: Path) -> Generator[DcmFile, None, None]:
        with open(save_file) as file_list:
            last_line = self.config_file.get_last_file()
            if not last_line:
                last_line = 0

            for i in range(last_line):
                file_list.readline()

            for line in file_list:
                path = Path(line.strip())
                context = DcmFile(self.config_file, path, last_line)
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
