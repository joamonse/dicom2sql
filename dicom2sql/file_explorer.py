from pathlib import Path
from typing import Generator

from dicom2sql.config_file import ConfigFile


def get_files(root: Path) -> Generator[Path]:
    config_file = ConfigFile(root)

    last_file = config_file.get_last_file()
    if last_file:
        searched_files = generate_directory_list(last_file, root)
    else:
        searched_files = [root]

    while len(searched_files) > 0:
        current_file = searched_files.pop(0)

        if current_file.is_file():
            config_file.set_last_file(current_file)
            yield current_file
            continue

        next_files = sorted(list(current_file.iterdir()), key=str, reverse=True)
        searched_files = next_files + searched_files

    config_file.remove()


def generate_directory_list(last_file: Path, root: Path):
    files = [last_file]
    while last_file != root:
        parent = last_file.parent
        next_files = sorted(list(parent.iterdir()), key=str, reverse=True)
        index = next_files.index(last_file)
        files += next_files[index + 1:]
        last_file = parent
    return files
