from pathlib import Path
from typing import Iterator
from platformdirs import user_config_dir


class ConfigFile:
    def __init__(self, root: Path) -> None:
        config_path = Path(user_config_dir(appname='dicom2sql', appauthor=False))
        config_path.mkdir(exist_ok=True)
        assert str(config_path) != '', 'Error getting user folder'
        root = root.resolve()
        self.config_file = config_path / '_'.join(['root' + root.parts[0].replace('/', '_')
                                                  .replace('\\', '_')
                                                  .replace(':', '_'), *root.parts[1:]])

    def get_last_file(self) -> Path | None:
        if not self.config_file.exists():
            return None

        with self.config_file.open() as f:
            return Path(f.read())

    def set_last_file(self, file: Path) -> None:
        with self.config_file.open('w') as f:
            f.write(str(file))

    def remove(self) -> None:
        self.config_file.unlink(missing_ok=True)


def get_files(root: Path) -> Iterator[Path]:
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

        next_files = sorted(list(current_file.iterdir()), key=str)
        searched_files = next_files + searched_files

    config_file.remove()


def generate_directory_list(last_file: Path, root: Path):
    files = [last_file]
    while last_file != root:
        parent = last_file.parent
        next_files = sorted(list(parent.iterdir()), key=str)
        index = next_files.index(last_file)
        files += next_files[index + 1:]
        last_file = parent
    return files
