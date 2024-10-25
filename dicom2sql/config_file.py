from pathlib import Path
from types import TracebackType

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

    def __enter__(self) -> None:
        self.file = self.config_file.open('w')

    def __exit__(self, exc_type: type[BaseException] | None,
                 exc_val: BaseException | None,
                 exc_tb: TracebackType | None
                 ) -> None:
        self.file.close()

    def get_last_file(self) -> Path | int | None:
        if not self.config_file.exists():
            return None

        with self.config_file.open() as f:
            value = f.read()
        try:
            return int(value)
        except ValueError:
            return Path()

    def set_last_file(self, file: Path | int) -> None:
        self.file.seek(0)
        self.file.truncate()
        self.file.write(str(file) + "\n")
        # Force a write to disk to keep the file up-to-date in case of a crash
        self.file.flush()

    def remove(self) -> None:
        self.config_file.unlink(missing_ok=True)
