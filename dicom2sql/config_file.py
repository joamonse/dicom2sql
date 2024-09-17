from pathlib import Path
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

    def get_last_file(self) -> Path | int | None:
        if not self.config_file.exists():
            return None

        with self.config_file.open() as f:
            return Path(f.read())

    def set_last_file(self, file: Path | int) -> None:
        with self.config_file.open('w') as f:
            f.write(str(file))

    def remove(self) -> None:
        self.config_file.unlink(missing_ok=True)
