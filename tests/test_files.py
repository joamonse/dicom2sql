from dicom2sql.file_explorer import get_files, ConfigFile

import unittest
from pathlib import Path


class TestFiles(unittest.TestCase):
    def test_config(self):
        config_file = ConfigFile(Path('test'))
        self.assertEqual(config_file.get_last_file(), None)

        config_file.set_last_file(Path('/etc/'))
        self.assertEqual(config_file.get_last_file(), Path('/etc'))
        self.assertEqual(config_file.get_last_file(), Path('/etc/'))
        self.assertNotEqual(config_file.get_last_file(), Path('/etc/lib'))
        self.assertNotEqual(config_file.get_last_file(), Path('/home/'))

        config_file.remove()
        self.assertEqual(config_file.get_last_file(), None)

    def test_files_one_folder(self):
        ConfigFile(Path('test_folder/1')).remove()
        paths = [Path('test_folder/1/a'), Path('test_folder/1/b'), Path('test_folder/1/c'),
                 Path('test_folder/1/d'), Path('test_folder/1/e'), Path('test_folder/1/f')]
        for generator, test in zip(get_files(Path('test_folder/1')), paths):
            self.assertEqual(test, generator)

        for i, _ in enumerate(zip(get_files(Path('test_folder/1')), paths)):
            if i == 3:
                break

        for generator, test in zip(get_files(Path('test_folder/1')), paths[3:]):
            self.assertEqual(test, generator)

    def test_files_subfolder_folder(self):
        ConfigFile(Path('test_folder/2')).remove()
        paths = [Path('test_folder/2/1/a'), Path('test_folder/2/1/b'), Path('test_folder/2/1/c'),
                 Path('test_folder/2/1/d'), Path('test_folder/2/2/e'), Path('test_folder/2/2/f'),
                 Path('test_folder/2/2/g'), Path('test_folder/2/3/a'), Path('test_folder/2/3/b'),
                 Path('test_folder/2/3/c'), Path('test_folder/2/3/d'), Path('test_folder/2/3/e'),
                 Path('test_folder/2/3/f'), Path('test_folder/2/3/g')]
        for generator, test in zip(get_files(Path('test_folder/2')), paths):
            self.assertEqual(test, generator)

        for i, _ in enumerate(zip(get_files(Path('test_folder/2')), paths)):
            if i == 3:
                break

        for generator, test in zip(get_files(Path('test_folder/2')), paths[3:]):
            self.assertEqual(test, generator)

    def test_unequal_folders(self):
        ConfigFile(Path('test_folder/3')).remove()
        paths = [Path('test_folder/3/a/1'), Path('test_folder/3/a/2'), Path('test_folder/3/a/3'),
                 Path('test_folder/3/a/4'), Path('test_folder/3/a/5'), Path('test_folder/3/b'),
                 Path('test_folder/3/c'), Path('test_folder/3/d'), Path('test_folder/3/e'),
                 Path('test_folder/3/f'), Path('test_folder/3/g')]
        for generator, test in zip(get_files(Path('test_folder/3')), paths):
            self.assertEqual(test, generator)

        for i, _ in enumerate(zip(get_files(Path('test_folder/3')), paths)):
            if i == 3:
                break

        for generator, test in zip(get_files(Path('test_folder/3')), paths[3:]):
            self.assertEqual(test, generator)

    def test_full_folders(self):
        ConfigFile(Path('test_folder/')).remove()
        paths = [Path('test_folder/1/a'), Path('test_folder/1/b'), Path('test_folder/1/c'),
                 Path('test_folder/1/d'), Path('test_folder/1/e'), Path('test_folder/1/f'),
                 Path('test_folder/2/1/a'), Path('test_folder/2/1/b'), Path('test_folder/2/1/c'),
                 Path('test_folder/2/1/d'), Path('test_folder/2/2/e'), Path('test_folder/2/2/f'),
                 Path('test_folder/2/2/g'), Path('test_folder/2/3/a'), Path('test_folder/2/3/b'),
                 Path('test_folder/2/3/c'), Path('test_folder/2/3/d'), Path('test_folder/2/3/e'),
                 Path('test_folder/2/3/f'), Path('test_folder/2/3/g'),
                 Path('test_folder/3/a/1'), Path('test_folder/3/a/2'), Path('test_folder/3/a/3'),
                 Path('test_folder/3/a/4'), Path('test_folder/3/a/5'), Path('test_folder/3/b'),
                 Path('test_folder/3/c'), Path('test_folder/3/d'), Path('test_folder/3/e'),
                 Path('test_folder/3/f'), Path('test_folder/3/g')]
        for generator, test in zip(get_files(Path('test_folder/')), paths):
            self.assertEqual(test, generator)

        for i, _ in enumerate(zip(get_files(Path('test_folder')), paths)):
            if i == 3:
                break

        for generator, test in zip(get_files(Path('test_folder/')), paths[3:]):
            self.assertEqual(test, generator)
if __name__ == '__main__':
    unittest.main()