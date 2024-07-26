import pydicom

from typing import TypedDict, List


class DicomTagDict(TypedDict):
    TAGS: str


class DicomExtractor:
    def __init__(self, tags: List[DicomTagDict]):
        self.tags = {t['TAGS'] for t in tags}
        self.tags = {*self.tags,*{
            #File Information: These tags provide information about the file itself.
            '00080016', #Storage SOP Class UID
            '00080018', #Storage SOP Instance UID

            #Patient Information:
            '00100010', #Patient's Name
            '00100020', #Patient ID
            '00100030', #Patient's Birth Date
            '00100040', #Patient's Sex

            #Session Information:
            '00080020', #Study Date
            '00080030', #Study Time
            '00080050', #Accession Number
            '00081030', #Study Description
            '0008103E', #Series Description
            '0020000D', #Study Instance UID
            '0020000E', #Series Instance UID
            '00200010', #Study ID
            '00200011', #Series Number
        }}

    def extract_tags(self, ds: pydicom.Dataset) -> dict:
        return {t:str(ds[t].value if t in ds else "") for t in self.tags}
