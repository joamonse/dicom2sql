from typing import List, Any
from typing import Optional

from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import MappedAsDataclass
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship

from pydicom.dataset import Dataset

import datetime

# Tutorial https://www.youtube.com/watch?v=Uym2DHnUEno
# https://github.com/zzzeek/python_web_conf_2023


def convert_datetime(date:str, time:str):
    print(date, time)
    date_obj = datetime.datetime.strptime(date, "%Y%m%d")

    if '.' in time:
        time_obj = datetime.datetime.strptime(time, "%H%M%S.%f").time()
    else:
        time_obj = datetime.datetime.strptime(time, "%H%M%S").time()

    datetime_obj = datetime.datetime.combine(date_obj, time_obj)

    return datetime_obj


tags_id: dict = {
        "patient_dicom_id": "00100020",
        "patient_name": "00100010",
        "birth_date": "00100030",
        "sex": "00100090",
        "age": "00101010",
        "weight": "00101030",
        "study_instance_uid": "0020000D",
        "study_id": "00200010",
        "accession_number": "00080050",
        "study_date": "00080020",
        "study_time":  "00080030",
        "modality": "00080060",
        "study_description": "00081030",
        "series_instance_uid": "0020000E",
        "series_description": "0008103E"
}


class Base(MappedAsDataclass, DeclarativeBase):
    pass


class Patient(Base):
    __tablename__ = "patient"

    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    patient_dicom_id: Mapped[str] = mapped_column(String(10), unique=True, index=True)
    patient_name: Mapped[str] = mapped_column(String(50), unique=True, index=True)
    birth_date: Mapped[datetime.date] 
    sex: Mapped[Optional[str]] = mapped_column(String(1))
    age: Mapped[Optional[int]]
    weight: Mapped[Optional[int]]

    studies: Mapped[List["Study"]] = relationship(
        back_populates="patient", cascade="all, delete-orphan", default_factory=list
    )

    def __init__(self, dicom: Dataset, **kw: Any):
        super().__init__(**kw)
        self.patient_dicom_id = str(dicom[tags_id["patient_dicom_id"]].value)
        self.patient_name = str(dicom[tags_id["patient_name"]].value)
        self.birth_date = datetime.datetime.strptime(dicom[tags_id["birth_date"]].value, "%Y%m%d")
        self.sex = str(dicom[tags_id["sex"]].value[0]) if tags_id["sex"] in dicom else None
        self.age = dicom[tags_id["age"]].value if tags_id["sex"] in dicom else None
        self.weight = dicom[tags_id["weight"]].value if tags_id["weight"] in dicom else None



class Study(Base):
    __tablename__ = "study"

    id: Mapped[int] = mapped_column( primary_key=True, init=False)
    study_instance_uid: Mapped[str] = mapped_column(String(64),unique=True, index=True) 
    study_id: Mapped[str] = mapped_column(String(16), index=True)
    accession_number: Mapped[str] = mapped_column(String(16),unique=True, index=True)
    study_datetime: Mapped[datetime.datetime] 
    modality: Mapped[str] = mapped_column(String(30))
    study_description: Mapped[str] = mapped_column(String(50))
    community: Mapped[str] = mapped_column(String(25))
    hospital: Mapped[str] = mapped_column(String(25))
    patient_id: Mapped[int] = mapped_column(ForeignKey("patient.id"), init=False)

    patient: Mapped["Patient"] = relationship(back_populates="studies", default=None)

    reports: Mapped[List["Report"]] = relationship(
        back_populates="study", cascade="all, delete-orphan", default_factory=list
    )

    series: Mapped[List["Series"]] = relationship(
        back_populates="study", cascade="all, delete-orphan", default_factory=list
    )

    def __init__(self, dicom: Dataset, community: str, **kw: Any):
        super().__init__(**kw)
        self.study_instance_uid = str(dicom[tags_id["study_instance_uid"]].value)
        self.study_id = str(dicom[tags_id["study_id"]].value)
        self.accession_number = str(dicom[tags_id["accession_number"]].value)
        self.study_datetime = convert_datetime(dicom[tags_id["study_date"]].value,dicom[tags_id["study_time"]].value)
        self.modality = str(dicom[tags_id["modality"]].value)
        self.study_description = str(dicom[tags_id["study_description"]].value)
        self.community = community
        self.hospital = self.accession_number[0:5]



class Report(Base):
    __tablename__ = "report"

    id: Mapped[int] = mapped_column( primary_key=True, init=False)
    text: Mapped[str] = mapped_column(String(255))
    study_id: Mapped[int] = mapped_column(ForeignKey("study.id"), init=False, index=True)

    study: Mapped["Study"] = relationship(back_populates="reports", default=None)


class Series(Base):
    __tablename__ = "series"

    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    series_instance_uid: Mapped[str] = mapped_column(String(64), index=True)
    series_description: Mapped[str] = mapped_column(String(50))
    study_id: Mapped[int] = mapped_column(ForeignKey("study.id"))

    study: Mapped["Study"] = relationship(back_populates="series", default=None)

    tags: Mapped[List["Tag"]] = relationship(
        back_populates="series", cascade="all, delete-orphan", default_factory=list
    )
    
    files: Mapped[List["File"]] = relationship(
        back_populates="series", cascade="all, delete-orphan", default_factory=list
    )

    def __init__(self, dicom: Dataset, **kw: Any):
        super().__init__(**kw)
        self.series_instance_uid = str(dicom[tags_id["series_instance_uid"]].value)
        self.series_description = str(dicom[tags_id["series_description"]].value)


class TagDescriptor(Base):
    __tablename__ = "tag_descriptor"

    id: Mapped[str] = mapped_column(String(25), primary_key=True)
    description: Mapped[str] = mapped_column(String(150))

    tags: Mapped[List["Tag"]] = relationship(
        back_populates="tag_descriptor", cascade="all, delete-orphan", default_factory=list
    )


class Tag(Base):
    __tablename__ = "tag"

    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    value: Mapped[str] = mapped_column(String(350))
    tag_id: Mapped[str] = mapped_column(ForeignKey("tag_descriptor.id"), init=False)
    series_id: Mapped[int] = mapped_column(ForeignKey("series.id"), init=False, index=True)

    tag_descriptor: Mapped["TagDescriptor"] = relationship(back_populates="tags", default=None)

    series: Mapped["Series"] = relationship(back_populates="tags", default=None)


class File(Base):
    __tablename__ = "file"

    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    filename: Mapped[str] = mapped_column(String(255))
    filepath: Mapped[str] = mapped_column(String(255))
    size: Mapped[int]
    series_id: Mapped[int] = mapped_column(ForeignKey("series.id"), init=False, index=True)

    series: Mapped["Series"] = relationship(back_populates="files", default=None)




