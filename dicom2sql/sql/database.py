import json
import traceback
from time import perf_counter_ns

from sqlalchemy import create_engine, event, Engine, text, insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select

from typing import TypedDict, List, NotRequired

from .schema import TagDescriptor, Patient, Study, Series, tags_id, FileInfo, Base, Tag, Report, Project

from pydicom.dataset import Dataset

from collections import defaultdict
from pathlib import Path

import logging


class DicomTagDict(TypedDict):
    tag: str
    tag_description: str
    name: str
    tag_object: NotRequired[TagDescriptor]


class Database:
    def __init__(self, url: str):
        self.engine = create_engine(url)
        self.session_factory = sessionmaker(bind=self.engine)
        self._is_tags_dirty = True
        self._searched_tags = None
        with self.engine.begin() as connection:
            Base.metadata.create_all(connection)

    @staticmethod
    def check_identifiers(data: Dataset) -> bool:
        return (not tags_id["accession_number"] in data
                or data[tags_id["accession_number"]].value == ''
                or data[tags_id["accession_number"]].value is None
                or not tags_id["patient_dicom_id"] in data
                or data[tags_id["patient_dicom_id"]].value == ''
                or data[tags_id["patient_dicom_id"]].value is None
                )
    #TODO: remove time checks
    def get_or_create_project(self, project_name:str) -> int:
        with self.session_factory() as session:
            session.expire_on_commit = False
            select_statement = select(Project).where(Project.name == project_name)
            project = session.execute(select_statement).scalars().first()
            if not project:
                project = Project(name=project_name)
                session.add(project)
                session.commit()

            return project.id

    def insert(self, data: Dataset, community: str, uri: str, project_id:int=None) -> None:
        community = community[:25]
        logger = logging.getLogger(__name__)
        if self.check_identifiers(data):
            logger.warning(f'File {uri} could not be processed. Accession number or patient id is null')
            return

        time_a = perf_counter_ns()
        with self.session_factory() as session:
            time_start = perf_counter_ns()
            time_b = perf_counter_ns()
            delta_dicom = time_b - time_a
            print(f"Time creating session: {delta_dicom}ns")

            time_a = perf_counter_ns()
            select_statement = (select(Patient,Study,Series)
                                .outerjoin(Study, (Patient.id == Study.patient_id) & (Study.accession_number == data[tags_id["accession_number"]].value))
                                .outerjoin(Series, (Study.id == Series.study_id) & (Series.series_instance_uid == data[tags_id["series_instance_uid"]].value))
                                .where(Patient.patient_dicom_id == data[tags_id["patient_dicom_id"]].value))

            patient,study,series = session.execute(select_statement).first() or (None, None, None)
            time_b = perf_counter_ns()
            delta_dicom = time_b - time_a
            print(f"Time get patient: {delta_dicom}ns")

            time_a = perf_counter_ns()
            if not patient:
                patient = Patient(data)
                session.add(patient)

            if not study:
                study = Study(data, community)
                study.patient = patient
                session.add(study)

            if not series:
                series = Series(data)
                series.study = study
                session.add(series)

            time_b = perf_counter_ns()
            delta_dicom = time_b - time_a
            print(f"Time create subs: {delta_dicom}ns")

            if project_id:
                select_statement = select(Project).where(Project.id == project_id)
                project = session.execute(select_statement).scalars().first()
                if project not in series.projects:
                    series.projects.append(project)


            existing_tags = defaultdict(list)
            for t in series.tags:
                existing_tags[t.tag_id].append(t.value)

            tags = []
            for i, tag_to_insert in enumerate(self.searched_tags):
                if tag_to_insert["tag"] in existing_tags and data[tag_to_insert["tag"]].value in existing_tags[tag_to_insert["tag"]]:
                    continue

                if tag_to_insert["tag"] not in data:
                    logger.info(f'Tag {tag_to_insert["tag"]} not found in file {uri}')
                    continue
                tag = {"value": str(data[tag_to_insert["tag"]].value),
                       "tag_id": tag_to_insert["tag"],
                       "series_id": series.id}


                tags.append(tag)
            if tags:
                session.execute(insert(Tag),tags)

            if tags_id["dicom_sr"] in data:
                json_data = data.to_json_dict()[tags_id["dicom_sr"]]
                report = Report(text=json.dumps(json_data))
                report.study = study
                session.add(report)

            file_uri = Path(uri)
            file = FileInfo(filename=file_uri.name, filepath=str(file_uri.parent), size=file_uri.stat().st_size)
            file.series = series

            session.add(file)

            time_a = perf_counter_ns()

            session.commit()

            time_b = perf_counter_ns()
            delta_dicom = time_b - time_a
            print(f"Time commit: {delta_dicom}ns")
            time_end = perf_counter_ns()
            delta_dicom = time_end - time_start
            print(f"Time full: {delta_dicom}ns")

    def get_tags_list(self) -> List[DicomTagDict]:
        with self.session_factory() as session:
            return [{'tag': t.id, 'tag_description': t.description, 'tag_object': t}
                    for t in session.execute(select(TagDescriptor)).scalars().all()]

    def set_tags_list(self, tag_list: List[DicomTagDict]) -> None:
        tag_list = [t for t in tag_list if not t['tag'] in set(tags_id.items())]
        with self.session_factory() as sess:
            for t in tag_list:
                tag = TagDescriptor(id=t['tag'], name=t['name'], description=t['tag_description'])
                sess.add(tag)
            sess.commit()
        self._is_tags_dirty = True

    @property
    def searched_tags(self) -> List[DicomTagDict]:
        if self._searched_tags is None or self._is_tags_dirty:
            self._searched_tags = self.get_tags_list()
            self._is_tags_dirty = False
        return self._searched_tags

#TODO: make it more agnostic
def get_image_paths(db_uri:str) -> list[(str,str)]:
    engine = create_engine(db_uri)

    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT id, path FROM new_images")
        )

        return [(row.id,row.path) for row in result]


def delete_image_paths(db_uri:str, ids:list) -> None:
    engine = create_engine(db_uri)
    max_size = 1000
    chunks = [ids[i:i + max_size] for i in range(0, len(ids), max_size)]
    with engine.connect() as conn:
        for chunk in chunks:
            result = conn.execute(
                text("DELETE FROM new_images WHERE id in :chunk"),
                {"chunk": ids}
            )
