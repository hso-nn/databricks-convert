import json
import os
import random
import re
import tempfile
import uuid
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo


class DatabricksConvert:
    def __init__(self, input_path, output_path):
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.temp_path = Path(tempfile.mkdtemp())

    def convert(self):
        """
        Converts source files/directories into databricks dbc archive
        """
        if self.input_path.is_file():
            self._convert_file(
                self.input_path, self.temp_path.joinpath(self.input_path.name)
            )
        else:
            self._convert_directory(self.input_path, self.temp_path)

        self._create_zip(
            self.temp_path,
            self.output_path.joinpath(self.input_path.with_suffix(".dbc").name),
        )

    def _convert_directory(self, input_path, output_path):
        """
        Recursively converts files into databricks json preserving folder structure.
        """
        for input_file in input_path.glob("**/*.*"):
            output_file = self.temp_path.joinpath(
                input_file.relative_to(input_path.parent)
            )
            output_file.parent.mkdir(exist_ok=True)
            self._convert_file(input_file, output_file)

    def _convert_file(self, input_file, output_file):
        """
        Converts source file into databricks json
        """

        content = (
            input_file.read_text()
            .replace("# MAGIC ", "")
            .replace("# Databricks notebook source", "")
        )
        n = random.randrange(100000000000000, 999999999999999)
        commands = [
            {
                "version": "CommandV1",
                "origId": n + i,
                "guid": str(uuid.uuid4()),
                "subtype": "command",
                "commandType": "auto",
                "position": float(i + 1),
                "command": x.strip(),
                "commandVersion": 1,
                "state": "finished",
                "results": {
                    "type": "html",
                    "data": '<div class="ansiout"></div>',
                    "arguments": {},
                    "addedWidgets": {},
                    "removedWidgets": [],
                    "datasetInfos": [],
                },
                "errorSummary": None,
                "error": None,
                "workflows": [],
                "startTime": 0,
                "submitTime": 0,
                "finishTime": 0,
                "collapsed": False,
                "bindings": {},
                "inputWidgets": {},
                "displayType": "table",
                "width": "auto",
                "height": "auto",
                "xColumns": None,
                "yColumns": None,
                "pivotColumns": None,
                "pivotAggregation": None,
                "useConsistentColors": False,
                "customPlotOptions": {},
                "commentThread": [],
                "commentsVisible": False,
                "parentHierarchy": [],
                "diffInserts": [],
                "diffDeletes": [],
                "globalVars": {},
                "latestUser": "",
                "latestUserId": None,
                "commandTitle": "",
                "showCommandTitle": False,
                "hideCommandCode": False,
                "hideCommandResult": False,
                "isLockedInExamMode": False,
                "iPythonMetadata": None,
                "streamStates": {},
                "datasetPreviewNameToCmdIdMap": {},
                "nuid": str(uuid.uuid4()),
            }
            for i, x in enumerate(re.split(r"[#\/\s]+COMMAND -+", content))
        ]

        language = "python" if output_file.suffix == ".py" else "scala"
        notebook = {
            "version": "NotebookV1",
            "name": output_file.stem,
            "language": language,
            "commands": commands,
            "guid": str(uuid.uuid4()),
            "origId": n,
        }

        output_file.with_suffix(f".{language}").write_text(
            json.dumps(notebook, indent=4)
        )

    def _create_zip(self, input_path, output_file):
        """
        Zips file to create the .dbc archive
        """
        directories = list(
            set([x.parent.relative_to(input_path) for x in input_path.glob("**/*.*")])
        )
        with ZipFile(output_file, "w", compression=ZIP_DEFLATED) as zipf:

            for directory in directories:
                zi = ZipInfo(str(directory) + "/")
                # zi.external_attr = 0x10
                zi.compress_type = ZIP_DEFLATED
                zipf.writestr(zi, "")

            for path in input_path.glob("**/*.*"):
                base_path = path.relative_to(input_path)
                zi = ZipInfo(str(base_path))
                zi.compress_type = ZIP_DEFLATED
                zi.flag_bits = None
                # zi.external_attr = 0x20
                zipf.writestr(zi, path.read_text())
