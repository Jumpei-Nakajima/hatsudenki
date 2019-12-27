import os
from datetime import datetime
from pathlib import Path

from hatsudenki.commands.base import BaseCommand
from hatsudenki.packages.command.files import yaml_write


class Command(BaseCommand):
    async def handle(self, *args, **options):
        c = Path(os.getcwd())

        dsl_path = c / 'dsl'

        if dsl_path.exists():
            print(f'{dsl_path} is already exists. skipped...')
            return

        dsl_path.mkdir(parents=True, exist_ok=True)

        master_path = dsl_path / 'master'
        enum_path = dsl_path / 'enum'
        dynamo_path = dsl_path / 'dynamo'
        define_path = dsl_path / 'define'

        master_path.mkdir(parents=True, exist_ok=True)
        enum_path.mkdir(parents=True, exist_ok=True)
        dynamo_path.mkdir(parents=True, exist_ok=True)
        define_path.mkdir(parents=True, exist_ok=True)

        all_tag = {
            'name': 'ALL',
            'comment': 'all output (for development)',
            'release_at': 'nothing',
            'define_at': datetime.now().isoformat()
        }
        yaml_write((define_path / 'master_tag.yml'), [all_tag])

        sc = {
            "attributes": {
                "user_id": {
                    "hash": True,
                    "type": "uuid",
                    "comment": "unique user id (UUID4)"
                },
                "kind": {
                    "range": True,
                    "type": "string",
                    "comment": "This sentence will not be used for processing"
                }
            },
            "comment": "Test table schema root definition"
        }
        yaml_write((dynamo_path / 'example' / 'root.yml'), sc)

        sc = {
            "attributes": {
                "one_number_value": {
                    "type": "number",
                    "comment": "simple number value"
                },
                "one_string_value": {
                    "type": "string",
                    "comment": "simple string value"
                }
            },
            "comment": "child solo table sample."
        }
        yaml_write((dynamo_path / 'example' / 'one.yml'), sc)

        sc = {
            "attributes": {
                "child_range_value": {
                    "type": "number",
                    "range": True,
                    "comment": "child table's range key"
                },
                "ref_master": {
                    "type": "master_one",
                    "to": "example",
                    "comment": "reference to master table(to 'master_example' table)"
                },
                "enum_value": {
                    "type": "enum",
                    "to": "example",
                    "comment": "referece to enum (to 'example' enum)"
                }
            },
            "comment": "sample child multi table."
        }
        yaml_write((dynamo_path / 'example' / 'child.yml'), sc)

        sc = {
            "name": "example",
            "origin": 0,
            "values": [
                {
                    "name": "none",
                    "ref_name": "nothing"
                },
                {
                    "name": "sample_one",
                    "ref_name": "sample_one"
                },
                {
                    "name": "sample_two",
                    "ref_name": "sample_two"
                }
            ]
        }
        yaml_write((enum_path / 'example.yml'), sc)

        sc = {
            "name": "sample",
            "excel": "sample_master_input_excel",
            "column": {
                "id": {
                    "type": "string",
                    "name": "unique_id",
                    "hash": True
                },
                "enum_sample": {
                    "name": "reference to example enum",
                    "type": "enum",
                    "to": "example"
                },
                "relation_sample": {
                    "name": "reference to master relation example",
                    "type": "master",
                    "to": "relation_example"
                }
            }
        }
        yaml_write((master_path / 'example.yml'), sc)

        sc = {
            "name": "relation_example",
            "excel": "sample_master_input_excel",
            "column": {
                "id": {
                    "type": "string",
                    "name": "unique_id",
                    "hash": True
                },
                "number_value": {
                    "type": "number",
                    "name": "number value"
                }
            }
        }
        yaml_write((master_path / 'relation_example.yml'), sc)
