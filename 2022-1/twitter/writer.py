import json
import os


class Writer(object):
    def __init__(self, file_name, output_dir=None):
        if output_dir:
            self.file_name = os.path.join(output_dir, file_name)
        else:
            self.file_name = file_name

    def write(self, data):
        try:
            if self.file_name.endswith('.json'):
                with open(self.file_name, 'w', encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
            else:
                with open(self.file_name, 'w', encoding="utf-8") as f:
                    f.write(data)
        except Exception as e:
            raise e
