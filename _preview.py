import json
from download import target_path

with open("out/resources_probed.jsonl") as f:
    for i, line in enumerate(f):
        if i >= 5:
            break
        r = json.loads(line)
        print(target_path(r))
