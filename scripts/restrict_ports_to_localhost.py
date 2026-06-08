import re
import os

files = [
    "compose/docker-compose.core.yml",
    "compose/docker-compose.sources.yml",
    "compose/docker-compose.governance.yml"
]

# Pattern matches lines like:       - '9092:9092' or       - 9092:9092
# Group 1: leading indentation and dash (e.g., "      - ")
# Group 2: optional opening quote
# Group 3: the port mapping digits (e.g., "9092:9092")
# Group 4: optional closing quote
port_pattern = re.compile(r"^(\s+-\s+)(['\"]?)(\d+:\d+)(['\"]?)$")

for rel_path in files:
    abs_path = os.path.abspath(rel_path)
    if not os.path.exists(abs_path):
        print(f"Skipping missing file: {rel_path}")
        continue
    
    with open(abs_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    modified_lines = []
    changes = 0
    for line in lines:
        match = port_pattern.match(line.rstrip('\r\n'))
        if match:
            # Check if it's already bound to localhost or an IP
            if "127.0.0.1:" in line:
                modified_lines.append(line)
            else:
                indent_and_dash = match.group(1)
                quote_start = match.group(2)
                ports = match.group(3)
                quote_end = match.group(4)
                
                # If there were quotes, keep them. Otherwise force single quotes for yaml safety
                q = quote_start if quote_start else "'"
                new_line = f"{indent_and_dash}{q}127.0.0.1:{ports}{q}\n"
                modified_lines.append(new_line)
                changes += 1
        else:
            modified_lines.append(line)
            
    if changes > 0:
        with open(abs_path, 'w', encoding='utf-8') as f:
            f.writelines(modified_lines)
        print(f"Updated {rel_path}: restricted {changes} port mappings to 127.0.0.1")
    else:
        print(f"No changes needed for {rel_path}")
