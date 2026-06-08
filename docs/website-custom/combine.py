import os
import re

dir_path = r'd:\ClickHouse\ebc_lakehouse_updated_v1\docs\website-custom'

pages = [
    'index.html',
    'architecture.html',
    'medallion.html',
    'services.html',
    'dataflow.html',
    'reference.html',
    'runbook.html',
    'data-dictionary.html',
    'onboarding.html'
]

# Get the header and footer from the current index.html
with open(os.path.join(dir_path, 'index.html'), 'r', encoding='utf-8') as f:
    index_html = f.read()

main_start_match = re.search(r'<main class="main-content">', index_html)
main_end_match = re.search(r'</main>', index_html)

header_part = index_html[:main_start_match.end()]
footer_part = index_html[main_end_match.start():]

combined_content = ""

for page in pages:
    if not os.path.exists(os.path.join(dir_path, page)): continue
    with open(os.path.join(dir_path, page), 'r', encoding='utf-8') as f:
        html = f.read()
    
    match_start = re.search(r'<!-- ======== CONTENT ======== -->', html)
    match_end = re.search(r'</main>', html)
    
    if match_start and match_end:
        content = html[match_start.end():match_end.start()].strip()
        # if this is index.html, it has the hero section
        combined_content += "\n" + content + "\n"

# Now we rewrite index.html with the combined content
full_html = header_part + "\n    <!-- ======== CONTENT ======== -->\n" + combined_content + "\n    " + footer_part

# Remove the components.js script tag since we will put the sidebar back into index.html
# Wait, keeping components.js is fine, it injects the sidebar. 
# But we need to change components.js to use #hrefs instead of .html links.

with open(os.path.join(dir_path, 'index-spa.html'), 'w', encoding='utf-8') as f:
    f.write(full_html)

print("Combined into index-spa.html")
