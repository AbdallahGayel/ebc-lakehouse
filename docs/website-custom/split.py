import os
import re

dir_path = r'd:\ClickHouse\ebc_lakehouse_updated_v1\docs\website-custom'
index_path = os.path.join(dir_path, 'index.html')

with open(index_path, 'r', encoding='utf-8') as f:
    html = f.read()

# 1. Remove hardcoded sidebar and replace with container
html = re.sub(r'<button class="mobile-toggle".*?</aside>', '<div id="sidebar-container"></div>', html, flags=re.DOTALL)

# 2. Add components.js script tag to <head>
html = html.replace('</head>', '  <script src="components.js"></script>\n</head>')

# 3. Extract header, footer, main content
main_start_match = re.search(r'<main class="main-content">', html)
main_end_match = re.search(r'</main>', html)

header_part = html[:main_start_match.end()]
footer_part = html[main_end_match.start():]
main_content = html[main_start_match.end():main_end_match.start()]

# Split by section comment
parts = re.split(r'<!-- ======== SECTION \d: .*? ======== -->', main_content)

pages = {
    'index.html': parts[1] if len(parts) > 1 else '',
    'architecture.html': parts[2] if len(parts) > 2 else '',
    'medallion.html': parts[3] if len(parts) > 3 else '',
    'services.html': parts[4] if len(parts) > 4 else '',
    'dataflow.html': parts[5] if len(parts) > 5 else '',
    'reference.html': (parts[6] if len(parts) > 6 else '') + "\n<!-- ======== SECTION 8: GLOSSARY ======== -->\n" + (re.sub(r'<!-- ======== FOOTER ======== -->.*', '', parts[8], flags=re.DOTALL) if len(parts) > 8 else ''),
    'runbook.html': parts[7] if len(parts) > 7 else ''
}

for filename, content in pages.items():
    if not content.strip(): continue
    full_html = header_part + "\n    <!-- ======== CONTENT ======== -->\n" + content + "\n    " + footer_part
    with open(os.path.join(dir_path, filename), 'w', encoding='utf-8') as f:
        f.write(full_html)
    print(f'Created {filename}')

# Placeholders
placeholder_template = lambda title: f"""{header_part}
    <section class="section">
      <div class="section-label">Coming Soon</div>
      <h2 class="reveal">{title}</h2>
      <p class="subtitle reveal reveal-delay-1">This section is currently under construction.</p>
    </section>
{footer_part}"""

with open(os.path.join(dir_path, 'data-dictionary.html'), 'w', encoding='utf-8') as f:
    f.write(placeholder_template('Data Dictionary'))
with open(os.path.join(dir_path, 'onboarding.html'), 'w', encoding='utf-8') as f:
    f.write(placeholder_template('Onboarding Guide'))

print('Created placeholders')
