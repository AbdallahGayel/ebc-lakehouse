const fs = require('fs');
const path = require('path');

const dir = 'd:\\ClickHouse\\ebc_lakehouse_updated_v1\\docs\\website-custom';
const indexPath = path.join(dir, 'index.html');

let html = fs.readFileSync(indexPath, 'utf-8');

// 1. Remove the hardcoded sidebar and search overlay (if any)
const sidebarRegex = /<button class="mobile-toggle".*?<\/aside>/s;
html = html.replace(sidebarRegex, '<div id="sidebar-container"></div>');

// 2. Add components.js script tag to the <head>
html = html.replace('</head>', '  <script src="components.js"></script>\n</head>');

// 3. Extract the Sections
const sections = [
  { id: 'hero', file: 'index.html' },
  { id: 'architecture', file: 'architecture.html' },
  { id: 'medallion', file: 'medallion.html' },
  { id: 'services', file: 'services.html' },
  { id: 'dataflow', file: 'dataflow.html' },
  { id: 'reference', file: 'reference.html' },
  { id: 'runbook', file: 'runbook.html' },
  { id: 'glossary', file: 'glossary.html' } // We will combine glossary with reference or keep it separate. The plan says 6 pages. Let's keep it separate or rename index.html as needed.
];

// Split the HTML into header/footer and sections
const mainStartRegex = /<main class="main-content">/;
const mainEndRegex = /<\/main>/;

const matchStart = html.match(mainStartRegex);
const matchEnd = html.match(mainEndRegex);

const headerPart = html.substring(0, matchStart.index + matchStart[0].length);
const footerPart = html.substring(matchEnd.index);
const mainContent = html.substring(matchStart.index + matchStart[0].length, matchEnd.index);

// For each section, we extract its content and create a new HTML file.
// The index.html will contain ONLY the hero section.
const sectionRegex = /<!-- ======== SECTION \d: .*? ======== -->(.*?)<!-- ======== (SECTION \d|FOOTER)/s;

let currentContent = mainContent;

const pages = {
  'index.html': [],
  'architecture.html': [],
  'medallion.html': [],
  'services.html': [],
  'dataflow.html': [],
  'reference.html': [],
  'runbook.html': []
};

// We will manually split based on the comments in the HTML.
const parts = mainContent.split(/<!-- ======== SECTION \d: .*? ======== -->/);
// parts[0] is empty whitespace
// parts[1] is Hero
// parts[2] is Architecture
// parts[3] is Medallion
// parts[4] is Services
// parts[5] is Data Flow
// parts[6] is Reference
// parts[7] is Runbook
// parts[8] is Glossary

pages['index.html'] = parts[1];
pages['architecture.html'] = parts[2];
pages['medallion.html'] = parts[3];
pages['services.html'] = parts[4];
pages['dataflow.html'] = parts[5];
pages['reference.html'] = parts[6] + "\n<!-- ======== SECTION 8: GLOSSARY ======== -->\n" + parts[8].replace(/<!-- ======== FOOTER ======== -->.*/s, '');
pages['runbook.html'] = parts[7];

for (const [filename, content] of Object.entries(pages)) {
  if (!content) continue;
  
  // Create the full HTML for the page
  const fullHtml = headerPart + "\n    <!-- ======== CONTENT ======== -->\n" + content + "\n    " + footerPart;
  
  fs.writeFileSync(path.join(dir, filename), fullHtml);
  console.log('Created ' + filename);
}

// Create placeholders for data-dictionary and onboarding
const placeholderTemplate = (title) => headerPart + `
    <section class="section">
      <div class="section-label">Coming Soon</div>
      <h2 class="reveal">${title}</h2>
      <p class="subtitle reveal reveal-delay-1">This section is currently under construction.</p>
    </section>
` + footerPart;

fs.writeFileSync(path.join(dir, 'data-dictionary.html'), placeholderTemplate('Data Dictionary'));
fs.writeFileSync(path.join(dir, 'onboarding.html'), placeholderTemplate('Onboarding Guide'));
console.log('Created placeholders');

