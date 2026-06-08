/* ============================================================
   EBC Lakehouse — Interactive Documentation
   Application JavaScript
   ============================================================ */

// Global Scroll Observer variable
let scrollObserver = null;

document.addEventListener('DOMContentLoaded', () => {
  initThemeToggle();
  initSidebarNav();
  initScrollReveal();
  initAnimatedCounters();
  initMobileMenu();
  initGlobalSearch();
  initDelegatedEvents();
});

/* --- Sidebar Navigation & SPA Tab Logic --- */
function initSidebarNav() {
  const sections = document.querySelectorAll('section.section');
  const navLinks = document.querySelectorAll('.nav-link[data-section]');

  if (!sections.length || !navLinks.length) return;

  function showSection(id) {
    if (!id) id = '#hero';
    id = id.replace('#', '');
    
    // Ensure the section actually exists
    const targetSection = document.getElementById(id);
    if (!targetSection) return;

    // Hide all sections, show target
    sections.forEach(sec => {
      if (sec.id === id) {
        sec.style.display = 'block';
        // Re-observe hidden reveal elements in the newly shown section
        if (scrollObserver) {
          sec.querySelectorAll('.reveal:not(.visible)').forEach(el => {
            scrollObserver.observe(el);
          });
        }
      } else {
        sec.style.display = 'none';
      }
    });

    // Update active nav link
    navLinks.forEach(link => {
      link.classList.toggle('active', link.dataset.section === id);
    });

    // Scroll to top
    window.scrollTo(0, 0);
  }

  // Handle click events on navLinks
  navLinks.forEach((link) => {
    link.addEventListener('click', (e) => {
      e.preventDefault();
      const targetId = link.dataset.section;
      window.location.hash = '#' + targetId;
      closeMobileSidebar();
    });
  });

  // Handle hash change globally for routing
  window.addEventListener('hashchange', () => {
    showSection(window.location.hash);
  });

  // Handle initial load
  const initialHash = window.location.hash || '#hero';
  showSection(initialHash);
}

/* --- Scroll Reveal Animations --- */
function initScrollReveal() {
  const reveals = document.querySelectorAll('.reveal');
  if (!reveals.length) return;

  scrollObserver = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (entry.isIntersecting) {
          entry.target.classList.add('visible');
          scrollObserver.unobserve(entry.target);
        }
      });
    },
    {
      rootMargin: '0px 0px -80px 0px',
      threshold: 0.1,
    }
  );

  reveals.forEach((el) => scrollObserver.observe(el));
}

/* --- Animated Counters --- */
function initAnimatedCounters() {
  const counters = document.querySelectorAll('.stat-number[data-target]');
  if (!counters.length) return;

  let animated = false;

  const observer = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (entry.isIntersecting && !animated) {
          animated = true;
          counters.forEach((counter) => animateCounter(counter));
        }
      });
    },
    { threshold: 0.5 }
  );

  counters.forEach((counter) => observer.observe(counter));
}

function animateCounter(el) {
  const target = parseInt(el.dataset.target, 10);
  const suffix = el.dataset.suffix || '';
  const duration = 2000;
  const startTime = performance.now();

  function update(currentTime) {
    const elapsed = currentTime - startTime;
    const progress = Math.min(elapsed / duration, 1);
    // Ease out cubic
    const eased = 1 - Math.pow(1 - progress, 3);
    const current = Math.round(eased * target);

    el.textContent = current + suffix;

    if (progress < 1) {
      requestAnimationFrame(update);
    } else {
      el.textContent = target + suffix;
    }
  }

  requestAnimationFrame(update);
}

/* --- Mobile Menu --- */
function initMobileMenu() {
  const toggle = document.getElementById('mobileToggle');
  const overlay = document.getElementById('sidebarOverlay');

  if (toggle) {
    toggle.addEventListener('click', () => {
      document.getElementById('sidebar').classList.toggle('open');
      overlay.classList.toggle('visible');
    });
  }

  if (overlay) {
    overlay.addEventListener('click', closeMobileSidebar);
  }
}

function closeMobileSidebar() {
  const sidebar = document.getElementById('sidebar');
  const overlay = document.getElementById('sidebarOverlay');
  if (sidebar) sidebar.classList.remove('open');
  if (overlay) overlay.classList.remove('visible');
}

/* --- Theme Toggle --- */
function initThemeToggle() {
  // Check local storage
  const savedTheme = localStorage.getItem('ebc-theme');
  if (savedTheme === 'light') {
    document.documentElement.setAttribute('data-theme', 'light');
  }
}

function toggleTheme() {
  const current = document.documentElement.getAttribute('data-theme');
  if (current === 'light') {
    document.documentElement.removeAttribute('data-theme');
    localStorage.setItem('ebc-theme', 'dark');
  } else {
    document.documentElement.setAttribute('data-theme', 'light');
    localStorage.setItem('ebc-theme', 'light');
  }
}

/* --- Event Delegation for UI Components --- */
function initDelegatedEvents() {
  // Click event delegation
  document.addEventListener('click', (e) => {
    // 1. Service card header click (expand/collapse)
    const serviceHeader = e.target.closest('.service-card-header');
    if (serviceHeader) {
      const card = serviceHeader.closest('.service-card');
      if (card) {
        const isExpanded = card.classList.contains('expanded');
        card.classList.toggle('expanded', !isExpanded);
        serviceHeader.setAttribute('aria-expanded', !isExpanded);
      }
      return;
    }

    // 2. Accordion header click (open/close)
    const accHeader = e.target.closest('.accordion-header');
    if (accHeader) {
      const item = accHeader.closest('.accordion-item');
      if (item) {
        const isOpen = item.classList.contains('open');
        item.classList.toggle('open', !isOpen);
        accHeader.setAttribute('aria-expanded', !isOpen);
      }
      return;
    }

    // 3. Code copy button click
    const copyBtn = e.target.closest('.code-copy-btn');
    if (copyBtn) {
      copyCode(copyBtn);
      return;
    }

    // 4. Service card tab button click
    const tabBtn = e.target.closest('.service-tab-btn');
    if (tabBtn) {
      const card = tabBtn.closest('.service-card');
      const tabName = tabBtn.dataset.tab;
      if (card && tabName) {
        // Toggle active class on tab buttons
        card.querySelectorAll('.service-tab-btn').forEach(btn => {
          btn.classList.toggle('active', btn === tabBtn);
          btn.setAttribute('aria-selected', btn === tabBtn);
        });
        // Toggle active class on tab content panes
        card.querySelectorAll('.service-tab-pane').forEach(pane => {
          pane.classList.toggle('active', pane.dataset.tab === tabName);
        });
      }
      return;
    }

    // 5. Theme toggle click
    const themeBtn = e.target.closest('#themeToggle');
    if (themeBtn) {
      toggleTheme();
      return;
    }

    // 6. Architecture Node click (routes to Service Catalog and scrolls to item)
    const node = e.target.closest('.arch-node[data-service]');
    if (node) {
      const serviceId = 'svc-' + node.dataset.service;
      window.location.hash = '#services';
      
      setTimeout(() => {
        const serviceCard = document.getElementById(serviceId);
        if (serviceCard) {
          serviceCard.scrollIntoView({ behavior: 'smooth', block: 'center' });
          if (!serviceCard.classList.contains('expanded')) {
            serviceCard.classList.add('expanded');
            const header = serviceCard.querySelector('.service-card-header');
            if (header) header.setAttribute('aria-expanded', 'true');
          }
          serviceCard.style.boxShadow = '0 0 40px rgba(6, 182, 212, 0.4)';
          setTimeout(() => {
            serviceCard.style.boxShadow = '';
          }, 2000);
        }
      }, 150);
      return;
    }

    // 7. DAG Node click (details popup)
    const dagNode = e.target.closest('.dag-node[data-node]');
    if (dagNode) {
      const nodeKey = dagNode.dataset.node;
      const details = dagNodeDetails[nodeKey];
      const card = document.getElementById('dagDetailsCard');
      const title = document.getElementById('dagDetailsTitle');
      const body = document.getElementById('dagDetailsBody');
      
      if (details && card && title && body) {
        title.innerHTML = details.title;
        body.innerHTML = details.body;
        card.style.display = 'block';
        card.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      }
      return;
    }

    // 8. DAG Details close click
    if (e.target.id === 'dagDetailsClose') {
      const card = document.getElementById('dagDetailsCard');
      if (card) card.style.display = 'none';
      return;
    }
  });

  // Keyboard delegation for accessibility (Space/Enter triggers click)
  document.addEventListener('keydown', (e) => {
    if (e.key === ' ' || e.key === 'Enter') {
      const target = e.target.closest('.service-card-header, .accordion-header, .service-tab-btn, .medallion-stage, .dag-node');
      if (target) {
        e.preventDefault();
        target.click();
      }
    }
  });

  // Input event delegation for Glossary search
  document.addEventListener('input', (e) => {
    if (e.target.id === 'glossarySearch') {
      filterGlossary();
    }
  });
}

/* --- Copy Code to Clipboard --- */
function copyCode(btn) {
  const codeBlock = btn.closest('.code-block');
  if (!codeBlock) return;

  const code = codeBlock.querySelector('pre code');
  if (!code) return;

  const text = code.textContent;

  navigator.clipboard
    .writeText(text)
    .then(() => {
      btn.textContent = 'Copied!';
      btn.classList.add('copied');
      setTimeout(() => {
        btn.textContent = 'Copy';
        btn.classList.remove('copied');
      }, 2000);
    })
    .catch(() => {
      // Fallback
      const textarea = document.createElement('textarea');
      textarea.value = text;
      textarea.style.position = 'fixed';
      textarea.style.opacity = '0';
      document.body.appendChild(textarea);
      textarea.select();
      try {
        document.execCommand('copy');
        btn.textContent = 'Copied!';
        btn.classList.add('copied');
        setTimeout(() => {
          btn.textContent = 'Copy';
          btn.classList.remove('copied');
        }, 2000);
      } catch (e) {
        btn.textContent = 'Failed';
      }
      document.body.removeChild(textarea);
    });
}

/* --- Glossary Search / Filter --- */
function filterGlossary() {
  const input = document.getElementById('glossarySearch');
  const items = document.querySelectorAll('.glossary-item');
  if (!input || !items.length) return;

  const query = input.value.toLowerCase().trim();

  items.forEach((item) => {
    const term = item.querySelector('.glossary-term');
    const def = item.querySelector('.glossary-def');
    const text =
      (term ? term.textContent : '') + ' ' + (def ? def.textContent : '');

    if (query === '' || text.toLowerCase().includes(query)) {
      item.classList.remove('hidden');
    } else {
      item.classList.add('hidden');
    }
  });
}

/* --- Global Search Modal --- */
function initGlobalSearch() {
  const sidebarInput = document.getElementById('globalSearchInput');
  const overlay = document.getElementById('searchOverlay');
  const modalInput = document.getElementById('searchModalInput');
  const closeBtn = document.getElementById('closeSearchBtn');
  const resultsContainer = document.getElementById('searchResults');
  
  if (!sidebarInput || !overlay) return;

  let searchIndex = null;
  let searchDocs = [];

  function buildSearchIndexFromDOM() {
    searchDocs = [];
    document.querySelectorAll('section.section').forEach((sec) => {
      const id = sec.id;
      // Skip the hero or empty sections
      if (id === 'hero' && !sec.querySelector('h2')) return;
      const titleEl = sec.querySelector('h2') || sec.querySelector('h1');
      const title = titleEl ? titleEl.textContent.trim() : id;
      
      const textParts = [];
      sec.querySelectorAll('h2, h3, h4, p, li, td, .glossary-term, .glossary-def, .service-name, .service-role').forEach(el => {
        if (!el.classList.contains('code-copy-btn') && !el.closest('.service-tabs')) {
          textParts.push(el.textContent.trim());
        }
      });

      searchDocs.push({
        id: '#' + id,
        title: title,
        content: textParts.join(' ').replace(/\s+/g, ' ').trim()
      });
    });

    if (typeof lunr !== 'undefined') {
      searchIndex = lunr(function () {
        this.ref('id');
        this.field('title', { boost: 10 });
        this.field('content');
        
        searchDocs.forEach((doc) => {
          this.add(doc);
        }, this);
      });
    }
  }

  function renderResults(results) {
    if (results.length === 0) {
      resultsContainer.innerHTML = '<div class="search-empty-state">No results found.</div>';
      return;
    }

    let html = '';
    results.forEach(res => {
      const doc = searchDocs.find(d => d.id === res.ref);
      if (doc) {
        const excerpt = doc.content.substring(0, 120) + '...';
        html += `
          <a href="${doc.id}" class="search-result-item">
            <div class="search-result-title">${doc.title}</div>
            <div class="search-result-excerpt">${excerpt}</div>
          </a>
        `;
      }
    });
    resultsContainer.innerHTML = html;
  }

  function handleSearch(e) {
    if (!searchIndex) buildSearchIndexFromDOM();
    const query = e.target.value.trim();
    if (!query) {
      resultsContainer.innerHTML = '<div class="search-empty-state">Type to start searching...</div>';
      return;
    }

    if (searchIndex) {
      try {
        const results = searchIndex.search(query + '^100 ' + query + '*^10 ' + '*' + query + '*');
        renderResults(results);
      } catch (err) {
        // Quietly handle search syntax errors
      }
    }
  }

  function openSearch() {
    overlay.classList.add('visible');
    modalInput.focus();
    if (!searchIndex) buildSearchIndexFromDOM();
  }

  function closeSearch() {
    overlay.classList.remove('visible');
    sidebarInput.value = '';
    modalInput.value = '';
    resultsContainer.innerHTML = '<div class="search-empty-state">Type to start searching...</div>';
  }

  sidebarInput.addEventListener('focus', openSearch);
  modalInput.addEventListener('input', handleSearch);
  closeBtn.addEventListener('click', closeSearch);
  
  overlay.addEventListener('click', (e) => {
    if (e.target === overlay) closeSearch();
  });

  resultsContainer.addEventListener('click', (e) => {
    const item = e.target.closest('.search-result-item');
    if (item) {
      closeSearch();
    }
  });

  // Cmd+K / Ctrl+K shortcut
  document.addEventListener('keydown', (e) => {
    if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
      e.preventDefault();
      openSearch();
    }
    if (e.key === 'Escape' && overlay.classList.contains('visible')) {
      closeSearch();
    }
  });
}

const dagNodeDetails = {
  // Silver
  's1': {
    title: 'check_bronze_freshness (SLA Gate)',
    body: 'Queries the metadata of all Iceberg Bronze tables via Trino to check if the last commit timestamp is within the SLA threshold (default 6 hours). If any table is stale, the DAG halts to prevent processing outdated logs.'
  },
  's2': {
    title: 'dbt_run_silver (dbt Run)',
    body: 'Executes dbt models with the <code>silver</code> tag. Compiles and runs SQL MERGE statements to stage, clean, cast types, and incrementally load records from Bronze to Silver.'
  },
  's3': {
    title: 'dbt_test_silver (dbt Test)',
    body: 'Runs automated data quality audits (unique checks, null validations, custom range assertions) on the newly populated Silver tables. Fails if schema rules are violated.'
  },
  's4': {
    title: 'maintain_silver (Optimize & Expire)',
    body: 'Runs Iceberg maintenance SQL commands: (1) <code>OPTIMIZE</code> to compact small Parquet files generated by streaming sinks, and (2) <code>EXPIRE SNAPSHOTS</code> to reclaim deleted files older than 7 days.'
  },
  's5': {
    title: 'record_pipeline_metrics (Utility)',
    body: 'Writes transaction counts, processing lag, and SLA status to the centralized <code>pipeline_metrics</code> monitoring table for dashboard tracking.'
  },
  's6': {
    title: 'trigger_dbt_gold (Orchestration Trigger)',
    body: 'Executes a trigger operator that asynchronously runs the downstream <code>ebc_dbt_gold</code> DAG once the Silver verification completes.'
  },
  // Gold
  'g1': {
    title: 'dbt_run_gold (dbt Run)',
    body: 'Executes dbt models with the <code>gold</code> tag. Performs full-refresh aggregations (daily volumes, scheme performance, bank-pair settlements) to build consolidated marts.'
  },
  'g2': {
    title: 'dbt_test_gold (dbt Test)',
    body: 'Verifies matching cross-rail record counts and validates mathematical consistency within the aggregated Gold mart schemas.'
  },
  'g3': {
    title: 'maintain_gold (Optimize)',
    body: 'Performs file compaction (optimize) on the Gold tables to ensure query speeds remain optimized for reporting.'
  },
  'g4': {
    title: 'trigger_dbt_serving (Orchestration Trigger)',
    body: 'Executes a trigger operator to run the final downstream <code>ebc_dbt_serving</code> DAG once Gold marts are populated and validated.'
  },
  // Serving
  'v1': {
    title: 'dbt_run_serving (dbt Run)',
    body: 'Executes dbt models with the <code>serving</code> tag. Computes high-speed incremental views representing a hot rolling 7-day window, adding a <code>refreshed_at</code> cache-busting timestamp.'
  },
  'v2': {
    title: 'dbt_test_serving (dbt Test)',
    body: 'Asserts that records are within the 7-day hot window boundary and audits format compatibility before BI consumption.'
  },
  'v3': {
    title: 'maintain_serving (Compaction)',
    body: 'Compacts Parquet blocks on Serving layer tables to minimize BI query scanning latency.'
  },
  'v4': {
    title: 'dbt_generate_docs (Docs)',
    body: 'Triggers a script to compile updated dbt project schemas, schema descriptions, and dependency graphs, pushing them to the metadata repository.'
  }
};
