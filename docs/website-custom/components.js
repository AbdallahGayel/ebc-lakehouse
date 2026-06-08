/* ============================================================
   EBC Lakehouse — Shared Components
   Injects Sidebar, Navigation, and Search into multi-page HTML
   ============================================================ */

const sidebarHTML = `
  <button class="mobile-toggle" id="mobileToggle" aria-label="Toggle navigation">☰</button>
  <div class="sidebar-overlay" id="sidebarOverlay"></div>

  <aside class="sidebar" id="sidebar">
    <div class="sidebar-header">
      <div class="sidebar-logo">
        <div class="sidebar-logo-icon">EBC</div>
        <div class="sidebar-logo-text">
          <strong>EBC Lakehouse</strong>
          <span>v4.0 — Flink CDC Edition</span>
        </div>
      </div>
      <button id="themeToggle" class="theme-toggle" aria-label="Toggle light/dark mode">
        <span class="sun-icon">☀️</span>
        <span class="moon-icon">🌙</span>
      </button>
    </div>

    <!-- Search Bar -->
    <div class="sidebar-search">
      <input type="text" id="globalSearchInput" placeholder="🔍 Search docs (Cmd+K)..." autocomplete="off">
    </div>

    <nav class="sidebar-nav">
      <div class="nav-group">
        <div class="nav-group-label">Overview</div>
        <a class="nav-link" href="#hero" data-section="hero">
          <span class="nav-icon">🏠</span> Home
        </a>
        <a class="nav-link" href="#architecture" data-section="architecture">
          <span class="nav-icon">🏗️</span> Architecture
        </a>
        <a class="nav-link" href="#medallion" data-section="medallion">
          <span class="nav-icon">🥇</span> Medallion Layers
        </a>
      </div>
      <div class="nav-group">
        <div class="nav-group-label">Deep Dives</div>
        <a class="nav-link" href="#services" data-section="services">
          <span class="nav-icon">⚙️</span> Service Catalog
        </a>
        <a class="nav-link" href="#dataflow" data-section="dataflow">
          <span class="nav-icon">🔄</span> Data Flow
        </a>
        <a class="nav-link" href="#data-dictionary" data-section="data-dictionary">
          <span class="nav-icon">📋</span> Data Dictionary
          <span class="nav-badge">Soon</span>
        </a>
      </div>
      <div class="nav-group">
        <div class="nav-group-label">Reference</div>
        <a class="nav-link" href="#reference" data-section="reference">
          <span class="nav-icon">🛠️</span> Configuration
        </a>
        <a class="nav-link" href="#runbook" data-section="runbook">
          <span class="nav-icon">📖</span> Runbook
        </a>
        <a class="nav-link" href="#onboarding" data-section="onboarding">
          <span class="nav-icon">🎓</span> Onboarding Guide
          <span class="nav-badge">Soon</span>
        </a>
      </div>
    </nav>
  </aside>

  <!-- Global Search Overlay -->
  <div class="search-overlay" id="searchOverlay">
    <div class="search-modal">
      <div class="search-modal-header">
        <input type="text" id="searchModalInput" placeholder="Search documentation..." autocomplete="off">
        <button id="closeSearchBtn">✕</button>
      </div>
      <div class="search-results" id="searchResults">
        <div class="search-empty-state">Type to start searching...</div>
      </div>
    </div>
  </div>
`;

function injectSidebar() {
  const container = document.getElementById('sidebar-container');
  if (container) {
    container.innerHTML = sidebarHTML;
  }

  // Inject Lunr.js dynamically (search)
  if (!document.getElementById('lunr-script')) {
    const script = document.createElement('script');
    script.id = 'lunr-script';
    script.src = 'https://cdn.jsdelivr.net/npm/lunr/lunr.min.js';
    script.onload = () => window.dispatchEvent(new Event('lunr-ready'));
    document.head.appendChild(script);
  }

  // Inject Mermaid.js dynamically (DAG diagrams in dataflow section)
  if (!document.getElementById('mermaid-script')) {
    const script = document.createElement('script');
    script.id = 'mermaid-script';
    script.src = 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js';
    script.onload = () => {
      if (window.mermaid) {
        const isLight = document.documentElement.getAttribute('data-theme') === 'light';
        window.mermaid.initialize({
          theme: isLight ? 'default' : 'dark',
          securityLevel: 'loose',
          themeVariables: isLight ? {} : { background: '#0f172a', primaryColor: '#1e293b' }
        });
        setTimeout(() => window.mermaid.run({ querySelector: '.mermaid' }), 0);
      }
    };
    document.head.appendChild(script);
  }
}

// Call injection when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', injectSidebar);
} else {
  injectSidebar();
}
