// include-nav.js (safe cross-browser version)

// ========== CONFIGURATION ==========
const NAVBAR_CONFIG = {
  'app.tbc.city': {
    showMaps: true,
    showTeam: true
  },
  'maps.tbc.city': {
    showMaps: true,
    showTeam: false
  },
  'default': {
    showMaps: true,
    showTeam: true
  }
};
// ===================================

(function injectNavbar(){
  var mount = document.getElementById('navbar-include');
  if (!mount) return;

  var hostname = window.location.hostname;
  var config = NAVBAR_CONFIG[hostname] || NAVBAR_CONFIG['default'];

  // bump to defeat caches when updating
  var VERSION = '2025-12-27-1';

  fetch('/navbar.html?v=' + encodeURIComponent(VERSION), { cache: 'no-cache' })
    .then(function(r){ return r.text(); })
    .then(function(html){
      mount.innerHTML = html;

      try { applyNavbarConfig(mount, config); } catch (e) { console.error('applyNavbarConfig error:', e); }

      // execute any inline scripts in navbar.html
      var scripts = mount.querySelectorAll('script');
      scripts.forEach(function(oldScript){
        var s = document.createElement('script');
        if (oldScript.src) s.src = oldScript.src;
        if (oldScript.type) s.type = oldScript.type;
        s.text = oldScript.text || oldScript.textContent;
        oldScript.replaceWith(s);
      });

      // ---------- Active-link highlight ----------
      var file = (location.pathname.split('/').pop() || 'index.html').split('?')[0].split('#')[0];

      var map = {
        'network_map.html': 'a[href="network_map.html"]',
        'org_map.html': 'a[href="org_map.html"]',
        'disaster_data.html': 'a[href="disaster_data.html"]',
        'touchpoint.html': 'a[href="touchpoint.html"]',
        'organizations.html': 'a[href="organizations.html"]',
        'leaders.html': 'a[href="leaders.html"]',
        'networks.html': 'a[href="networks.html"]',
        'network_prayer_request.html': 'a[href="/network_prayer_request.html"]',
        // Prayer App Mappings
        'input.html': 'a[href="/prayer/input.html"]',
        'index.html': 'a[href="/prayer/index.html"]',
        'history.html': 'a[href="/prayer/history.html"]'
      };

      var sel = map[file];
      if (sel) {
        var link = mount.querySelector(sel);
        if (link) {
          link.classList.add('active');
          link.setAttribute('aria-current', 'page');
        }
      }

      // Highlight Maps parent
      var mapsFiles = ['network_map.html', 'org_map.html', 'disaster_data.html'];
      if (mapsFiles.indexOf(file) !== -1) {
        var mapsDropdown = findDropdownByLabel(mount, 'Maps');
        if (mapsDropdown) {
          var toggle = mapsDropdown.querySelector('.dropdown-toggle');
          if (toggle) toggle.classList.add('active');
        }
      }

      // Highlight Team parent
      var teamFiles = ['organizations.html','leaders.html','networks.html','touchpoint.html'];
      if (teamFiles.indexOf(file) !== -1) {
        var teamDropdown = findDropdownByLabel(mount, 'Team');
        if (teamDropdown) {
          var toggle2 = teamDropdown.querySelector('.dropdown-toggle');
          if (toggle2) toggle2.classList.add('active');
        }
      }

      // Highlight Personal Prayer parent
      var prayerFiles = ['input.html', 'index.html', 'history.html'];
      if (prayerFiles.indexOf(file) !== -1 && location.pathname.includes('/prayer/')) {
        var prayerDropdown = findDropdownByLabel(mount, 'Personal Prayer');
        if (prayerDropdown) {
          var toggle3 = prayerDropdown.querySelector('.dropdown-toggle');
          if (toggle3) toggle3.classList.add('active');
        }
      }

      // After render, toggle auth-based items
      initAuthVisibility(mount);
    })
    .catch(function(err){
      console.error('Navbar include failed:', err);
      mount.innerHTML = '<div style="background:#bf3426;color:#fff;padding:8px 12px;">Navigation failed to load</div>';
    });

  // ------ Helpers ------
  function findDropdownByLabel(root, labelText) {
    var dropdowns = root.querySelectorAll('.dropdown');
    for (var i = 0; i < dropdowns.length; i++) {
      var toggle = dropdowns[i].querySelector('.dropdown-toggle');
      if (toggle && typeof toggle.textContent === 'string' && toggle.textContent.indexOf(labelText) !== -1) {
        return dropdowns[i];
      }
    }
    return null;
  }

  function hideElement(el) { if (el && el.style) el.style.display = 'none'; }
  function showElement(el) { if (el && el.style) el.style.display = ''; }

  function applyNavbarConfig(mount, config) {
    if (!config.showMaps) {
      var mapsDropdown = findDropdownByLabel(mount, 'Maps');
      hideElement(mapsDropdown);
    }
    if (!config.showTeam) {
      var teamDropdown = findDropdownByLabel(mount, 'Team');
      hideElement(teamDropdown);
    }
  }

  // Check auth status and toggle Dropdowns + Sign in/out links
  function initAuthVisibility(mount) {
    var teamDropdown = findDropdownByLabel(mount, 'Team');
    var prayerDropdown = findDropdownByLabel(mount, 'Personal Prayer');
    var signin = mount.querySelector('#signin-link');
    var signout = mount.querySelector('#signout-link');

    // Default to logged-out view
    if (teamDropdown) hideElement(teamDropdown);
    if (prayerDropdown) hideElement(prayerDropdown);
    if (signout) hideElement(signout);
    if (signin) showElement(signin);

    // Try to detect Cloudflare Access auth
    fetch('/auth/status', { credentials: 'include' })
      .then(function(r){ return r.ok ? r.json() : { authenticated:false }; })
      .then(function(data){
        var authed = !!(data && data.authenticated);
        if (authed) {
          if (teamDropdown) showElement(teamDropdown);
          if (prayerDropdown) showElement(prayerDropdown);
          if (signout) showElement(signout);
          if (signin) hideElement(signin);
        } else {
          if (teamDropdown) hideElement(teamDropdown);
          if (prayerDropdown) hideElement(prayerDropdown);
          if (signout) hideElement(signout);
          if (signin) showElement(signin);
        }
      })
      .catch(function(){
        // On failure, stay in logged-out view
      });
  }
})();
