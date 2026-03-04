// ── Ceibo – Gestión de Rutas ──────────────────────────────────
// Stores route data in localStorage so it persists across page reloads.

const STORAGE_KEY = 'ceibo_routes';

// ── Helpers ───────────────────────────────────────────────────
function loadRoutes() {
  try {
    return JSON.parse(localStorage.getItem(STORAGE_KEY)) || [];
  } catch {
    return [];
  }
}

function saveRoutes(routes) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(routes));
}

// ── Render ────────────────────────────────────────────────────
function renderRoutes() {
  const routes = loadRoutes();
  const list = document.getElementById('routeList');
  const empty = document.getElementById('emptyMessage');

  list.innerHTML = '';

  if (routes.length === 0) {
    empty.style.display = 'block';
    return;
  }

  empty.style.display = 'none';
  routes.forEach((route, index) => {
    const li = document.createElement('li');
    li.className = 'route-item';

    const distanceText = route.distance
      ? ` &bull; ${escapeHtml(String(route.distance))} km`
      : '';

    li.innerHTML = `
      <strong>${escapeHtml(route.name)}</strong>
      <span>${escapeHtml(route.origin)} &rarr; ${escapeHtml(route.destination)}${distanceText}</span>
      <button class="delete-btn" data-index="${index}" aria-label="Eliminar ruta">&times;</button>
    `;

    list.appendChild(li);
  });
}

// ── Security helper ───────────────────────────────────────────
function escapeHtml(text) {
  const div = document.createElement('div');
  div.appendChild(document.createTextNode(text));
  return div.innerHTML;
}

// ── Event Handlers ─────────────────────────────────────────────
document.getElementById('routeForm').addEventListener('submit', function (e) {
  e.preventDefault();

  const name = document.getElementById('routeName').value.trim();
  const origin = document.getElementById('routeOrigin').value.trim();
  const destination = document.getElementById('routeDestination').value.trim();
  const distanceRaw = document.getElementById('routeDistance').value.trim();
  const distance = distanceRaw !== '' ? parseFloat(distanceRaw) : null;

  if (!name || !origin || !destination) return;
  if (distance !== null && isNaN(distance)) return;

  const routes = loadRoutes();
  routes.push({ name, origin, destination, distance });
  saveRoutes(routes);
  renderRoutes();

  this.reset();
  document.getElementById('routeName').focus();
});

document.getElementById('routeList').addEventListener('click', function (e) {
  const btn = e.target.closest('.delete-btn');
  if (!btn) return;

  const index = parseInt(btn.dataset.index, 10);
  const routes = loadRoutes();
  routes.splice(index, 1);
  saveRoutes(routes);
  renderRoutes();
});

// ── Init ──────────────────────────────────────────────────────
document.getElementById('year').textContent = new Date().getFullYear();
renderRoutes();
