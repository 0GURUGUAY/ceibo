import { routeSegment, distanceNm, getBearing, computeTWA, movePoint } from './polarRouter.js';
import { feature as topojsonFeature } from 'https://cdn.jsdelivr.net/npm/topojson-client@3/+esm';
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

delete L.Icon.Default.prototype._getIconUrl;
L.Icon.Default.mergeOptions({
    iconRetinaUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png',
    iconUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png',
    shadowUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png'
});

let map;
let activeTabName = 'cloud';
let standardTileLayer;
let satelliteTileLayer;
let marineDepthLayer;
let marineHazardLayer;
let isobarLayer;
let activeBaseLayer;
let baseLayerControl;
let routePoints = [];
let markers = [];
let routeLayer = null;
let windLayer = null;
const nowLocal = new Date();
let departureDate = `${nowLocal.getFullYear()}-${String(nowLocal.getMonth() + 1).padStart(2, '0')}-${String(nowLocal.getDate()).padStart(2, '0')}`;
let departureTime = `${String(nowLocal.getHours()).padStart(2, '0')}:00`;
let tackingTimeHours = 0.5;
let sailMode = 'auto';
const weatherCache = new Map();
let lastWeatherUpdateAt = null;
let waypointWindDirectionLayers = [];
let waveDirectionSegmentLayers = [];
let waypointPassageSlots = new Map();
let lastRouteBounds = null;
let generatedWaypointMarkers = [];
let measureModeEnabled = false;
let measurePoints = [];
let measurePolylineLayer = null;
let measurePointLayers = [];
let measureLabelLayers = [];
let lastComputedReportData = null;
let forecastWindowDays = 3;
let arrivalPoiMarkers = [];
let lastDepartureSuggestion = null;
let selectedUserWaypointIndex = -1;
let currentLoadedRouteIndex = -1;
let waypointPhotoEntries = [];
let pendingWaypointPhotoDraft = null;
let waypointPhotoPreviewObjectUrl = null;
let editingWaypointPhotoId = null;
const waypointPhotoMarkersById = new Map();
let waypointEditorMap = null;
let waypointEditorMarker = null;
let waypointEditorIsUpdating = false;
const waypointReverseGeocodeCache = new Map();
let waypointPhotoInputProcessing = false;
const MOTOR_WIND_THRESHOLD_KN = 5;
const MOTOR_SPEED_KN = 7;
const RECOMMENDED_MAX_WIND_KN = 20;
const OVERPASS_URLS = [
    'https://overpass.kumi.systems/api/interpreter',
    'https://overpass.private.coffee/api/interpreter',
    'https://overpass-api.de/api/interpreter'
];
const OVERPASS_MIN_INTERVAL_MS = 900;
const OVERPASS_CACHE_TTL_MS = 8 * 60 * 1000;
const OVERPASS_429_COOLDOWN_MS = 10 * 60 * 1000;
const WAYPOINT_PHOTOS_STORAGE_KEY = 'ceiboWaypointPhotos';
const SAVED_ROUTES_STORAGE_KEY = 'savedRoutes';
const CLOUD_CONFIG_STORAGE_KEY = 'ceiboCloudConfigV1';
const NAV_LOG_STORAGE_KEY = 'ceiboNavLogV1';
const ENGINE_LOG_STORAGE_KEY = 'ceiboEngineLogV1';
const CLOUD_TABLE_NAME = 'ceibo_route_collections';
const CLOUD_ALLOWED_USERS_TABLE = 'allowed_users';
const CLOUD_AUTO_PULL_INTERVAL_MS = 45000;
const CLOUD_LOGBOOK_PUSH_DEBOUNCE_MS = 12000;
let savedRoutesCache = [];
let cloudClient = null;
let cloudConfig = null;
let cloudConnected = false;
let cloudAuthUser = null;
let cloudAuthSubscription = null;
let cloudAutoPullTimer = null;
let cloudAutoPullInFlight = false;
let cloudLogbookPushTimer = null;
let cloudWhitelistCheckInFlight = false;
let navLogEntries = [];
let navWatchId = null;
let navLatestHeelDeg = null;
let navLatestSpeedKn = null;
let navMotionListenerBound = false;
let engineLogEntries = [];
let lastAiRouteCandidates = [];
let aiTrafficEntries = [];
let aiTrafficAutoHideTimer = null;
let weatherFocusMarker = null;
let weatherFocusPoint = null;
let weatherPointerPlacementMode = false;
let maintenanceBoards = [];
let selectedMaintenanceBoardId = null;
let activeMaintenanceAnnotationId = null;
let maintenanceSchemaManagerVisible = false;
let maintenanceExpenses = [];
let maintenanceSuppliers = [];
let activeMaintenanceSubtab = 'tasks';
let maintenanceTesseractLoader = null;
let maintenancePdfJsLoader = null;
let maintenanceInvoicePreviewUrl = '';
let maintenanceInvoicePreviewType = '';
let maintenanceLastScannedText = '';
let protectedDataLoaded = false;
let overpassLastRequestAt = 0;
const overpassQueryCache = new Map();
const overpassEndpointCooldownUntil = new Map();
let overpassPreferredEndpoint = OVERPASS_URLS[0];
const MAP_STYLE_STORAGE_KEY = 'ceiboMapStyle';
const MAP_VIEW_STORAGE_KEY = 'ceiboMapView';
const APP_LANGUAGE_STORAGE_KEY = 'ceiboAppLanguage';
const TRANSPARENT_TILE_DATA_URI = 'data:image/gif;base64,R0lGODlhAQABAAD/ACwAAAAAAQABAAACADs=';
const OWM_TILE_APPID_STORAGE_KEY = 'ceiboOwmTileAppId';
const MAINTENANCE_BOARDS_STORAGE_KEY = 'ceiboMaintenanceBoardsV1';
const MAINTENANCE_EXPENSES_STORAGE_KEY = 'ceiboMaintenanceExpensesV1';
const MAINTENANCE_SUPPLIERS_STORAGE_KEY = 'ceiboMaintenanceSuppliersV1';
const MAINTENANCE_LLM_PROVIDER_STORAGE_KEY = 'ceiboMaintenanceLlmProviderV1';
const MAINTENANCE_LLM_MODEL_STORAGE_KEY = 'ceiboMaintenanceLlmModelV1';
const MAINTENANCE_LLM_API_KEY_STORAGE_KEY = 'ceiboMaintenanceLlmApiKeyV1';
const MAINTENANCE_COLOR_ORDER = ['red', 'orange', 'green', 'blue'];
const MAINTENANCE_TASK_STATUS_ORDER = ['active', 'planned', 'done'];
const MAINTENANCE_COLORS = {
    green: { key: 'green', hex: '#33c26f', fr: 'Vert · pas urgent', es: 'Verde · no urgente', groupFr: 'Vert · Pas urgent', groupEs: 'Verde · No urgente' },
    orange: { key: 'orange', hex: '#ff9f2f', fr: 'Orange · important', es: 'Naranja · importante', groupFr: 'Orange · Important', groupEs: 'Naranja · Importante' },
    red: { key: 'red', hex: '#ff5c5c', fr: 'Rouge · urgent', es: 'Rojo · urgente', groupFr: 'Rouge · Urgent', groupEs: 'Rojo · Urgente' },
    blue: { key: 'blue', hex: '#4ca3ff', fr: 'Bleu · information', es: 'Azul · información', groupFr: 'Bleu · Information', groupEs: 'Azul · Información' }
};
const MAINTENANCE_TASK_STATUSES = {
    active: { key: 'active', fr: 'Actif', es: 'Activo' },
    planned: { key: 'planned', fr: 'À prévoir', es: 'A prever' },
    done: { key: 'done', fr: 'Fini', es: 'Terminado' }
};
const STRONG_WAVE_THRESHOLD_M = 1.8;
const LAND_DATA_SOURCES = [
    {
        url: 'https://cdn.jsdelivr.net/npm/world-atlas@2/land-50m.json',
        objectName: 'land'
    },
    {
        url: 'https://cdn.jsdelivr.net/npm/world-atlas@2/land-110m.json',
        objectName: 'land'
    }
];
const COASTAL_CLEARANCE_NM = 5;
const AUTO_WP_MIN_SPACING_NM = 10;
const AUTO_WP_MAX_INTERMEDIATE = 24;
const AUTO_WEATHER_SPLIT_MAX_HOURS = 2;
const AUTO_WEATHER_SPLIT_MAX_SUBSEGMENTS = 8;
let autoWpMinSpacingNm = null;
let currentLanguage = 'fr';

let landGeometry = null;

function normalizeHourTime(timeValue) {
    const [rawHour, rawMinute] = String(timeValue || '12:00').split(':');
    let hour = parseInt(rawHour, 10);
    let minute = parseInt(rawMinute, 10);

    if (!Number.isFinite(hour)) hour = 12;
    if (!Number.isFinite(minute)) minute = 0;

    hour = Math.max(0, Math.min(23, hour));
    minute = Math.max(0, Math.min(59, minute));

    minute = Math.round(minute / 10) * 10;
    if (minute === 60) {
        hour = (hour + 1) % 24;
        minute = 0;
    }

    return `${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}`;
}

function updateDepartureDateTimeInput() {
    const input = document.getElementById('departureDateTimeInput');
    if (!input) return;
    input.value = `${departureDate}T${normalizeHourTime(departureTime)}`;
}

function setDepartureFromDateTimeInput(rawValue) {
    if (!rawValue || !rawValue.includes('T')) return;
    const [datePart, timePart] = rawValue.split('T');
    if (datePart) departureDate = datePart;
    departureTime = normalizeHourTime(timePart || '12:00');
    updateDepartureDateTimeInput();
}

function normalizeMapStyle(value) {
    return value === 'satellite' ? 'satellite' : 'standard';
}

function isAuthRequiredRuntime() {
    try {
        const locationObj = window?.location;
        const protocol = String(locationObj?.protocol || '').toLowerCase();
        const hostname = String(locationObj?.hostname || '').toLowerCase();

        if (protocol === 'file:') return false;
        if (!hostname) return false;
        if (hostname === 'localhost' || hostname === '127.0.0.1' || hostname === '::1') return false;
        return true;
    } catch (_error) {
        return false;
    }
}

function normalizeLanguage(language) {
    return String(language || '').toLowerCase() === 'es' ? 'es' : 'fr';
}

function t(frText, esText) {
    return currentLanguage === 'es' ? esText : frText;
}

function getCurrentLocale() {
    return currentLanguage === 'es' ? 'es-ES' : 'fr-FR';
}

function setElementText(selector, value) {
    const node = document.querySelector(selector);
    if (!node || typeof value !== 'string') return;
    node.textContent = value;
}

function setElementPlaceholder(selector, value) {
    const node = document.querySelector(selector);
    if (!node || typeof value !== 'string') return;
    node.placeholder = value;
}

function updateLanguageButtonsUi() {
    const frBtn = document.getElementById('langFrBtn');
    const esBtn = document.getElementById('langEsBtn');
    if (frBtn) frBtn.classList.toggle('active', currentLanguage === 'fr');
    if (esBtn) esBtn.classList.toggle('active', currentLanguage === 'es');
}

function applyLanguageToUi() {
    document.documentElement.lang = currentLanguage;

    setElementText('#appTitle', 'CEIBO crm');
    setElementText('#cloudTabBtn', t('Cloud', 'Nube'));
    setElementText('#routesTabBtn', t('Routes', 'Rutas'));
    setElementText('#routingTabBtn', t('Routage', 'Navegación'));
    setElementText('#navLogTabBtn', t('Journal nav', 'Diario nav'));
    setElementText('#engineTabBtn', t('Moteur', 'Motor'));
    setElementText('#weatherTabBtn', t('Météo', 'Meteo'));
    setElementText('#arrivalTabBtn', t('Arrivée', 'Llegada'));
    setElementText('#waypointTabBtn', t('Waypoint', 'Waypoint'));
    setElementText('#maintenanceTabBtn', t('Maintenance', 'Mantenimiento'));

    setElementText('label[for="routeNameInput"]', t('Nom de la route:', 'Nombre de la ruta:'));
    setElementPlaceholder('#routeNameInput', t('Nom de la route', 'Nombre de la ruta'));
    setElementText('#saveRouteBtn', t('Sauvegarder', 'Guardar'));
    setElementText('#exportRouteBtn', t('Exporter', 'Exportar'));
    setElementText('#exportRouteGpxBtn', t('Exporter GPX', 'Exportar GPX'));
    setElementText('#resetBtn', t('Reset', 'Reiniciar'));
    setElementText('#reverseRouteBtn', t('Route retour', 'Ruta retorno'));
    setElementText('#exportVoyagePdfBtn', t('Exporter rapport PDF', 'Exportar informe PDF'));
    setElementText('label[for="savedRoutesSelect"]', t('Routes sauvegardées:', 'Rutas guardadas:'));
    setElementText('#loadRouteBtn', t('Charger', 'Cargar'));
    setElementText('#deleteRouteBtn', t('Supprimer', 'Eliminar'));
    setElementText('#measureClearBtn', t('Effacer mesure', 'Borrar medición'));
    setElementText('label[for="importRouteInput"]', t('Importer (JSON / GPX):', 'Importar (JSON / GPX):'));

    setElementText('#computeBtn', t('Calculer', 'Calcular'));
    setElementText('#deleteSelectedWpBtn', t('Supprimer WP', 'Eliminar WP'));
    setElementText('#recenterBtn', t('Recentrer route', 'Centrar ruta'));
    setElementText('#suggestDepartureBtn', t('Conseiller départ météo (≤ 20 kn)', 'Sugerir salida meteo (≤ 20 kn)'));
    setElementText('#suggestAiRoutesBtn', t('Proposer routes IA (safe / perf)', 'Proponer rutas IA (safe / perf)'));
    setElementText('label[for="departureDateTimeInput"]', t('Départ (date + heure):', 'Salida (fecha + hora):'));
    setElementText('label[for="tackingTimeInput"]', t('Temps du bord (min):', 'Tiempo de bordo (min):'));
    setElementText('label[for="sailModeSelect"]', t('Mode voiles:', 'Modo velas:'));
    setElementText('label[for="autoWpSpacingInput"]', t('Contournement côte:', 'Rodeo costa:'));
    setElementText('label[for="forecastWindowDaysSelect"]', t('Fenêtre analyse:', 'Ventana análisis:'));
    setElementText('#departureSuggestionInfo', t('Suggestion départ: en attente', 'Sugerencia salida: en espera'));
    setElementText('#aiRouteSuggestionInfo', t('Routes IA: en attente', 'Rutas IA: en espera'));

    setElementText('#tackingTimeInput option[value="0.25"]', t('15 minutes', '15 minutos'));
    setElementText('#tackingTimeInput option[value="0.33"]', t('20 minutes', '20 minutos'));
    setElementText('#tackingTimeInput option[value="0.5"]', t('30 minutes', '30 minutos'));
    setElementText('#tackingTimeInput option[value="0.75"]', t('45 minutes', '45 minutos'));
    setElementText('#tackingTimeInput option[value="1"]', t('1 heure', '1 hora'));
    setElementText('#tackingTimeInput option[value="1.5"]', t('1.5 heures', '1.5 horas'));
    setElementText('#tackingTimeInput option[value="2"]', t('2 heures', '2 horas'));
    setElementText('#sailModeSelect option[value="prudent"]', t('Prudent', 'Prudente'));
    setElementText('#sailModeSelect option[value="auto"]', t('Auto', 'Auto'));
    setElementText('#sailModeSelect option[value="performance"]', t('Performance', 'Rendimiento'));
    setElementText('#autoWpSpacingInput option[value="off"]', t('Désactivé', 'Desactivado'));
    setElementText('#forecastWindowDaysSelect option[value="2"]', t('2 jours', '2 días'));
    setElementText('#forecastWindowDaysSelect option[value="3"]', t('3 jours', '3 días'));
    setElementText('#forecastWindowDaysSelect option[value="5"]', t('5 jours', '5 días'));
    setElementText('#forecastWindowDaysSelect option[value="7"]', t('7 jours', '7 días'));
    setElementPlaceholder('#watchCrewInput', t('Ex: Max + Ana', 'Ej: Max + Ana'));
    setElementPlaceholder('#watchHeadingInput', t('Ex: 235', 'Ej: 235'));
    setElementPlaceholder('#watchWindDirInput', t('Ex: 260', 'Ej: 260'));
    setElementPlaceholder('#watchWindSpeedInput', t('Ex: 16.5', 'Ej: 16.5'));
    setElementPlaceholder('#watchSailConfigInput', t('Ex: GV 1 ris + génois', 'Ej: Mayor 1 rizo + génova'));
    setElementPlaceholder('#watchBarometerInput', t('Ex: 1014.8', 'Ej: 1014.8'));
    setElementPlaceholder('#watchLogNmInput', t('Ex: 842.3', 'Ej: 842.3'));
    setElementPlaceholder('#watchEventsInput', t('Changement de voile, prise de ris, trafic, sécurité, etc.', 'Cambio de vela, rizo, tráfico, seguridad, etc.'));
    setElementPlaceholder('#engineHoursInput', t('Ex: 1250.4', 'Ej: 1250.4'));
    setElementPlaceholder('#fuelAddedInput', t('Ex: 35', 'Ej: 35'));
    setElementPlaceholder('#engineLogNoteInput', t('Maintenance, bruit, filtre, etc.', 'Mantenimiento, ruido, filtro, etc.'));
    setElementPlaceholder('#owmApiKeyInput', t('Ta clé OWM', 'Tu clave OWM'));
    setElementPlaceholder('#waypointPlaceNameInput', t('Ex: Cala Blanca', 'Ej: Cala Blanca'));
    setElementPlaceholder('#waypointCommentInput', t("Ex: Bon abri par vent d'ouest, tenue correcte", 'Ej: Buen abrigo con viento oeste, agarre correcto'));
    setElementPlaceholder('#waypointBottomTypeInput', t('Ex: sable, vase, herbiers', 'Ej: arena, fango, praderas'));

    setElementText('#waypointRatingInput option[value="1"]', '1 / 5');
    setElementText('#waypointRatingInput option[value="2"]', '2 / 5');
    setElementText('#waypointRatingInput option[value="3"]', '3 / 5');
    setElementText('#waypointRatingInput option[value="4"]', '4 / 5');
    setElementText('#waypointRatingInput option[value="5"]', '5 / 5');
    setElementText('#waypointCleanlinessInput option[value="1"]', '1 / 5');
    setElementText('#waypointCleanlinessInput option[value="2"]', '2 / 5');
    setElementText('#waypointCleanlinessInput option[value="3"]', '3 / 5');
    setElementText('#waypointCleanlinessInput option[value="4"]', '4 / 5');
    setElementText('#waypointCleanlinessInput option[value="5"]', '5 / 5');
    setElementText('#waypointDepthInput option[value="5"]', '5 m');
    setElementText('#waypointDepthInput option[value="10"]', '10 m');
    setElementText('#waypointDepthInput option[value="15"]', '15 m');
    setElementText('#waypointDepthInput option[value="20"]', '20 m');
    setElementText('#waypointDepthInput option[value="25"]', '25 m');
    setElementText('#waypointDepthInput option[value="30"]', '30 m');

    setElementText('#cloudRefreshBtn', t('Rafraîchir cloud', 'Actualizar nube'));
    const cloudTitles = document.querySelectorAll('.cloud-config-title');
    if (cloudTitles[0]) cloudTitles[0].textContent = t('V5 · Base de données partagée', 'V5 · Base de datos compartida');
    if (cloudTitles[1]) cloudTitles[1].textContent = t('Compte utilisateur (Email + mot de passe)', 'Cuenta de usuario (Email + contraseña)');
    setElementPlaceholder('#cloudEmailInput', t('Email utilisateur', 'Email usuario'));
    setElementPlaceholder('#cloudUserPasswordInput', t('Mot de passe utilisateur', 'Contraseña usuario'));
    setElementText('#cloudEmailSignInBtn', t('Se connecter email', 'Conectar email'));
    setElementText('#cloudEmailSignUpBtn', t('Créer compte', 'Crear cuenta'));
    setElementText('#cloudSignOutBtn', t('Se déconnecter', 'Desconectar'));
    setElementText('#cloudAuthStatus', t('Utilisateur: non connecté', 'Usuario: no conectado'));
    setElementText('#cloudStatus', t('Mode local (pas de cloud configuré)', 'Modo local (nube no configurada)'));
    setElementText('#cloudDataSourceStatus', t('Données routes/photos: en attente', 'Datos rutas/fotos: en espera'));
    setElementText('#cloudAutoSyncInfo', t('Routes + photos waypoint + maintenance: synchronisation cloud automatique.', 'Rutas + fotos waypoint + mantenimiento: sincronización nube automática.'));

    setElementText('#startNavLogBtn', t('Démarrer log GPS', 'Iniciar log GPS'));
    setElementText('#stopNavLogBtn', t('Arrêter log GPS', 'Detener log GPS'));
    setElementText('#requestMotionPermissionBtn', t('Activer capteur inclinaison', 'Activar sensor inclinación'));
    setElementText('#clearNavLogBtn', t('Effacer journal nav', 'Borrar diario nav'));
    setElementText('#addManualNavLogBtn', t('Ajouter entrée jour de bord', 'Añadir entrada de bitácora'));
    setElementText('#navLogStatus', t('Journal navigation: en attente', 'Diario navegación: en espera'));
    setElementText('label[for="watchTimeInput"]', t('Heure du quart:', 'Hora de guardia:'));
    setElementText('label[for="watchCrewInput"]', t('Équipage / quart:', 'Tripulación / guardia:'));
    setElementText('label[for="watchHeadingInput"]', t('Cap compas (°):', 'Rumbo compás (°):'));
    setElementText('label[for="watchWindDirInput"]', t('Vent direction (°):', 'Viento dirección (°):'));
    setElementText('label[for="watchWindSpeedInput"]', t('Vent force (kn):', 'Viento fuerza (kn):'));
    setElementText('label[for="watchSeaStateInput"]', t('État de mer:', 'Estado de mar:'));
    setElementText('label[for="watchSailConfigInput"]', t('Voilure:', 'Velamen:'));
    setElementText('label[for="watchBarometerInput"]', t('Baromètre (hPa):', 'Barómetro (hPa):'));
    setElementText('label[for="watchLogNmInput"]', t('Loch total (NM):', 'Corredera total (NM):'));
    setElementText('label[for="watchEventsInput"]', t('Événements / manœuvres:', 'Eventos / maniobras:'));
    setElementText('#watchSeaStateInput option[value="calme"]', t('Calme', 'Calma'));
    setElementText('#watchSeaStateInput option[value="peu agitée"]', t('Peu agitée', 'Poco agitada'));
    setElementText('#watchSeaStateInput option[value="agitée"]', t('Agitée', 'Agitada'));
    setElementText('#watchSeaStateInput option[value="forte"]', t('Forte', 'Fuerte'));
    setElementText('#navLogTab label[style*="Entrées navigation"]', t('Entrées navigation:', 'Entradas navegación:'));

    setElementText('#saveEngineLogBtn', t('Ajouter entrée moteur', 'Añadir entrada motor'));
    setElementText('#clearEngineLogBtn', t('Effacer livre moteur', 'Borrar libro motor'));
    setElementText('label[for="engineHoursInput"]', t('Compteur moteur (h):', 'Contador motor (h):'));
    setElementText('label[for="fuelAddedInput"]', t('Carburant ajouté (L):', 'Combustible añadido (L):'));
    setElementText('label[for="engineLogNoteInput"]', t('Note:', 'Nota:'));
    setElementText('#engineTab label[style*="Historique moteur"]', t('Historique moteur:', 'Historial motor:'));

    setElementText('#placeWeatherPointerBtn', t('Placer pointeur météo', 'Colocar puntero meteo'));
    setElementText('#useMapCenterWeatherBtn', t('Centre carte → pointeur', 'Centro mapa → puntero'));
    setElementText('#refreshWeatherOutlookBtn', t('Actualiser météo', 'Actualizar meteo'));
    setElementText('#testOwmApiKeyBtn', t('Tester clé OWM', 'Probar clave OWM'));
    setElementText('#saveOwmApiKeyBtn', t('Enregistrer clé OWM', 'Guardar clave OWM'));
    setElementText('#clearOwmApiKeyBtn', t('Supprimer clé OWM', 'Borrar clave OWM'));
    setElementText('#toggleWeatherApiConfigBtn', t('Afficher API météo', 'Mostrar API meteo'));
    setElementText('#owmApiKeyStatus', t('Clé OWM: non testée', 'Clave OWM: no probada'));
    setElementText('#weatherApiConfigSummary', t('API météo connectée.', 'API meteo conectada.'));
    setElementText('#weatherOutlookStatus', t('Météo: en attente', 'Meteo: en espera'));

    setElementText('#analyzeArrivalBtn', t('Conseiller mouillage à l\'arrivée', 'Sugerir fondeo a la llegada'));
    setElementText('#arrivalSummary', t('Analyse mouillage: en attente', 'Análisis fondeo: en espera'));
    setElementText('#nearbyRestaurantsLabel', t('Restaurants proches:', 'Restaurantes cercanos:'));
    setElementText('#nearbyShopsLabel', t('Magasins / courses:', 'Tiendas / compras:'));
    setElementText('label[for="waypointPhotoInput"]', t('Photo mouillage:', 'Foto fondeo:'));
    setElementText('#saveWaypointPhotoBtn', t('Ajouter ce waypoint photo', 'Añadir este waypoint foto'));
    setElementText('#cancelWaypointPhotoEditBtn', t('Annuler modification', 'Cancelar edición'));
    setElementText('#waypointQuickCaptureBtn', t('📷 Prendre photo (WP auto)', '📷 Tomar foto (WP auto)'));
    setElementText('#waypointPhotoStatus', t("Coordonnées: en attente d'une photo", 'Coordenadas: esperando una foto'));
    setElementText('#waypointGoogleMapLink', t('Ouvrir dans Google Maps', 'Abrir en Google Maps'));
    setElementText('label[for="waypointPlaceNameInput"]', t('Nom du lieu:', 'Nombre del lugar:'));
    setElementText('label[for="waypointCommentInput"]', t('Commentaire:', 'Comentario:'));
    setElementText('label[for="waypointRatingInput"]', t('Note globale:', 'Nota global:'));
    setElementText('label[for="waypointCleanlinessInput"]', t('Propreté:', 'Limpieza:'));
    setElementText('#waypointTab .waypoint-protection-item > label', t('Protection (rose des vents):', 'Protección (rosa de vientos):'));
    setElementText('label[for="waypointDepthInput"]', t('Profondeur:', 'Profundidad:'));
    setElementText('label[for="waypointBottomTypeInput"]', t('Type de fond:', 'Tipo de fondo:'));
    setElementText('#waypointSavedAnchoragesLabel', t('Mouillages enregistrés:', 'Fondeos guardados:'));

    setElementText('label[for="maintenanceSchemaNameInput"]', t('Nom du schéma:', 'Nombre del esquema:'));
    setElementPlaceholder('#maintenanceSchemaNameInput', t('Ex: Compartiment moteur', 'Ej: Compartimento motor'));
    setElementText('#maintenanceTasksSubtabBtn', t('Tâches', 'Tareas'));
    setElementText('#maintenanceExpensesSubtabBtn', t('Dépenses & factures', 'Gastos y facturas'));
    setElementText('#maintenanceSuppliersSubtabBtn', t('Fournisseurs', 'Proveedores'));
    setElementText('label[for="maintenanceSchemaInput"]', t('Importer schéma (image):', 'Importar esquema (imagen):'));
    setElementText('#maintenanceToggleSchemaManagerBtn', t('Gérer les schémas', 'Gestionar esquemas'));
    setElementText('#maintenanceAddSchemaBtn', t('Ajouter schéma', 'Añadir esquema'));
    setElementText('#maintenanceDeleteSchemaBtn', t('Supprimer schéma', 'Eliminar esquema'));
    setElementText('label[for="maintenanceSchemaSelect"]', t('Schémas enregistrés:', 'Esquemas guardados:'));
    setElementText('label[for="maintenancePinColorInput"]', t('Couleur pastille:', 'Color marcador:'));
    setElementText('#maintenancePinColorInput option[value="green"]', t('Vert · pas urgent', 'Verde · no urgente'));
    setElementText('#maintenancePinColorInput option[value="orange"]', t('Orange · important', 'Naranja · importante'));
    setElementText('#maintenancePinColorInput option[value="red"]', t('Rouge · urgent', 'Rojo · urgente'));
    setElementText('#maintenancePinColorInput option[value="blue"]', t('Bleu · information', 'Azul · información'));
    setElementText('label[for="maintenanceTaskStatusInput"]', t('État:', 'Estado:'));
    setElementText('#maintenanceTaskStatusInput option[value="active"]', t('Actif', 'Activo'));
    setElementText('#maintenanceTaskStatusInput option[value="done"]', t('Fini', 'Terminado'));
    setElementText('#maintenanceTaskStatusInput option[value="planned"]', t('À prévoir', 'A prever'));
    setElementText('label[for="maintenanceLegendInput"]', t('Légende:', 'Leyenda:'));
    setElementPlaceholder('#maintenanceLegendInput', t('Ex: Changer turbine pompe eau', 'Ej: Cambiar impulsor bomba agua'));
    setElementText('#maintenanceCanvasPlaceholder', t('Aucun schéma sélectionné', 'Ningún esquema seleccionado'));
    setElementText('#maintenanceLegendListLabel', t('Tâches par schéma:', 'Tareas por esquema:'));
    setElementText('label[for="maintenanceInvoiceInput"]', t('Uploader facture (image/PDF):', 'Subir factura (imagen/PDF):'));
    setElementText('#maintenanceScanInvoiceBtn', t('Scanner facture', 'Escanear factura'));
    setElementText('#maintenanceInvoiceScanStatus', t('Scan facture: en attente', 'Escaneo factura: en espera'));
    setElementText('#maintenanceSupplierSuggestionsLabel', t('Suggestions fournisseur:', 'Sugerencias proveedor:'));
    setElementText('#maintenanceInvoiceReviewTitle', t('Copie rapide depuis l\'aperçu PDF', 'Copia rápida desde la vista previa PDF'));
    setElementText('#maintenanceInvoiceReviewHint', t('1) Sélectionne du texte dans l’aperçu à droite 2) copie (⌘C) 3) colle dans le champ choisi.', '1) Selecciona texto en la vista previa derecha 2) copia (⌘C) 3) pega en el campo elegido.'));
    setElementText('label[for="maintenanceManualPasteTargetSelect"]', t('Champ de destination:', 'Campo de destino:'));
    setElementText('#maintenancePasteSelectedTextBtn', t('Coller texte copié dans ce champ', 'Pegar texto copiado en este campo'));
    setElementText('#maintenanceAltLlmTitle', t('Analyse IA alternative', 'Análisis IA alternativo'));
    setElementText('label[for="maintenanceLlmProviderSelect"]', t('Provider:', 'Proveedor:'));
    setElementText('#maintenanceLlmProviderSelect option[value=""]', t('Désactivé', 'Desactivado'));
    setElementText('label[for="maintenanceLlmApiKeyInput"]', t('API key:', 'Clave API:'));
    setElementText('label[for="maintenanceLlmModelInput"]', t('Model:', 'Modelo:'));
    setElementPlaceholder('#maintenanceLlmModelInput', t('gpt-4o-mini / claude-3-5-haiku-latest', 'gpt-4o-mini / claude-3-5-haiku-latest'));
    setElementText('#maintenanceTestAltLlmBtn', t('Tester connexion API', 'Probar conexión API'));
    setElementText('#maintenanceRunAltLlmBtn', t('Analyser avec IA', 'Analizar con IA'));
    setElementText('#maintenanceInvoicePreviewTitle', t('Aperçu facture', 'Vista previa factura'));
    setElementText('#maintenanceInvoicePreviewPlaceholder', t('Charge une facture pour afficher l’aperçu', 'Carga una factura para mostrar la vista previa'));
    const manualPasteTargetSelect = document.getElementById('maintenanceManualPasteTargetSelect');
    if (manualPasteTargetSelect) {
        const previousValue = manualPasteTargetSelect.value;
        manualPasteTargetSelect.innerHTML = '';
        const options = [
            { value: 'expenseSupplier', label: t('Fournisseur (dépense)', 'Proveedor (gasto)') },
            { value: 'supplierContact', label: t('Contact fournisseur (nom)', 'Contacto proveedor (nombre)') },
            { value: 'supplierPhone', label: t('Téléphone contact', 'Teléfono contacto') },
            { value: 'supplierEmailToNote', label: t('Email contact (note fournisseur)', 'Email contacto (nota proveedor)') },
            { value: 'expenseIban', label: t('IBAN fournisseur', 'IBAN proveedor') },
            { value: 'expenseAmount', label: t('Montant total', 'Importe total') },
            { value: 'expenseAiComment', label: t('Commentaire IA', 'Comentario IA') },
            { value: 'expenseNote', label: t('Note dépense', 'Nota gasto') }
        ];
        options.forEach(item => {
            const option = document.createElement('option');
            option.value = item.value;
            option.textContent = item.label;
            manualPasteTargetSelect.appendChild(option);
        });
        if (options.some(item => item.value === previousValue)) {
            manualPasteTargetSelect.value = previousValue;
        }
    }
    setElementText('label[for="maintenanceExpenseDateInput"]', t('Date:', 'Fecha:'));
    setElementText('label[for="maintenanceExpenseTotalInput"]', t('Montant total:', 'Importe total:'));
    setElementText('label[for="maintenanceExpenseCurrencyInput"]', t('Devise:', 'Moneda:'));
    setElementText('label[for="maintenanceExpensePayerSelect"]', t('Qui paye:', 'Quién paga:'));
    setElementText('label[for="maintenanceExpensePaymentStatusSelect"]', t('État paiement:', 'Estado pago:'));
    setElementText('#maintenanceExpensePaymentStatusSelect option[value="pending"]', t('À payer', 'Pendiente'));
    setElementText('#maintenanceExpensePaymentStatusSelect option[value="partial"]', t('Partiel', 'Parcial'));
    setElementText('#maintenanceExpensePaymentStatusSelect option[value="paid"]', t('Payé', 'Pagado'));
    setElementText('label[for="maintenanceExpenseSupplierInput"]', t('Fournisseur:', 'Proveedor:'));
    setElementText('label[for="maintenanceExpenseSupplierIbanInput"]', t('IBAN fournisseur:', 'IBAN proveedor:'));
    setElementText('label[for="maintenanceExpenseLinesInput"]', t('Lignes de travaux/produits (une ligne = libellé ; quantité ; prix ; total):', 'Líneas de trabajos/productos (una línea = concepto ; cantidad ; precio ; total):'));
    setElementText('label[for="maintenanceExpenseNoteInput"]', t('Note:', 'Nota:'));
    setElementText('label[for="maintenanceExpenseAiCommentInput"]', t('Commentaire IA:', 'Comentario IA:'));
    setElementPlaceholder('#maintenanceExpenseAiCommentInput', t('Analyse automatique du scan', 'Análisis automático del escaneo'));
    setElementText('#maintenanceAddExpenseBtn', t('Ajouter dépense', 'Añadir gasto'));
    setElementText('#maintenanceExpensesListLabel', t('Dépenses:', 'Gastos:'));
    setElementText('label[for="maintenanceSupplierNameInput"]', t('Nom fournisseur:', 'Nombre proveedor:'));
    setElementText('label[for="maintenanceSupplierContactInput"]', t('Contact:', 'Contacto:'));
    setElementText('label[for="maintenanceSupplierPhoneInput"]', t('Téléphone urgence:', 'Teléfono urgencia:'));
    setElementText('label[for="maintenanceSupplierIbanInput"]', t('IBAN:', 'IBAN:'));
    setElementText('label[for="maintenanceSupplierNoteInput"]', t('Note:', 'Nota:'));
    setElementText('#maintenanceAddSupplierBtn', t('Ajouter fournisseur', 'Añadir proveedor'));
    setElementText('#maintenanceSuppliersListLabel', t('Fournisseurs:', 'Proveedores:'));

    const creator = document.querySelector('.creator-credit');
    if (creator) creator.textContent = t('Programme créé par Max Patissier', 'Programa creado por Max Patissier');

    const helpFab = document.getElementById('helpFab');
    if (helpFab) {
        helpFab.title = t('Aide', 'Ayuda');
        helpFab.textContent = `❓ ${t('AIDE', 'AYUDA')}`;
    }

    updateSelectedWaypointInfo();
    updateMaintenanceSchemaManagerToggleText();
    renderMaintenanceBoard();
    renderMaintenanceExpenses();
    renderMaintenanceSuppliers();
    updateMeasureInfo();
    setMeasureMode(measureModeEnabled);
    refreshBaseLayerControlLanguage();
    updateLanguageButtonsUi();
}

function getLayerControlLabels() {
    return {
        standard: t('Standard', 'Estándar'),
        satellite: t('Satellite', 'Satélite'),
        marineDepth: t('Maritime · Profondeurs', 'Marítimo · Profundidades'),
        marineHazard: t('Maritime · Dangers', 'Marítimo · Peligros'),
        isobars: t('Météo · Isobares (lignes · clé OWM)', 'Meteo · Isobaras (líneas · clave OWM)')
    };
}

function refreshBaseLayerControlLanguage() {
    if (!map || !baseLayerControl) return;

    const labels = getLayerControlLabels();
    map.removeControl(baseLayerControl);

    baseLayerControl = L.control.layers(
        {
            [labels.standard]: standardTileLayer,
            [labels.satellite]: satelliteTileLayer
        },
        {
            [labels.marineDepth]: marineDepthLayer,
            [labels.marineHazard]: marineHazardLayer,
            [labels.isobars]: isobarLayer
        },
        { position: 'topright', collapsed: false }
    ).addTo(map);
}

function setLanguage(language, options = {}) {
    const { persist = true } = options;
    currentLanguage = normalizeLanguage(language);
    if (persist) {
        localStorage.setItem(APP_LANGUAGE_STORAGE_KEY, currentLanguage);
    }
    applyLanguageToUi();
}

function initializeLanguageSwitcher() {
    const frBtn = document.getElementById('langFrBtn');
    const esBtn = document.getElementById('langEsBtn');

    const storedLanguage = normalizeLanguage(localStorage.getItem(APP_LANGUAGE_STORAGE_KEY));
    currentLanguage = storedLanguage;

    if (frBtn) {
        frBtn.addEventListener('click', () => setLanguage('fr'));
    }
    if (esBtn) {
        esBtn.addEventListener('click', () => setLanguage('es'));
    }

    applyLanguageToUi();
}

function isAuthGateLocked() {
    return isAuthRequiredRuntime() && !cloudAuthUser;
}

function setProtectedTabsEnabled(enabled) {
    const protectedTabIds = [
        'routesTabBtn',
        'routingTabBtn',
        'navLogTabBtn',
        'engineTabBtn',
        'weatherTabBtn',
        'arrivalTabBtn',
        'waypointTabBtn',
        'maintenanceTabBtn'
    ];

    protectedTabIds.forEach(id => {
        const button = document.getElementById(id);
        if (button) {
            button.disabled = !enabled;
            button.classList.toggle('tab-btn--locked', !enabled);
        }
    });
}

function clearProtectedUiData() {
    savedRoutesCache = [];
    refreshSavedList();
    setWaypointPhotoEntries([], { persistLocal: false, refreshUi: true });
    setMaintenanceBoards([], { persistLocal: false, refreshUi: true, syncCloud: false });
    setMaintenanceExpenses([], { persistLocal: false, refreshUi: true, syncCloud: false });
    setMaintenanceSuppliers([], { persistLocal: false, refreshUi: true, syncCloud: false });
    navLogEntries = [];
    renderNavLogList();
    engineLogEntries = [];
    renderEngineLogList();
    clearCurrentRoute();
    updateCloudDataSourceStatus('verrouillé (auth requise)', 0, waypointPhotoEntries.length);
}

async function applyAuthGateState({ clearWhenLocked = true } = {}) {
    const locked = isAuthGateLocked();
    setProtectedTabsEnabled(!locked);

    if (locked) {
        if (activeTabName !== 'cloud') {
            const cloudBtn = document.getElementById('cloudTabBtn');
            if (cloudBtn) cloudBtn.click();
        }

        if (clearWhenLocked) {
            clearProtectedUiData();
            protectedDataLoaded = false;
        }

        setCloudStatus(t('Accès verrouillé: authentifie-toi (email/mot de passe).', 'Acceso bloqueado: autentícate (email/contraseña).'), true);
        return;
    }

    if (!protectedDataLoaded) {
        if (isAuthRequiredRuntime()) {
            savedRoutesCache = [];
            refreshSavedList();
            setWaypointPhotoEntries([], { persistLocal: false, refreshUi: true });
            setMaintenanceBoards([], { persistLocal: false, refreshUi: true, syncCloud: false });
            setMaintenanceExpenses([], { persistLocal: false, refreshUi: true, syncCloud: false });
            setMaintenanceSuppliers([], { persistLocal: false, refreshUi: true, syncCloud: false });
            navLogEntries = [];
            renderNavLogList();
            engineLogEntries = [];
            renderEngineLogList();
            updateCloudDataSourceStatus('attente authentification', 0, 0);
        } else {
            loadWaypointPhotoEntries();
            renderWaypointPhotoList();
            syncWaypointPhotoMarkersInView();
            loadMaintenanceBoards();
            loadMaintenanceExpenses();
            loadMaintenanceSuppliers();
            renderMaintenanceBoard();
            renderMaintenanceExpenses();
            renderMaintenanceSuppliers();
            loadNavigationLogbook();
            loadEngineLogbook();
            setSavedRoutes(loadRoutesFromLocalStorage());
            refreshSavedList();
            updateCloudDataSourceStatus('cache local', getSavedRoutes().length, waypointPhotoEntries.length);
        }

        protectedDataLoaded = true;
    }

    if (isCloudReady()) {
        try {
            const routes = await pullRoutesFromCloud();
            refreshSavedList();
            setCloudStatus(t(`Cloud connecté · ${routes.length} route(s) partagée(s)`, `Nube conectada · ${routes.length} ruta(s) compartida(s)`));
            updateCloudDataSourceStatus('cloud', routes.length, waypointPhotoEntries.length);
        } catch (error) {
            const localRoutes = loadRoutesFromLocalStorage();
            if (localRoutes.length > 0) {
                setSavedRoutes(localRoutes);
                refreshSavedList();
                loadWaypointPhotoEntries();
                renderWaypointPhotoList();
                syncWaypointPhotoMarkersInView();
                loadMaintenanceBoards();
                loadMaintenanceExpenses();
                loadMaintenanceSuppliers();
                renderMaintenanceBoard();
                renderMaintenanceExpenses();
                renderMaintenanceSuppliers();
                setCloudStatus(`Cloud indisponible, affichage cache local (${localRoutes.length} route(s))`, true);
                updateCloudDataSourceStatus('cache local (fallback)', localRoutes.length, waypointPhotoEntries.length);
            } else {
                setCloudStatus(`Récupération cloud impossible: ${formatCloudError(error)}`, true);
                updateCloudDataSourceStatus('indisponible', 0, waypointPhotoEntries.length);
            }
        }
    }
}

function escapeHtml(value) {
    return String(value ?? '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
}

function toLocalDateTimeInputValue(dateObj) {
    if (!(dateObj instanceof Date) || Number.isNaN(dateObj.getTime())) return '';
    const rounded = new Date(dateObj.getTime());
    rounded.setMinutes(Math.round(rounded.getMinutes() / 10) * 10, 0, 0);
    const year = rounded.getFullYear();
    const month = String(rounded.getMonth() + 1).padStart(2, '0');
    const day = String(rounded.getDate()).padStart(2, '0');
    const hour = String(rounded.getHours()).padStart(2, '0');
    const minute = String(rounded.getMinutes()).padStart(2, '0');
    return `${year}-${month}-${day}T${hour}:${minute}`;
}

function loadSavedMapView() {
    try {
        const raw = localStorage.getItem(MAP_VIEW_STORAGE_KEY);
        if (!raw) return null;
        const parsed = JSON.parse(raw);
        const lat = Number(parsed?.lat);
        const lng = Number(parsed?.lng);
        const zoom = Number(parsed?.zoom);
        if (!Number.isFinite(lat) || !Number.isFinite(lng) || !Number.isFinite(zoom)) return null;
        if (lat < -90 || lat > 90 || lng < -180 || lng > 180) return null;
        return { lat, lng, zoom: Math.max(2, Math.min(19, Math.round(zoom))) };
    } catch (_error) {
        return null;
    }
}

function persistMapView() {
    if (!map) return;
    const center = map.getCenter();
    localStorage.setItem(MAP_VIEW_STORAGE_KEY, JSON.stringify({
        lat: Number(center.lat.toFixed(6)),
        lng: Number(center.lng.toFixed(6)),
        zoom: map.getZoom()
    }));
}

function getStoredOpenWeatherTileAppId() {
    return String(localStorage.getItem(OWM_TILE_APPID_STORAGE_KEY) || '').trim();
}

function createIsobarOverlayLayer(appId) {
    if (!appId) {
        return L.layerGroup();
    }

    const contoursLegacy = L.tileLayer(`https://tile.openweathermap.org/map/pressure_cntr/{z}/{x}/{y}.png?appid=${encodeURIComponent(appId)}`, {
        attribution: 'Isobares legacy © OpenWeatherMap',
        opacity: 0.92,
        maxNativeZoom: 18,
        errorTileUrl: TRANSPARENT_TILE_DATA_URI,
        crossOrigin: true
    });

    const isobarGroup = L.layerGroup([contoursLegacy]);

    let contourTileErrorCount = 0;
    let contourDisabled = false;

    contoursLegacy.on('tileerror', () => {
        contourTileErrorCount += 1;

        if (contourDisabled) return;
        if (contourTileErrorCount < 6) return;

        contourDisabled = true;
        if (isobarGroup.hasLayer(contoursLegacy)) {
            isobarGroup.removeLayer(contoursLegacy);
        }

        setCloudStatus(t('Isobares lignes indisponibles (OWM timeout).', 'Isobaras de líneas no disponibles (timeout OWM).'), true);
    });

    contoursLegacy.on('tileload', () => {
        contourTileErrorCount = 0;
    });

    return isobarGroup;
}

async function testOpenWeatherApiKey(appId) {
    const cleanedKey = String(appId || '').trim();
    if (!cleanedKey) {
        return { ok: false, message: t('Clé vide.', 'Clave vacía.') };
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 9000);

    try {
        const url = `https://api.openweathermap.org/data/2.5/weather?lat=41.3851&lon=2.1734&appid=${encodeURIComponent(cleanedKey)}`;
        const response = await fetch(url, { signal: controller.signal });
        clearTimeout(timeout);

        let payload = null;
        try {
            payload = await response.json();
        } catch (_error) {
            payload = null;
        }

        if (response.ok) {
            return { ok: true, message: t('Clé valide (API OpenWeatherMap accessible).', 'Clave válida (API OpenWeatherMap accesible).') };
        }

        const apiMessage = String(payload?.message || '').trim();
        return {
            ok: false,
            message: apiMessage
                ? `${t('Clé invalide', 'Clave inválida')} (${response.status}): ${apiMessage}`
                : `${t('Clé invalide', 'Clave inválida')} (${response.status}).`
        };
    } catch (error) {
        clearTimeout(timeout);
        if (error?.name === 'AbortError') {
            return { ok: false, message: t('Timeout: test API trop long.', 'Timeout: prueba API demasiado larga.') };
        }
        return { ok: false, message: t('Erreur réseau pendant le test API.', 'Error de red durante la prueba API.') };
    }
}

function applyLastDepartureSuggestion() {
    if (!lastDepartureSuggestion?.departureDateTime) return;
    const inputValue = toLocalDateTimeInputValue(lastDepartureSuggestion.departureDateTime);
    if (!inputValue) return;
    setDepartureFromDateTimeInput(inputValue);
}

function estimateEffectiveSpeedForLegSplit(startPoint, endPoint, weather) {
    const windSpeed = Number.isFinite(weather?.windSpeed) ? weather.windSpeed : 10;
    const windDirection = Number.isFinite(weather?.windDirection) ? weather.windDirection : 0;

    if (windSpeed < MOTOR_WIND_THRESHOLD_KN) {
        return MOTOR_SPEED_KN;
    }

    try {
        const rawSegment = routeSegment(startPoint, endPoint, windDirection, windSpeed, tackingTimeHours);
        if (!rawSegment || !Number.isFinite(rawSegment.speed)) return 6;

        const twa = computeTWA(rawSegment.bearing, windDirection);
        const sailSetup = getSailRecommendation({
            isMotorSegment: false,
            tws: windSpeed,
            twa,
            sailModeValue: sailMode
        });

        const sailFactor = getSailPerformanceFactor({
            isMotorSegment: false,
            sailModeValue: sailMode,
            tws: windSpeed,
            twa,
            sailSetup
        });

        const effectiveSpeed = rawSegment.speed * sailFactor;
        return Number.isFinite(effectiveSpeed) ? Math.max(3, effectiveSpeed) : 6;
    } catch (_error) {
        return 6;
    }
}

function densifyPolylineForWeather(points, maxDistanceNm) {
    if (!Array.isArray(points) || points.length < 2) return points;
    if (!Number.isFinite(maxDistanceNm) || maxDistanceNm <= 0) return points;

    const densified = [points[0]];

    for (let i = 0; i < points.length - 1; i++) {
        const start = points[i];
        const end = points[i + 1];

        const startLon = Number.isFinite(start?.lon) ? start.lon : start?.lng;
        const endLon = Number.isFinite(end?.lon) ? end.lon : end?.lng;
        if (!Number.isFinite(start?.lat) || !Number.isFinite(startLon) || !Number.isFinite(end?.lat) || !Number.isFinite(endLon)) {
            continue;
        }

        const segmentDistance = distanceNm(start.lat, startLon, end.lat, endLon);
        const requiredSteps = Math.ceil(segmentDistance / maxDistanceNm);
        const stepCount = Math.max(1, Math.min(AUTO_WEATHER_SPLIT_MAX_SUBSEGMENTS, requiredSteps));

        const bearing = getBearing({ lat: start.lat, lon: startLon }, { lat: end.lat, lon: endLon });
        const stepDistance = segmentDistance / stepCount;

        for (let step = 1; step <= stepCount; step++) {
            if (step === stepCount) {
                densified.push({ lat: end.lat, lon: endLon });
            } else {
                const intermediate = movePoint(start.lat, startLon, bearing, stepDistance * step);
                densified.push({ lat: intermediate.lat, lon: intermediate.lon });
            }
        }
    }

    return densified;
}

function clampStarRating(value) {
    const parsed = parseInt(String(value ?? '3'), 10);
    if (!Number.isFinite(parsed)) return 3;
    return Math.max(1, Math.min(5, parsed));
}

function starsLabel(value) {
    const rating = clampStarRating(value);
    return `${'★'.repeat(rating)}${'☆'.repeat(5 - rating)}`;
}

function normalizeDepthMeters(value) {
    const parsed = parseInt(String(value ?? '10'), 10);
    if (!Number.isFinite(parsed)) return 10;
    const bounded = Math.max(5, Math.min(30, parsed));
    const stepped = Math.round(bounded / 5) * 5;
    return Math.max(5, Math.min(30, stepped));
}

const CARDINAL_DIRECTIONS = [
    { code: 'N', bearing: 0 },
    { code: 'NE', bearing: 45 },
    { code: 'E', bearing: 90 },
    { code: 'SE', bearing: 135 },
    { code: 'S', bearing: 180 },
    { code: 'SO', bearing: 225 },
    { code: 'O', bearing: 270 },
    { code: 'NO', bearing: 315 }
];

function normalizeProtectionList(value) {
    const allowed = ['N', 'NE', 'E', 'SE', 'S', 'SO', 'O', 'NO'];
    let source = [];

    if (Array.isArray(value)) {
        source = value;
    } else if (typeof value === 'string') {
        source = value.split(/[\s,;/|]+/).filter(Boolean);
    }

    const normalized = source
        .map(item => String(item || '').toUpperCase())
        .filter(item => allowed.includes(item));

    return [...new Set(normalized)];
}

function formatProtectionList(value) {
    const list = normalizeProtectionList(value);
    return list.length ? list.join(', ') : 'Aucune';
}

async function detectProtectionAxesFromLand(lat, lng) {
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) return [];

    const geometry = await ensureLandGeometryLoaded();
    if (!geometry) return [];

    const origin = { lat, lon: lng };
    const scores = CARDINAL_DIRECTIONS.map(direction => {
        let landHits = 0;
        let sampleCount = 0;

        for (let distance = 0.25; distance <= 2.5; distance += 0.35) {
            sampleCount += 1;
            const sample = movePoint(origin.lat, origin.lon, direction.bearing, distance);
            if (isPointOnLand(sample.lat, sample.lon)) {
                landHits += 1;
            }
        }

        const ratio = sampleCount > 0 ? landHits / sampleCount : 0;
        return {
            code: direction.code,
            ratio,
            hits: landHits,
            sampleCount
        };
    });

    const sorted = [...scores].sort((a, b) => b.ratio - a.ratio);
    const best = sorted[0];
    if (!best || best.hits <= 0) return [];

    const robust = sorted
        .filter(item => item.ratio >= 0.28)
        .map(item => item.code);

    if (robust.length > 0) {
        return robust;
    }

    const fallback = sorted
        .filter(item => item.ratio >= 0.14)
        .slice(0, 2)
        .map(item => item.code);

    return fallback.length > 0 ? fallback : [best.code];
}

async function applyAutoProtectionSuggestion(lat, lng) {
    if (editingWaypointPhotoId) return;

    const currentSelection = getProtectionCheckboxValues();
    if (currentSelection.length > 0) return;

    const suggested = await detectProtectionAxesFromLand(lat, lng);
    if (!suggested.length) return;

    setProtectionCheckboxValues(suggested);

    const status = document.getElementById('waypointPhotoStatus');
    if (status) {
        status.textContent = `${status.textContent} · Protection suggérée: ${suggested.join(', ')}`;
    }
}

function setProtectionCheckboxValues(value) {
    const selected = new Set(normalizeProtectionList(value));
    document.querySelectorAll('input[name="waypointProtection"]').forEach(input => {
        input.checked = selected.has(String(input.value || '').toUpperCase());
    });
}

function getProtectionCheckboxValues() {
    const selected = Array.from(document.querySelectorAll('input[name="waypointProtection"]:checked'))
        .map(input => String(input.value || '').toUpperCase());
    return normalizeProtectionList(selected);
}

function buildGoogleMapsUrl(lat, lng) {
    return `https://www.google.com/maps?q=${lat},${lng}`;
}

function buildReverseGeocodeCacheKey(lat, lng) {
    return `${Number(lat).toFixed(4)},${Number(lng).toFixed(4)}`;
}

function setWaypointDraftCoordinates(lat, lng, options = {}) {
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;

    const { fetchPlaceName = true, suggestProtection = true } = options;
    pendingWaypointPhotoDraft = {
        ...(pendingWaypointPhotoDraft || {}),
        lat,
        lng
    };

    const status = document.getElementById('waypointPhotoStatus');
    if (status) {
        status.textContent = editingWaypointPhotoId
            ? `Modification du waypoint: ${lat.toFixed(5)}, ${lng.toFixed(5)}`
            : `Coordonnées détectées: ${lat.toFixed(5)}, ${lng.toFixed(5)}`;
    }

    updateWaypointGoogleMapPreview(lat, lng);

    if (fetchPlaceName) {
        fillWaypointPlaceNameFromCoordinates(lat, lng);
    }

    if (suggestProtection) {
        applyAutoProtectionSuggestion(lat, lng);
    }
}

async function fillWaypointPlaceNameFromCoordinates(lat, lng) {
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;

    const cacheKey = buildReverseGeocodeCacheKey(lat, lng);
    const placeNameInput = document.getElementById('waypointPlaceNameInput');
    if (!placeNameInput) return;

    if (waypointReverseGeocodeCache.has(cacheKey)) {
        const cachedName = waypointReverseGeocodeCache.get(cacheKey);
        if (cachedName) placeNameInput.value = cachedName;
        return;
    }

    try {
        const url = `https://nominatim.openstreetmap.org/reverse?format=jsonv2&lat=${encodeURIComponent(lat)}&lon=${encodeURIComponent(lng)}&zoom=16&addressdetails=1`;
        const response = await fetch(url, {
            headers: {
                Accept: 'application/json'
            }
        });

        if (!response.ok) return;
        const payload = await response.json();
        const address = payload?.address || {};
        const name = payload?.name
            || address?.harbour
            || address?.marina
            || address?.bay
            || address?.beach
            || address?.village
            || address?.town
            || address?.city
            || payload?.display_name
            || '';

        const cleanName = String(name).split(',')[0].trim();
        waypointReverseGeocodeCache.set(cacheKey, cleanName);
        if (cleanName) {
            placeNameInput.value = cleanName;
        }
    } catch (error) {
    }
}

function ensureWaypointEditorMap() {
    const mapContainer = document.getElementById('waypointEditorMap');
    if (!mapContainer) return null;

    if (!waypointEditorMap) {
        waypointEditorMap = L.map(mapContainer, {
            zoomControl: true,
            attributionControl: false
        }).setView([41.3851, 2.1734], 10);

        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '© OpenStreetMap',
            crossOrigin: true
        }).addTo(waypointEditorMap);

        waypointEditorMarker = L.marker([41.3851, 2.1734], {
            draggable: true,
            keyboard: false,
            zIndexOffset: 700
        }).addTo(waypointEditorMap);

        waypointEditorMarker.on('dragend', () => {
            const pos = waypointEditorMarker.getLatLng();
            setWaypointDraftCoordinates(pos.lat, pos.lng);
        });

        waypointEditorMap.on('click', e => {
            const lat = e?.latlng?.lat;
            const lng = e?.latlng?.lng;
            if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;

            if (waypointEditorMarker) {
                waypointEditorMarker.setLatLng([lat, lng]);
            }
            setWaypointDraftCoordinates(lat, lng);
        });
    }

    return waypointEditorMap;
}

function updateWaypointGoogleMapPreview(lat, lng) {
    const mapContainer = document.getElementById('waypointEditorMap');
    const googleLink = document.getElementById('waypointGoogleMapLink');
    const editor = ensureWaypointEditorMap();
    if (!mapContainer || !googleLink || !editor) return;

    if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
        mapContainer.style.display = 'none';
        googleLink.style.display = 'none';
        googleLink.removeAttribute('href');
        return;
    }

    mapContainer.style.display = 'block';
    googleLink.style.display = 'inline-block';
    googleLink.href = buildGoogleMapsUrl(lat, lng);

    waypointEditorIsUpdating = true;
    editor.setView([lat, lng], Math.max(editor.getZoom(), 13));
    if (waypointEditorMarker) {
        waypointEditorMarker.setLatLng([lat, lng]);
    }
    waypointEditorIsUpdating = false;

    setTimeout(() => {
        try {
            editor.invalidateSize();
        } catch (error) {
        }
    }, 0);
}

function getWaypointPhotoStorageList() {
    const raw = localStorage.getItem(WAYPOINT_PHOTOS_STORAGE_KEY);
    if (!raw) return [];

    try {
        const parsed = JSON.parse(raw);
        return Array.isArray(parsed) ? parsed : [];
    } catch (error) {
        return [];
    }
}

function setWaypointPhotoStorageList(list) {
    try {
        localStorage.setItem(WAYPOINT_PHOTOS_STORAGE_KEY, JSON.stringify(list));
        return true;
    } catch (error) {
        return false;
    }
}

function normalizeWaypointPhotoEntry(entry) {
    const lat = Number(entry?.lat);
    const lng = Number(entry?.lng);
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;

    const legacyDepthRating = clampStarRating(entry?.depth);
    const derivedDepth = normalizeDepthMeters(Number(entry?.depthMeters ?? 0) || legacyDepthRating * 5);

    return {
        id: String(entry?.id || `wp-photo-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`),
        lat,
        lng,
        placeName: String(entry?.placeName || ''),
        imageDataUrl: String(entry?.imageDataUrl || ''),
        comment: String(entry?.comment || ''),
        rating: clampStarRating(entry?.rating),
        cleanliness: clampStarRating(entry?.cleanliness),
        protection: normalizeProtectionList(entry?.protection),
        depthMeters: derivedDepth,
        bottomType: String(entry?.bottomType || ''),
        createdAt: String(entry?.createdAt || new Date().toISOString()),
        updatedAt: String(entry?.updatedAt || entry?.createdAt || new Date().toISOString())
    };
}

function loadWaypointPhotoEntries() {
    waypointPhotoEntries = getWaypointPhotoStorageList()
        .map(normalizeWaypointPhotoEntry)
        .filter(Boolean)
        .sort((a, b) => String(b.createdAt).localeCompare(String(a.createdAt)));
}

function setWaypointPhotoEntries(list, { persistLocal = true, refreshUi = true } = {}) {
    waypointPhotoEntries = (Array.isArray(list) ? list : [])
        .map(normalizeWaypointPhotoEntry)
        .filter(Boolean)
        .sort((a, b) => String(b.createdAt).localeCompare(String(a.createdAt)));

    if (persistLocal) {
        setWaypointPhotoStorageList(waypointPhotoEntries);
    }

    if (refreshUi) {
        renderWaypointPhotoList();
        syncWaypointPhotoMarkersInView();
    }
}

function persistWaypointPhotoEntries() {
    const ok = setWaypointPhotoStorageList(waypointPhotoEntries);

    if (ok && isCloudReady()) {
        pushRoutesToCloud()
            .then(() => setCloudStatus(`Cloud synchronisé · ${getSavedRoutes().length} route(s) + ${waypointPhotoEntries.length} photo(s)`))
            .catch(error => setCloudStatus(`Photos locales OK, synchro cloud échouée: ${formatCloudError(error)}`, true));
    }

    return ok;
}

function resetWaypointPhotoFormValues() {
    const placeNameInput = document.getElementById('waypointPlaceNameInput');
    if (placeNameInput) placeNameInput.value = '';

    const commentInput = document.getElementById('waypointCommentInput');
    if (commentInput) commentInput.value = '';
    const bottomInput = document.getElementById('waypointBottomTypeInput');
    if (bottomInput) bottomInput.value = '';

    const ratingInput = document.getElementById('waypointRatingInput');
    if (ratingInput) ratingInput.value = '3';
    const cleanInput = document.getElementById('waypointCleanlinessInput');
    if (cleanInput) cleanInput.value = '3';
    setProtectionCheckboxValues([]);
    const depthInput = document.getElementById('waypointDepthInput');
    if (depthInput) depthInput.value = '10';
}

function setWaypointPhotoEditMode(entry) {
    const saveBtn = document.getElementById('saveWaypointPhotoBtn');
    const cancelBtn = document.getElementById('cancelWaypointPhotoEditBtn');

    if (!entry) {
        editingWaypointPhotoId = null;
        if (saveBtn) saveBtn.textContent = t('Ajouter ce waypoint photo', 'Añadir este waypoint foto');
        if (cancelBtn) cancelBtn.style.display = 'none';
        return;
    }

    editingWaypointPhotoId = entry.id;
    if (saveBtn) saveBtn.textContent = t('Mettre à jour ce waypoint photo', 'Actualizar este waypoint foto');
    if (cancelBtn) cancelBtn.style.display = 'block';
}

function clearWaypointPhotoDraft() {
    pendingWaypointPhotoDraft = null;

    const input = document.getElementById('waypointPhotoInput');
    if (input) input.value = '';

    const status = document.getElementById('waypointPhotoStatus');
    if (status) {
        status.textContent = editingWaypointPhotoId
            ? 'Modification: ajoute une nouvelle photo pour remplacer l\'existante (optionnel).'
            : 'Coordonnées: en attente d\'une photo';
    }

    const preview = document.getElementById('waypointPhotoPreview');
    if (preview) {
        if (editingWaypointPhotoId) {
            const entry = waypointPhotoEntries.find(item => item.id === editingWaypointPhotoId);
            if (entry?.imageDataUrl) {
                preview.src = entry.imageDataUrl;
                preview.style.display = 'block';
            } else {
                preview.style.display = 'none';
                preview.removeAttribute('src');
            }
        } else {
            preview.style.display = 'none';
            preview.removeAttribute('src');
        }
    }

    if (waypointPhotoPreviewObjectUrl) {
        URL.revokeObjectURL(waypointPhotoPreviewObjectUrl);
        waypointPhotoPreviewObjectUrl = null;
    }

    if (editingWaypointPhotoId) {
        const entry = waypointPhotoEntries.find(item => item.id === editingWaypointPhotoId);
        updateWaypointGoogleMapPreview(entry?.lat, entry?.lng);
    } else {
        updateWaypointGoogleMapPreview(null, null);
    }
}

function cancelWaypointPhotoEdit() {
    setWaypointPhotoEditMode(null);
    resetWaypointPhotoFormValues();
    clearWaypointPhotoDraft();
}

function startEditWaypointPhoto(id) {
    const entry = waypointPhotoEntries.find(item => item.id === id);
    if (!entry) return;

    const placeNameInput = document.getElementById('waypointPlaceNameInput');
    if (placeNameInput) placeNameInput.value = entry.placeName || '';

    const commentInput = document.getElementById('waypointCommentInput');
    if (commentInput) commentInput.value = entry.comment || '';
    const bottomInput = document.getElementById('waypointBottomTypeInput');
    if (bottomInput) bottomInput.value = entry.bottomType || '';

    const ratingInput = document.getElementById('waypointRatingInput');
    if (ratingInput) ratingInput.value = String(clampStarRating(entry.rating));
    const cleanInput = document.getElementById('waypointCleanlinessInput');
    if (cleanInput) cleanInput.value = String(clampStarRating(entry.cleanliness));
    setProtectionCheckboxValues(entry.protection);
    const depthInput = document.getElementById('waypointDepthInput');
    if (depthInput) depthInput.value = String(normalizeDepthMeters(entry.depthMeters));

    setWaypointPhotoEditMode(entry);
    pendingWaypointPhotoDraft = {
        file: null,
        lat: entry.lat,
        lng: entry.lng,
        existingImageDataUrl: entry.imageDataUrl
    };

    const status = document.getElementById('waypointPhotoStatus');
    if (status) status.textContent = `Modification du waypoint: ${entry.lat.toFixed(5)}, ${entry.lng.toFixed(5)}`;

    const preview = document.getElementById('waypointPhotoPreview');
    if (preview && entry.imageDataUrl) {
        preview.src = entry.imageDataUrl;
        preview.style.display = 'block';
    }

    updateWaypointGoogleMapPreview(entry.lat, entry.lng);
}

async function extractGpsCoordinatesFromPhoto(file) {
    if (!file || !window.exifr) return null;

    try {
        if (typeof window.exifr.gps === 'function') {
            const gps = await window.exifr.gps(file);
            if (Number.isFinite(gps?.latitude) && Number.isFinite(gps?.longitude)) {
                return { lat: gps.latitude, lng: gps.longitude };
            }
        }

        if (typeof window.exifr.parse === 'function') {
            const parsed = await window.exifr.parse(file, ['latitude', 'longitude']);
            if (Number.isFinite(parsed?.latitude) && Number.isFinite(parsed?.longitude)) {
                return { lat: parsed.latitude, lng: parsed.longitude };
            }
        }
    } catch (error) {
        return null;
    }

    return null;
}

function imageFileToDataUrl(file) {
    return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = () => resolve(String(reader.result || ''));
        reader.onerror = () => reject(new Error('Lecture image impossible'));
        reader.readAsDataURL(file);
    });
}

async function imageFileToCompressedDataUrl(file, maxSide = 720, quality = 0.68) {
    const originalDataUrl = await imageFileToDataUrl(file);

    return new Promise(resolve => {
        const image = new Image();
        image.onload = () => {
            const width = image.naturalWidth || image.width || 1;
            const height = image.naturalHeight || image.height || 1;
            const ratio = Math.min(1, maxSide / Math.max(width, height));
            const targetWidth = Math.max(1, Math.round(width * ratio));
            const targetHeight = Math.max(1, Math.round(height * ratio));

            const canvas = document.createElement('canvas');
            canvas.width = targetWidth;
            canvas.height = targetHeight;

            const context = canvas.getContext('2d');
            if (!context) {
                resolve(originalDataUrl);
                return;
            }

            context.drawImage(image, 0, 0, targetWidth, targetHeight);
            resolve(canvas.toDataURL('image/jpeg', quality));
        };

        image.onerror = () => resolve(originalDataUrl);
        image.src = originalDataUrl;
    });
}

function buildWaypointPhotoPopupContent(entry, weatherHtml = '') {
    const title = entry.placeName ? escapeHtml(entry.placeName) : t('Mouillage noté', 'Fondeo valorado');
    const comment = entry.comment ? `<div style="margin-top:6px;">${escapeHtml(entry.comment)}</div>` : '';
    const bottom = entry.bottomType ? `<div>${t('Fond', 'Fondo')}: ${escapeHtml(entry.bottomType)}</div>` : '';
    const image = entry.imageDataUrl
        ? `<img src="${entry.imageDataUrl}" alt="${t('Photo mouillage', 'Foto fondeo')}" style="margin-top:6px; width:100%; max-width:180px; border-radius:8px;">`
        : '';

    return `<strong>${title}</strong><br>` +
        `${entry.lat.toFixed(5)}, ${entry.lng.toFixed(5)}<br>` +
        `${t('Global', 'Global')}: ${starsLabel(entry.rating)}<br>` +
        `${t('Propreté', 'Limpieza')}: ${starsLabel(entry.cleanliness)} · ${t('Protection', 'Protección')}: ${escapeHtml(formatProtectionList(entry.protection))} · ${t('Profondeur', 'Profundidad')}: ${entry.depthMeters} m` +
        `${bottom}` +
        `${comment}` +
        `${weatherHtml}` +
        `${image}`;
}

async function enrichWaypointPhotoPopupWithCurrentWeather(marker, entry) {
    if (!marker || !entry) return;

    const loadingWeatherHtml = `<div style="margin-top:6px;"><strong>${t('Météo actuelle', 'Meteo actual')}</strong><br>${t('Chargement...', 'Cargando...')}</div>`;
    marker.setPopupContent(buildWaypointPhotoPopupContent(entry, loadingWeatherHtml));

    try {
        const weather = await getCurrentWeatherAtWaypoint(entry.lat, entry.lng);
        const nowLabel = formatWeekdayHourUtc(new Date());
        const weatherLine = formatWeatherTooltipContent(weather, nowLabel).replace('<strong>Météo</strong><br>', '');
        const weatherHtml = `<div style="margin-top:6px;"><strong>${t('Météo actuelle', 'Meteo actual')}</strong><br>${weatherLine}</div>`;
        marker.setPopupContent(buildWaypointPhotoPopupContent(entry, weatherHtml));
    } catch (error) {
        const weatherHtml = `<div style="margin-top:6px;"><strong>${t('Météo actuelle', 'Meteo actual')}</strong><br>${t('Indisponible', 'No disponible')}</div>`;
        marker.setPopupContent(buildWaypointPhotoPopupContent(entry, weatherHtml));
    }
}

function ensureWaypointPhotoMarker(entry) {
    if (!map || !entry) return null;

    let marker = waypointPhotoMarkersById.get(entry.id);
    if (!marker) {
        const icon = L.divIcon({
            className: 'waypoint-photo-map-icon',
            html: '<div class="waypoint-photo-map-icon__dot" style="width:28px;height:28px;border-radius:999px;background:#10223a;border:2px solid #7fd8ff;color:#7fd8ff;font:16px/26px Arial,sans-serif;text-align:center;box-shadow:0 2px 8px rgba(0,0,0,0.45);">📷</div>',
            iconSize: [28, 28],
            iconAnchor: [14, 14]
        });

        marker = L.marker([entry.lat, entry.lng], {
            icon,
            keyboard: false,
            zIndexOffset: 2000
        });

        waypointPhotoMarkersById.set(entry.id, marker);
    } else {
        marker.setLatLng([entry.lat, entry.lng]);
    }

    marker._ceiboWaypointPhotoId = entry.id;

    marker.bindPopup(buildWaypointPhotoPopupContent(entry), { maxWidth: 260, autoPan: false });

    if (!marker._ceiboWeatherPopupBound) {
        marker.on('popupopen', () => {
            const entryId = marker._ceiboWaypointPhotoId;
            const liveEntry = waypointPhotoEntries.find(item => item.id === entryId) || entry;
            enrichWaypointPhotoPopupWithCurrentWeather(marker, liveEntry);
        });
        marker._ceiboWeatherPopupBound = true;
    }

    return marker;
}

function syncWaypointPhotoMarkersInView() {
    if (!map) return;

    const knownIds = new Set();

    waypointPhotoEntries.forEach(entry => {
        const marker = ensureWaypointPhotoMarker(entry);
        if (!marker) return;

        knownIds.add(entry.id);
        if (!map.hasLayer(marker)) marker.addTo(map);
    });

    waypointPhotoMarkersById.forEach((marker, id) => {
        if (!knownIds.has(id)) {
            if (map.hasLayer(marker)) map.removeLayer(marker);
            waypointPhotoMarkersById.delete(id);
        }
    });
}

function removeWaypointPhotoById(id) {
    waypointPhotoEntries = waypointPhotoEntries.filter(entry => entry.id !== id);
    persistWaypointPhotoEntries();

    const marker = waypointPhotoMarkersById.get(id);
    if (marker && map?.hasLayer(marker)) {
        map.removeLayer(marker);
    }
    waypointPhotoMarkersById.delete(id);

    if (editingWaypointPhotoId === id) {
        cancelWaypointPhotoEdit();
    }

    renderWaypointPhotoList();
    syncWaypointPhotoMarkersInView();
}

function goToWaypointPhotoEntry(entry) {
    if (!entry || !Number.isFinite(entry.lat) || !Number.isFinite(entry.lng) || !map) return;

    const waypoint = L.latLng(entry.lat, entry.lng);
    addUserWaypoint(waypoint, { select: true, invalidate: true });
    map.setView([entry.lat, entry.lng], Math.max(map.getZoom(), 11));
}

function renderWaypointPhotoList() {
    const container = document.getElementById('waypointPhotoList');
    if (!container) return;

    if (!waypointPhotoEntries.length) {
        container.innerHTML = `<div class="arrival-list__item">${t('Aucun mouillage enregistré.', 'No hay fondeos guardados.')}</div>`;
        return;
    }

    container.innerHTML = waypointPhotoEntries.map(entry => {
        const title = entry.placeName ? escapeHtml(entry.placeName) : t('Mouillage sans nom', 'Fondeo sin nombre');
        const bottom = entry.bottomType ? `<div>${t('Fond', 'Fondo')}: ${escapeHtml(entry.bottomType)}</div>` : '';
        const comment = entry.comment ? `<div style="margin-top:4px;">${escapeHtml(entry.comment)}</div>` : '';
        const image = entry.imageDataUrl ? `<img class="waypoint-photo-card__img" src="${entry.imageDataUrl}" alt="${t('Photo mouillage', 'Foto fondeo')}">` : '';

        return `<div class="waypoint-photo-card">
            <div><strong>${title}</strong></div>
            <div><strong>${starsLabel(entry.rating)}</strong> · ${entry.lat.toFixed(4)}, ${entry.lng.toFixed(4)}</div>
            <div>${t('Propreté', 'Limpieza')} ${starsLabel(entry.cleanliness)} · ${t('Protection', 'Protección')} ${escapeHtml(formatProtectionList(entry.protection))} · ${t('Profondeur', 'Profundidad')} ${entry.depthMeters} m</div>
            ${bottom}
            ${comment}
            ${image}
            <div class="button-row">
                <button type="button" class="js-waypoint-photo-go" data-id="${escapeHtml(entry.id)}" style="flex:1;">${t('Y aller', 'Ir')}</button>
                <button type="button" class="js-waypoint-photo-center" data-id="${escapeHtml(entry.id)}" style="flex:1;">${t('Voir sur carte', 'Ver en mapa')}</button>
                <button type="button" class="js-waypoint-photo-edit" data-id="${escapeHtml(entry.id)}" style="flex:1;">${t('Modifier', 'Editar')}</button>
                <button type="button" class="js-waypoint-photo-delete" data-id="${escapeHtml(entry.id)}" style="flex:1;">${t('Supprimer', 'Eliminar')}</button>
            </div>
        </div>`;
    }).join('');

    container.querySelectorAll('.js-waypoint-photo-go').forEach(button => {
        button.addEventListener('click', () => {
            const id = String(button.getAttribute('data-id') || '');
            const entry = waypointPhotoEntries.find(item => item.id === id);
            if (!entry) return;
            goToWaypointPhotoEntry(entry);
        });
    });

    container.querySelectorAll('.js-waypoint-photo-center').forEach(button => {
        button.addEventListener('click', () => {
            const id = String(button.getAttribute('data-id') || '');
            const entry = waypointPhotoEntries.find(item => item.id === id);
            if (!entry || !map) return;

            map.setView([entry.lat, entry.lng], Math.max(map.getZoom(), 11));
            updateWaypointGoogleMapPreview(entry.lat, entry.lng);
            syncWaypointPhotoMarkersInView();
            const marker = waypointPhotoMarkersById.get(id);
            if (marker) marker.openPopup();
        });
    });

    container.querySelectorAll('.js-waypoint-photo-edit').forEach(button => {
        button.addEventListener('click', () => {
            const id = String(button.getAttribute('data-id') || '');
            startEditWaypointPhoto(id);
        });
    });

    container.querySelectorAll('.js-waypoint-photo-delete').forEach(button => {
        button.addEventListener('click', () => {
            const id = String(button.getAttribute('data-id') || '');
            removeWaypointPhotoById(id);
        });
    });
}

async function handleWaypointPhotoInputChange(event) {
    const file = event?.target?.files?.[0];
    const status = document.getElementById('waypointPhotoStatus');
    const preview = document.getElementById('waypointPhotoPreview');

    if (!file) {
        clearWaypointPhotoDraft();
        return;
    }

    if (!file.type.startsWith('image/')) {
        if (status) status.textContent = t('Ce fichier n\'est pas une image.', 'Este archivo no es una imagen.');
        pendingWaypointPhotoDraft = null;
        return;
    }

    pendingWaypointPhotoDraft = {
        ...(pendingWaypointPhotoDraft || {}),
        file
    };

    waypointPhotoInputProcessing = true;

    try {
        if (status) status.textContent = t('Lecture des coordonnées GPS...', 'Leyendo coordenadas GPS...');

        const coords = await extractGpsCoordinatesFromPhoto(file);
        if (!coords) {
            if (editingWaypointPhotoId) {
                const entry = waypointPhotoEntries.find(item => item.id === editingWaypointPhotoId);
                if (entry) {
                    pendingWaypointPhotoDraft = { file, lat: entry.lat, lng: entry.lng };
                    if (status) status.textContent = t(
                        `Pas de GPS dans la nouvelle photo: coordonnées conservées (${entry.lat.toFixed(5)}, ${entry.lng.toFixed(5)}).`,
                        `Sin GPS en la nueva foto: coordenadas conservadas (${entry.lat.toFixed(5)}, ${entry.lng.toFixed(5)}).`
                    );
                    updateWaypointGoogleMapPreview(entry.lat, entry.lng);
                }
            } else {
                const editor = ensureWaypointEditorMap();
                const markerPos = waypointEditorMarker?.getLatLng?.();
                const editorCenter = editor?.getCenter?.();
                const fallbackLat = Number(markerPos?.lat ?? editorCenter?.lat ?? map?.getCenter?.().lat ?? 41.3851);
                const fallbackLng = Number(markerPos?.lng ?? editorCenter?.lng ?? map?.getCenter?.().lng ?? 2.1734);

                pendingWaypointPhotoDraft = { file, lat: fallbackLat, lng: fallbackLng };
                updateWaypointGoogleMapPreview(fallbackLat, fallbackLng);

                if (waypointEditorMarker) {
                    waypointEditorMarker.setLatLng([fallbackLat, fallbackLng]);
                }

                if (status) {
                    status.textContent = t(
                        'Pas de GPS EXIF: position manuelle activée sur la mini-carte (déplace le marqueur puis sauvegarde).',
                        'Sin GPS EXIF: posición manual activada en el mini-mapa (mueve el marcador y guarda).'
                    );
                }
            }
        } else {
            setWaypointDraftCoordinates(coords.lat, coords.lng);
        }

        if (waypointPhotoPreviewObjectUrl) {
            URL.revokeObjectURL(waypointPhotoPreviewObjectUrl);
        }
        waypointPhotoPreviewObjectUrl = URL.createObjectURL(file);

        if (preview) {
            preview.src = waypointPhotoPreviewObjectUrl;
            preview.style.display = 'block';
        }
    } catch (error) {
        pendingWaypointPhotoDraft = null;
        if (status) status.textContent = t('Erreur de lecture image. Essaie une autre photo (JPG/PNG).', 'Error al leer la imagen. Prueba otra foto (JPG/PNG).');
    } finally {
        waypointPhotoInputProcessing = false;
    }
}

async function handleQuickWaypointCaptureChange(event) {
    const file = event?.target?.files?.[0];
    if (!file) return;

    const status = document.getElementById('waypointPhotoStatus');

    try {
        if (!file.type.startsWith('image/')) {
            if (status) status.textContent = t('Ce fichier n\'est pas une image.', 'Este archivo no es una imagen.');
            return;
        }

        if (status) status.textContent = t('Photo rapide: lecture GPS...', 'Foto rápida: leyendo GPS...');

        const coords = await extractGpsCoordinatesFromPhoto(file);
        if (!coords) {
            if (status) status.textContent = t('Photo rapide: GPS EXIF introuvable (WP auto non créé).', 'Foto rápida: GPS EXIF no encontrado (WP auto no creado).');
            alert(t('La photo ne contient pas de coordonnées GPS. Utilise le mode manuel pour positionner le waypoint.', 'La foto no contiene coordenadas GPS. Usa el modo manual para posicionar el waypoint.'));
            return;
        }

        const imageDataUrl = await imageFileToCompressedDataUrl(file);
        const nowIso = new Date().toISOString();
        const entry = normalizeWaypointPhotoEntry({
            id: `wp-photo-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
            lat: coords.lat,
            lng: coords.lng,
            placeName: '',
            imageDataUrl,
            comment: '',
            rating: 3,
            cleanliness: 3,
            protection: [],
            depthMeters: 10,
            bottomType: '',
            createdAt: nowIso,
            updatedAt: nowIso
        });

        if (!entry) {
            if (status) status.textContent = t('Photo rapide: coordonnées invalides.', 'Foto rápida: coordenadas inválidas.');
            return;
        }

        waypointPhotoEntries.unshift(entry);
        const persisted = persistWaypointPhotoEntries();
        if (!persisted) {
            waypointPhotoEntries.shift();
            alert(t('Stockage saturé: impossible d\'enregistrer cette photo.', 'Almacenamiento lleno: no se puede guardar esta foto.'));
            return;
        }

        renderWaypointPhotoList();
        syncWaypointPhotoMarkersInView();
        if (map) {
            map.setView([entry.lat, entry.lng], Math.max(map.getZoom(), 11));
        }

        if (status) {
            status.textContent = t(
                `Photo rapide: WP créé automatiquement (${entry.lat.toFixed(5)}, ${entry.lng.toFixed(5)}).`,
                `Foto rápida: WP creado automáticamente (${entry.lat.toFixed(5)}, ${entry.lng.toFixed(5)}).`
            );
        }
    } catch (_error) {
        if (status) status.textContent = t('Photo rapide: échec de création du WP.', 'Foto rápida: fallo al crear el WP.');
    } finally {
        if (event?.target) event.target.value = '';
    }
}

async function saveWaypointPhotoEntry() {
    if (waypointPhotoInputProcessing) {
        alert(t('Photo en cours de traitement, attends 1-2 secondes puis réessaie.', 'Foto en procesamiento, espera 1-2 segundos y vuelve a intentarlo.'));
        return;
    }

    const isEdit = Number.isFinite(waypointPhotoEntries.findIndex(entry => entry.id === editingWaypointPhotoId))
        && waypointPhotoEntries.findIndex(entry => entry.id === editingWaypointPhotoId) !== -1;

    if (!pendingWaypointPhotoDraft?.file && !isEdit) {
        alert(t('Choisis une photo avec coordonnées GPS intégrées.', 'Elige una foto con coordenadas GPS integradas.'));
        return;
    }

    const placeName = String(document.getElementById('waypointPlaceNameInput')?.value || '').trim();
    const comment = String(document.getElementById('waypointCommentInput')?.value || '').trim();
    const bottomType = String(document.getElementById('waypointBottomTypeInput')?.value || '').trim();
    const rating = clampStarRating(document.getElementById('waypointRatingInput')?.value);
    const cleanliness = clampStarRating(document.getElementById('waypointCleanlinessInput')?.value);
    const protection = getProtectionCheckboxValues();
    const depthMeters = normalizeDepthMeters(document.getElementById('waypointDepthInput')?.value);

    try {
    if (isEdit) {
        const editIndex = waypointPhotoEntries.findIndex(entry => entry.id === editingWaypointPhotoId);
        const current = waypointPhotoEntries[editIndex];
        if (!current) {
            alert(t('Waypoint à modifier introuvable.', 'No se encontró el waypoint a modificar.'));
            return;
        }

        const hasNewPhoto = !!pendingWaypointPhotoDraft?.file;
        const imageDataUrl = hasNewPhoto
            ? await imageFileToCompressedDataUrl(pendingWaypointPhotoDraft.file)
            : current.imageDataUrl;

        const previousEntry = { ...current };

        waypointPhotoEntries[editIndex] = {
            ...current,
            lat: Number.isFinite(pendingWaypointPhotoDraft?.lat) ? pendingWaypointPhotoDraft.lat : current.lat,
            lng: Number.isFinite(pendingWaypointPhotoDraft?.lng) ? pendingWaypointPhotoDraft.lng : current.lng,
            placeName,
            imageDataUrl,
            comment,
            rating,
            cleanliness,
            protection,
            depthMeters,
            bottomType,
            updatedAt: new Date().toISOString()
        };

        const persisted = persistWaypointPhotoEntries();
        if (!persisted) {
            waypointPhotoEntries[editIndex] = previousEntry;
            alert(t('Enregistrement impossible: stockage local saturé. Essaie une image plus légère ou supprime des anciennes photos.', 'No se puede guardar: almacenamiento local lleno. Prueba una imagen más ligera o elimina fotos antiguas.'));
            return;
        }

        renderWaypointPhotoList();
        syncWaypointPhotoMarkersInView();
        cancelWaypointPhotoEdit();
        alert(t('Waypoint photo modifié.', 'Waypoint foto modificado.'));
        return;
    }

    if (!Number.isFinite(pendingWaypointPhotoDraft?.lat) || !Number.isFinite(pendingWaypointPhotoDraft?.lng)) {
        alert(t('Coordonnées GPS manquantes pour ce waypoint photo.', 'Faltan coordenadas GPS para este waypoint foto.'));
        return;
    }

    const imageDataUrl = await imageFileToCompressedDataUrl(pendingWaypointPhotoDraft.file);

    const entry = {
        id: `wp-photo-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
        lat: pendingWaypointPhotoDraft.lat,
        lng: pendingWaypointPhotoDraft.lng,
        placeName,
        imageDataUrl,
        comment,
        rating,
        cleanliness,
        protection,
        depthMeters,
        bottomType,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString()
    };

    waypointPhotoEntries.unshift(entry);
    const persisted = persistWaypointPhotoEntries();
    if (!persisted) {
        waypointPhotoEntries.shift();
        alert(t('Enregistrement impossible: stockage local saturé. Essaie une image plus légère ou supprime des anciennes photos.', 'No se puede guardar: almacenamiento local lleno. Prueba una imagen más ligera o elimina fotos antiguas.'));
        return;
    }

    renderWaypointPhotoList();
    syncWaypointPhotoMarkersInView();

    resetWaypointPhotoFormValues();
    clearWaypointPhotoDraft();
    alert(t('Waypoint photo enregistré.', 'Waypoint foto guardado.'));
    } catch (error) {
        alert(t('Impossible d\'enregistrer la photo. Essaie une image plus légère (JPG), puis recommence.', 'No se puede guardar la foto. Prueba una imagen más ligera (JPG) y vuelve a intentarlo.'));
    }
}

function updateSelectedWaypointInfo() {
    const info = document.getElementById('selectedWpInfo');
    if (!info) return;

    if (!Number.isInteger(selectedUserWaypointIndex) || selectedUserWaypointIndex < 0 || selectedUserWaypointIndex >= markers.length) {
        info.textContent = t(
            'WP sélectionné: aucun · clic sur WP pour sélectionner · clic droit pour supprimer',
            'WP seleccionado: ninguno · clic en WP para seleccionar · clic derecho para eliminar'
        );
        return;
    }

    const marker = markers[selectedUserWaypointIndex];
    const latlng = marker?.getLatLng?.();
    if (!latlng) {
        info.textContent = t(
            'WP sélectionné: aucun · clic sur WP pour sélectionner · clic droit pour supprimer',
            'WP seleccionado: ninguno · clic en WP para seleccionar · clic derecho para eliminar'
        );
        return;
    }

    info.textContent = t(
        `WP sélectionné: ${selectedUserWaypointIndex + 1} (${latlng.lat.toFixed(4)}, ${latlng.lng.toFixed(4)})`,
        `WP seleccionado: ${selectedUserWaypointIndex + 1} (${latlng.lat.toFixed(4)}, ${latlng.lng.toFixed(4)})`
    );
}

function invalidateComputedRouteDisplay() {
    if (routeLayer && map?.hasLayer(routeLayer)) {
        map.removeLayer(routeLayer);
    }
    routeLayer = null;

    if (windLayer && map?.hasLayer(windLayer)) {
        map.removeLayer(windLayer);
    }
    windLayer = null;

    clearWaypointWindDirectionLayers();
    clearWaveDirectionSegmentLayers();
    waypointPassageSlots.clear();
    lastRouteBounds = null;

    const info = document.getElementById('info');
    if (info) info.innerHTML = '';
    const windLegend = document.getElementById('windSpeedLegend');
    if (windLegend) windLegend.innerHTML = '';
}

function selectUserWaypoint(marker) {
    const index = markers.indexOf(marker);
    selectedUserWaypointIndex = index;
    updateSelectedWaypointInfo();
}

function deleteUserWaypointAtIndex(index) {
    if (!Number.isInteger(index) || index < 0 || index >= markers.length) return;

    const marker = markers[index];
    if (marker && map?.hasLayer(marker)) {
        map.removeLayer(marker);
    }

    markers.splice(index, 1);
    routePoints.splice(index, 1);

    if (selectedUserWaypointIndex === index) {
        selectedUserWaypointIndex = -1;
    } else if (selectedUserWaypointIndex > index) {
        selectedUserWaypointIndex -= 1;
    }

    invalidateComputedRouteDisplay();
    updateSelectedWaypointInfo();
}

function getLogicalInsertionIndexFromRouteClick(clickLatLng) {
    if (!Array.isArray(routePoints) || routePoints.length < 2) return routePoints.length;
    if (!clickLatLng || !map) return routePoints.length;

    const clickPoint = map.latLngToLayerPoint(clickLatLng);
    let bestIndex = routePoints.length;
    let bestDistance = Number.POSITIVE_INFINITY;

    for (let i = 0; i < routePoints.length - 1; i += 1) {
        const a = routePoints[i];
        const b = routePoints[i + 1];
        if (!a || !b) continue;

        const aPt = map.latLngToLayerPoint(a);
        const bPt = map.latLngToLayerPoint(b);
        const distancePx = L.LineUtil.pointToSegmentDistance(clickPoint, aPt, bPt);

        if (distancePx < bestDistance) {
            bestDistance = distancePx;
            bestIndex = i + 1;
        }
    }

    return bestIndex;
}

function addUserWaypoint(latlng, options = {}) {
    if (!latlng || !Number.isFinite(latlng.lat) || !Number.isFinite(latlng.lng)) return null;

    const { select = true, invalidate = true, insertIndex = null } = options;

    const hasValidInsertIndex = Number.isInteger(insertIndex) && insertIndex >= 0 && insertIndex <= routePoints.length;
    const targetIndex = hasValidInsertIndex ? insertIndex : routePoints.length;

    routePoints.splice(targetIndex, 0, latlng);
    const marker = createWaypointMarker(latlng);
    markers.splice(targetIndex, 0, marker);

    if (select) {
        selectUserWaypoint(marker);
    } else {
        updateSelectedWaypointInfo();
    }

    if (invalidate) {
        invalidateComputedRouteDisplay();
    }

    return marker;
}

function getArrivalReferenceDateTime() {
    if (lastComputedReportData?.arrivalIso) {
        const fromReport = new Date(lastComputedReportData.arrivalIso);
        if (!Number.isNaN(fromReport.getTime())) return fromReport;
    }

    return new Date(`${departureDate}T${departureTime}:00`);
}

function clearArrivalPoiMarkers() {
    arrivalPoiMarkers.forEach(marker => {
        if (map?.hasLayer(marker)) map.removeLayer(marker);
    });
    arrivalPoiMarkers = [];
}

function addArrivalPoiMarker(lat, lng, label, type = 'poi') {
    if (!map || !Number.isFinite(lat) || !Number.isFinite(lng)) return null;

    const marker = L.circleMarker([lat, lng], {
        radius: type === 'anchorage' ? 6 : 4,
        color: type === 'anchorage' ? '#7fd8ff' : '#ffcf6a',
        weight: 2,
        fillColor: type === 'anchorage' ? '#133341' : '#3a2a13',
        fillOpacity: 0.9
    }).addTo(map);

    marker.bindPopup(`<strong>${escapeHtml(label)}</strong>`, { maxWidth: 260, autoPan: false });
    arrivalPoiMarkers.push(marker);
    return marker;
}

function elementToPoint(element) {
    const lat = Number(element?.lat ?? element?.center?.lat);
    const lon = Number(element?.lon ?? element?.center?.lon);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
    return { lat, lon, tags: element?.tags || {} };
}

function getPoiLabel(point, fallback) {
    return point?.tags?.name || fallback;
}

async function fetchOverpassElements(query) {
    const normalizedQuery = String(query || '').trim();
    if (!normalizedQuery) return [];

    const cached = overpassQueryCache.get(normalizedQuery);
    if (cached && (Date.now() - cached.ts) < OVERPASS_CACHE_TTL_MS) {
        return cached.elements;
    }

    let lastError = null;

    const now = Date.now();
    const orderedEndpoints = [
        overpassPreferredEndpoint,
        ...OVERPASS_URLS.filter(url => url !== overpassPreferredEndpoint)
    ];

    for (const endpoint of orderedEndpoints) {
        const blockedUntil = overpassEndpointCooldownUntil.get(endpoint) || 0;
        if (blockedUntil > now) continue;

        try {
            const requestNow = Date.now();
            const waitMs = Math.max(0, OVERPASS_MIN_INTERVAL_MS - (requestNow - overpassLastRequestAt));
            if (waitMs > 0) {
                await new Promise(resolve => setTimeout(resolve, waitMs));
            }

            const body = new URLSearchParams({ data: normalizedQuery }).toString();
            const response = await fetch(endpoint, {
                method: 'POST',
                headers: { 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8' },
                body
            });
            overpassLastRequestAt = Date.now();

            const text = await response.text();
            if (!response.ok) {
                if (response.status === 429) {
                    overpassEndpointCooldownUntil.set(endpoint, Date.now() + OVERPASS_429_COOLDOWN_MS);
                    await new Promise(resolve => setTimeout(resolve, 1200));
                }
                lastError = new Error(`Overpass HTTP ${response.status}`);
                continue;
            }

            let data = null;
            try {
                data = JSON.parse(text);
            } catch (_parseError) {
                lastError = new Error('Overpass JSON invalide');
                continue;
            }

            const elements = Array.isArray(data?.elements) ? data.elements : [];
            overpassPreferredEndpoint = endpoint;
            overpassQueryCache.set(normalizedQuery, {
                ts: Date.now(),
                elements
            });
            return elements;
        } catch (error) {
            lastError = error;
        }
    }

    throw lastError || new Error('Overpass indisponible');
}

async function fetchNearbyAnchorages(lat, lon, radiusM = 18000) {
    const query = `
        [out:json][timeout:25];
        (
          node(around:${radiusM},${lat},${lon})["seamark:type"="anchorage"];
          way(around:${radiusM},${lat},${lon})["seamark:type"="anchorage"];
          relation(around:${radiusM},${lat},${lon})["seamark:type"="anchorage"];
          node(around:${radiusM},${lat},${lon})["leisure"="marina"];
          way(around:${radiusM},${lat},${lon})["leisure"="marina"];
        );
        out center tags;
    `;

    const elements = await fetchOverpassElements(query);
    return elements
        .map(elementToPoint)
        .filter(Boolean)
        .map((point, idx) => ({
            ...point,
            name: getPoiLabel(point, `Mouillage ${idx + 1}`),
            distanceNm: distanceNm(lat, lon, point.lat, point.lon)
        }))
        .sort((a, b) => a.distanceNm - b.distanceNm)
        .slice(0, 12);
}

async function fetchNearbyAmenityList(lat, lon, kind = 'restaurant', radiusM = 6000) {
    const filter = kind === 'shop'
        ? '"shop"~"supermarket|convenience|greengrocer|bakery|butcher"'
        : '"amenity"~"restaurant|fast_food|cafe"';

    const query = `
        [out:json][timeout:25];
        (
          node(around:${radiusM},${lat},${lon})[${filter}];
          way(around:${radiusM},${lat},${lon})[${filter}];
        );
        out center tags;
    `;

    const elements = await fetchOverpassElements(query);
    return elements
        .map(elementToPoint)
        .filter(Boolean)
        .map((point, idx) => ({
            ...point,
            name: getPoiLabel(point, kind === 'shop' ? `Magasin ${idx + 1}` : `Restaurant ${idx + 1}`),
            distanceNm: distanceNm(lat, lon, point.lat, point.lon)
        }))
        .sort((a, b) => a.distanceNm - b.distanceNm)
        .slice(0, 8);
}

async function scoreAnchoragesForArrival(anchorages, arrivalDateTime) {
    const arrivalSlot = toDateAndHourUtc(arrivalDateTime);

    const scored = [];
    for (const anchorage of anchorages) {
        const weather = await getWeatherAtDateHour(anchorage.lat, anchorage.lon, arrivalSlot.date, arrivalSlot.hour);
        const wind = Number.isFinite(weather?.windSpeed) ? weather.windSpeed : 12;
        const wave = Number.isFinite(weather?.waveHeight) ? weather.waveHeight : 0.8;

        const penalty =
            anchorage.distanceNm * 3 +
            Math.max(0, wind - RECOMMENDED_MAX_WIND_KN) * 25 +
            Math.abs(wind - 12) * 0.8 +
            Math.max(0, wave - 1.6) * 10;

        scored.push({
            ...anchorage,
            weather,
            score: penalty,
            confidence: wind <= 20 && wave <= 1.8 ? 'élevée' : (wind <= 24 ? 'moyenne' : 'prudence')
        });
    }

    return scored.sort((a, b) => a.score - b.score).slice(0, 3);
}

function renderNearbyList(containerId, items, emptyText) {
    const container = document.getElementById(containerId);
    if (!container) return;

    if (!Array.isArray(items) || items.length === 0) {
        container.innerHTML = `<div class="arrival-list__item">${escapeHtml(emptyText)}</div>`;
        return;
    }

    container.innerHTML = `<div class="arrival-list">${items.map(item =>
        `<div class="arrival-list__item"><strong>${escapeHtml(item.name)}</strong><br>${item.distanceNm.toFixed(2)} nm</div>`
    ).join('')}</div>`;
}

function applyAnchorageAsFinalWaypoint(anchorage) {
    if (!anchorage || !Number.isFinite(anchorage.lat) || !Number.isFinite(anchorage.lon)) return;

    const latlng = L.latLng(anchorage.lat, anchorage.lon);
    if (routePoints.length === 0) {
        routePoints.push(latlng);
        const marker = createWaypointMarker(latlng);
        markers.push(marker);
    } else {
        const lastIndex = routePoints.length - 1;
        routePoints[lastIndex] = latlng;
        const lastMarker = markers[lastIndex];
        if (lastMarker) {
            lastMarker.setLatLng(latlng);
        } else {
            const marker = createWaypointMarker(latlng);
            markers[lastIndex] = marker;
        }
    }

    map.panTo(latlng, { animate: true, duration: 0.45 });
}

function renderAnchorageRecommendations(recommendations) {
    const container = document.getElementById('anchorageRecommendations');
    if (!container) return;

    if (!Array.isArray(recommendations) || recommendations.length === 0) {
        container.innerHTML = `<div class="arrival-card">${t('Aucun mouillage recommandé trouvé.', 'No se encontró fondeo recomendado.')}</div>`;
        return;
    }

    container.innerHTML = recommendations.map((item, index) => {
        const wind = Number.isFinite(item?.weather?.windSpeed) ? `${item.weather.windSpeed.toFixed(1)} kn` : 'N/A';
        const wave = Number.isFinite(item?.weather?.waveHeight) ? `${item.weather.waveHeight.toFixed(1)} m` : 'N/A';
        return `
            <div class="arrival-card">
                <div class="arrival-card__head">
                    <span>#${index + 1} ${escapeHtml(item.name)}</span>
                    <span>${item.distanceNm.toFixed(2)} nm</span>
                </div>
                <div>${t('Vent ETA', 'Viento ETA')}: ${wind} · ${t('Houle ETA', 'Oleaje ETA')}: ${wave} · ${t('Confiance', 'Confianza')}: ${escapeHtml(item.confidence)}</div>
                <button type="button" class="apply-anchorage-btn" data-anch-index="${index}">${t('Utiliser comme WP final', 'Usar como WP final')}</button>
            </div>
        `;
    }).join('');

    const buttons = container.querySelectorAll('.apply-anchorage-btn');
    buttons.forEach(btn => {
        btn.addEventListener('click', () => {
            const idx = Number(btn.getAttribute('data-anch-index'));
            const selected = recommendations[idx];
            if (selected) applyAnchorageAsFinalWaypoint(selected);
        });
    });
}

async function analyzeArrivalZone() {
    if (routePoints.length < 2) {
        alert(t('Ajoute au moins 2 waypoints pour analyser la zone d\'arrivée.', 'Añade al menos 2 waypoints para analizar la zona de llegada.'));
        return;
    }

    const button = document.getElementById('analyzeArrivalBtn');
    const summary = document.getElementById('arrivalSummary');
    if (button) {
        button.disabled = true;
        button.textContent = t('Analyse en cours...', 'Análisis en curso...');
    }
    if (summary) summary.textContent = t('Analyse mouillage: récupération des données...', 'Análisis fondeo: recuperando datos...');

    clearArrivalPoiMarkers();

    try {
        const destination = routePoints[routePoints.length - 1];
        const arrivalTime = getArrivalReferenceDateTime();

        const anchorages = await fetchNearbyAnchorages(destination.lat, destination.lng);
        const restaurants = await fetchNearbyAmenityList(destination.lat, destination.lng, 'restaurant');
        const shops = await fetchNearbyAmenityList(destination.lat, destination.lng, 'shop');

        const recommendations = await scoreAnchoragesForArrival(anchorages, arrivalTime);

        recommendations.forEach(item => addArrivalPoiMarker(item.lat, item.lon, item.name, 'anchorage'));
        restaurants.forEach(item => addArrivalPoiMarker(item.lat, item.lon, item.name, 'restaurant'));
        shops.forEach(item => addArrivalPoiMarker(item.lat, item.lon, item.name, 'shop'));

        renderAnchorageRecommendations(recommendations);
        renderNearbyList('nearbyRestaurants', restaurants, t('Aucun restaurant proche trouvé', 'No se encontraron restaurantes cercanos'));
        renderNearbyList('nearbyShops', shops, t('Aucun magasin proche trouvé', 'No se encontraron tiendas cercanas'));

        if (summary) {
            if (recommendations.length) {
                summary.innerHTML = `<strong>${t('Top mouillage:', 'Mejor fondeo:')}</strong> ${escapeHtml(recommendations[0].name)} · ${recommendations[0].distanceNm.toFixed(2)} nm ${t('de l\'arrivée', 'de la llegada')}`;
            } else {
                summary.textContent = t('Analyse mouillage: aucun mouillage adapté trouvé à proximité.', 'Análisis fondeo: no se encontró un fondeo adecuado cerca.');
            }
        }
    } catch (_error) {
        if (summary) summary.textContent = t('Analyse mouillage: erreur de récupération des données.', 'Análisis fondeo: error al recuperar datos.');
    } finally {
        if (button) {
            button.disabled = false;
            button.textContent = t('Conseiller mouillage à l\'arrivée', 'Sugerir fondeo a la llegada');
        }
    }
}

function formatDurationHours(totalHours) {
    if (!Number.isFinite(totalHours)) return 'N/A';
    const hours = Math.floor(totalHours);
    const minutes = Math.round((totalHours - hours) * 60);
    if (hours <= 0) return `${minutes} min`;
    return `${hours} h ${String(minutes).padStart(2, '0')}`;
}

function getWeatherFavorabilityPenalty(weather) {
    const wind = Number(weather?.windSpeed);
    const wave = Number(weather?.waveHeight);
    const precip = Number(weather?.precipitation);

    let penalty = 0;
    if (Number.isFinite(wind)) {
        if (wind > RECOMMENDED_MAX_WIND_KN) penalty += (wind - RECOMMENDED_MAX_WIND_KN) * 20;
        penalty += Math.abs(wind - 12) * 0.9;
    } else {
        penalty += 8;
    }

    if (Number.isFinite(wave)) {
        if (wave > 2.5) penalty += (wave - 2.5) * 12;
        else penalty += wave * 2;
    }

    if (Number.isFinite(precip)) penalty += precip * 2.5;
    return penalty;
}

function normalizeRouteScenarioPoints(source) {
    if (!Array.isArray(source)) return [];

    return source
        .map(point => {
            const lat = Number(point?.lat);
            const lng = Number(point?.lng ?? point?.lon);
            if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;
            return { lat, lng };
        })
        .filter(Boolean);
}

async function estimateRouteForDepartureOnPoints(routeScenarioPoints, departureDateTime) {
    const points = normalizeRouteScenarioPoints(routeScenarioPoints);
    if (points.length < 2) return null;

    let totalTimeHours = 0;
    let maxWind = 0;
    let maxGust = 0;
    let minWind = Number.POSITIVE_INFINITY;
    let hasMotorSegment = false;
    let motorTimeHours = 0;

    for (let i = 0; i < points.length - 1; i++) {
        const start = { lat: points[i].lat, lon: points[i].lng };
        const end = { lat: points[i + 1].lat, lon: points[i + 1].lng };
        const legStartDateTime = new Date(departureDateTime.getTime() + totalTimeHours * 3600 * 1000);
        const legStartSlot = toDateAndHourUtc(legStartDateTime);
        const legStartWeather = await getWeatherAtDateHour(start.lat, start.lon, legStartSlot.date, legStartSlot.hour);
        const effectiveSpeed = estimateEffectiveSpeedForLegSplit(start, end, legStartWeather);
        const maxDistanceForWeatherNm = Math.max(4, effectiveSpeed * AUTO_WEATHER_SPLIT_MAX_HOURS);
        const legPoints = densifyPolylineForWeather([start, end], maxDistanceForWeatherNm);

        for (let legIndex = 0; legIndex < legPoints.length - 1; legIndex++) {
            const legStart = legPoints[legIndex];
            const legEnd = legPoints[legIndex + 1];

            const passageDateTime = new Date(departureDateTime.getTime() + totalTimeHours * 3600 * 1000);
            const passageSlot = toDateAndHourUtc(passageDateTime);
            const weather = await getWeatherAtDateHour(legStart.lat, legStart.lon, passageSlot.date, passageSlot.hour);

            const windSpeed = Number.isFinite(weather?.windSpeed) ? weather.windSpeed : 10;
            const windGust = Number.isFinite(weather?.windGust) ? weather.windGust : windSpeed;
            const windDirection = Number.isFinite(weather?.windDirection) ? weather.windDirection : 0;
            maxWind = Math.max(maxWind, windSpeed);
            maxGust = Math.max(maxGust, windGust);
            minWind = Math.min(minWind, windSpeed);

            const isMotorSegment = windSpeed < MOTOR_WIND_THRESHOLD_KN;
            if (isMotorSegment) hasMotorSegment = true;
            const rawSegment = isMotorSegment
                ? {
                    distance: distanceNm(legStart.lat, legStart.lon, legEnd.lat, legEnd.lon),
                    speed: MOTOR_SPEED_KN,
                    bearing: getBearing(legStart, legEnd),
                    timeHours: distanceNm(legStart.lat, legStart.lon, legEnd.lat, legEnd.lon) / MOTOR_SPEED_KN,
                    type: 'motor'
                }
                : routeSegment(legStart, legEnd, windDirection, windSpeed, tackingTimeHours);

            const twa = isMotorSegment ? null : computeTWA(rawSegment.bearing, windDirection);
            const sailSetup = getSailRecommendation({
                isMotorSegment,
                tws: windSpeed,
                twa,
                sailModeValue: sailMode
            });

            const sailFactor = getSailPerformanceFactor({
                isMotorSegment,
                sailModeValue: sailMode,
                tws: windSpeed,
                twa,
                sailSetup
            });

            const timeHours = isMotorSegment ? rawSegment.timeHours : rawSegment.timeHours / sailFactor;
            if (isMotorSegment) motorTimeHours += timeHours;
            totalTimeHours += timeHours;
        }
    }

    const departureSlot = toDateAndHourUtc(departureDateTime);
    const arrivalDateTime = new Date(departureDateTime.getTime() + totalTimeHours * 3600 * 1000);
    const arrivalSlot = toDateAndHourUtc(arrivalDateTime);

    const departureWeather = await getWeatherAtDateHour(
        points[0].lat,
        points[0].lng,
        departureSlot.date,
        departureSlot.hour
    );

    const arrivalWeather = await getWeatherAtDateHour(
        points[points.length - 1].lat,
        points[points.length - 1].lng,
        arrivalSlot.date,
        arrivalSlot.hour
    );

    const score =
        getWeatherFavorabilityPenalty(departureWeather) +
        getWeatherFavorabilityPenalty(arrivalWeather) +
        (maxWind > RECOMMENDED_MAX_WIND_KN ? (maxWind - RECOMMENDED_MAX_WIND_KN) * 50 : 0) +
        totalTimeHours * 0.2 +
        motorTimeHours * 8;

    const windFloorOk = Number.isFinite(minWind) && minWind > MOTOR_WIND_THRESHOLD_KN;

    return {
        departureDateTime,
        arrivalDateTime,
        totalTimeHours,
        maxWind,
        maxGust,
        minWind,
        hasMotorSegment,
        motorTimeHours,
        departureWeather,
        arrivalWeather,
        score,
        isSafe: maxWind <= RECOMMENDED_MAX_WIND_KN,
        isNoMotor: windFloorOk && !hasMotorSegment,
        routePointCount: points.length
    };
}

async function estimateRouteForDeparture(departureDateTime) {
    return estimateRouteForDepartureOnPoints(routePoints, departureDateTime);
}

function renderDepartureSuggestion(result) {
    const container = document.getElementById('departureSuggestionInfo');
    if (!container) return;

    if (!result) {
        lastDepartureSuggestion = null;
        container.textContent = t('Suggestion départ: aucune fenêtre météo favorable trouvée.', 'Sugerencia salida: no se encontró una ventana meteo favorable.');
        container.classList.remove('suggestion-clickable');
        return;
    }

    lastDepartureSuggestion = result;
    container.classList.add('suggestion-clickable');

    const depWind = Number.isFinite(result?.departureWeather?.windSpeed) ? `${result.departureWeather.windSpeed.toFixed(1)} kn` : 'N/A';
    const arrWind = Number.isFinite(result?.arrivalWeather?.windSpeed) ? `${result.arrivalWeather.windSpeed.toFixed(1)} kn` : 'N/A';
    const maxWind = Number.isFinite(result?.maxWind) ? `${result.maxWind.toFixed(1)} kn` : 'N/A';
    const maxGust = Number.isFinite(result?.maxGust) ? `${result.maxGust.toFixed(1)} kn` : 'N/A';
    const minWind = Number.isFinite(result?.minWind) ? `${result.minWind.toFixed(1)} kn` : 'N/A';
    const motorTime = Number.isFinite(result?.motorTimeHours) ? result.motorTimeHours : 0;
    const motorShare = Number.isFinite(result?.totalTimeHours) && result.totalTimeHours > 0
        ? Math.round((motorTime / result.totalTimeHours) * 100)
        : 0;

    container.innerHTML =
        `<strong>${t('Départ conseillé', 'Salida recomendada')}:</strong> ${formatWeekdayHourUtc(result.departureDateTime)}<br>` +
        `${t('Arrivée estimée', 'Llegada estimada')}: ${formatWeekdayHourUtc(result.arrivalDateTime)} · ${formatDurationHours(result.totalTimeHours)}<br>` +
        `${t('Vent départ', 'Viento salida')}: ${depWind} · ${t('Vent arrivée', 'Viento llegada')}: ${arrWind} · ${t('Vent min trajet', 'Viento min trayecto')}: ${minWind}<br>` +
        `${t('Vent maxi trajet', 'Viento máx trayecto')}: ${maxWind} · ${t('Rafale maxi trajet', 'Ráfaga máx trayecto')}: ${maxGust}<br>` +
        `${t('Moteur estimé', 'Motor estimado')}: ${formatDurationHours(motorTime)} (${motorShare}%)`;
}

async function suggestBestDeparture() {
    if (routePoints.length < 2) {
        alert(t('Ajoute au moins 2 waypoints pour analyser un départ.', 'Añade al menos 2 waypoints para analizar una salida.'));
        return;
    }

    const button = document.getElementById('suggestDepartureBtn');
    if (button) {
        button.disabled = true;
        button.textContent = t('Analyse météo...', 'Análisis meteo...');
    }

    beginAiTrafficSession('Conseil départ météo');

    try {
        const baseDateTime = new Date(`${departureDate}T${departureTime}:00`);
        if (Number.isNaN(baseDateTime.getTime())) throw new Error('invalid-departure');

        const candidates = [];
        const stepHours = 3;
        const maxCandidates = Math.min(32, Math.max(8, forecastWindowDays * 8));

        for (let i = 0; i < maxCandidates; i++) {
            const candidateDate = new Date(baseDateTime.getTime() + i * stepHours * 3600 * 1000);
            if ((candidateDate.getTime() - baseDateTime.getTime()) > forecastWindowDays * 24 * 3600 * 1000) break;
            pushAiTrafficLog(`Évaluation départ candidat ${i + 1}/${maxCandidates} · ${formatWeekdayHourUtc(candidateDate)}`);
            const estimate = await estimateRouteForDeparture(candidateDate);
            if (estimate) candidates.push(estimate);
        }

        if (candidates.length === 0) {
            renderDepartureSuggestion(null);
            endAiTrafficSession('Aucune fenêtre météo viable trouvée');
            return;
        }

        const safeCandidates = candidates.filter(c => c.isSafe);
        const pool = safeCandidates.length ? safeCandidates : [];
        pool.sort((a, b) => a.score - b.score);
        const best = pool[0] || null;

        if (!best) {
            const container = document.getElementById('departureSuggestionInfo');
            lastDepartureSuggestion = null;
            if (container) {
                container.classList.remove('suggestion-clickable');
                container.textContent = t('Suggestion départ: aucune fenêtre météo ≤ 20 kn trouvée.', 'Sugerencia salida: no se encontró ventana meteo ≤ 20 kn.');
            }
            endAiTrafficSession('Aucune fenêtre ≤ 20 kn');
            return;
        }

        renderDepartureSuggestion(best);
        applyLastDepartureSuggestion();
        endAiTrafficSession('Suggestion départ terminée');
    } catch (_error) {
        endAiTrafficSession('Erreur pendant le calcul de suggestion');
        alert(t('Impossible de calculer une suggestion de départ pour le moment.', 'No se puede calcular una sugerencia de salida por ahora.'));
    } finally {
        if (button) {
            button.disabled = false;
            button.textContent = t('Conseiller départ météo (≤ 20 kn)', 'Sugerir salida meteo (≤ 20 kn)');
        }
    }
}

function updateRoutingControlsUiFromState() {
    const sailModeSelect = document.getElementById('sailModeSelect');
    if (sailModeSelect) sailModeSelect.value = sailMode;

    const tackingInput = document.getElementById('tackingTimeInput');
    if (tackingInput) tackingInput.value = String(tackingTimeHours);
}

function renderAiRouteCandidates(candidates) {
    const info = document.getElementById('aiRouteSuggestionInfo');
    const container = document.getElementById('aiRouteSuggestions');
    if (!info || !container) return;

    if (!Array.isArray(candidates) || candidates.length === 0) {
        lastAiRouteCandidates = [];
        info.textContent = t('Routes IA: aucune proposition disponible.', 'Rutas IA: ninguna propuesta disponible.');
        container.innerHTML = '';
        return;
    }

    lastAiRouteCandidates = candidates;
    info.textContent = t(
        `Routes IA: ${candidates.length} proposition(s). Clique "Appliquer" pour charger un profil.`,
        `Rutas IA: ${candidates.length} propuesta(s). Haz clic en "Aplicar" para cargar un perfil.`
    );

    container.innerHTML = candidates.map((item, index) => {
        const depWind = Number.isFinite(item?.departureWeather?.windSpeed) ? `${item.departureWeather.windSpeed.toFixed(1)} kn` : 'N/A';
        const maxWind = Number.isFinite(item?.maxWind) ? `${item.maxWind.toFixed(1)} kn` : 'N/A';
        const motor = Number.isFinite(item?.motorTimeHours) ? formatDurationHours(item.motorTimeHours) : 'N/A';
        const score = Number.isFinite(item?.score) ? item.score.toFixed(1) : 'N/A';
        const routeVariant = item?.routeVariantLabel || t('Trajectoire standard', 'Trayectoria estándar');
        return `<div class="ai-route-card"><strong>${escapeHtml(item.label)}</strong><br>` +
            `${t('Trajectoire', 'Trayectoria')}: ${escapeHtml(routeVariant)} · ${t('Points', 'Puntos')}: ${Number(item?.routePointCount || 0)}<br>` +
            `${t('Départ', 'Salida')}: ${formatWeekdayHourUtc(item.departureDateTime)} · ${t('Durée', 'Duración')}: ${formatDurationHours(item.totalTimeHours)}<br>` +
            `${t('Vent départ', 'Viento salida')}: ${depWind} · ${t('Vent max trajet', 'Viento máx trayecto')}: ${maxWind} · ${t('Moteur', 'Motor')}: ${motor}<br>` +
            `${t('Mode', 'Modo')}: ${escapeHtml(item.sailMode)} · ${t('Bord', 'Bordo')}: ${Math.round(item.tackingTimeHours * 60)} min · Score: ${score}<br>` +
            `<button type="button" class="apply-ai-route-btn" data-ai-route-index="${index}" style="margin-top:6px;">${t('Appliquer ce profil', 'Aplicar este perfil')}</button></div>`;
    }).join('');
}

function replaceRouteWithScenarioPoints(points) {
    const normalized = normalizeRouteScenarioPoints(points);
    if (normalized.length < 2 || !map) return;

    markers.forEach(marker => {
        if (map.hasLayer(marker)) map.removeLayer(marker);
    });
    markers = [];
    routePoints = [];
    selectedUserWaypointIndex = -1;
    currentLoadedRouteIndex = -1;

    if (routeLayer && map.hasLayer(routeLayer)) {
        map.removeLayer(routeLayer);
    }
    routeLayer = null;

    if (windLayer && map.hasLayer(windLayer)) {
        map.removeLayer(windLayer);
    }
    windLayer = null;

    clearWaypointWindDirectionLayers();
    clearWaveDirectionSegmentLayers();
    clearGeneratedWaypointMarkers();
    clearArrivalPoiMarkers();
    waypointPassageSlots.clear();
    lastRouteBounds = null;
    lastComputedReportData = null;

    normalized.forEach(point => {
        const marker = createWaypointMarker([point.lat, point.lng]);
        markers.push(marker);
        routePoints.push(marker.getLatLng());
    });

    drawRoute(routePoints);
    updateSelectedWaypointInfo();
}

function applyAiRouteCandidate(index) {
    const candidate = lastAiRouteCandidates[index];
    if (!candidate) return;

    if (Array.isArray(candidate.routeScenarioPoints) && candidate.routeScenarioPoints.length >= 2) {
        replaceRouteWithScenarioPoints(candidate.routeScenarioPoints);
    }

    sailMode = candidate.sailMode;
    tackingTimeHours = candidate.tackingTimeHours;
    updateRoutingControlsUiFromState();

    if (candidate?.departureDateTime instanceof Date && !Number.isNaN(candidate.departureDateTime.getTime())) {
        const inputValue = toLocalDateTimeInputValue(candidate.departureDateTime);
        setDepartureFromDateTimeInput(inputValue);
    }

    const info = document.getElementById('aiRouteSuggestionInfo');
    if (info) {
        const routeVariant = candidate.routeVariantLabel || t('Trajectoire standard', 'Trayectoria estándar');
        info.textContent = t(
            `Profil appliqué: ${candidate.label} · ${routeVariant}. Tu peux maintenant cliquer Calculer.`,
            `Perfil aplicado: ${candidate.label} · ${routeVariant}. Ahora puedes hacer clic en Calcular.`
        );
    }
}

function scoreAiCandidate(estimate, profile) {
    const windPenalty = Number.isFinite(estimate?.maxWind) ? Math.max(0, estimate.maxWind - RECOMMENDED_MAX_WIND_KN) * 12 : 20;
    const motorPenalty = Number.isFinite(estimate?.motorTimeHours) ? estimate.motorTimeHours * (profile.priority === 'safe' ? 10 : 5) : 0;
    const timePenalty = Number.isFinite(estimate?.totalTimeHours) ? estimate.totalTimeHours * (profile.priority === 'performance' ? 0.1 : 0.3) : 10;
    return (estimate.score || 0) + windPenalty + motorPenalty + timePenalty;
}

function buildAiRouteVariants() {
    const currentPath = normalizeRouteScenarioPoints(routePoints);
    if (currentPath.length < 2) return [];

    const variants = [
        {
            routeVariantLabel: t('Route actuelle', 'Ruta actual'),
            points: currentPath
        }
    ];

    const start = currentPath[0];
    const end = currentPath[currentPath.length - 1];
    const bearing = getBearing({ lat: start.lat, lon: start.lng }, { lat: end.lat, lon: end.lng });
    const totalNm = distanceNm(start.lat, start.lng, end.lat, end.lng);
    const detourNm = Math.max(12, Math.min(40, totalNm * 0.18));
    const mid = movePoint(start.lat, start.lng, bearing, totalNm / 2);

    const northDetour = movePoint(mid.lat, mid.lon, bearing + 90, detourNm);
    const southDetour = movePoint(mid.lat, mid.lon, bearing - 90, detourNm);

    variants.push({
        routeVariantLabel: t('Contournement nord', 'Rodeo norte'),
        points: [start, { lat: northDetour.lat, lng: northDetour.lon }, end]
    });
    variants.push({
        routeVariantLabel: t('Contournement sud', 'Rodeo sur'),
        points: [start, { lat: southDetour.lat, lng: southDetour.lon }, end]
    });

    if (currentPath.length > 2) {
        variants.push({
            routeVariantLabel: t('Directe départ-arrivée', 'Directa salida-llegada'),
            points: [start, end]
        });
    }

    return variants;
}

async function suggestAiRouteOptions() {
    if (routePoints.length < 2) {
        alert(t('Ajoute au moins 2 waypoints pour analyser des routes IA.', 'Añade al menos 2 waypoints para analizar rutas IA.'));
        return;
    }

    const button = document.getElementById('suggestAiRoutesBtn');
    const info = document.getElementById('aiRouteSuggestionInfo');

    if (button) {
        button.disabled = true;
        button.textContent = t('Calcul IA...', 'Cálculo IA...');
    }
    if (info) info.textContent = t('Routes IA: calcul en cours...', 'Rutas IA: cálculo en curso...');
    beginAiTrafficSession('Routage IA multi-scénarios');

    const originalSailMode = sailMode;
    const originalTackingTimeHours = tackingTimeHours;

    try {
        const baseDateTime = new Date(`${departureDate}T${departureTime}:00`);
        if (Number.isNaN(baseDateTime.getTime())) throw new Error('invalid-departure');

        const routeVariants = buildAiRouteVariants();
        if (!routeVariants.length) throw new Error('no-route-variant');
        pushAiTrafficLog(`Variantes trajectoires détectées: ${routeVariants.length}`);

        const profiles = [
            { label: 'Safe', sailMode: 'prudent', tackingTimeHours: 0.5, priority: 'safe' },
            { label: 'Équilibré', sailMode: 'auto', tackingTimeHours: 0.5, priority: 'balanced' },
            { label: 'Performance', sailMode: 'performance', tackingTimeHours: 0.33, priority: 'performance' }
        ];

        const results = [];
        const stepHours = 6;
        const horizonHours = Math.min(48, Math.max(24, forecastWindowDays * 24));

        for (const profile of profiles) {
            sailMode = profile.sailMode;
            tackingTimeHours = profile.tackingTimeHours;
            pushAiTrafficLog(`Profil ${profile.label}: mode ${profile.sailMode}, bord ${Math.round(profile.tackingTimeHours * 60)} min`);

            for (const variant of routeVariants) {
                pushAiTrafficLog(`↳ Analyse variante: ${variant.routeVariantLabel}`);
                let best = null;
                for (let offset = 0; offset <= horizonHours; offset += stepHours) {
                    const candidateDate = new Date(baseDateTime.getTime() + offset * 3600 * 1000);
                    pushAiTrafficLog(`   - Run météo ${formatWeekdayHourUtc(candidateDate)}`);
                    const estimate = await estimateRouteForDepartureOnPoints(variant.points, candidateDate);
                    if (!estimate) continue;
                    const aiScore = scoreAiCandidate(estimate, profile);
                    const enriched = {
                        ...estimate,
                        aiScore,
                        label: profile.label,
                        sailMode: profile.sailMode,
                        tackingTimeHours: profile.tackingTimeHours,
                        routeVariantLabel: variant.routeVariantLabel,
                        routeScenarioPoints: variant.points
                    };

                    if (!best || enriched.aiScore < best.aiScore) {
                        best = enriched;
                    }
                }

                if (best) results.push(best);
            }
        }

        results.sort((a, b) => a.aiScore - b.aiScore);
        renderAiRouteCandidates(results.slice(0, 6));
        pushAiTrafficLog(`Classement final: ${Math.min(results.length, 6)} résultat(s) affiché(s)`);
        endAiTrafficSession('Routage IA terminé');
    } catch (_error) {
        renderAiRouteCandidates([]);
        if (info) info.textContent = t('Routes IA: impossible de calculer des options pour le moment.', 'Rutas IA: no se pueden calcular opciones por ahora.');
        endAiTrafficSession('Erreur pendant le routage IA');
    } finally {
        sailMode = originalSailMode;
        tackingTimeHours = originalTackingTimeHours;
        updateRoutingControlsUiFromState();
        if (button) {
            button.disabled = false;
            button.textContent = t('Proposer routes IA (safe / perf)', 'Proponer rutas IA (safe / perf)');
        }
    }
}

function buildRouteVectorDataUrl(reportData) {
    const polyline = reportData?.routeVector?.polyline;
    const waypoints = Array.isArray(reportData?.routeVector?.waypoints)
        ? reportData.routeVector.waypoints
        : [];
    if (!Array.isArray(polyline) || polyline.length < 2) return null;

    const lats = polyline.map(p => Number(p?.lat)).filter(Number.isFinite);
    const lngs = polyline.map(p => Number(p?.lng)).filter(Number.isFinite);
    if (lats.length < 2 || lngs.length < 2) return null;

    const minLat = Math.min(...lats);
    const maxLat = Math.max(...lats);
    const minLng = Math.min(...lngs);
    const maxLng = Math.max(...lngs);

    const width = 1100;
    const height = 460;
    const pad = 34;
    const innerW = width - pad * 2;
    const innerH = height - pad * 2;
    const latSpan = Math.max(1e-9, maxLat - minLat);
    const lngSpan = Math.max(1e-9, maxLng - minLng);

    const toXY = (point) => {
        const x = pad + ((point.lng - minLng) / lngSpan) * innerW;
        const y = pad + ((maxLat - point.lat) / latSpan) * innerH;
        return { x, y };
    };

    const projected = polyline.map(toXY);
    const pathD = projected
        .map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x.toFixed(1)} ${p.y.toFixed(1)}`)
        .join(' ');

    const start = projected[0];
    const end = projected[projected.length - 1];

    const projectedWaypoints = waypoints
        .map(wp => {
            const lat = Number(wp?.lat);
            const lng = Number(wp?.lng);
            if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;
            const p = toXY({ lat, lng });
            return {
                ...p,
                label: String(wp?.label || ''),
                icon: String(wp?.icon || '•')
            };
        })
        .filter(Boolean);

    const gridLines = [];
    const steps = 5;
    for (let i = 1; i < steps; i++) {
        const gx = pad + (innerW * i) / steps;
        const gy = pad + (innerH * i) / steps;
        gridLines.push(`<line x1="${gx.toFixed(1)}" y1="${pad}" x2="${gx.toFixed(1)}" y2="${height - pad}" stroke="#1f3a51" stroke-opacity="0.35" stroke-width="1" />`);
        gridLines.push(`<line x1="${pad}" y1="${gy.toFixed(1)}" x2="${width - pad}" y2="${gy.toFixed(1)}" stroke="#1f3a51" stroke-opacity="0.35" stroke-width="1" />`);
    }

    const waypointNodes = projectedWaypoints.map((wp, idx) => {
        const textX = Math.min(width - 100, wp.x + 8).toFixed(1);
        const textY = Math.max(14, wp.y - 8).toFixed(1);
        return `
            <circle cx="${wp.x.toFixed(1)}" cy="${wp.y.toFixed(1)}" r="4.5" fill="#7fd8ff" stroke="#ffffff" stroke-width="1.5" />
            <text x="${textX}" y="${textY}" fill="#d8f4ff" font-size="11" font-family="Arial">${escapeHtml(wp.icon || '')}${escapeHtml(wp.label || `WP ${idx + 1}`)}</text>
        `;
    }).join('');

    const northX = width - 52;
    const northY = 30;

    const svg = `
        <svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">
            <rect x="0" y="0" width="${width}" height="${height}" rx="16" ry="16" fill="#0e1620"/>
            <rect x="${pad}" y="${pad}" width="${innerW}" height="${innerH}" rx="10" ry="10" fill="#102434" stroke="#2a4d67" stroke-width="1.2"/>
            ${gridLines.join('')}
            <path d="${pathD}" fill="none" stroke="#ff9a3a" stroke-width="4" stroke-linecap="round" stroke-linejoin="round"/>
            ${waypointNodes}
            <circle cx="${start.x.toFixed(1)}" cy="${start.y.toFixed(1)}" r="6" fill="#37d67a" stroke="#ffffff" stroke-width="2"/>
            <circle cx="${end.x.toFixed(1)}" cy="${end.y.toFixed(1)}" r="6" fill="#ff5f6d" stroke="#ffffff" stroke-width="2"/>
            <text x="${Math.min(width - 90, start.x + 10).toFixed(1)}" y="${Math.max(18, start.y - 10).toFixed(1)}" fill="#bde9ff" font-size="13" font-family="Arial">${t('Départ', 'Salida')}</text>
            <text x="${Math.min(width - 90, end.x + 10).toFixed(1)}" y="${Math.max(18, end.y - 10).toFixed(1)}" fill="#ffc8ce" font-size="13" font-family="Arial">${t('Arrivée', 'Llegada')}</text>
            <circle cx="${northX}" cy="${northY}" r="16" fill="#102434" stroke="#7fd8ff" stroke-width="1.2"/>
            <path d="M ${northX} ${northY - 10} L ${northX - 5} ${northY + 4} L ${northX} ${northY + 1} L ${northX + 5} ${northY + 4} Z" fill="#7fd8ff"/>
            <text x="${northX}" y="${northY + 13}" text-anchor="middle" fill="#d8f4ff" font-size="10" font-family="Arial" font-weight="700">N</text>
        </svg>`;

    return `data:image/svg+xml;base64,${btoa(unescape(encodeURIComponent(svg)))}`;
}

async function captureMapImageDataUrl() {
    if (!map || typeof window.html2canvas !== 'function') return null;

    let previousCenter = null;
    let previousZoom = null;
    let previousBaseLayer = null;

    try {
        previousCenter = map.getCenter();
        previousZoom = map.getZoom();
        previousBaseLayer = activeBaseLayer;

        if (activeBaseLayer !== standardTileLayer) {
            if (activeBaseLayer && map.hasLayer(activeBaseLayer)) map.removeLayer(activeBaseLayer);
            if (!map.hasLayer(standardTileLayer)) map.addLayer(standardTileLayer);
            activeBaseLayer = standardTileLayer;
        }

        if (lastRouteBounds?.isValid?.()) {
            map.fitBounds(lastRouteBounds, { padding: [40, 40], animate: false });
        }

        map.invalidateSize(false);
        await new Promise(resolve => setTimeout(resolve, 380));

        const canvas = await window.html2canvas(map.getContainer(), {
            useCORS: true,
            allowTaint: false,
            backgroundColor: '#0e1620',
            scale: 2,
            logging: false,
            width: map.getSize().x,
            height: map.getSize().y
        });

        return canvas.toDataURL('image/jpeg', 0.92);
    } catch (error) {
        return null;
    } finally {
        if (previousBaseLayer && activeBaseLayer !== previousBaseLayer) {
            if (activeBaseLayer && map.hasLayer(activeBaseLayer)) map.removeLayer(activeBaseLayer);
            if (!map.hasLayer(previousBaseLayer)) map.addLayer(previousBaseLayer);
            activeBaseLayer = previousBaseLayer;
        }

        if (previousCenter && Number.isFinite(previousZoom)) {
            map.setView(previousCenter, previousZoom, { animate: false });
            map.invalidateSize(false);
        }
    }
}

function buildVoyageReportHtml(data, mapImageDataUrl, vectorImageDataUrl) {
    const metrics = data?.metrics || {};
    const routeName = escapeHtml(data?.routeName || t('Route', 'Ruta'));
    const computedAt = escapeHtml(formatUtcDateTime(data?.computedAt));
    const departure = escapeHtml(formatUtcDateTime(data?.departureIso));
    const arrival = escapeHtml(formatUtcDateTime(data?.arrivalIso));
    const weatherUpdatedAt = escapeHtml(formatUtcDateTime(data?.weatherUpdatedAt));

    const segmentRows = (data?.segments || []).map(seg => `
        <tr>
            <td>${escapeHtml(seg.startIcon || '')}</td>
            <td>${escapeHtml(seg.number)}</td>
            <td>${escapeHtml(seg.startLabel || '')}</td>
            <td>${escapeHtml(seg.departureLabel || '')}</td>
            <td>${escapeHtml(seg.arrivalLabel || '')}</td>
            <td>${escapeHtml(seg.bearing)}°</td>
            <td>${escapeHtml(seg.distance)} nm</td>
            <td>${escapeHtml(seg.time)} h</td>
            <td>${escapeHtml(seg.speed)} kn</td>
            <td>${escapeHtml(seg.sailSetup || '')}</td>
        </tr>
    `).join('');

    const waypointRows = (data?.waypoints || []).map(wp => `
        <tr>
            <td>${escapeHtml(wp.label)}</td>
            <td>${escapeHtml(wp.passageLabel || '')}</td>
            <td>${escapeHtml(wp.windSpeed)}</td>
            <td>${escapeHtml(wp.windDirection)}</td>
            <td>${escapeHtml(wp.pressure)}</td>
            <td>${escapeHtml(wp.waveHeight)}</td>
            <td>${escapeHtml(wp.summary)}</td>
        </tr>
    `).join('');

    const mapSection = mapImageDataUrl
        ? `<img class="map-image" src="${mapImageDataUrl}" alt="${t('Carte de navigation', 'Mapa de navegación')}" />`
        : `<div class="map-placeholder">${t('Carte indisponible', 'Mapa no disponible')}</div>`;

    const vectorSection = vectorImageDataUrl
        ? `<img class="map-image" src="${vectorImageDataUrl}" alt="${t('Tracé 2D', 'Trazado 2D')}" />`
        : `<div class="map-placeholder">${t('Tracé 2D indisponible', 'Trazado 2D no disponible')}</div>`;

    return `
    <div class="pdf-report">
        <header class="hero">
            <div>
                <h1>${t('Carnet de Voyage', 'Cuaderno de Viaje')}</h1>
                <h2>${routeName}</h2>
                <p>${t('Généré le', 'Generado el')} ${computedAt}</p>
            </div>
            <div class="hero-meta">
                <div><strong>${t('Départ', 'Salida')}</strong><span>${departure}</span></div>
                <div><strong>${t('Arrivée', 'Llegada')}</strong><span>${arrival}</span></div>
                <div><strong>${t('Météo MAJ', 'Meteo ACT')}</strong><span>${weatherUpdatedAt}</span></div>
            </div>
        </header>

        <section class="cards">
            <article><span>${t('Distance', 'Distancia')}</span><strong>${escapeHtml(metrics.totalDistanceNm)} nm</strong></article>
            <article><span>${t('Durée', 'Duración')}</span><strong>${escapeHtml(metrics.totalTimeLabel)}</strong></article>
            <article><span>${t('Segments', 'Segmentos')}</span><strong>${escapeHtml(metrics.segmentCount)}</strong></article>
            <article><span>${t('WP auto', 'WP auto')}</span><strong>${escapeHtml(metrics.generatedWaypointCount)}</strong></article>
        </section>

        <section>
            <h3>${t('Carte de navigation', 'Mapa de navegación')}</h3>
            ${mapSection}
        </section>

        <section>
            <h3>${t('Tracé de navigation (2D)', 'Trazado de navegación (2D)')}</h3>
            ${vectorSection}
        </section>

        <section>
            <h3>${t('Segments', 'Segmentos')}</h3>
            <table>
                <thead>
                    <tr><th>WP</th><th>Seg</th><th>${t('Nom', 'Nombre')}</th><th>${t('Départ', 'Salida')}</th><th>${t('Arrivée', 'Llegada')}</th><th>${t('Cap', 'Rumbo')}</th><th>${t('Dist', 'Dist')}</th><th>${t('Temps', 'Tiempo')}</th><th>${t('Vit', 'Vel')}</th><th>${t('Voiles', 'Velas')}</th></tr>
                </thead>
                <tbody>${segmentRows}</tbody>
            </table>
        </section>

        <section>
            <h3>${t('Météo aux waypoints', 'Meteo en waypoints')}</h3>
            <table>
                <thead>
                    <tr><th>WP</th><th>${t('Passage', 'Paso')}</th><th>${t('Vent', 'Viento')}</th><th>${t('Dir', 'Dir')}</th><th>${t('Pression', 'Presión')}</th><th>${t('Houle', 'Oleaje')}</th><th>${t('Résumé', 'Resumen')}</th></tr>
                </thead>
                <tbody>${waypointRows}</tbody>
            </table>
        </section>
    </div>`;
}

function createReportStyles() {
    return `
        <style>
            body { margin:0; background:#fff; font-family: Inter, Segoe UI, Arial, sans-serif; color:#0d2233; }
            .pdf-report { width: 790px; margin: 0 auto; padding: 20px 24px 32px; box-sizing: border-box; }
            .hero { display:flex; justify-content:space-between; align-items:flex-start; background:linear-gradient(135deg,#0e2134,#19415f); color:#fff; border-radius:14px; padding:18px 20px; }
            .hero h1 { margin:0; font-size:30px; }
            .hero h2 { margin:4px 0 6px; font-size:18px; font-weight:600; color:#9edcff; }
            .hero p { margin:0; font-size:12px; opacity:.85; }
            .hero-meta { display:grid; gap:8px; min-width:210px; }
            .hero-meta div { display:flex; justify-content:space-between; gap:12px; font-size:12px; }
            .hero-meta strong { color:#9edcff; }
            h3 { margin:18px 0 8px; font-size:16px; color:#14324a; }
            .cards { display:grid; grid-template-columns: repeat(4,1fr); gap:10px; margin-top:12px; }
            .cards article { border:1px solid #d8e6f0; border-radius:10px; padding:10px; background:#f7fbff; }
            .cards span { display:block; font-size:11px; color:#48647c; }
            .cards strong { display:block; margin-top:4px; font-size:20px; color:#0f3048; }
            .map-image { width:100%; max-height:250px; object-fit:contain; background:#0e1620; border-radius:12px; border:1px solid #d2e0ea; display:block; }
            .map-placeholder { border:1px dashed #9bb3c6; border-radius:12px; padding:28px; text-align:center; color:#4f6a80; background:#f7fbff; }
            table { width:100%; border-collapse:collapse; font-size:11px; }
            th, td { border-bottom:1px solid #e0e9f0; padding:6px 5px; text-align:left; vertical-align:top; }
            th { background:#f3f8fc; color:#23465f; font-weight:600; }
            tr:nth-child(even) td { background:#fbfdff; }
        </style>
    `;
}

async function exportVoyagePdfReport() {
    if (!lastComputedReportData) {
        alert(t('Calcule une route avant d\'exporter le rapport PDF.', 'Calcula una ruta antes de exportar el informe PDF.'));
        return;
    }

    if (!window?.jspdf?.jsPDF) {
        alert(t('jsPDF non disponible dans le navigateur.', 'jsPDF no disponible en el navegador.'));
        return;
    }

    const button = document.getElementById('exportVoyagePdfBtn');
    if (button) {
        button.disabled = true;
        button.textContent = t('Génération PDF...', 'Generación PDF...');
    }

    try {
        const mapImageDataUrl = await captureMapImageDataUrl();
        const vectorDataUrl = buildRouteVectorDataUrl(lastComputedReportData);
        const reportHtml = buildVoyageReportHtml(lastComputedReportData, mapImageDataUrl, vectorDataUrl);
        const wrapper = document.createElement('div');
        wrapper.style.position = 'fixed';
        wrapper.style.left = '-10000px';
        wrapper.style.top = '0';
        wrapper.style.width = '790px';
        wrapper.style.background = '#fff';
        wrapper.innerHTML = `${createReportStyles()}${reportHtml}`;
        document.body.appendChild(wrapper);

        const canvas = await window.html2canvas(wrapper, {
            useCORS: true,
            allowTaint: false,
            backgroundColor: '#ffffff',
            scale: 2
        });
        document.body.removeChild(wrapper);

        const { jsPDF } = window.jspdf;
        const pdf = new jsPDF('p', 'mm', 'a4');

        const pageWidth = pdf.internal.pageSize.getWidth();
        const pageHeight = pdf.internal.pageSize.getHeight();
        const imgData = canvas.toDataURL('image/jpeg', 0.95);
        const imgWidth = pageWidth;
        const imgHeight = (canvas.height * imgWidth) / canvas.width;

        let heightLeft = imgHeight;
        let position = 0;
        pdf.addImage(imgData, 'JPEG', 0, position, imgWidth, imgHeight);
        heightLeft -= pageHeight;

        while (heightLeft > 0) {
            position = heightLeft - imgHeight;
            pdf.addPage();
            pdf.addImage(imgData, 'JPEG', 0, position, imgWidth, imgHeight);
            heightLeft -= pageHeight;
        }

        const safeName = (lastComputedReportData.routeName || 'voyage').replace(/[^a-z0-9\-_]/gi, '_');
        const dateTag = new Date().toISOString().slice(0, 10);
        pdf.save(`rapport_${safeName}_${dateTag}.pdf`);
    } catch (error) {
        alert(t('Impossible de générer le PDF.', 'No se puede generar el PDF.'));
    } finally {
        if (button) {
            button.disabled = false;
            button.textContent = t('Exporter rapport PDF', 'Exportar informe PDF');
        }
    }
}

function getSeaComfortLevel(weather) {
    const waveHeight = weather?.waveHeight;
    const windSpeed = weather?.windSpeed;

    if (Number.isFinite(waveHeight)) {
        if (waveHeight < 0.7) return t('Confort: calme', 'Confort: calma');
        if (waveHeight < 1.5) return t('Confort: modéré', 'Confort: moderado');
        if (waveHeight < 2.5) return t('Confort: agité', 'Confort: agitado');
        return t('Confort: difficile', 'Confort: difícil');
    }

    if (Number.isFinite(windSpeed)) {
        if (windSpeed < 10) return t('Confort: calme', 'Confort: calma');
        if (windSpeed < 18) return t('Confort: modéré', 'Confort: moderado');
        if (windSpeed < 25) return t('Confort: agité', 'Confort: agitado');
        return t('Confort: difficile', 'Confort: difícil');
    }

    return t('Confort: N/A', 'Confort: N/A');
}

function getSailRecommendation({ isMotorSegment, tws, twa, sailModeValue }) {
    if (isMotorSegment) return t('Moteur', 'Motor');

    const prudentOffset = sailModeValue === 'prudent' ? -2 : 0;
    const perfOffset = sailModeValue === 'performance' ? 2 : 0;

    if (tws >= (22 + prudentOffset)) return t('GV 2 ris + trinquette', 'Mayor 2 rizos + trinqueta');
    if (tws >= (16 + prudentOffset)) {
        if (twa > 130 && sailModeValue === 'performance') return t('GV 1 ris + spi', 'Mayor 1 rizo + spi');
        return t('GV 1 ris + génois réduit', 'Mayor 1 rizo + génova reducida');
    }

    if (twa < 60) return sailModeValue === 'prudent'
        ? t('GV pleine + génois réduit', 'Mayor completa + génova reducida')
        : t('GV pleine + génois', 'Mayor completa + génova');
    if (twa < 115) return t('GV pleine + génois', 'Mayor completa + génova');
    if (twa < 145) return sailModeValue === 'prudent'
        ? t('GV + génois tangonné', 'Mayor + génova tangonada')
        : t('GV + gennaker', 'Mayor + gennaker');

    if (sailModeValue === 'performance' && tws < (18 + perfOffset)) return t('GV + spi', 'Mayor + spi');
    return t('GV + génois tangonné', 'Mayor + génova tangonada');
}

function getSailPerformanceFactor({ isMotorSegment, sailModeValue, tws, twa, sailSetup }) {
    if (isMotorSegment) return 1;

    let baseFactor = 1;
    if (sailModeValue === 'prudent') {
        baseFactor = tws >= 24 ? 0.88 : 0.92;
    } else if (sailModeValue === 'performance') {
        baseFactor = tws >= 24 ? 1.0 : 1.08;
    }

    const isGennakerSetup = typeof sailSetup === 'string' && sailSetup.toLowerCase().includes('gennaker');
    const gennakerBoost = isGennakerSetup && Number.isFinite(twa) && twa >= 95 && twa <= 150 ? 1.1 : 1;

    return baseFactor * gennakerBoost;
}

function getSailComment({ sailSetup, sailModeValue, tws, twa, isMotorSegment }) {
    if (isMotorSegment) return t('Vent faible (< 5 kn) : passage au moteur à 7 kn.', 'Viento flojo (< 5 kn): paso a motor a 7 kn.');

    const twaText = Number.isFinite(twa) ? `${Math.round(twa)}°` : 'N/A';
    const twsText = Number.isFinite(tws) ? `${tws.toFixed(1)} kn` : 'N/A';
    const modeLabel = sailModeValue === 'prudent'
        ? t('Prudent', 'Prudente')
        : (sailModeValue === 'performance' ? t('Performance', 'Rendimiento') : t('Auto', 'Auto'));

    return `${t('Mode', 'Modo')} ${modeLabel} · TWS ${twsText} · TWA ${twaText} → ${sailSetup}`;
}

async function ensureLandGeometryLoaded() {
    if (landGeometry) return landGeometry;

    try {
        for (const source of LAND_DATA_SOURCES) {
            const response = await fetch(source.url);
            if (!response.ok) continue;

            const topo = await response.json();
            if (!topo?.objects?.[source.objectName]) continue;

            landGeometry = topojsonFeature(topo, topo.objects[source.objectName]);
            if (landGeometry) return landGeometry;
        }

        throw new Error('No usable land geometry source');
    } catch (error) {
        console.warn('Chargement des données côtières impossible, contournement désactivé pour ce calcul.', error);
        return null;
    }
}

function pointInRing(point, ring) {
    let inside = false;
    for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
        const xi = ring[i][0], yi = ring[i][1];
        const xj = ring[j][0], yj = ring[j][1];

        const intersects = ((yi > point[1]) !== (yj > point[1])) &&
            (point[0] < ((xj - xi) * (point[1] - yi)) / ((yj - yi) || 1e-12) + xi);

        if (intersects) inside = !inside;
    }
    return inside;
}

function pointInPolygonGeometry(lon, lat, geometry) {
    if (!geometry || (geometry.type !== 'Polygon' && geometry.type !== 'MultiPolygon')) return false;

    const testPoint = [lon, lat];
    const polygons = geometry.type === 'Polygon' ? [geometry.coordinates] : geometry.coordinates;

    for (const poly of polygons) {
        const outer = poly[0];
        if (!pointInRing(testPoint, outer)) continue;

        let inHole = false;
        for (let h = 1; h < poly.length; h++) {
            if (pointInRing(testPoint, poly[h])) {
                inHole = true;
                break;
            }
        }
        if (!inHole) return true;
    }

    return false;
}

function geometryContainsPoint(lon, lat, geometry) {
    if (!geometry) return false;

    if (geometry.type === 'Polygon' || geometry.type === 'MultiPolygon') {
        return pointInPolygonGeometry(lon, lat, geometry);
    }

    if (geometry.type === 'GeometryCollection' && Array.isArray(geometry.geometries)) {
        return geometry.geometries.some(g => geometryContainsPoint(lon, lat, g));
    }

    return false;
}

function isPointOnLand(lat, lon) {
    if (!landGeometry) return false;

    if (landGeometry.type === 'Feature') {
        return geometryContainsPoint(lon, lat, landGeometry.geometry);
    }

    if (landGeometry.type === 'FeatureCollection' && Array.isArray(landGeometry.features)) {
        return landGeometry.features.some(feature => geometryContainsPoint(lon, lat, feature?.geometry));
    }

    return geometryContainsPoint(lon, lat, landGeometry);
}

function segmentCrossesLand(start, end, samples = null) {
    const legDistance = distanceNm(start.lat, start.lon, end.lat, end.lon);
    const sampleCount = Number.isFinite(samples)
        ? Math.max(12, Math.floor(samples))
        : Math.max(48, Math.min(720, Math.ceil(legDistance * 8)));

    for (let i = 0; i <= sampleCount; i++) {
        const t = i / sampleCount;
        const lat = start.lat + (end.lat - start.lat) * t;
        const lon = start.lon + (end.lon - start.lon) * t;
        if (isPointOnLand(lat, lon)) return true;
    }
    return false;
}

function polylineCrossesLand(points) {
    for (let i = 0; i < points.length - 1; i++) {
        if (segmentCrossesLand(points[i], points[i + 1])) return true;
    }
    return false;
}

function polylineLatLngCrossesLand(latlngs) {
    if (!Array.isArray(latlngs) || latlngs.length < 2) return false;

    for (let i = 0; i < latlngs.length - 1; i++) {
        const start = { lat: latlngs[i][0], lon: latlngs[i][1] };
        const end = { lat: latlngs[i + 1][0], lon: latlngs[i + 1][1] };
        if (segmentCrossesLand(start, end)) return true;
    }

    return false;
}

function polylineDistanceNm(points) {
    let total = 0;
    for (let i = 0; i < points.length - 1; i++) {
        total += distanceNm(points[i].lat, points[i].lon, points[i + 1].lat, points[i + 1].lon);
    }
    return total;
}

function compressGeneratedWaypoints(points, minSpacingNm = AUTO_WP_MIN_SPACING_NM, maxIntermediate = AUTO_WP_MAX_INTERMEDIATE) {
    if (!Array.isArray(points) || points.length <= 2) return points;

    const result = [points[0]];
    let lastKept = points[0];

    for (let i = 1; i < points.length - 1; i++) {
        const point = points[i];
        const spacing = distanceNm(lastKept.lat, lastKept.lon, point.lat, point.lon);
        if (spacing >= minSpacingNm) {
            result.push(point);
            lastKept = point;
        }
    }

    result.push(points[points.length - 1]);

    if (result.length - 2 <= maxIntermediate) {
        return result;
    }

    const compressed = [result[0]];
    const intermediate = result.slice(1, -1);
    const step = Math.ceil(intermediate.length / maxIntermediate);

    for (let i = 0; i < intermediate.length; i += step) {
        compressed.push(intermediate[i]);
    }

    compressed.push(result[result.length - 1]);
    return compressed;
}

async function buildCoastalBypassWaypoints(startPoint, endPoint, baseClearanceNm = COASTAL_CLEARANCE_NM, minSpacingNm = AUTO_WP_MIN_SPACING_NM) {
    const geometry = await ensureLandGeometryLoaded();
    if (!geometry) {
        return [startPoint, endPoint];
    }

    if (!segmentCrossesLand(startPoint, endPoint)) {
        return [startPoint, endPoint];
    }

    const directBearing = getBearing(startPoint, endPoint);
    const mid = {
        lat: (startPoint.lat + endPoint.lat) / 2,
        lon: (startPoint.lon + endPoint.lon) / 2
    };

    const candidateRoutes = [];
    const clearanceSteps = [baseClearanceNm, 8, 12, 18, 26, 36, 50, 70, 90];

    for (const clearance of clearanceSteps) {
        for (const side of [-1, 1]) {
            const candidateMid = movePoint(mid.lat, mid.lon, directBearing + side * 90, clearance);
            const oneWpPath = [
                startPoint,
                { lat: candidateMid.lat, lon: candidateMid.lon },
                endPoint
            ];

            if (!polylineCrossesLand(oneWpPath)) {
                candidateRoutes.push({
                    path: oneWpPath,
                    clearance,
                    wpCount: 1
                });
            }

            const firstThird = {
                lat: startPoint.lat + (endPoint.lat - startPoint.lat) / 3,
                lon: startPoint.lon + (endPoint.lon - startPoint.lon) / 3
            };
            const secondThird = {
                lat: startPoint.lat + 2 * (endPoint.lat - startPoint.lat) / 3,
                lon: startPoint.lon + 2 * (endPoint.lon - startPoint.lon) / 3
            };

            const wp1 = movePoint(firstThird.lat, firstThird.lon, directBearing + side * 90, clearance);
            const wp2 = movePoint(secondThird.lat, secondThird.lon, directBearing + side * 90, clearance);

            const twoWpPath = [
                startPoint,
                { lat: wp1.lat, lon: wp1.lon },
                { lat: wp2.lat, lon: wp2.lon },
                endPoint
            ];

            if (!polylineCrossesLand(twoWpPath)) {
                candidateRoutes.push({
                    path: twoWpPath,
                    clearance,
                    wpCount: 2
                });
            }
        }
    }

    if (candidateRoutes.length === 0) return null;

    function candidateScore(candidate) {
        const distanceScore = polylineDistanceNm(candidate.path);
        const clearancePenalty = Math.abs(candidate.clearance - baseClearanceNm) * 3;
        const waypointPenalty = candidate.wpCount > 1 ? 0.35 : 0;
        return distanceScore + clearancePenalty + waypointPenalty;
    }

    candidateRoutes.sort((a, b) => {
        const scoreDiff = candidateScore(a) - candidateScore(b);
        if (Math.abs(scoreDiff) > 1e-6) return scoreDiff;
        return polylineDistanceNm(a.path) - polylineDistanceNm(b.path);
    });

    return compressGeneratedWaypoints(candidateRoutes[0].path, minSpacingNm, AUTO_WP_MAX_INTERMEDIATE);
}

function getMeasureTotalNm(points) {
    if (!Array.isArray(points) || points.length < 2) return 0;
    let total = 0;
    for (let i = 0; i < points.length - 1; i++) {
        total += distanceNm(points[i].lat, points[i].lng, points[i + 1].lat, points[i + 1].lng);
    }
    return total;
}

function updateMeasureInfo() {
    const info = document.getElementById('measureInfo');
    if (!info) return;
    const total = getMeasureTotalNm(measurePoints);
    info.textContent = t(
        `Mesure: ${total.toFixed(2)} nm`,
        `Medición: ${total.toFixed(2)} nm`
    );
}

function clearMeasureLabels() {
    measureLabelLayers.forEach(layer => {
        if (map.hasLayer(layer)) map.removeLayer(layer);
    });
    measureLabelLayers = [];
}

function redrawMeasurePolylineAndLabels() {
    if (!map) return;

    if (measurePolylineLayer && map.hasLayer(measurePolylineLayer)) {
        map.removeLayer(measurePolylineLayer);
    }
    clearMeasureLabels();

    if (measurePoints.length >= 2) {
        measurePolylineLayer = L.polyline(measurePoints, {
            color: '#8fe7ff',
            weight: 3,
            opacity: 0.95,
            dashArray: '8,6'
        }).addTo(map);

        for (let i = 0; i < measurePoints.length - 1; i++) {
            const a = measurePoints[i];
            const b = measurePoints[i + 1];
            const segmentNm = distanceNm(a.lat, a.lng, b.lat, b.lng);

            const mid = {
                lat: (a.lat + b.lat) / 2,
                lng: (a.lng + b.lng) / 2
            };

            const label = L.marker([mid.lat, mid.lng], {
                icon: L.divIcon({
                    className: 'measure-distance-label',
                    html: `${segmentNm.toFixed(2)} nm`,
                    iconSize: null
                }),
                interactive: false,
                keyboard: false,
                zIndexOffset: 1200
            }).addTo(map);

            measureLabelLayers.push(label);
        }
    } else {
        measurePolylineLayer = null;
    }

    updateMeasureInfo();
}

function clearMeasureTool() {
    if (measurePolylineLayer && map?.hasLayer(measurePolylineLayer)) {
        map.removeLayer(measurePolylineLayer);
    }
    measurePolylineLayer = null;

    measurePointLayers.forEach(layer => {
        if (map?.hasLayer(layer)) map.removeLayer(layer);
    });
    measurePointLayers = [];

    clearMeasureLabels();
    measurePoints = [];
    updateMeasureInfo();
}

function addMeasurePoint(latlng) {
    measurePoints.push({ lat: latlng.lat, lng: latlng.lng });

    const pointLayer = L.circleMarker(latlng, {
        radius: 4,
        color: '#8fe7ff',
        weight: 2,
        fillColor: '#133341',
        fillOpacity: 0.9
    }).addTo(map);

    measurePointLayers.push(pointLayer);
    redrawMeasurePolylineAndLabels();
}

function setMeasureMode(enabled) {
    measureModeEnabled = Boolean(enabled);

    const btn = document.getElementById('measureToggleBtn');
    if (btn) {
        btn.textContent = t(
            `Mesure NM: ${measureModeEnabled ? 'ON' : 'OFF'}`,
            `Medición NM: ${measureModeEnabled ? 'ON' : 'OFF'}`
        );
        btn.classList.toggle('active', measureModeEnabled);
    }

    if (map) {
        const shouldUseCrosshair = measureModeEnabled || (weatherPointerPlacementMode && activeTabName === 'weather');
        map.getContainer().style.cursor = shouldUseCrosshair ? 'crosshair' : '';
    }
}

function createNauticalScaleControl() {
    if (!map) return;

    const nauticalScaleControl = L.control({ position: 'bottomleft' });

    nauticalScaleControl.onAdd = function() {
        const container = L.DomUtil.create('div', 'nautical-scale-control');
        container.innerHTML =
            '<div class="nautical-scale-label">-- nm</div>' +
            '<div class="nautical-scale-bar"></div>';
        L.DomEvent.disableClickPropagation(container);
        return container;
    };

    nauticalScaleControl.addTo(map);

    const niceStepsNm = [0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500];

    function updateNauticalScale() {
        const container = nauticalScaleControl.getContainer();
        if (!container || !map) return;

        const label = container.querySelector('.nautical-scale-label');
        const bar = container.querySelector('.nautical-scale-bar');
        if (!label || !bar) return;

        const mapSize = map.getSize();
        const probePx = Math.max(90, Math.min(160, Math.floor(mapSize.x * 0.18)));
        const centerY = Math.floor(mapSize.y / 2);
        const leftX = Math.floor((mapSize.x - probePx) / 2);

        const p1 = L.point(leftX, centerY);
        const p2 = L.point(leftX + probePx, centerY);
        const ll1 = map.containerPointToLatLng(p1);
        const ll2 = map.containerPointToLatLng(p2);

        const maxNm = distanceNm(ll1.lat, ll1.lng, ll2.lat, ll2.lng);
        if (!Number.isFinite(maxNm) || maxNm <= 0) {
            label.textContent = 'N/A';
            bar.style.width = '0px';
            return;
        }

        let chosenNm = niceStepsNm[0];
        for (const step of niceStepsNm) {
            if (step <= maxNm) chosenNm = step;
            else break;
        }

        const widthPx = Math.max(20, Math.round((chosenNm / maxNm) * probePx));
        label.textContent = `${chosenNm} nm`;
        bar.style.width = `${widthPx}px`;
    }

    map.on('zoom move', updateNauticalScale);
    updateNauticalScale();
}

function formatDateTimeFr(dateInput) {
    const dateObj = new Date(dateInput);
    if (Number.isNaN(dateObj.getTime())) return 'N/A';
    return dateObj.toLocaleString(getCurrentLocale(), {
        day: '2-digit',
        month: '2-digit',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
    });
}

function loadArrayFromStorage(storageKey) {
    try {
        const raw = localStorage.getItem(storageKey);
        const parsed = raw ? JSON.parse(raw) : [];
        return Array.isArray(parsed) ? parsed : [];
    } catch (_error) {
        return [];
    }
}

function saveArrayToStorage(storageKey, data) {
    const safeArray = Array.isArray(data) ? data : [];
    localStorage.setItem(storageKey, JSON.stringify(safeArray));
}

function getMaintenanceColorMeta(rawValue) {
    const normalized = String(rawValue || '').trim().toLowerCase();
    if (MAINTENANCE_COLORS[normalized]) return MAINTENANCE_COLORS[normalized];

    const byHex = Object.values(MAINTENANCE_COLORS).find(color => color.hex.toLowerCase() === normalized);
    if (byHex) return byHex;

    return MAINTENANCE_COLORS.blue;
}

function updateMaintenanceSchemaManagerToggleText() {
    const toggleBtn = document.getElementById('maintenanceToggleSchemaManagerBtn');
    if (!toggleBtn) return;

    toggleBtn.textContent = maintenanceSchemaManagerVisible
        ? t('Masquer gestion des schémas', 'Ocultar gestión de esquemas')
        : t('Gérer les schémas', 'Gestionar esquemas');
}

function setMaintenanceSchemaManagerVisibility(visible) {
    maintenanceSchemaManagerVisible = !!visible;
    const manager = document.getElementById('maintenanceSchemaManager');
    if (manager) {
        manager.style.display = maintenanceSchemaManagerVisible ? '' : 'none';
    }
    updateMaintenanceSchemaManagerToggleText();
}

function normalizeMaintenanceTaskStatus(value) {
    const key = String(value || '').trim().toLowerCase();
    return MAINTENANCE_TASK_STATUSES[key] ? key : 'active';
}

function getMaintenanceTaskStatusMeta(value) {
    return MAINTENANCE_TASK_STATUSES[normalizeMaintenanceTaskStatus(value)] || MAINTENANCE_TASK_STATUSES.active;
}

function toFiniteAmount(value) {
    const num = Number(value);
    if (!Number.isFinite(num)) return 0;
    return Math.max(0, Math.round(num * 100) / 100);
}

function parseExpenseLinesText(raw) {
    const text = String(raw || '');
    return text
        .split(/\r?\n/)
        .map(line => line.trim())
        .filter(Boolean)
        .map((line, index) => {
            const parts = line.split(';').map(part => part.trim());
            return {
                id: `expense-line-${Date.now()}-${index}`,
                label: String(parts[0] || line),
                quantity: parts[1] ? toFiniteAmount(parts[1].replace(',', '.')) : null,
                unitPrice: parts[2] ? toFiniteAmount(parts[2].replace(',', '.')) : null,
                total: parts[3] ? toFiniteAmount(parts[3].replace(',', '.')) : null
            };
        });
}

function sanitizeMaintenanceExpense(entry, fallbackIndex = 0) {
    const rawLines = Array.isArray(entry?.lines) ? entry.lines : [];
    const lines = rawLines
        .map((line, lineIndex) => ({
            id: String(line?.id || `expense-line-${Date.now()}-${lineIndex}`),
            label: String(line?.label || '').trim(),
            quantity: line?.quantity == null ? null : toFiniteAmount(line.quantity),
            unitPrice: line?.unitPrice == null ? null : toFiniteAmount(line.unitPrice),
            total: line?.total == null ? null : toFiniteAmount(line.total)
        }))
        .filter(line => line.label);

    return {
        id: String(entry?.id || `expense-${Date.now()}-${fallbackIndex}`),
        invoiceName: String(entry?.invoiceName || '').trim(),
        date: String(entry?.date || new Date().toISOString().slice(0, 10)),
        supplierName: String(entry?.supplierName || '').trim(),
        supplierIban: String(entry?.supplierIban || '').trim(),
        payer: String(entry?.payer || 'PATISSIER').toUpperCase() === 'KLENIK' ? 'KLENIK' : 'PATISSIER',
        paymentStatus: ['pending', 'partial', 'paid'].includes(String(entry?.paymentStatus || 'pending')) ? String(entry?.paymentStatus) : 'pending',
        totalAmount: toFiniteAmount(entry?.totalAmount),
        currency: String(entry?.currency || 'EUR').trim().toUpperCase() || 'EUR',
        lines,
        note: String(entry?.note || '').trim(),
        aiComment: String(entry?.aiComment || '').trim(),
        scannedText: String(entry?.scannedText || ''),
        createdAt: String(entry?.createdAt || new Date().toISOString()),
        updatedAt: String(entry?.updatedAt || new Date().toISOString())
    };
}

function sanitizeMaintenanceSupplier(entry, fallbackIndex = 0) {
    return {
        id: String(entry?.id || `supplier-${Date.now()}-${fallbackIndex}`),
        name: String(entry?.name || '').trim(),
        contact: String(entry?.contact || '').trim(),
        emergencyPhone: String(entry?.emergencyPhone || '').trim(),
        iban: String(entry?.iban || '').trim(),
        note: String(entry?.note || '').trim(),
        createdAt: String(entry?.createdAt || new Date().toISOString()),
        updatedAt: String(entry?.updatedAt || new Date().toISOString())
    };
}

function loadMaintenanceExpenses() {
    setMaintenanceExpenses(loadArrayFromStorage(MAINTENANCE_EXPENSES_STORAGE_KEY), { persistLocal: false, refreshUi: false, syncCloud: false });
}

function setMaintenanceExpenses(list, { persistLocal = true, refreshUi = true, syncCloud = false } = {}) {
    maintenanceExpenses = (Array.isArray(list) ? list : [])
        .map((entry, index) => sanitizeMaintenanceExpense(entry, index))
        .sort((a, b) => String(b.date || '').localeCompare(String(a.date || '')));

    if (persistLocal) {
        saveArrayToStorage(MAINTENANCE_EXPENSES_STORAGE_KEY, maintenanceExpenses);
    }
    if (syncCloud && isCloudReady()) {
        pushRoutesToCloud().catch(() => null);
    }
    if (refreshUi) {
        renderMaintenanceExpenses();
    }
}

function persistMaintenanceExpenses({ syncCloud = true } = {}) {
    saveArrayToStorage(MAINTENANCE_EXPENSES_STORAGE_KEY, maintenanceExpenses);
    if (syncCloud && isCloudReady()) {
        pushRoutesToCloud().catch(() => null);
    }
}

function loadMaintenanceSuppliers() {
    setMaintenanceSuppliers(loadArrayFromStorage(MAINTENANCE_SUPPLIERS_STORAGE_KEY), { persistLocal: false, refreshUi: false, syncCloud: false });
}

function setMaintenanceSuppliers(list, { persistLocal = true, refreshUi = true, syncCloud = false } = {}) {
    maintenanceSuppliers = (Array.isArray(list) ? list : [])
        .map((entry, index) => sanitizeMaintenanceSupplier(entry, index))
        .filter(entry => entry.name)
        .sort((a, b) => a.name.localeCompare(b.name));

    if (persistLocal) {
        saveArrayToStorage(MAINTENANCE_SUPPLIERS_STORAGE_KEY, maintenanceSuppliers);
    }
    if (syncCloud && isCloudReady()) {
        pushRoutesToCloud().catch(() => null);
    }
    if (refreshUi) {
        renderMaintenanceSuppliers();
    }
}

function persistMaintenanceSuppliers({ syncCloud = true } = {}) {
    saveArrayToStorage(MAINTENANCE_SUPPLIERS_STORAGE_KEY, maintenanceSuppliers);
    if (syncCloud && isCloudReady()) {
        pushRoutesToCloud().catch(() => null);
    }
}

function renderMaintenanceExpenses() {
    const container = document.getElementById('maintenanceExpensesList');
    if (!container) return;

    if (!maintenanceExpenses.length) {
        container.innerHTML = `<div class="maintenance-legend-empty">${t('Aucune dépense enregistrée.', 'No hay gastos registrados.')}</div>`;
        return;
    }

    container.innerHTML = '';
    maintenanceExpenses.forEach((expense, index) => {
        const card = document.createElement('div');
        card.className = 'maintenance-expense-card';

        const paymentLabelMap = {
            pending: t('À payer', 'Pendiente'),
            partial: t('Partiel', 'Parcial'),
            paid: t('Payé', 'Pagado')
        };

        const linesText = expense.lines.map(line => {
            const details = [line.label];
            if (line.quantity != null) details.push(`x${line.quantity}`);
            if (line.unitPrice != null) details.push(`${line.unitPrice.toFixed(2)}`);
            if (line.total != null) details.push(`= ${line.total.toFixed(2)}`);
            return `<div class="maintenance-expense-line">• ${escapeHtml(details.join(' '))}</div>`;
        }).join('');
        const aiCommentHtml = escapeHtml(expense.aiComment || '').replace(/\n/g, '<br>');

        card.innerHTML =
            `<strong>#${index + 1} · ${escapeHtml(expense.supplierName || t('Fournisseur non renseigné', 'Proveedor no indicado'))}</strong><br>` +
            `${escapeHtml(expense.date)} · ${expense.totalAmount.toFixed(2)} ${escapeHtml(expense.currency)}<br>` +
            `${t('Payeur', 'Pagador')}: ${escapeHtml(expense.payer)} · ${t('Paiement', 'Pago')}: ${escapeHtml(paymentLabelMap[expense.paymentStatus] || expense.paymentStatus)}<br>` +
            `${t('IBAN', 'IBAN')}: ${escapeHtml(expense.supplierIban || '—')}` +
            (expense.invoiceName ? `<br>${t('Facture', 'Factura')}: ${escapeHtml(expense.invoiceName)}` : '') +
            (expense.note ? `<br>${t('Note', 'Nota')}: ${escapeHtml(expense.note)}` : '') +
            (expense.aiComment ? `<br><strong>${t('Commentaire IA', 'Comentario IA')}</strong>:<br>${aiCommentHtml}` : '') +
            (linesText ? `<div style="margin-top:6px;"><strong>${t('Lignes', 'Líneas')}:</strong>${linesText}</div>` : '');

        const actions = document.createElement('div');
        actions.className = 'maintenance-card-actions';
        const deleteBtn = document.createElement('button');
        deleteBtn.type = 'button';
        deleteBtn.className = 'maintenance-delete-btn';
        deleteBtn.textContent = t('Supprimer', 'Eliminar');
        deleteBtn.addEventListener('click', () => {
            maintenanceExpenses = maintenanceExpenses.filter(item => item.id !== expense.id);
            persistMaintenanceExpenses();
            renderMaintenanceExpenses();
        });
        actions.appendChild(deleteBtn);
        card.appendChild(actions);

        container.appendChild(card);
    });
}

function renderMaintenanceSuppliers() {
    const container = document.getElementById('maintenanceSuppliersList');
    if (!container) return;

    if (!maintenanceSuppliers.length) {
        container.innerHTML = `<div class="maintenance-legend-empty">${t('Aucun fournisseur enregistré.', 'Ningún proveedor registrado.')}</div>`;
        return;
    }

    container.innerHTML = '';
    maintenanceSuppliers.forEach((supplier, index) => {
        const card = document.createElement('div');
        card.className = 'maintenance-supplier-card';
        card.innerHTML =
            `<strong>#${index + 1} · ${escapeHtml(supplier.name)}</strong><br>` +
            `${t('Contact', 'Contacto')}: ${escapeHtml(supplier.contact || '—')}<br>` +
            `${t('Urgence', 'Urgencia')}: ${escapeHtml(supplier.emergencyPhone || '—')}<br>` +
            `${t('IBAN', 'IBAN')}: ${escapeHtml(supplier.iban || '—')}` +
            (supplier.note ? `<br>${t('Note', 'Nota')}: ${escapeHtml(supplier.note)}` : '');

        const actions = document.createElement('div');
        actions.className = 'maintenance-card-actions';
        const deleteBtn = document.createElement('button');
        deleteBtn.type = 'button';
        deleteBtn.className = 'maintenance-delete-btn';
        deleteBtn.textContent = t('Supprimer', 'Eliminar');
        deleteBtn.addEventListener('click', () => {
            maintenanceSuppliers = maintenanceSuppliers.filter(item => item.id !== supplier.id);
            persistMaintenanceSuppliers();
            renderMaintenanceSuppliers();
        });
        actions.appendChild(deleteBtn);
        card.appendChild(actions);

        container.appendChild(card);
    });
}

function extractIbanFromText(text) {
    const raw = String(text || '').toUpperCase();
    const lines = raw.split(/\r?\n/).map(line => line.trim()).filter(Boolean);

    const isValidIban = (iban) => {
        const compact = String(iban || '').replace(/\s+/g, '').toUpperCase();
        if (!/^[A-Z]{2}\d{2}[A-Z0-9]{11,30}$/.test(compact)) return false;
        const reordered = `${compact.slice(4)}${compact.slice(0, 4)}`;
        let transformed = '';
        for (const char of reordered) {
            if (/[A-Z]/.test(char)) {
                transformed += String(char.charCodeAt(0) - 55);
            } else {
                transformed += char;
            }
        }

        let remainder = 0;
        for (const digit of transformed) {
            remainder = (remainder * 10 + Number(digit)) % 97;
        }
        return remainder === 1;
    };

    const extractCandidate = (value) => {
        const compact = String(value || '').replace(/[^A-Z0-9]/g, '');
        if (!/^[A-Z]{2}\d{2}[A-Z0-9]{11,30}$/.test(compact)) return '';
        if (compact.length < 15 || compact.length > 34) return '';
        if (!isValidIban(compact)) return '';
        return compact;
    };

    const ibanLine = lines.find(line => line.includes('IBAN'));
    if (ibanLine) {
        const afterLabel = ibanLine.split('IBAN').slice(1).join(' ').replace(/[:\s]+/, ' ').trim();
        const tokenized = afterLabel.match(/[A-Z0-9\s-]{15,45}/g) || [];
        for (const token of tokenized) {
            const iban = extractCandidate(token);
            if (iban) return iban;
        }
    }

    const globalTokens = raw.match(/[A-Z]{2}[\s-]*\d{2}(?:[\s-]*[A-Z0-9]){11,34}/g) || [];
    for (const token of globalTokens) {
        const iban = extractCandidate(token);
        if (iban) return iban;
    }

    return '';
}

function extractTotalFromText(text) {
    const rawText = String(text || '');
    const lines = rawText.split(/\r?\n/).map(line => line.trim()).filter(Boolean);
    const amountPattern = /(?:\d{1,3}(?:[\s.,]\d{3})+|\d+)(?:[.,]\d{2})/g;

    const parseMoney = (token) => {
        const value = String(token || '').replace(/\s/g, '');
        if (!value) return null;

        const lastComma = value.lastIndexOf(',');
        const lastDot = value.lastIndexOf('.');
        let normalized = value;

        if (lastComma !== -1 && lastDot !== -1) {
            if (lastComma > lastDot) {
                normalized = value.replace(/\./g, '').replace(',', '.');
            } else {
                normalized = value.replace(/,/g, '');
            }
        } else if (lastComma !== -1) {
            normalized = /,\d{2}$/.test(value) ? value.replace(/\./g, '').replace(',', '.') : value.replace(/,/g, '');
        } else {
            normalized = /\.\d{2}$/.test(value) ? value.replace(/,/g, '') : value.replace(/\./g, '');
        }

        const num = Number(normalized);
        if (!Number.isFinite(num) || num <= 0 || num > 100000000) return null;
        return num;
    };

    const collectLineAmounts = (line) => {
        const tokens = String(line || '').match(amountPattern) || [];
        return tokens.map(parseMoney).filter(value => Number.isFinite(value));
    };

    const prioritizedPatterns = [
        /\b(total\s*ttc|montant\s*total\s*ttc|grand\s*total|amount\s*due|importe\s*total|a\s*payer|à\s*payer)\b/i,
        /\b(total|montant\s*total|importe\s*total)\b/i
    ];

    for (const pattern of prioritizedPatterns) {
        const line = lines.find(current => pattern.test(current));
        if (!line) continue;
        const amounts = collectLineAmounts(line);
        if (amounts.length) return Math.max(...amounts);
    }

    const allAmounts = lines.flatMap(collectLineAmounts);
    if (!allAmounts.length) return null;
    return Math.max(...allAmounts);
}

function normalizeSupplierText(value) {
    return String(value || '')
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .toLowerCase()
        .replace(/[^a-z0-9\s]/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}

function cleanSupplierCandidateName(value) {
    return String(value || '')
        .replace(/^(fournisseur|supplier|vendor|vendeur|prestataire|societe|société|from)\s*[:\-]?\s*/i, '')
        .replace(/\b(iban|bic|swift|facture|invoice|tva|vat|siret|nif|cif|email|tel|phone)\b.*$/i, '')
        .replace(/\s+/g, ' ')
        .trim();
}

function looksLikeAdministrativeLine(value) {
    const line = String(value || '').trim();
    if (!line) return true;
    if (/@|https?:\/\//i.test(line)) return true;
    if (/\b(iban|bic|swift|facture|invoice|total|montant|amount|date|tva|vat|siret|nif|cif|adresse|address|tel|phone|email|qty|quantite|cantidad)\b/i.test(line)) return true;
    if (/\d{5,}/.test(line)) return true;
    return false;
}

function normalizeIbanValue(value) {
    return String(value || '').replace(/[^A-Za-z0-9]/g, '').toUpperCase();
}

function findMaintenanceSupplierByName(name) {
    const normalized = normalizeSupplierText(name);
    if (!normalized) return null;
    return maintenanceSuppliers.find(item => normalizeSupplierText(item?.name || '') === normalized) || null;
}

function getSupplierCandidatesFromText(text, fileName = '') {
    const candidates = [];
    const seen = new Set();
    const pushCandidate = ({ name = '', confidence = 'low', source = 'heuristic', score = 0 } = {}) => {
        const cleanName = cleanSupplierCandidateName(name);
        const normalized = normalizeSupplierText(cleanName);
        if (!normalized || normalized.length < 3) return;
        if (seen.has(normalized)) return;
        seen.add(normalized);
        candidates.push({ name: cleanName, confidence, source, score });
    };

    const rawLines = String(text || '').split(/\r?\n/).map(line => line.trim()).filter(Boolean);
    const lines = rawLines.length <= 2
        ? String(text || '').split(/\s{2,}|\t+/).map(line => line.trim()).filter(Boolean)
        : rawLines;
    const firstLines = lines.slice(0, 14);

    const normalizedFullText = normalizeSupplierText(text);
    maintenanceSuppliers.forEach(item => {
        const supplierName = String(item?.name || '').trim();
        const normalizedName = normalizeSupplierText(supplierName);
        if (!normalizedName || normalizedName.length < 4) return;
        if (normalizedFullText.includes(normalizedName)) {
            pushCandidate({ name: supplierName, confidence: 'high', source: 'directory', score: 2.5 + Math.min(0.4, normalizedName.length / 100) });
        }
    });

    const labeledLine = firstLines.find(line => /(fournisseur|supplier|vendor|vendeur|prestataire|societe|société|from)\s*[:\-]/i.test(line));
    if (labeledLine) {
        const right = cleanSupplierCandidateName(labeledLine.split(/[:\-]/).slice(1).join(' '));
        if (right && right.length >= 4 && !looksLikeAdministrativeLine(right)) {
            pushCandidate({ name: right, confidence: 'high', source: 'label', score: 2.2 });
        }
    }

    const candidatePool = firstLines
        .map(cleanSupplierCandidateName)
        .filter(line =>
            line.length >= 4 &&
            line.length <= 64 &&
            /[a-zA-ZÀ-ÿ]/.test(line) &&
            !looksLikeAdministrativeLine(line) &&
            (line.match(/\d/g) || []).length <= 3
        );

    const legalEntityPattern = /(SARL|SAS|SL|SA|S\.A\.|S\.L\.|GMBH|LTD|SRL|SNC|EURL|BV|LLC|INC)\b/i;
    candidatePool
        .map((line, index) => {
            const letters = (line.match(/[A-Za-zÀ-ÿ]/g) || []).length;
            const uppercase = (line.match(/[A-ZÀ-Ý]/g) || []).length;
            const upperRatio = letters ? uppercase / letters : 0;
            const suffixBonus = legalEntityPattern.test(line) ? 0.7 : 0;
            const earlyLineBonus = Math.max(0, 0.35 - index * 0.05);
            const titleCaseBonus = /^[A-ZÀ-Ý][A-Za-zÀ-ÿ\s.'&-]+$/.test(line) ? 0.2 : 0;
            const digitPenalty = (line.match(/\d/g) || []).length * 0.25;
            const score = upperRatio + suffixBonus + earlyLineBonus + titleCaseBonus - digitPenalty;
            return { line, score };
        })
        .sort((a, b) => b.score - a.score)
        .slice(0, 6)
        .forEach(item => {
            const confidence = item.score >= 1.15 ? 'high' : item.score >= 0.8 ? 'medium' : item.score >= 0.55 ? 'low' : 'none';
            if (confidence === 'none') return;
            pushCandidate({ name: item.line, confidence, source: 'heuristic', score: item.score });
        });

    const fallback = guessSupplierFromFilename(fileName);
    if (fallback) {
        pushCandidate({ name: fallback, confidence: 'low', source: 'filename', score: 0.45 });
    }

    return candidates
        .sort((a, b) => b.score - a.score)
        .slice(0, 3);
}

function guessSupplierFromText(text, fileName = '') {
    const candidates = getSupplierCandidatesFromText(text, fileName);
    if (!candidates.length) return { name: '', confidence: 'none', source: 'none' };
    return {
        name: candidates[0].name,
        confidence: candidates[0].confidence,
        source: candidates[0].source
    };
}

function guessSupplierFromFilename(fileName) {
    const base = String(fileName || '')
        .replace(/\.[^.]+$/, '')
        .replace(/[_-]+/g, ' ')
        .replace(/\b(scan|scanner|facture|invoice|pdf|image|photo|document|doc)\b/gi, '')
        .replace(/\s+/g, ' ')
        .trim();
    if (!base || base.length < 3) return '';
    return base;
}

function extractDateFromText(text) {
    const raw = String(text || '');

    const normalizeDate = (year, month, day) => {
        const y = Number(year);
        const m = Number(month);
        const d = Number(day);
        if (!Number.isFinite(y) || !Number.isFinite(m) || !Number.isFinite(d)) return '';
        if (y < 2000 || y > 2100 || m < 1 || m > 12 || d < 1 || d > 31) return '';
        return `${String(y).padStart(4, '0')}-${String(m).padStart(2, '0')}-${String(d).padStart(2, '0')}`;
    };

    const ymd = raw.match(/\b(20\d{2})[\/.\-](\d{1,2})[\/.\-](\d{1,2})\b/);
    if (ymd) {
        const normalized = normalizeDate(ymd[1], ymd[2], ymd[3]);
        if (normalized) return normalized;
    }

    const dmy = raw.match(/\b(\d{1,2})[\/.\-](\d{1,2})[\/.\-](20\d{2}|\d{2})\b/);
    if (dmy) {
        const year = dmy[3].length === 2 ? `20${dmy[3]}` : dmy[3];
        const normalized = normalizeDate(year, dmy[2], dmy[1]);
        if (normalized) return normalized;
    }

    return '';
}

function extractInvoiceFields(text, fileName = '') {
    const supplierGuess = guessSupplierFromText(text, fileName);
    return {
        supplierName: supplierGuess.name,
        supplierConfidence: supplierGuess.confidence,
        supplierSource: supplierGuess.source,
        date: extractDateFromText(text),
        totalAmount: extractTotalFromText(text),
        iban: extractIbanFromText(text)
    };
}

function buildExpensePreventionComment({ supplierName = '', totalAmount = null, detectedLines = [] } = {}) {
    const cleanedLines = (Array.isArray(detectedLines) ? detectedLines : [])
        .map(line => String(line || '').trim())
        .filter(Boolean)
        .slice(0, 12);
    const joined = cleanedLines.join(' ').toLowerCase();

    const themes = [
        {
            key: 'water',
            regex: /(pompe|pump|impeller|turbine|water pump|eau de mer|waterline|cooling)/i,
            risk: t('Risque refroidissement moteur: panne turbine/pompe ou débit eau insuffisant.', 'Riesgo de refrigeración motor: fallo turbina/bomba o caudal insuficiente.'),
            controls: t('Contrôler aspiration eau de mer, filtre, turbine et température moteur après intervention.', 'Controlar aspiración de agua de mar, filtro, turbina y temperatura motor tras la intervención.'),
            prevention: t('Programmer contrôle visuel toutes les 50h et remplacement préventif turbine selon manuel.', 'Programar control visual cada 50h y sustitución preventiva de turbina según manual.')
        },
        {
            key: 'engine',
            regex: /(filtre|filter|huile|oil|fuel|injecteur|injector|courroie|belt)/i,
            risk: t('Risque performance moteur: encrassement ou usure prématurée.', 'Riesgo de rendimiento motor: ensuciamiento o desgaste prematuro.'),
            controls: t('Vérifier heures moteur, référence pièces montées et conformité viscosité/grade.', 'Verificar horas motor, referencia de piezas montadas y conformidad de viscosidad/grado.'),
            prevention: t('Créer un plan périodique filtres/huile avec seuils heures moteur et stock mini à bord.', 'Crear un plan periódico de filtros/aceite con umbrales de horas y stock mínimo a bordo.')
        },
        {
            key: 'electric',
            regex: /(batterie|battery|alternateur|alternator|chargeur|charger|cable|câble|fusible|relay|relais)/i,
            risk: t('Risque électrique: perte de charge, coupure ou panne intermittente.', 'Riesgo eléctrico: pérdida de carga, corte o fallo intermitente.'),
            controls: t('Mesurer tension repos/charge, serrage bornes et chute de tension sous charge.', 'Medir tensión en reposo/carga, apriete de bornes y caída de tensión bajo carga.'),
            prevention: t('Planifier test de capacité batterie et inspection connexions anticorrosion.', 'Planificar test de capacidad de batería e inspección de conexiones anticorrosión.')
        },
        {
            key: 'seal',
            regex: /(joint|seal|gasket|durite|hose|collier|etancheite|étanchéité|fuite|leak)/i,
            risk: t('Risque fuite: perte de fluide et avarie secondaire.', 'Riesgo de fuga: pérdida de fluido y avería secundaria.'),
            controls: t('Recontrôler l’étanchéité à chaud et à froid 24h après remise en service.', 'Revisar estanqueidad en caliente y en frío 24h después de la puesta en servicio.'),
            prevention: t('Ajouter contrôle anti-fuite en ronde et remplacer consommables d’étanchéité par lot.', 'Añadir control antifugas en ronda y sustituir consumibles de estanqueidad por lote.')
        }
    ];

    const matchedThemes = themes.filter(theme => theme.regex.test(joined));
    const keyLines = cleanedLines
        .filter(line => !/\b(total|ttc|ht|tv[ao]|amount|importe|montant|iban)\b/i.test(line))
        .slice(0, 4);

    const risks = [];
    const controls = [];
    const prevention = [];

    if (Number.isFinite(totalAmount) && totalAmount >= 1000) {
        risks.push(t('Montant élevé: impact budget significatif et risque de validation incomplète.', 'Importe elevado: impacto presupuestario significativo y riesgo de validación incompleta.'));
        controls.push(t('Exiger devis comparatif, preuve d’exécution et validation croisée avant paiement.', 'Exigir presupuesto comparativo, prueba de ejecución y validación cruzada antes del pago.'));
    }
    if (Number.isFinite(totalAmount) && totalAmount >= 3000) {
        prevention.push(t('Mettre en place un seuil d’alerte > 3000 avec approbation à deux niveaux.', 'Configurar umbral de alerta > 3000 con aprobación en dos niveles.'));
    }

    matchedThemes.forEach(theme => {
        risks.push(theme.risk);
        controls.push(theme.controls);
        prevention.push(theme.prevention);
    });

    if (!risks.length) {
        risks.push(t('Risque principal non catégorisé: possible écart entre prestation facturée et besoin réel.', 'Riesgo principal no categorizado: posible desvío entre servicio facturado y necesidad real.'));
    }
    if (!controls.length) {
        controls.push(t('Contrôler cohérence facture/pièces remplacées/main d’œuvre avant clôture.', 'Controlar coherencia factura/piezas sustituidas/mano de obra antes del cierre.'));
    }
    if (!prevention.length) {
        prevention.push(t('Documenter cause racine, action corrective et date du prochain contrôle.', 'Documentar causa raíz, acción correctiva y fecha del próximo control.'));
    }

    const docs = [
        t('Archiver facture + photos avant/après + référence des pièces.', 'Archivar factura + fotos antes/después + referencia de piezas.'),
        t('Noter heures moteur et test de validation après intervention.', 'Anotar horas motor y prueba de validación tras la intervención.')
    ];

    const header = t('Commentaire IA · Analyse facture', 'Comentario IA · Análisis de factura');
    const supplierLine = supplierName ? `${t('Fournisseur', 'Proveedor')}: ${supplierName}` : t('Fournisseur: non confirmé', 'Proveedor: no confirmado');
    const amountLine = Number.isFinite(totalAmount) && totalAmount > 0 ? `${t('Montant détecté', 'Importe detectado')}: ${totalAmount.toFixed(2)}` : t('Montant détecté: non trouvé', 'Importe detectado: no encontrado');
    const keyLinesBlock = keyLines.length
        ? keyLines.map(line => `- ${line}`).join('\n')
        : `- ${t('Aucune ligne exploitable détectée dans le scan.', 'No se detectaron líneas aprovechables en el escaneo.')}`;

    return [
        header,
        `${supplierLine} · ${amountLine}`,
        '',
        t('Éléments détectés', 'Elementos detectados'),
        keyLinesBlock,
        '',
        t('Risques probables', 'Riesgos probables'),
        risks.map(item => `- ${item}`).join('\n'),
        '',
        t('Contrôles à faire avant paiement', 'Controles antes del pago'),
        controls.map(item => `- ${item}`).join('\n'),
        '',
        t('Prévention (30-90 jours)', 'Prevención (30-90 días)'),
        prevention.map(item => `- ${item}`).join('\n'),
        '',
        t('Pièces à archiver', 'Documentos a archivar'),
        docs.map(item => `- ${item}`).join('\n')
    ].join('\n');
}

async function loadMaintenancePdfJs() {
    if (!maintenancePdfJsLoader) {
        maintenancePdfJsLoader = import('https://cdn.jsdelivr.net/npm/pdfjs-dist@4.7.76/build/pdf.min.mjs');
    }
    const pdfjsLib = await maintenancePdfJsLoader;
    if (pdfjsLib?.GlobalWorkerOptions) {
        pdfjsLib.GlobalWorkerOptions.workerSrc = 'https://cdn.jsdelivr.net/npm/pdfjs-dist@4.7.76/build/pdf.worker.min.mjs';
    }
    return pdfjsLib;
}

async function renderSelectablePdfPreview(file, container) {
    if (!file || !container || !String(file.type || '').includes('pdf')) return;

    const pdfjsLib = await loadMaintenancePdfJs();
    const arrayBuffer = await file.arrayBuffer();
    const loadingTask = pdfjsLib.getDocument({ data: arrayBuffer });
    const pdf = await loadingTask.promise;
    const maxPages = Math.min(pdf.numPages || 1, 8);

    container.innerHTML = '';
    for (let pageNumber = 1; pageNumber <= maxPages; pageNumber++) {
        const page = await pdf.getPage(pageNumber);
        const viewport = page.getViewport({ scale: 1.25 });

        const pageNode = document.createElement('div');
        pageNode.className = 'maintenance-pdf-page';
        pageNode.style.width = `${Math.floor(viewport.width)}px`;
        pageNode.style.height = `${Math.floor(viewport.height)}px`;

        const canvas = document.createElement('canvas');
        canvas.className = 'maintenance-pdf-canvas';
        canvas.width = Math.max(1, Math.floor(viewport.width));
        canvas.height = Math.max(1, Math.floor(viewport.height));
        canvas.style.width = `${Math.floor(viewport.width)}px`;
        canvas.style.height = `${Math.floor(viewport.height)}px`;
        const context = canvas.getContext('2d');
        if (context) {
            await page.render({ canvasContext: context, viewport }).promise;
        }

        const textLayer = document.createElement('div');
        textLayer.className = 'maintenance-pdf-text-layer';
        textLayer.style.width = `${Math.floor(viewport.width)}px`;
        textLayer.style.height = `${Math.floor(viewport.height)}px`;

        const textContent = await page.getTextContent();
        const items = Array.isArray(textContent?.items) ? textContent.items : [];
        items.forEach(item => {
            const content = String(item?.str || '');
            if (!content.trim()) return;

            const transform = pdfjsLib.Util.transform(viewport.transform, item.transform);
            const fontHeight = Math.hypot(transform[2], transform[3]);
            const angle = Math.atan2(transform[1], transform[0]);

            const span = document.createElement('span');
            span.textContent = content;
            span.style.left = `${transform[4]}px`;
            span.style.top = `${transform[5] - fontHeight}px`;
            span.style.fontSize = `${fontHeight}px`;
            span.style.fontFamily = String(item?.fontName || 'sans-serif');
            const scaleX = fontHeight ? (Math.hypot(transform[0], transform[1]) / fontHeight) : 1;
            span.style.transform = `rotate(${angle}rad) scaleX(${scaleX})`;

            textLayer.appendChild(span);
        });

        pageNode.appendChild(canvas);
        pageNode.appendChild(textLayer);
        container.appendChild(pageNode);
    }
}

function buildAlternativeLlmPrompt({ supplierName = '', invoiceDate = '', iban = '', totalAmount = null, currency = 'EUR', rawText = '' } = {}) {
    const supplier = supplierName || t('Non confirmé', 'No confirmado');
    const date = invoiceDate || t('Non détectée', 'No detectada');
    const ibanValue = iban || t('Non détecté', 'No detectado');
    const amount = Number.isFinite(totalAmount) && totalAmount > 0
        ? `${totalAmount.toFixed(2)} ${String(currency || 'EUR').toUpperCase()}`
        : t('Non détecté', 'No detectado');
    const excerpt = String(rawText || '').trim().slice(0, 12000);

    return currentLanguage === 'es'
        ? [
            'Analiza críticamente esta factura náutica y devuelve un informe accionable.',
            `Proveedor: ${supplier}`,
            `Fecha: ${date}`,
            `IBAN: ${ibanValue}`,
            `Importe: ${amount}`,
            '',
            'Texto OCR/PDF:',
            excerpt || '(sin texto)',
            '',
            'Responde con secciones: Riesgos, Controles antes de pago, Prevención 30-90 días, Alertas de fraude/error, Resumen ejecutivo.'
        ].join('\n')
        : [
            'Analyse cette facture nautique de façon critique et produis un rapport actionnable.',
            `Fournisseur: ${supplier}`,
            `Date: ${date}`,
            `IBAN: ${ibanValue}`,
            `Montant: ${amount}`,
            '',
            'Texte OCR/PDF:',
            excerpt || '(pas de texte)',
            '',
            'Réponds avec les sections: Risques, Contrôles avant paiement, Prévention 30-90 jours, Alertes fraude/erreur, Résumé exécutif.'
        ].join('\n');
}

async function requestAlternativeLlmAnalysis({ provider = '', apiKey = '', model = '', prompt = '' } = {}) {
    const safeProvider = String(provider || '').trim().toLowerCase();
    const safeApiKey = sanitizeApiKey(apiKey);
    const safeModel = String(model || '').trim();
    if (!safeProvider || !safeApiKey || !safeModel || !prompt) {
        throw new Error(t('Paramètres IA incomplets.', 'Parámetros IA incompletos.'));
    }

    if (safeProvider === 'openai') {
        const response = await fetch('https://api.openai.com/v1/chat/completions', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${safeApiKey}`
            },
            body: JSON.stringify({
                model: safeModel,
                messages: [
                    { role: 'system', content: 'You are a marine maintenance invoice analyst. Be precise and practical.' },
                    { role: 'user', content: prompt }
                ],
                temperature: 0.2
            })
        });
        if (!response.ok) {
            throw await buildAlternativeLlmHttpError('OpenAI', response);
        }
        const data = await response.json();
        return String(data?.choices?.[0]?.message?.content || '').trim();
    }

    if (safeProvider === 'anthropic') {
        const response = await fetch('https://api.anthropic.com/v1/messages', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-api-key': safeApiKey,
                'anthropic-version': '2023-06-01'
            },
            body: JSON.stringify({
                model: safeModel,
                max_tokens: 1400,
                temperature: 0.2,
                messages: [
                    { role: 'user', content: prompt }
                ]
            })
        });
        if (!response.ok) {
            throw await buildAlternativeLlmHttpError('Anthropic', response);
        }
        const data = await response.json();
        const blocks = Array.isArray(data?.content) ? data.content : [];
        return blocks.map(item => String(item?.text || '')).join('\n').trim();
    }

    throw new Error(t('Provider IA non supporté.', 'Proveedor IA no soportado.'));
}

async function testAlternativeLlmConnection({ provider = '', apiKey = '', model = '' } = {}) {
    const safeProvider = String(provider || '').trim().toLowerCase();
    const safeApiKey = sanitizeApiKey(apiKey);
    const safeModel = String(model || '').trim();
    if (!safeProvider || !safeApiKey || !safeModel) {
        throw new Error(t('Paramètres IA incomplets.', 'Parámetros IA incompletos.'));
    }

    if (safeProvider === 'openai') {
        const response = await fetch('https://api.openai.com/v1/chat/completions', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${safeApiKey}`
            },
            body: JSON.stringify({
                model: safeModel,
                messages: [{ role: 'user', content: 'Ping' }],
                max_tokens: 3,
                temperature: 0
            })
        });
        if (!response.ok) {
            throw await buildAlternativeLlmHttpError('OpenAI', response);
        }
        return true;
    }

    if (safeProvider === 'anthropic') {
        const response = await fetch('https://api.anthropic.com/v1/messages', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-api-key': safeApiKey,
                'anthropic-version': '2023-06-01'
            },
            body: JSON.stringify({
                model: safeModel,
                max_tokens: 3,
                temperature: 0,
                messages: [{ role: 'user', content: 'Ping' }]
            })
        });
        if (!response.ok) {
            throw await buildAlternativeLlmHttpError('Anthropic', response);
        }
        return true;
    }

    throw new Error(t('Provider IA non supporté.', 'Proveedor IA no soportado.'));
}

function formatAlternativeLlmRuntimeError(error, provider) {
    const raw = String(error?.message || error || '');
    const normalizedProvider = String(provider || '').toLowerCase();
    const failedFetch = /failed to fetch|networkerror|network error|load failed/i.test(raw);
    if (failedFetch) {
        if (normalizedProvider === 'anthropic') {
            return t(
                'Connexion impossible (navigateur/CORS). Anthropic bloque souvent les appels directs depuis une app web locale. Utilise un proxy backend (ex: Supabase Edge Function) ou OpenAI dans cette app.',
                'Conexión imposible (navegador/CORS). Anthropic suele bloquear llamadas directas desde una app web local. Usa un proxy backend (ej: Supabase Edge Function) u OpenAI en esta app.'
            );
        }
        return t(
            'Connexion réseau impossible vers le provider IA (CORS/pare-feu/adblock).',
            'Conexión de red imposible con el proveedor IA (CORS/firewall/adblock).'
        );
    }
    return raw;
}

function sanitizeApiKey(rawValue) {
    return String(rawValue || '')
        .normalize('NFKC')
        .replace(/[\u200B-\u200D\uFEFF]/g, '')
        .replace(/[\u00A0\u202F]/g, ' ')
        .replace(/[`'"“”‘’]/g, '')
        .replace(/\s+/g, '')
        .replace(/[^\x20-\x7E]/g, '')
        .trim();
}

async function buildAlternativeLlmHttpError(providerLabel, response) {
    const status = Number(response?.status || 0);
    let details = '';

    try {
        const payload = await response.json();
        details = String(
            payload?.error?.message ||
            payload?.message ||
            payload?.detail ||
            ''
        ).trim();
    } catch (_) {
        try {
            const text = String(await response.text()).trim();
            details = text.slice(0, 220);
        } catch (__){
            details = '';
        }
    }

    let hint = '';
    if (status === 401) {
        hint = t(
            'clé invalide/expirée ou sans droit API; vérifie la clé complète et le projet API activé',
            'clave inválida/caducada o sin permisos API; verifica clave completa y proyecto API activo'
        );
    } else if (status === 404) {
        hint = t(
            'modèle introuvable; vérifie le nom exact du modèle',
            'modelo no encontrado; verifica el nombre exacto del modelo'
        );
    } else if (status === 429) {
        hint = t(
            'quota/limite atteinte; attends un peu ou augmente les quotas',
            'cuota/límite alcanzado; espera o aumenta cuotas'
        );
    }

    const parts = [`${providerLabel} ${status || ''}`.trim()];
    if (details) parts.push(details);
    if (hint) parts.push(hint);
    return new Error(parts.join(' · '));
}

async function runTesseractRecognition(input) {
    if (!maintenanceTesseractLoader) {
        maintenanceTesseractLoader = import('https://cdn.jsdelivr.net/npm/tesseract.js@5.1.0/dist/tesseract.esm.min.js');
    }
    const tesseractModule = await maintenanceTesseractLoader;
    const { data } = await tesseractModule.recognize(input, 'eng+spa+fra');
    return String(data?.text || '');
}

async function runInvoiceScanFromImage(file) {
    if (!file || !file.type.startsWith('image/')) {
        throw new Error(t('Le scan image nécessite un fichier image.', 'El escaneo de imagen requiere un archivo de imagen.'));
    }
    return runTesseractRecognition(file);
}

async function runInvoiceScanFromPdf(file) {
    if (!file || !String(file.type || '').includes('pdf')) {
        throw new Error(t('Le scan PDF nécessite un fichier PDF.', 'El escaneo PDF requiere un archivo PDF.'));
    }

    const pdfjsLib = await loadMaintenancePdfJs();

    const arrayBuffer = await file.arrayBuffer();
    const loadingTask = pdfjsLib.getDocument({ data: arrayBuffer });
    const pdf = await loadingTask.promise;
    const maxPages = Math.min(pdf.numPages || 1, 4);

    const textChunks = [];
    for (let pageNumber = 1; pageNumber <= maxPages; pageNumber++) {
        const page = await pdf.getPage(pageNumber);
        const content = await page.getTextContent();
        const items = Array.isArray(content?.items) ? content.items : [];
        if (!items.length) continue;

        const rows = new Map();
        items.forEach(item => {
            const y = Number(item?.transform?.[5]);
            const key = Number.isFinite(y) ? String(Math.round(y / 2) * 2) : '0';
            const chunk = String(item?.str || '').trim();
            if (!chunk) return;
            if (!rows.has(key)) rows.set(key, []);
            rows.get(key).push(chunk);
        });

        const lines = [...rows.entries()]
            .sort((a, b) => Number(b[0]) - Number(a[0]))
            .map(([, row]) => row.join(' '))
            .filter(Boolean);

        if (lines.length) {
            textChunks.push(lines.join('\n'));
        }
    }

    let extractedText = textChunks.join('\n');
    if (extractedText.replace(/\s+/g, '').length >= 40) {
        return extractedText;
    }

    const ocrChunks = [];
    for (let pageNumber = 1; pageNumber <= maxPages; pageNumber++) {
        const page = await pdf.getPage(pageNumber);
        const viewport = page.getViewport({ scale: 1.6 });
        const canvas = document.createElement('canvas');
        const context = canvas.getContext('2d');
        if (!context) continue;
        canvas.width = Math.max(1, Math.floor(viewport.width));
        canvas.height = Math.max(1, Math.floor(viewport.height));
        await page.render({ canvasContext: context, viewport }).promise;
        const pageText = await runTesseractRecognition(canvas);
        if (pageText.trim()) {
            ocrChunks.push(pageText.trim());
        }
    }

    extractedText = [extractedText, ...ocrChunks].filter(Boolean).join('\n');
    return extractedText;
}

async function runInvoiceScan(file) {
    if (!file) {
        throw new Error(t('Aucun fichier sélectionné.', 'Ningún archivo seleccionado.'));
    }
    if (String(file.type || '').includes('pdf') || /\.pdf$/i.test(String(file.name || ''))) {
        return runInvoiceScanFromPdf(file);
    }
    return runInvoiceScanFromImage(file);
}

function sanitizeMaintenanceBoard(board, fallbackIndex = 0) {
    const safeName = String(board?.name || `${t('Schéma', 'Esquema')} ${fallbackIndex + 1}`).trim() || `${t('Schéma', 'Esquema')} ${fallbackIndex + 1}`;
    const safeImageDataUrl = typeof board?.imageDataUrl === 'string' ? board.imageDataUrl : '';
    const rawAnnotations = Array.isArray(board?.annotations) ? board.annotations : [];

    const annotations = rawAnnotations
        .map((annotation, index) => {
            const xPercent = Number(annotation?.xPercent);
            const yPercent = Number(annotation?.yPercent);
            if (!Number.isFinite(xPercent) || !Number.isFinite(yPercent)) return null;

            const colorMeta = getMaintenanceColorMeta(annotation?.colorKey || annotation?.color);

            return {
                id: String(annotation?.id || `maintenance-ann-${Date.now()}-${index}`),
                xPercent: Math.max(0, Math.min(100, xPercent)),
                yPercent: Math.max(0, Math.min(100, yPercent)),
                colorKey: colorMeta.key,
                statusKey: normalizeMaintenanceTaskStatus(annotation?.statusKey),
                legend: String(annotation?.legend || '').trim(),
                createdAt: String(annotation?.createdAt || new Date().toISOString())
            };
        })
        .filter(Boolean);

    return {
        id: String(board?.id || `maintenance-${Date.now()}-${fallbackIndex}`),
        name: safeName,
        imageDataUrl: safeImageDataUrl,
        annotations,
        createdAt: String(board?.createdAt || new Date().toISOString()),
        updatedAt: String(board?.updatedAt || new Date().toISOString())
    };
}

function loadMaintenanceBoards() {
    const fromStorage = loadArrayFromStorage(MAINTENANCE_BOARDS_STORAGE_KEY);
    setMaintenanceBoards(fromStorage, { persistLocal: false, refreshUi: false, syncCloud: false });
}

function setMaintenanceBoards(list, { persistLocal = true, refreshUi = true, syncCloud = false } = {}) {
    maintenanceBoards = (Array.isArray(list) ? list : [])
        .map((board, index) => sanitizeMaintenanceBoard(board, index));

    if (!maintenanceBoards.length) {
        selectedMaintenanceBoardId = null;
    } else {
        const selectedStillExists = maintenanceBoards.some(board => board.id === selectedMaintenanceBoardId);
        if (!selectedStillExists) {
            selectedMaintenanceBoardId = maintenanceBoards[0].id;
        }
    }

    if (persistLocal) {
        saveArrayToStorage(MAINTENANCE_BOARDS_STORAGE_KEY, maintenanceBoards);
    }

    if (syncCloud && isCloudReady()) {
        pushRoutesToCloud()
            .then(() => setCloudStatus(`Cloud synchronisé · ${getSavedRoutes().length} route(s) + ${waypointPhotoEntries.length} photo(s) + ${maintenanceBoards.length} schéma(s)`))
            .catch(error => setCloudStatus(`Maintenance locale OK, synchro cloud échouée: ${formatCloudError(error)}`, true));
    }

    if (refreshUi) {
        renderMaintenanceBoard();
    }
}

function persistMaintenanceBoards({ syncCloud = true } = {}) {
    saveArrayToStorage(MAINTENANCE_BOARDS_STORAGE_KEY, maintenanceBoards);

    if (syncCloud && isCloudReady()) {
        pushRoutesToCloud()
            .then(() => setCloudStatus(`Cloud synchronisé · ${getSavedRoutes().length} route(s) + ${waypointPhotoEntries.length} photo(s) + ${maintenanceBoards.length} schéma(s)`))
            .catch(error => setCloudStatus(`Maintenance locale OK, synchro cloud échouée: ${formatCloudError(error)}`, true));
    }
}

function getSelectedMaintenanceBoard() {
    return maintenanceBoards.find(board => board.id === selectedMaintenanceBoardId) || null;
}

function setMaintenanceStatus(message, isError = false) {
    const status = document.getElementById('maintenanceStatus');
    if (!status) return;
    status.textContent = message;
    status.style.color = isError ? '#ff8f8f' : '';
}

function refreshMaintenanceBoardSelect() {
    const select = document.getElementById('maintenanceSchemaSelect');
    if (!select) return;

    select.innerHTML = '';
    maintenanceBoards.forEach((board, index) => {
        const option = document.createElement('option');
        option.value = board.id;
        option.textContent = `${index + 1}. ${board.name}`;
        select.appendChild(option);
    });

    if (!maintenanceBoards.length) return;
    const selectedIndex = maintenanceBoards.findIndex(board => board.id === selectedMaintenanceBoardId);
    select.selectedIndex = selectedIndex >= 0 ? selectedIndex : 0;
}

function setActiveMaintenanceAnnotation(annotationId) {
    activeMaintenanceAnnotationId = annotationId || null;

    document.querySelectorAll('#maintenancePinsLayer .maintenance-pin').forEach(node => {
        const isActive =
            node.getAttribute('data-ann-id') === activeMaintenanceAnnotationId &&
            node.getAttribute('data-board-id') === selectedMaintenanceBoardId;
        node.classList.toggle('maintenance-pin--active', isActive);
    });

    document.querySelectorAll('#maintenanceLegendList .maintenance-legend-item').forEach(node => {
        const isActive =
            node.getAttribute('data-ann-id') === activeMaintenanceAnnotationId &&
            node.getAttribute('data-board-id') === selectedMaintenanceBoardId;
        node.classList.toggle('maintenance-legend-item--active', isActive);
    });
}

function setActiveMaintenanceSubtab(tabKey) {
    activeMaintenanceSubtab = ['tasks', 'expenses', 'suppliers'].includes(tabKey) ? tabKey : 'tasks';

    const tabBtnMap = {
        tasks: document.getElementById('maintenanceTasksSubtabBtn'),
        expenses: document.getElementById('maintenanceExpensesSubtabBtn'),
        suppliers: document.getElementById('maintenanceSuppliersSubtabBtn')
    };
    const panelMap = {
        tasks: document.getElementById('maintenanceTasksPanel'),
        expenses: document.getElementById('maintenanceExpensesPanel'),
        suppliers: document.getElementById('maintenanceSuppliersPanel')
    };

    Object.entries(tabBtnMap).forEach(([key, node]) => {
        if (!node) return;
        node.classList.toggle('active', key === activeMaintenanceSubtab);
    });

    Object.entries(panelMap).forEach(([key, node]) => {
        if (!node) return;
        node.classList.toggle('active', key === activeMaintenanceSubtab);
    });

    window.dispatchEvent(new CustomEvent('ceibo:maintenance-subtab-changed'));
}

function renderMaintenanceBoard() {
    const image = document.getElementById('maintenanceSchemaImage');
    const pinsLayer = document.getElementById('maintenancePinsLayer');
    const placeholder = document.getElementById('maintenanceCanvasPlaceholder');
    const legendList = document.getElementById('maintenanceLegendList');
    if (!image || !pinsLayer || !placeholder || !legendList) return;

    refreshMaintenanceBoardSelect();

    const board = getSelectedMaintenanceBoard();
    pinsLayer.innerHTML = '';
    legendList.innerHTML = '';

    if (!board) {
        image.style.display = 'none';
        image.removeAttribute('src');
        placeholder.style.display = 'flex';
        activeMaintenanceAnnotationId = null;
        setMaintenanceSchemaManagerVisibility(true);
        setMaintenanceStatus(t('Maintenance: ajoute un schéma pour commencer.', 'Mantenimiento: añade un esquema para empezar.'));
        return;
    }

    if (board.imageDataUrl) {
        image.src = board.imageDataUrl;
        image.style.display = 'block';
        placeholder.style.display = 'none';
    } else {
        image.style.display = 'none';
        image.removeAttribute('src');
        placeholder.style.display = 'flex';
    }

    if (activeMaintenanceAnnotationId && !board.annotations.some(item => item.id === activeMaintenanceAnnotationId)) {
        activeMaintenanceAnnotationId = null;
    }

    board.annotations.forEach((annotation, index) => {
        const pointLabel = `${t('Point', 'Punto')} ${index + 1}`;
        const pinColorMeta = getMaintenanceColorMeta(annotation.colorKey);
        const statusMeta = getMaintenanceTaskStatusMeta(annotation.statusKey);

        const pin = document.createElement('div');
        pin.className = 'maintenance-pin';
        if (statusMeta.key === 'done') {
            pin.classList.add('maintenance-pin--done');
        }
        pin.setAttribute('data-ann-id', annotation.id);
        pin.setAttribute('data-board-id', board.id);
        pin.style.left = `${annotation.xPercent}%`;
        pin.style.top = `${annotation.yPercent}%`;
        pin.style.backgroundColor = pinColorMeta.hex;
        pin.title = annotation.legend || pointLabel;
        pinsLayer.appendChild(pin);
    });

    maintenanceBoards.forEach((schemaBoard, schemaIndex) => {
        const schemaGroup = document.createElement('div');
        schemaGroup.className = 'maintenance-schema-group';

        const schemaHeader = document.createElement('button');
        schemaHeader.type = 'button';
        schemaHeader.className = 'maintenance-schema-title';
        if (schemaBoard.id === selectedMaintenanceBoardId) {
            schemaHeader.classList.add('active');
        }
        schemaHeader.textContent = `${schemaIndex + 1}. ${schemaBoard.name} (${schemaBoard.annotations.length})`;
        schemaHeader.addEventListener('click', () => {
            selectedMaintenanceBoardId = schemaBoard.id;
            activeMaintenanceAnnotationId = null;
            renderMaintenanceBoard();
            setMaintenanceStatus(t(`Schéma chargé: ${schemaBoard.name}`, `Esquema cargado: ${schemaBoard.name}`));
        });
        schemaGroup.appendChild(schemaHeader);

        if (!schemaBoard.annotations.length) {
            const empty = document.createElement('div');
            empty.className = 'maintenance-legend-empty';
            empty.textContent = t('Aucune tâche sur ce schéma.', 'Sin tareas en este esquema.');
            schemaGroup.appendChild(empty);
            legendList.appendChild(schemaGroup);
            return;
        }

        const sortedAnnotations = [...schemaBoard.annotations].sort((a, b) => {
            const statusRankA = MAINTENANCE_TASK_STATUS_ORDER.indexOf(normalizeMaintenanceTaskStatus(a.statusKey));
            const statusRankB = MAINTENANCE_TASK_STATUS_ORDER.indexOf(normalizeMaintenanceTaskStatus(b.statusKey));
            if (statusRankA !== statusRankB) return statusRankA - statusRankB;
            const colorA = getMaintenanceColorMeta(a.colorKey).key;
            const colorB = getMaintenanceColorMeta(b.colorKey).key;
            const rankA = MAINTENANCE_COLOR_ORDER.indexOf(colorA);
            const rankB = MAINTENANCE_COLOR_ORDER.indexOf(colorB);
            if (rankA !== rankB) return rankA - rankB;
            return String(a.createdAt || '').localeCompare(String(b.createdAt || ''));
        });

        sortedAnnotations.forEach((annotation, index) => {
            const colorMeta = getMaintenanceColorMeta(annotation.colorKey);
            const statusMeta = getMaintenanceTaskStatusMeta(annotation.statusKey);
            const pointLabel = `${t('Point', 'Punto')} ${index + 1}`;

            const item = document.createElement('div');
            item.className = 'maintenance-legend-item';
            if (statusMeta.key === 'done') {
                item.classList.add('maintenance-legend-item--done');
            }
            item.setAttribute('data-ann-id', annotation.id);
            item.setAttribute('data-board-id', schemaBoard.id);

            const head = document.createElement('div');
            head.className = 'maintenance-legend-head';

            const left = document.createElement('span');
            const dot = document.createElement('span');
            dot.className = 'maintenance-legend-dot';
            dot.style.backgroundColor = colorMeta.hex;

            const title = document.createElement('strong');
            title.textContent = pointLabel;
            left.appendChild(dot);
            left.appendChild(title);
            left.appendChild(document.createTextNode(` — ${annotation.legend || t('Sans légende', 'Sin leyenda')}`));

            const statusBadge = document.createElement('span');
            statusBadge.className = 'maintenance-status-badge';
            statusBadge.textContent = t(statusMeta.fr, statusMeta.es);
            left.appendChild(statusBadge);

            const controlsWrap = document.createElement('div');
            controlsWrap.style.display = 'flex';
            controlsWrap.style.gap = '6px';
            controlsWrap.style.alignItems = 'center';

            const statusSelect = document.createElement('select');
            statusSelect.className = 'maintenance-status-select';
            statusSelect.style.padding = '4px 6px';
            statusSelect.style.fontSize = '10px';
            statusSelect.innerHTML =
                `<option value="active">${t('Actif', 'Activo')}</option>` +
                `<option value="planned">${t('À prévoir', 'A prever')}</option>` +
                `<option value="done">${t('Fini', 'Terminado')}</option>`;
            statusSelect.value = statusMeta.key;
            statusSelect.addEventListener('click', event => event.stopPropagation());
            statusSelect.addEventListener('change', event => {
                event.stopPropagation();
                annotation.statusKey = normalizeMaintenanceTaskStatus(statusSelect.value);
                schemaBoard.updatedAt = new Date().toISOString();
                persistMaintenanceBoards();
                renderMaintenanceBoard();
            });

            const deleteBtn = document.createElement('button');
            deleteBtn.type = 'button';
            deleteBtn.className = 'maintenance-delete-btn';
            deleteBtn.textContent = t('Supprimer', 'Eliminar');
            deleteBtn.addEventListener('click', event => {
                event.stopPropagation();
                schemaBoard.annotations = schemaBoard.annotations.filter(item => item.id !== annotation.id);
                schemaBoard.updatedAt = new Date().toISOString();
                if (activeMaintenanceAnnotationId === annotation.id && selectedMaintenanceBoardId === schemaBoard.id) {
                    activeMaintenanceAnnotationId = null;
                }
                persistMaintenanceBoards();
                renderMaintenanceBoard();
                setMaintenanceStatus(t('Pastille supprimée.', 'Marcador eliminado.'));
            });

            item.addEventListener('click', () => {
                selectedMaintenanceBoardId = schemaBoard.id;
                activeMaintenanceAnnotationId = annotation.id;
                renderMaintenanceBoard();
            });

            head.appendChild(left);
            controlsWrap.appendChild(statusSelect);
            controlsWrap.appendChild(deleteBtn);
            head.appendChild(controlsWrap);
            item.appendChild(head);
            schemaGroup.appendChild(item);
        });

        legendList.appendChild(schemaGroup);
    });

    setActiveMaintenanceAnnotation(activeMaintenanceAnnotationId);

    if (!board.annotations.length) {
        setMaintenanceStatus(t('Clique sur le schéma pour ajouter une pastille.', 'Haz clic en el esquema para añadir un marcador.'));
    }
}

function initializeMaintenanceFeature() {
    const tasksSubtabBtn = document.getElementById('maintenanceTasksSubtabBtn');
    const expensesSubtabBtn = document.getElementById('maintenanceExpensesSubtabBtn');
    const suppliersSubtabBtn = document.getElementById('maintenanceSuppliersSubtabBtn');
    const schemaNameInput = document.getElementById('maintenanceSchemaNameInput');
    const schemaInput = document.getElementById('maintenanceSchemaInput');
    const addBtn = document.getElementById('maintenanceAddSchemaBtn');
    const deleteBtn = document.getElementById('maintenanceDeleteSchemaBtn');
    const schemaSelect = document.getElementById('maintenanceSchemaSelect');
    const toggleSchemaManagerBtn = document.getElementById('maintenanceToggleSchemaManagerBtn');
    const pinColorInput = document.getElementById('maintenancePinColorInput');
    const taskStatusInput = document.getElementById('maintenanceTaskStatusInput');
    const legendInput = document.getElementById('maintenanceLegendInput');
    const canvas = document.getElementById('maintenanceCanvas');
    const image = document.getElementById('maintenanceSchemaImage');
    const invoiceInput = document.getElementById('maintenanceInvoiceInput');
    const scanInvoiceBtn = document.getElementById('maintenanceScanInvoiceBtn');
    const invoiceScanStatus = document.getElementById('maintenanceInvoiceScanStatus');
    const supplierSuggestionsLabel = document.getElementById('maintenanceSupplierSuggestionsLabel');
    const supplierSuggestionsContainer = document.getElementById('maintenanceSupplierSuggestions');
    const invoiceReviewPanel = document.getElementById('maintenanceInvoiceReviewPanel');
    const manualPasteTargetSelect = document.getElementById('maintenanceManualPasteTargetSelect');
    const pasteSelectedTextBtn = document.getElementById('maintenancePasteSelectedTextBtn');
    const llmProviderSelect = document.getElementById('maintenanceLlmProviderSelect');
    const llmApiKeyInput = document.getElementById('maintenanceLlmApiKeyInput');
    const llmModelInput = document.getElementById('maintenanceLlmModelInput');
    const testAltLlmBtn = document.getElementById('maintenanceTestAltLlmBtn');
    const runAltLlmBtn = document.getElementById('maintenanceRunAltLlmBtn');
    const invoicePreviewImage = document.getElementById('maintenanceInvoicePreviewImage');
    const invoicePreviewPdfContainer = document.getElementById('maintenanceInvoicePreviewPdfContainer');
    const invoicePreviewPlaceholder = document.getElementById('maintenanceInvoicePreviewPlaceholder');
    const invoicePreviewTitle = document.getElementById('maintenanceInvoicePreviewTitle');
    const expenseDateInput = document.getElementById('maintenanceExpenseDateInput');
    const expenseTotalInput = document.getElementById('maintenanceExpenseTotalInput');
    const expenseCurrencyInput = document.getElementById('maintenanceExpenseCurrencyInput');
    const expensePayerSelect = document.getElementById('maintenanceExpensePayerSelect');
    const expensePaymentStatusSelect = document.getElementById('maintenanceExpensePaymentStatusSelect');
    const expenseSupplierInput = document.getElementById('maintenanceExpenseSupplierInput');
    const expenseSupplierIbanInput = document.getElementById('maintenanceExpenseSupplierIbanInput');
    const expenseLinesInput = document.getElementById('maintenanceExpenseLinesInput');
    const expenseNoteInput = document.getElementById('maintenanceExpenseNoteInput');
    const expenseAiCommentInput = document.getElementById('maintenanceExpenseAiCommentInput');
    const addExpenseBtn = document.getElementById('maintenanceAddExpenseBtn');
    const supplierNameInput = document.getElementById('maintenanceSupplierNameInput');
    const supplierContactInput = document.getElementById('maintenanceSupplierContactInput');
    const supplierPhoneInput = document.getElementById('maintenanceSupplierPhoneInput');
    const supplierIbanInput = document.getElementById('maintenanceSupplierIbanInput');
    const supplierNoteInput = document.getElementById('maintenanceSupplierNoteInput');
    const addSupplierBtn = document.getElementById('maintenanceAddSupplierBtn');

    if (!tasksSubtabBtn || !expensesSubtabBtn || !suppliersSubtabBtn || !schemaNameInput || !schemaInput || !addBtn || !deleteBtn || !schemaSelect || !toggleSchemaManagerBtn || !pinColorInput || !taskStatusInput || !legendInput || !canvas || !image || !invoiceInput || !scanInvoiceBtn || !invoiceScanStatus || !expenseDateInput || !expenseTotalInput || !expenseCurrencyInput || !expensePayerSelect || !expensePaymentStatusSelect || !expenseSupplierInput || !expenseSupplierIbanInput || !expenseLinesInput || !expenseNoteInput || !expenseAiCommentInput || !addExpenseBtn || !supplierNameInput || !supplierContactInput || !supplierPhoneInput || !supplierIbanInput || !supplierNoteInput || !addSupplierBtn) return;

    const applyClipboardTextToTarget = (text) => {
        const value = String(text || '').trim();
        if (!value) return false;

        const target = String(manualPasteTargetSelect?.value || 'expenseSupplier');
        if (target === 'expenseSupplier') {
            expenseSupplierInput.value = value;
            supplierNameInput.value = value;
            return true;
        }
        if (target === 'supplierContact') {
            supplierContactInput.value = value;
            return true;
        }
        if (target === 'supplierPhone') {
            supplierPhoneInput.value = value;
            return true;
        }
        if (target === 'supplierEmailToNote') {
            const current = String(supplierNoteInput.value || '').trim();
            const emailLine = `Email: ${value}`;
            supplierNoteInput.value = current ? `${current}\n${emailLine}` : emailLine;
            return true;
        }
        if (target === 'expenseIban') {
            expenseSupplierIbanInput.value = normalizeIbanValue(value) || value;
            supplierIbanInput.value = expenseSupplierIbanInput.value;
            return true;
        }
        if (target === 'expenseAmount') {
            const parsed = toFiniteAmount(value.replace(/\s/g, '').replace(',', '.'));
            if (!Number.isFinite(parsed) || parsed <= 0) return false;
            expenseTotalInput.value = parsed.toFixed(2);
            return true;
        }
        if (target === 'expenseAiComment') {
            expenseAiCommentInput.value = value;
            return true;
        }
        if (target === 'expenseNote') {
            expenseNoteInput.value = value;
            return true;
        }
        return false;
    };

    const updateInvoicePreview = (file) => {
        if (!invoicePreviewImage || !invoicePreviewPdfContainer || !invoicePreviewPlaceholder || !invoicePreviewTitle) {
            return;
        }
        if (maintenanceInvoicePreviewUrl) {
            URL.revokeObjectURL(maintenanceInvoicePreviewUrl);
            maintenanceInvoicePreviewUrl = '';
        }
        maintenanceInvoicePreviewType = '';

        if (!file) {
            invoicePreviewImage.style.display = 'none';
            invoicePreviewImage.removeAttribute('src');
            invoicePreviewPdfContainer.style.display = 'none';
            invoicePreviewPdfContainer.innerHTML = '';
            invoicePreviewPlaceholder.style.display = 'flex';
            invoicePreviewTitle.style.display = 'none';
            window.dispatchEvent(new CustomEvent('ceibo:maintenance-subtab-changed'));
            return;
        }

        maintenanceInvoicePreviewUrl = URL.createObjectURL(file);
        maintenanceInvoicePreviewType = String(file.type || '').toLowerCase();

        invoicePreviewPlaceholder.style.display = 'none';
        invoicePreviewTitle.style.display = 'flex';

        if (maintenanceInvoicePreviewType.includes('pdf')) {
            invoicePreviewImage.style.display = 'none';
            invoicePreviewImage.removeAttribute('src');
            invoicePreviewPdfContainer.style.display = 'block';
            invoicePreviewPdfContainer.innerHTML = `<div class="maintenance-canvas-placeholder" style="min-height:120px;">${t('Chargement PDF...', 'Cargando PDF...')}</div>`;
            renderSelectablePdfPreview(file, invoicePreviewPdfContainer).catch(error => {
                invoicePreviewPdfContainer.innerHTML = `<div class="maintenance-canvas-placeholder" style="min-height:120px;">${escapeHtml(t('Impossible d\'afficher le PDF', 'No se puede mostrar el PDF'))}: ${escapeHtml(String(error?.message || error))}</div>`;
            });
        } else {
            invoicePreviewPdfContainer.style.display = 'none';
            invoicePreviewPdfContainer.innerHTML = '';
            invoicePreviewImage.style.display = 'block';
            invoicePreviewImage.src = maintenanceInvoicePreviewUrl;
        }

        window.dispatchEvent(new CustomEvent('ceibo:maintenance-subtab-changed'));
    };

    invoiceInput.addEventListener('change', () => {
        const file = invoiceInput.files?.[0] || null;
        updateInvoicePreview(file);
        if (invoiceReviewPanel) {
            invoiceReviewPanel.style.display = file ? 'block' : 'none';
        }
    });

    const defaultModelByProvider = {
        openai: 'gpt-4o-mini',
        anthropic: 'claude-3-5-haiku-latest'
    };

    if (llmProviderSelect && llmApiKeyInput && llmModelInput) {
        llmProviderSelect.value = String(localStorage.getItem(MAINTENANCE_LLM_PROVIDER_STORAGE_KEY) || '');
        llmApiKeyInput.value = String(localStorage.getItem(MAINTENANCE_LLM_API_KEY_STORAGE_KEY) || '');
        llmModelInput.value = String(localStorage.getItem(MAINTENANCE_LLM_MODEL_STORAGE_KEY) || '');
        if (!llmModelInput.value && defaultModelByProvider[llmProviderSelect.value]) {
            llmModelInput.value = defaultModelByProvider[llmProviderSelect.value];
        }

        llmProviderSelect.addEventListener('change', () => {
            const provider = String(llmProviderSelect.value || '');
            localStorage.setItem(MAINTENANCE_LLM_PROVIDER_STORAGE_KEY, provider);
            if (!llmModelInput.value && defaultModelByProvider[provider]) {
                llmModelInput.value = defaultModelByProvider[provider];
            }
        });
        llmApiKeyInput.addEventListener('change', () => {
            const sanitized = sanitizeApiKey(llmApiKeyInput.value);
            llmApiKeyInput.value = sanitized;
            localStorage.setItem(MAINTENANCE_LLM_API_KEY_STORAGE_KEY, sanitized);
        });
        llmModelInput.addEventListener('change', () => {
            localStorage.setItem(MAINTENANCE_LLM_MODEL_STORAGE_KEY, String(llmModelInput.value || '').trim());
        });
    }

    const applySupplierSelection = (name) => {
        const selectedName = String(name || '').trim();
        if (!selectedName) return;
        expenseSupplierInput.value = selectedName;
        supplierNameInput.value = selectedName;

        const existingSupplier = findMaintenanceSupplierByName(selectedName);
        if (existingSupplier) {
            supplierContactInput.value = existingSupplier.contact || '';
            supplierPhoneInput.value = existingSupplier.emergencyPhone || '';
            supplierNoteInput.value = existingSupplier.note || '';
            if (existingSupplier.iban) {
                expenseSupplierIbanInput.value = existingSupplier.iban;
                supplierIbanInput.value = existingSupplier.iban;
            }
        }
    };

    const renderSupplierSuggestions = (candidates) => {
        if (!supplierSuggestionsLabel || !supplierSuggestionsContainer) return;
        const safeList = Array.isArray(candidates) ? candidates.filter(item => String(item?.name || '').trim()) : [];
        supplierSuggestionsContainer.innerHTML = '';
        if (!safeList.length) {
            supplierSuggestionsLabel.style.display = 'none';
            supplierSuggestionsContainer.style.display = 'none';
            return;
        }

        supplierSuggestionsLabel.style.display = 'block';
        supplierSuggestionsContainer.style.display = 'flex';

        safeList.slice(0, 3).forEach(item => {
            const btn = document.createElement('button');
            btn.type = 'button';
            btn.className = 'maintenance-supplier-suggestion-btn';
            const confidenceLabel = item.confidence === 'high'
                ? t('fiable', 'fiable')
                : item.confidence === 'medium'
                    ? t('moyen', 'medio')
                    : t('faible', 'baja');
            btn.textContent = `${item.name} · ${confidenceLabel}`;
            btn.addEventListener('click', () => {
                applySupplierSelection(item.name);
                invoiceScanStatus.textContent = `${t('Fournisseur sélectionné', 'Proveedor seleccionado')}: ${item.name}`;
            });
            supplierSuggestionsContainer.appendChild(btn);
        });
    };

    pinColorInput.value = 'red';
    taskStatusInput.value = 'active';
    expenseCurrencyInput.value = 'EUR';
    expenseDateInput.value = new Date().toISOString().slice(0, 10);
    setMaintenanceSchemaManagerVisibility(false);
    setActiveMaintenanceSubtab('tasks');

    loadMaintenanceBoards();
    loadMaintenanceExpenses();
    loadMaintenanceSuppliers();
    if (!maintenanceBoards.length) {
        setMaintenanceSchemaManagerVisibility(true);
    }
    renderMaintenanceBoard();
    renderMaintenanceExpenses();
    renderMaintenanceSuppliers();

    tasksSubtabBtn.addEventListener('click', () => setActiveMaintenanceSubtab('tasks'));
    expensesSubtabBtn.addEventListener('click', () => setActiveMaintenanceSubtab('expenses'));
    suppliersSubtabBtn.addEventListener('click', () => setActiveMaintenanceSubtab('suppliers'));

    toggleSchemaManagerBtn.addEventListener('click', () => {
        setMaintenanceSchemaManagerVisibility(!maintenanceSchemaManagerVisible);
    });

    schemaSelect.addEventListener('change', () => {
        selectedMaintenanceBoardId = String(schemaSelect.value || '');
        activeMaintenanceAnnotationId = null;
        renderMaintenanceBoard();
    });

    addBtn.addEventListener('click', () => {
        const file = schemaInput.files?.[0];
        if (!file || !file.type.startsWith('image/')) {
            setMaintenanceStatus(t('Choisis une image valide pour ajouter un schéma.', 'Elige una imagen válida para añadir un esquema.'), true);
            return;
        }

        const reader = new FileReader();
        reader.onload = () => {
            const boardName = String(schemaNameInput.value || '').trim() || `${t('Schéma', 'Esquema')} ${maintenanceBoards.length + 1}`;
            const newBoard = sanitizeMaintenanceBoard({
                id: `maintenance-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
                name: boardName,
                imageDataUrl: String(reader.result || ''),
                annotations: []
            }, maintenanceBoards.length);

            maintenanceBoards.unshift(newBoard);
            selectedMaintenanceBoardId = newBoard.id;
            persistMaintenanceBoards();
            schemaInput.value = '';
            legendInput.value = '';
            setMaintenanceSchemaManagerVisibility(false);
            renderMaintenanceBoard();
            setMaintenanceStatus(t('Schéma ajouté. Clique sur l\'image pour poser une pastille.', 'Esquema añadido. Haz clic en la imagen para colocar un marcador.'));
        };
        reader.readAsDataURL(file);
    });

    deleteBtn.addEventListener('click', () => {
        const board = getSelectedMaintenanceBoard();
        if (!board) return;

        const confirmed = window.confirm(t(`Supprimer le schéma "${board.name}" et ses pastilles ?`, `¿Eliminar el esquema "${board.name}" y sus marcadores?`));
        if (!confirmed) return;

        maintenanceBoards = maintenanceBoards.filter(item => item.id !== board.id);
        selectedMaintenanceBoardId = maintenanceBoards[0]?.id || null;
        activeMaintenanceAnnotationId = null;
        persistMaintenanceBoards();
        if (!maintenanceBoards.length) {
            setMaintenanceSchemaManagerVisibility(true);
        }
        renderMaintenanceBoard();
        setMaintenanceStatus(t('Schéma supprimé.', 'Esquema eliminado.'));
    });

    canvas.addEventListener('click', event => {
        const board = getSelectedMaintenanceBoard();
        if (!board || !board.imageDataUrl) return;

        const legend = String(legendInput.value || '').trim();
        if (!legend) {
            setMaintenanceStatus(t('Renseigne une légende avant d\'ajouter une pastille.', 'Escribe una leyenda antes de añadir un marcador.'), true);
            return;
        }

        const rect = image.getBoundingClientRect();
        if (!rect.width || !rect.height) return;
        if (event.clientX < rect.left || event.clientX > rect.right || event.clientY < rect.top || event.clientY > rect.bottom) return;

        const xPercent = ((event.clientX - rect.left) / rect.width) * 100;
        const yPercent = ((event.clientY - rect.top) / rect.height) * 100;
        const colorMeta = getMaintenanceColorMeta(pinColorInput.value);

        board.annotations.push({
            id: `maintenance-ann-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
            xPercent: Math.max(0, Math.min(100, xPercent)),
            yPercent: Math.max(0, Math.min(100, yPercent)),
            colorKey: colorMeta.key,
            statusKey: normalizeMaintenanceTaskStatus(taskStatusInput.value),
            legend,
            createdAt: new Date().toISOString()
        });
        board.updatedAt = new Date().toISOString();

        persistMaintenanceBoards();
        legendInput.value = '';
        renderMaintenanceBoard();
        setMaintenanceStatus(t('Pastille ajoutée.', 'Marcador añadido.'));
    });

    scanInvoiceBtn.addEventListener('click', async () => {
        const file = invoiceInput.files?.[0];
        if (!file) {
            invoiceScanStatus.textContent = t('Scan facture: choisis un fichier image ou PDF.', 'Escaneo factura: elige un archivo imagen o PDF.');
            return;
        }

        try {
            invoiceScanStatus.textContent = t('Scan facture: analyse en cours...', 'Escaneo factura: analizando...');
            const text = await runInvoiceScan(file);
            maintenanceLastScannedText = text;
            const extracted = extractInvoiceFields(text, file.name);
            const supplierCandidates = getSupplierCandidatesFromText(text, file.name);
            const supplier = extracted.supplierName;
            const supplierConfidence = extracted.supplierConfidence;
            const supplierSource = extracted.supplierSource;
            const iban = extracted.iban;
            const total = extracted.totalAmount;
            const invoiceDate = extracted.date;
            const hasReliableSupplier = supplier && (
                supplierConfidence === 'high' ||
                (supplierConfidence === 'medium' && ['directory', 'label'].includes(String(supplierSource || '')))
            );
            const matchedSupplier = hasReliableSupplier ? findMaintenanceSupplierByName(supplier) : null;
            let ibanConflictDetected = false;

            if (invoiceDate) {
                expenseDateInput.value = invoiceDate;
            }

            if (invoiceReviewPanel) {
                invoiceReviewPanel.style.display = 'block';
            }

            renderSupplierSuggestions(supplierCandidates);

            if (hasReliableSupplier) {
                applySupplierSelection(supplier);
            }
            if (iban && hasReliableSupplier) {
                const scannedIban = normalizeIbanValue(iban);
                const knownIban = normalizeIbanValue(matchedSupplier?.iban || '');
                if (knownIban && scannedIban && knownIban !== scannedIban) {
                    ibanConflictDetected = true;
                    expenseSupplierIbanInput.value = matchedSupplier.iban;
                    supplierIbanInput.value = matchedSupplier.iban;
                } else {
                    expenseSupplierIbanInput.value = iban;
                    supplierIbanInput.value = iban;
                }
            }
            if (Number.isFinite(total) && total > 0) {
                expenseTotalInput.value = total.toFixed(2);
            }

            if (hasReliableSupplier && matchedSupplier) {
                supplierContactInput.value = matchedSupplier.contact || '';
                supplierPhoneInput.value = matchedSupplier.emergencyPhone || '';
                supplierNoteInput.value = matchedSupplier.note || '';
                if (!iban && matchedSupplier.iban) {
                    expenseSupplierIbanInput.value = matchedSupplier.iban;
                    supplierIbanInput.value = matchedSupplier.iban;
                    }
            }

            const rawCandidateLines = text
                .split(/\r?\n/)
                .map(line => line.trim())
                .filter(line => /\d/.test(line) && /[€$]|\b\d+[.,]\d{2}\b/.test(line))
                .slice(0, 12);
            const analysisLines = text
                .split(/\r?\n/)
                .map(line => line.trim())
                .filter(line => line.length >= 4)
                .slice(0, 30);
            const candidateLines = rawCandidateLines.map(line => `${line} ; ; ;`);
            if (candidateLines.length && !expenseLinesInput.value.trim()) {
                expenseLinesInput.value = candidateLines.join('\n');
            }

            const autoComment = buildExpensePreventionComment({
                supplierName: supplier,
                totalAmount: total,
                detectedLines: analysisLines
            });
            expenseAiCommentInput.value = autoComment;

            const summaryParts = [];
            if (supplier) {
                const confidenceLabel = supplierConfidence === 'high'
                    ? t('fiable', 'fiable')
                    : supplierConfidence === 'medium'
                        ? t('moyen', 'medio')
                        : t('faible', 'baja');
                summaryParts.push(`${t('fournisseur', 'proveedor')}: ${supplier} (${confidenceLabel}/${String(supplierSource || 'heuristic')})`);
                if (supplierConfidence === 'low') {
                    summaryParts.push(t('vérifier fournisseur', 'verificar proveedor'));
                }
            }
            if (invoiceDate) summaryParts.push(`${t('date', 'fecha')}: ${invoiceDate}`);
            if (Number.isFinite(total) && total > 0) summaryParts.push(`${t('montant', 'importe')}: ${total.toFixed(2)}`);
            if (iban) summaryParts.push(`IBAN: ${iban}`);
            if (ibanConflictDetected) {
                summaryParts.push(t('IBAN en conflit: annuaire fournisseur conservé', 'IBAN en conflicto: se conserva el del directorio'));
            }

            invoiceScanStatus.textContent = summaryParts.length
                ? `${t('Scan facture: extraction terminée', 'Escaneo factura: extracción terminada')} · ${summaryParts.join(' · ')}`
                : t('Scan facture: extraction terminée (vérifie les champs).', 'Escaneo factura: extracción terminada (verifica los campos).');
        } catch (error) {
            renderSupplierSuggestions([]);
            if (invoiceReviewPanel) {
                invoiceReviewPanel.style.display = 'none';
            }
            invoiceScanStatus.textContent = `${t('Scan facture: échec', 'Escaneo factura: error')} (${String(error?.message || error)})`;
        }
    });

    if (pasteSelectedTextBtn && manualPasteTargetSelect) {
        pasteSelectedTextBtn.addEventListener('click', async () => {
        try {
            const clipboardText = await navigator.clipboard.readText();
            if (!clipboardText.trim()) {
                invoiceScanStatus.textContent = t('Presse-papiers vide.', 'Portapapeles vacío.');
                return;
            }
            const ok = applyClipboardTextToTarget(clipboardText);
            invoiceScanStatus.textContent = ok
                ? t('Texte collé dans le champ sélectionné.', 'Texto pegado en el campo seleccionado.')
                : t('Texte incompatible avec ce champ (ex: montant).', 'Texto incompatible con este campo (ej: importe).');
        } catch (error) {
            invoiceScanStatus.textContent = `${t('Lecture presse-papiers impossible', 'No se puede leer el portapapeles')}: ${String(error?.message || error)}`;
        }
        });
    }

    if (runAltLlmBtn && llmProviderSelect && llmApiKeyInput && llmModelInput) {
        runAltLlmBtn.addEventListener('click', async () => {
        const provider = String(llmProviderSelect.value || '').trim();
        const apiKey = sanitizeApiKey(llmApiKeyInput.value);
        const model = String(llmModelInput.value || '').trim();
        if (!provider || !apiKey || !model) {
            invoiceScanStatus.textContent = t('Configure provider, API key et model.', 'Configura proveedor, clave API y modelo.');
            return;
        }

        llmApiKeyInput.value = apiKey;

        localStorage.setItem(MAINTENANCE_LLM_PROVIDER_STORAGE_KEY, provider);
        localStorage.setItem(MAINTENANCE_LLM_API_KEY_STORAGE_KEY, apiKey);
        localStorage.setItem(MAINTENANCE_LLM_MODEL_STORAGE_KEY, model);

        const totalValue = toFiniteAmount(expenseTotalInput.value);
        const prompt = buildAlternativeLlmPrompt({
            supplierName: expenseSupplierInput.value,
            invoiceDate: expenseDateInput.value,
            iban: expenseSupplierIbanInput.value,
            totalAmount: Number.isFinite(totalValue) ? totalValue : null,
            currency: expenseCurrencyInput.value,
            rawText: maintenanceLastScannedText
        });

        try {
            invoiceScanStatus.textContent = t('Analyse IA en cours...', 'Análisis IA en curso...');
            const analysis = await requestAlternativeLlmAnalysis({ provider, apiKey, model, prompt });
            if (!analysis) {
                invoiceScanStatus.textContent = t('IA: réponse vide.', 'IA: respuesta vacía.');
                return;
            }
            expenseAiCommentInput.value = analysis;
            invoiceScanStatus.textContent = t('Analyse IA injectée dans commentaire IA.', 'Análisis IA insertado en comentario IA.');
        } catch (error) {
            const errorMessage = String(error?.message || error);
            const isQuotaError = /\b429\b/.test(errorMessage) || /quota|rate limit|too many requests/i.test(errorMessage);
            if (isQuotaError) {
                const amount = toFiniteAmount(expenseTotalInput.value);
                const detectedLines = String(maintenanceLastScannedText || '')
                    .split(/\r?\n/)
                    .map(line => line.trim())
                    .filter(Boolean)
                    .slice(0, 30);
                expenseAiCommentInput.value = buildExpensePreventionComment({
                    supplierName: expenseSupplierInput.value,
                    totalAmount: Number.isFinite(amount) ? amount : null,
                    detectedLines
                });
                invoiceScanStatus.textContent = t(
                    'Quota IA atteint: analyse locale de secours injectée dans commentaire IA.',
                    'Cuota IA alcanzada: análisis local de respaldo insertado en comentario IA.'
                );
                return;
            }
            const readableError = formatAlternativeLlmRuntimeError(error, provider);
            invoiceScanStatus.textContent = `${t('Échec analyse IA', 'Error análisis IA')}: ${readableError}`;
        }
        });
    }

    if (testAltLlmBtn && llmProviderSelect && llmApiKeyInput && llmModelInput) {
        testAltLlmBtn.addEventListener('click', async () => {
        const provider = String(llmProviderSelect.value || '').trim();
        const apiKey = sanitizeApiKey(llmApiKeyInput.value);
        const model = String(llmModelInput.value || '').trim();
        if (!provider || !apiKey || !model) {
            invoiceScanStatus.textContent = t('Configure provider, API key et model.', 'Configura proveedor, clave API y modelo.');
            return;
        }

        llmApiKeyInput.value = apiKey;

        localStorage.setItem(MAINTENANCE_LLM_PROVIDER_STORAGE_KEY, provider);
        localStorage.setItem(MAINTENANCE_LLM_API_KEY_STORAGE_KEY, apiKey);
        localStorage.setItem(MAINTENANCE_LLM_MODEL_STORAGE_KEY, model);

        try {
            invoiceScanStatus.textContent = t('Test connexion API en cours...', 'Prueba conexión API en curso...');
            await testAlternativeLlmConnection({ provider, apiKey, model });
            invoiceScanStatus.textContent = t('Connexion API OK ✅', 'Conexión API OK ✅');
        } catch (error) {
            const readableError = formatAlternativeLlmRuntimeError(error, provider);
            invoiceScanStatus.textContent = `${t('Test API échoué', 'Prueba API fallida')}: ${readableError}`;
        }
        });
    }

    addExpenseBtn.addEventListener('click', () => {
        const totalAmount = toFiniteAmount(expenseTotalInput.value);
        if (!Number.isFinite(totalAmount) || totalAmount <= 0) {
            invoiceScanStatus.textContent = t('Renseigne un montant total valide.', 'Introduce un importe total válido.');
            return;
        }

        const lines = parseExpenseLinesText(expenseLinesInput.value);
        const entry = sanitizeMaintenanceExpense({
            id: `expense-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
            invoiceName: invoiceInput.files?.[0]?.name || '',
            date: expenseDateInput.value || new Date().toISOString().slice(0, 10),
            supplierName: expenseSupplierInput.value,
            supplierIban: expenseSupplierIbanInput.value,
            payer: expensePayerSelect.value,
            paymentStatus: expensePaymentStatusSelect.value,
            totalAmount,
            currency: expenseCurrencyInput.value || 'EUR',
            lines,
            note: expenseNoteInput.value,
            aiComment: expenseAiCommentInput.value,
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString()
        }, maintenanceExpenses.length);

        maintenanceExpenses.unshift(entry);
        persistMaintenanceExpenses();
        renderMaintenanceExpenses();

        expenseTotalInput.value = '';
        expenseLinesInput.value = '';
        expenseNoteInput.value = '';
        expenseAiCommentInput.value = '';
        invoiceInput.value = '';
        maintenanceLastScannedText = '';
        if (invoiceReviewPanel) {
            invoiceReviewPanel.style.display = 'none';
        }
        updateInvoicePreview(null);
        renderSupplierSuggestions([]);
        invoiceScanStatus.textContent = t('Dépense ajoutée.', 'Gasto añadido.');
    });

    addSupplierBtn.addEventListener('click', () => {
        const name = String(supplierNameInput.value || '').trim();
        if (!name) {
            return;
        }

        const entry = sanitizeMaintenanceSupplier({
            id: `supplier-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
            name,
            contact: supplierContactInput.value,
            emergencyPhone: supplierPhoneInput.value,
            iban: supplierIbanInput.value,
            note: supplierNoteInput.value,
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString()
        }, maintenanceSuppliers.length);

        const existingIndex = maintenanceSuppliers.findIndex(item => item.name.toLowerCase() === entry.name.toLowerCase());
        if (existingIndex >= 0) {
            maintenanceSuppliers[existingIndex] = entry;
        } else {
            maintenanceSuppliers.push(entry);
        }

        persistMaintenanceSuppliers();
        renderMaintenanceSuppliers();

        supplierNameInput.value = '';
        supplierContactInput.value = '';
        supplierPhoneInput.value = '';
        supplierIbanInput.value = '';
        supplierNoteInput.value = '';
    });
}

function setCloudAuthStatus(message, isError = false) {
    const status = document.getElementById('cloudAuthStatus');
    if (!status) return;
    status.textContent = message;
    status.style.color = isError ? '#ff8f8f' : '';
}

function unsubscribeCloudAuthSubscription() {
    const subscription =
        cloudAuthSubscription?.data?.subscription ||
        cloudAuthSubscription?.subscription ||
        cloudAuthSubscription;

    if (subscription?.unsubscribe) {
        subscription.unsubscribe();
    }

    cloudAuthSubscription = null;
}

function updateCloudAuthUi() {
    const emailSignInBtn = document.getElementById('cloudEmailSignInBtn');
    const emailSignUpBtn = document.getElementById('cloudEmailSignUpBtn');
    const signOutBtn = document.getElementById('cloudSignOutBtn');

    if (cloudAuthUser) {
        const label = cloudAuthUser.email || cloudAuthUser.user_metadata?.full_name || cloudAuthUser.id;
        setCloudAuthStatus(t(`Utilisateur: connecté (${label})`, `Usuario: conectado (${label})`));
    } else {
        setCloudAuthStatus(t('Utilisateur: non connecté', 'Usuario: no conectado'));
    }

    if (emailSignInBtn) emailSignInBtn.disabled = !cloudClient || !!cloudAuthUser;
    if (emailSignUpBtn) emailSignUpBtn.disabled = !cloudClient || !!cloudAuthUser;
    if (signOutBtn) signOutBtn.disabled = !cloudClient || !cloudAuthUser;
}

function readCloudUserCredentials() {
    const email = String(document.getElementById('cloudEmailInput')?.value || '').trim().toLowerCase();
    const password = String(document.getElementById('cloudUserPasswordInput')?.value || '').trim();
    return { email, password };
}

function normalizeEmailForCompare(value) {
    return String(value || '').trim().toLowerCase();
}

async function checkCloudEmailAllowed(email) {
    if (!cloudClient) return { allowed: false, reason: 'cloud-not-ready' };
    const normalizedEmail = normalizeEmailForCompare(email);
    if (!normalizedEmail) return { allowed: false, reason: 'missing-email' };

    try {
        const { data: exactData, error: exactError } = await cloudClient
            .from(CLOUD_ALLOWED_USERS_TABLE)
            .select('email')
            .eq('email', normalizedEmail)
            .limit(1);

        if (exactError) {
            const message = String(exactError.message || '');
            const code = String(exactError.code || '');
            if (code === '42P01' || message.toLowerCase().includes('does not exist')) {
                return { allowed: true, reason: 'table-missing' };
            }
            return { allowed: false, reason: `query-error:${message}` };
        }

        if (Array.isArray(exactData) && exactData.length > 0) {
            return { allowed: true, reason: 'listed' };
        }

        const { data: allData, error: allError } = await cloudClient
            .from(CLOUD_ALLOWED_USERS_TABLE)
            .select('email')
            .limit(1000);

        if (allError) {
            const message = String(allError.message || '');
            return { allowed: false, reason: `query-error:${message}` };
        }

        const normalizedAllowedEmails = (Array.isArray(allData) ? allData : [])
            .map(row => normalizeEmailForCompare(row?.email))
            .filter(Boolean);

        if (normalizedAllowedEmails.includes(normalizedEmail)) {
            return { allowed: true, reason: 'listed-normalized' };
        }

        return { allowed: false, reason: 'not-listed' };
    } catch (error) {
        return { allowed: false, reason: `exception:${String(error?.message || error)}` };
    }
}

async function enforceCloudWhitelistForCurrentUser() {
    if (!cloudClient || !cloudAuthUser?.email || cloudWhitelistCheckInFlight) return;
    cloudWhitelistCheckInFlight = true;

    try {
        const email = String(cloudAuthUser.email || '').trim().toLowerCase();
        const verdict = await checkCloudEmailAllowed(email);
        if (!verdict.allowed) {
            await cloudClient.auth.signOut();
            cloudAuthUser = null;
            updateCloudAuthUi();
            if (verdict.reason === 'not-listed') {
                setCloudAuthStatus(t(`Utilisateur refusé: ${email} absent de ${CLOUD_ALLOWED_USERS_TABLE}`, `Usuario rechazado: ${email} ausente de ${CLOUD_ALLOWED_USERS_TABLE}`), true);
            } else {
                setCloudAuthStatus(t(`Contrôle accès impossible: ${verdict.reason}`, `Control de acceso imposible: ${verdict.reason}`), true);
            }
        }
    } catch (_error) {
    } finally {
        cloudWhitelistCheckInFlight = false;
    }
}

async function refreshCloudAuthSession() {
    if (!cloudClient) {
        cloudAuthUser = null;
        updateCloudAuthUi();
        await applyAuthGateState({ clearWhenLocked: true });
        return;
    }

    try {
        const { data, error } = await cloudClient.auth.getSession();
        if (error) throw error;
        cloudAuthUser = data?.session?.user || null;
        updateCloudAuthUi();
        await enforceCloudWhitelistForCurrentUser();
        await applyAuthGateState({ clearWhenLocked: true });
    } catch (_error) {
        cloudAuthUser = null;
        updateCloudAuthUi();
        await applyAuthGateState({ clearWhenLocked: true });
    }
}

function setNavLogStatus(message, isError = false) {
    const status = document.getElementById('navLogStatus');
    if (!status) return;
    status.textContent = message;
    status.style.color = isError ? '#ff8f8f' : '';
}

function scheduleCloudLogbookPush() {
    if (!isCloudReady()) return;

    if (cloudLogbookPushTimer) {
        clearTimeout(cloudLogbookPushTimer);
    }

    cloudLogbookPushTimer = setTimeout(async () => {
        cloudLogbookPushTimer = null;
        if (!isCloudReady()) return;
        try {
            await pushRoutesToCloud();
            setCloudStatus(t(`Cloud synchro auto · ${getSavedRoutes().length} route(s) · logs OK`, `Nube sincronización auto · ${getSavedRoutes().length} ruta(s) · logs OK`));
        } catch (error) {
            setCloudStatus(t(`Synchro logs impossible: ${formatCloudError(error)}`, `Sincronización logs imposible: ${formatCloudError(error)}`), true);
        }
    }, CLOUD_LOGBOOK_PUSH_DEBOUNCE_MS);
}

function saveNavLogEntries() {
    saveArrayToStorage(NAV_LOG_STORAGE_KEY, navLogEntries);
    scheduleCloudLogbookPush();
}

function formatNowTimeLabel() {
    const now = new Date();
    return now.toLocaleTimeString(getCurrentLocale(), { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function renderAiTrafficEntries() {
    const list = document.getElementById('aiTrafficList');
    if (!list) return;

    list.innerHTML = aiTrafficEntries
        .slice(-180)
        .map(item => `<div class="ai-traffic-item"><strong>${escapeHtml(item.time)}</strong> · ${escapeHtml(item.message)}</div>`)
        .join('');

    list.scrollTop = list.scrollHeight;
}

function showAiTrafficOverlay() {
    const overlay = document.getElementById('aiTrafficOverlay');
    if (!overlay) return;
    overlay.style.display = 'block';
}

function hideAiTrafficOverlay() {
    const overlay = document.getElementById('aiTrafficOverlay');
    if (!overlay) return;
    overlay.style.display = 'none';
}

function clearAiTrafficOverlay() {
    aiTrafficEntries = [];
    renderAiTrafficEntries();
}

function pushAiTrafficLog(message) {
    aiTrafficEntries.push({
        time: formatNowTimeLabel(),
        message: String(message || '')
    });

    if (aiTrafficEntries.length > 260) {
        aiTrafficEntries = aiTrafficEntries.slice(aiTrafficEntries.length - 260);
    }

    renderAiTrafficEntries();
}

function beginAiTrafficSession(title) {
    if (aiTrafficAutoHideTimer) {
        clearTimeout(aiTrafficAutoHideTimer);
        aiTrafficAutoHideTimer = null;
    }

    clearAiTrafficOverlay();
    showAiTrafficOverlay();
    pushAiTrafficLog(`Session démarrée: ${title}`);
}

function endAiTrafficSession(message) {
    pushAiTrafficLog(message || 'Session terminée');

    if (aiTrafficAutoHideTimer) {
        clearTimeout(aiTrafficAutoHideTimer);
    }

    aiTrafficAutoHideTimer = setTimeout(() => {
        hideAiTrafficOverlay();
    }, 9000);
}

function getManualNavFormData() {
    const watchTimeInput = document.getElementById('watchTimeInput');
    const watchCrewInput = document.getElementById('watchCrewInput');
    const watchHeadingInput = document.getElementById('watchHeadingInput');
    const watchWindDirInput = document.getElementById('watchWindDirInput');
    const watchWindSpeedInput = document.getElementById('watchWindSpeedInput');
    const watchSeaStateInput = document.getElementById('watchSeaStateInput');
    const watchSailConfigInput = document.getElementById('watchSailConfigInput');
    const watchBarometerInput = document.getElementById('watchBarometerInput');
    const watchLogNmInput = document.getElementById('watchLogNmInput');
    const watchEventsInput = document.getElementById('watchEventsInput');

    const isoDate = watchTimeInput?.value ? `${watchTimeInput.value}:00` : new Date().toISOString();

    return {
        watchTimeIso: isoDate,
        watchCrew: String(watchCrewInput?.value || '').trim(),
        headingDeg: Number.parseFloat(String(watchHeadingInput?.value || '')),
        windDirectionDeg: Number.parseFloat(String(watchWindDirInput?.value || '')),
        windSpeedKn: Number.parseFloat(String(watchWindSpeedInput?.value || '')),
        seaState: String(watchSeaStateInput?.value || '').trim(),
        sailConfig: String(watchSailConfigInput?.value || '').trim(),
        barometerHpa: Number.parseFloat(String(watchBarometerInput?.value || '')),
        logDistanceNm: Number.parseFloat(String(watchLogNmInput?.value || '')),
        events: String(watchEventsInput?.value || '').trim()
    };
}

function drawHeelSpeedChart() {
    const canvas = document.getElementById('heelSpeedChart');
    if (!canvas) return;
    const context = canvas.getContext('2d');
    if (!context) return;

    const width = canvas.width;
    const height = canvas.height;

    context.clearRect(0, 0, width, height);
    context.fillStyle = '#0f1d2b';
    context.fillRect(0, 0, width, height);

    context.strokeStyle = 'rgba(255,255,255,0.15)';
    context.lineWidth = 1;
    context.beginPath();
    context.moveTo(36, 10);
    context.lineTo(36, height - 28);
    context.lineTo(width - 12, height - 28);
    context.stroke();

    const points = navLogEntries
        .filter(item => Number.isFinite(item?.heelDeg) && Number.isFinite(item?.speedKn))
        .slice(-140);

    if (!points.length) {
        context.fillStyle = '#9cb5c9';
        context.font = '12px Arial';
        context.fillText(t('Aucune donnée inclinaison/vitesse', 'Sin datos inclinación/velocidad'), 50, 26);
        return;
    }

    const maxHeel = Math.max(20, ...points.map(item => Math.abs(item.heelDeg)));
    const maxSpeed = Math.max(8, ...points.map(item => item.speedKn));

    points.forEach(item => {
        const x = 36 + (Math.abs(item.heelDeg) / maxHeel) * (width - 52);
        const y = (height - 28) - (item.speedKn / maxSpeed) * (height - 42);
        context.fillStyle = '#8fe7ff';
        context.beginPath();
        context.arc(x, y, 2.4, 0, Math.PI * 2);
        context.fill();
    });

    context.fillStyle = '#9cb5c9';
    context.font = '11px Arial';
    context.fillText(t(`Inclinaison max: ${maxHeel.toFixed(1)}°`, `Inclinación máx: ${maxHeel.toFixed(1)}°`), 40, height - 10);
    context.fillText(t(`Vitesse max: ${maxSpeed.toFixed(1)} kn`, `Velocidad máx: ${maxSpeed.toFixed(1)} kn`), width - 160, 20);
}

function renderNavLogList() {
    const container = document.getElementById('navLogList');
    if (!container) return;

    if (!Array.isArray(navLogEntries) || navLogEntries.length === 0) {
        container.innerHTML = `<div class="log-card">${t('Aucune entrée navigation pour le moment.', 'No hay entradas de navegación por ahora.')}</div>`;
        drawHeelSpeedChart();
        return;
    }

    const rows = navLogEntries
        .slice(-30)
        .reverse()
        .map(item => {
            const speed = Number.isFinite(item?.speedKn) ? `${item.speedKn.toFixed(1)} kn` : 'N/A';
            const heel = Number.isFinite(item?.heelDeg) ? `${item.heelDeg.toFixed(1)}°` : 'N/A';
            const heading = Number.isFinite(item?.headingDeg) ? `${Math.round(item.headingDeg)}°` : 'N/A';
            const windDir = Number.isFinite(item?.windDirectionDeg) ? `${Math.round(item.windDirectionDeg)}°` : 'N/A';
            const windSpeed = Number.isFinite(item?.windSpeedKn) ? `${item.windSpeedKn.toFixed(1)} kn` : 'N/A';
            const seaState = item?.seaState ? String(item.seaState) : 'N/A';
            const sailConfig = item?.sailConfig ? escapeHtml(item.sailConfig) : 'N/A';
            const baro = Number.isFinite(item?.barometerHpa) ? `${item.barometerHpa.toFixed(1)} hPa` : 'N/A';
            const loch = Number.isFinite(item?.logDistanceNm) ? `${item.logDistanceNm.toFixed(1)} NM` : 'N/A';
            const crew = item?.watchCrew ? escapeHtml(item.watchCrew) : 'N/A';
            const events = item?.events ? `<br><em>${escapeHtml(item.events)}</em>` : '';
            const positionLine = Number.isFinite(item?.lat) && Number.isFinite(item?.lng)
                ? `Lat: ${Number(item.lat).toFixed(5)} · Lng: ${Number(item.lng).toFixed(5)}`
                : t('Lat/Lng: N/A', 'Lat/Lng: N/A');
            const watchTime = item?.watchTimeIso ? formatDateTimeFr(item.watchTimeIso) : formatDateTimeFr(item?.timestamp);

            return `<div class="log-card"><strong>${watchTime}</strong><br>` +
                `${t('Quart', 'Guardia')}: ${crew} · ${t('Source', 'Origen')}: ${escapeHtml(item?.source || t('manual', 'manual'))}<br>` +
                `${positionLine}<br>` +
                `${t('Cap', 'Rumbo')}: ${heading} · ${t('Vent', 'Viento')}: ${windDir} / ${windSpeed} · ${t('Mer', 'Mar')}: ${escapeHtml(seaState)}<br>` +
                `${t('Voilure', 'Velamen')}: ${sailConfig} · ${t('Baro', 'Baro')}: ${baro} · ${t('Loch', 'Corredera')}: ${loch}<br>` +
                `${t('Vitesse', 'Velocidad')}: ${speed} · ${t('Inclinaison', 'Inclinación')}: ${heel}${events}</div>`;
        })
        .join('');

    container.innerHTML = rows;
    drawHeelSpeedChart();
}

function appendNavLogEntry({ lat, lng, speedKn, heelDeg, source = 'gps', watchTimeIso = null, watchCrew = '', headingDeg = null, windDirectionDeg = null, windSpeedKn = null, seaState = '', sailConfig = '', barometerHpa = null, logDistanceNm = null, events = '' }) {
    const hasPosition = Number.isFinite(lat) && Number.isFinite(lng);

    if (!hasPosition && source !== 'manual') return;

    navLogEntries.push({
        id: `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
        timestamp: new Date().toISOString(),
        watchTimeIso,
        lat: hasPosition ? lat : null,
        lng: hasPosition ? lng : null,
        speedKn: Number.isFinite(speedKn) ? speedKn : null,
        heelDeg: Number.isFinite(heelDeg) ? heelDeg : null,
        source,
        watchCrew,
        headingDeg: Number.isFinite(headingDeg) ? headingDeg : null,
        windDirectionDeg: Number.isFinite(windDirectionDeg) ? windDirectionDeg : null,
        windSpeedKn: Number.isFinite(windSpeedKn) ? windSpeedKn : null,
        seaState,
        sailConfig,
        barometerHpa: Number.isFinite(barometerHpa) ? barometerHpa : null,
        logDistanceNm: Number.isFinite(logDistanceNm) ? logDistanceNm : null,
        events
    });

    if (navLogEntries.length > 1200) {
        navLogEntries = navLogEntries.slice(navLogEntries.length - 1200);
    }

    saveNavLogEntries();
    renderNavLogList();
}

function addManualNavigationLogEntry() {
    const manualData = getManualNavFormData();

    appendNavLogEntry({
        lat: null,
        lng: null,
        speedKn: navLatestSpeedKn,
        heelDeg: navLatestHeelDeg,
        source: 'manual',
        ...manualData
    });

    setNavLogStatus(t(`Entrée jour de bord ajoutée · ${navLogEntries.length} entrée(s)`, `Entrada de bitácora añadida · ${navLogEntries.length} entrada(s)`));
}

function handleNavOrientation(event) {
    const gamma = Number(event?.gamma);
    if (!Number.isFinite(gamma)) return;
    navLatestHeelDeg = gamma;
}

function ensureMotionListenerBound() {
    if (navMotionListenerBound) return;
    window.addEventListener('deviceorientation', handleNavOrientation, true);
    navMotionListenerBound = true;
}

async function requestMotionPermissionIfNeeded() {
    try {
        if (typeof DeviceOrientationEvent !== 'undefined' && typeof DeviceOrientationEvent.requestPermission === 'function') {
            const permission = await DeviceOrientationEvent.requestPermission();
            if (permission !== 'granted') {
                setNavLogStatus(t('Inclinaison refusée par iOS.', 'Inclinación rechazada por iOS.'), true);
                return false;
            }
        }

        ensureMotionListenerBound();
        setNavLogStatus(t('Capteur inclinaison activé.', 'Sensor de inclinación activado.'));
        return true;
    } catch (_error) {
        setNavLogStatus(t('Impossible d’activer le capteur inclinaison.', 'No se puede activar el sensor de inclinación.'), true);
        return false;
    }
}

function startNavigationLogging() {
    if (!navigator.geolocation) {
        setNavLogStatus(t('GPS non disponible sur cet appareil.', 'GPS no disponible en este dispositivo.'), true);
        return;
    }

    if (navWatchId !== null) {
        setNavLogStatus(t('Log GPS déjà actif.', 'Log GPS ya activo.'));
        return;
    }

    navWatchId = navigator.geolocation.watchPosition(
        position => {
            const latitude = Number(position?.coords?.latitude);
            const longitude = Number(position?.coords?.longitude);
            const speedMs = Number(position?.coords?.speed);
            navLatestSpeedKn = Number.isFinite(speedMs) ? speedMs * 1.943844 : null;

            appendNavLogEntry({
                lat: latitude,
                lng: longitude,
                speedKn: navLatestSpeedKn,
                heelDeg: navLatestHeelDeg,
                source: 'gps-watch'
            });

            setNavLogStatus(t(`Log GPS actif · ${navLogEntries.length} point(s)`, `Log GPS activo · ${navLogEntries.length} punto(s)`));
        },
        error => {
            const details = error?.message ? `: ${error.message}` : '';
            setNavLogStatus(t(`Erreur GPS${details}`, `Error GPS${details}`), true);
        },
        {
            enableHighAccuracy: true,
            maximumAge: 0,
            timeout: 10000
        }
    );
}

function stopNavigationLogging() {
    if (navWatchId !== null && navigator.geolocation) {
        navigator.geolocation.clearWatch(navWatchId);
        navWatchId = null;
    }
    setNavLogStatus(t(`Log GPS arrêté · ${navLogEntries.length} point(s) enregistrés.`, `Log GPS detenido · ${navLogEntries.length} punto(s) guardado(s).`));
}

function clearNavigationLogbook() {
    stopNavigationLogging();
    navLogEntries = [];
    saveNavLogEntries();
    renderNavLogList();
    setNavLogStatus(t('Journal navigation effacé.', 'Diario de navegación borrado.'));
}

function loadNavigationLogbook() {
    navLogEntries = loadArrayFromStorage(NAV_LOG_STORAGE_KEY);
    const watchTimeInput = document.getElementById('watchTimeInput');
    if (watchTimeInput) {
        watchTimeInput.value = toLocalDateTimeInputValue(new Date());
    }
    renderNavLogList();
}

function saveEngineLogEntries() {
    saveArrayToStorage(ENGINE_LOG_STORAGE_KEY, engineLogEntries);
    scheduleCloudLogbookPush();
}

function renderEngineLogList() {
    const container = document.getElementById('engineLogList');
    if (!container) return;

    if (!Array.isArray(engineLogEntries) || engineLogEntries.length === 0) {
        container.innerHTML = `<div class="log-card">${t('Aucune entrée moteur pour le moment.', 'No hay entradas de motor por ahora.')}</div>`;
        return;
    }

    container.innerHTML = engineLogEntries
        .slice()
        .reverse()
        .map(entry => {
            const hours = Number.isFinite(entry?.hours) ? `${entry.hours.toFixed(1)} h` : 'N/A';
            const fuel = Number.isFinite(entry?.fuelAddedL) ? `${entry.fuelAddedL.toFixed(1)} L` : '0 L';
            return `<div class="log-card"><strong>${formatDateTimeFr(entry?.timestamp)}</strong><br>${t('Compteur', 'Contador')}: ${hours} · ${t('Carburant', 'Combustible')}: ${fuel}<br>${escapeHtml(entry?.note || '')}</div>`;
        })
        .join('');
}

function addEngineLogEntryFromForm() {
    const hoursInput = document.getElementById('engineHoursInput');
    const fuelInput = document.getElementById('fuelAddedInput');
    const noteInput = document.getElementById('engineLogNoteInput');

    const hours = parseFloat(String(hoursInput?.value || ''));
    const fuelAddedL = parseFloat(String(fuelInput?.value || ''));
    const note = String(noteInput?.value || '').trim();

    if (!Number.isFinite(hours)) {
        alert(t('Renseigne le compteur moteur (heures).', 'Introduce el contador motor (horas).'));
        return;
    }

    engineLogEntries.push({
        id: `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
        timestamp: new Date().toISOString(),
        hours,
        fuelAddedL: Number.isFinite(fuelAddedL) ? Math.max(0, fuelAddedL) : 0,
        note
    });

    if (engineLogEntries.length > 800) {
        engineLogEntries = engineLogEntries.slice(engineLogEntries.length - 800);
    }

    saveEngineLogEntries();
    renderEngineLogList();

    if (fuelInput) fuelInput.value = '';
    if (noteInput) noteInput.value = '';
}

function clearEngineLogbook() {
    engineLogEntries = [];
    saveEngineLogEntries();
    renderEngineLogList();
}

function loadEngineLogbook() {
    engineLogEntries = loadArrayFromStorage(ENGINE_LOG_STORAGE_KEY);
    renderEngineLogList();
}

function ensureWeatherFocusMarker() {
    if (!map || weatherFocusMarker) return;

    const icon = L.divIcon({
        className: 'weather-focus-icon',
        html: '<div class="weather-focus-icon__dot"></div>',
        iconSize: [18, 18],
        iconAnchor: [9, 9]
    });

    weatherFocusMarker = L.marker([0, 0], {
        icon,
        keyboard: false,
        interactive: false,
        zIndexOffset: 1400
    });
}

function setWeatherPointerPlacementMode(enabled) {
    weatherPointerPlacementMode = Boolean(enabled);
    if (!map) return;

    const shouldUseCrosshair = weatherPointerPlacementMode && activeTabName === 'weather' && !measureModeEnabled;
    map.getContainer().style.cursor = shouldUseCrosshair ? 'crosshair' : '';

    const status = document.getElementById('weatherOutlookStatus');
    if (status && weatherPointerPlacementMode) {
        status.textContent = t('Météo: clique sur la carte pour placer le pointeur.', 'Meteo: haz clic en el mapa para colocar el puntero.');
    }
}

function setWeatherFocusPoint(latlng, { refresh = true, sourceLabel = t('pointeur carte', 'puntero mapa') } = {}) {
    if (!Number.isFinite(latlng?.lat) || !Number.isFinite(latlng?.lng)) return;

    weatherFocusPoint = { lat: latlng.lat, lng: latlng.lng };
    ensureWeatherFocusMarker();
    if (weatherFocusMarker) {
        weatherFocusMarker.setLatLng([weatherFocusPoint.lat, weatherFocusPoint.lng]);
        if (!map.hasLayer(weatherFocusMarker)) {
            weatherFocusMarker.addTo(map);
        }
    }

    const status = document.getElementById('weatherOutlookStatus');
    if (status) {
        status.textContent = `${t('Météo', 'Meteo')}: ${sourceLabel} (${weatherFocusPoint.lat.toFixed(4)}, ${weatherFocusPoint.lng.toFixed(4)})`;
    }

    if (refresh) {
        refreshWeatherOutlook();
    }
}

function computeWeatherImpactLabel(weather) {
    const wind = Number(weather?.windSpeed);
    const wave = Number(weather?.waveHeight);

    if ((Number.isFinite(wind) && wind > 24) || (Number.isFinite(wave) && wave > 2.3)) {
        return { text: t('À risque', 'Con riesgo'), className: 'weather-card weather-card--risk' };
    }
    if ((Number.isFinite(wind) && wind > 18) || (Number.isFinite(wave) && wave > 1.6)) {
        return { text: t('Modéré', 'Moderado'), className: 'weather-card' };
    }
    return { text: t('Favorable', 'Favorable'), className: 'weather-card weather-card--ok' };
}

function getWeatherReferenceCoordinates(forceMapCenter = false) {
    if (map && forceMapCenter) {
        const center = map.getCenter();
        return { lat: center.lat, lng: center.lng, source: t('centre carte', 'centro mapa') };
    }

    if (weatherFocusPoint && Number.isFinite(weatherFocusPoint.lat) && Number.isFinite(weatherFocusPoint.lng)) {
        return { lat: weatherFocusPoint.lat, lng: weatherFocusPoint.lng, source: t('pointeur météo', 'puntero meteo') };
    }

    if (routePoints.length > 0) {
        const last = routePoints[routePoints.length - 1];
        return { lat: last.lat, lng: last.lng, source: t('dernier waypoint', 'último waypoint') };
    }

    if (map) {
        const center = map.getCenter();
        return { lat: center.lat, lng: center.lng, source: t('centre carte', 'centro mapa') };
    }

    return null;
}

async function refreshWeatherOutlook({ forceMapCenter = false } = {}) {
    const status = document.getElementById('weatherOutlookStatus');
    const list = document.getElementById('weatherOutlookList');
    if (!status || !list) return;

    const focus = getWeatherReferenceCoordinates(forceMapCenter);
    if (!focus) {
        status.textContent = t('Météo: coordonnées indisponibles.', 'Meteo: coordenadas no disponibles.');
        return;
    }

    status.textContent = `${t('Météo', 'Meteo')}: ${t('chargement', 'cargando')} (${focus.source})...`;

    try {
        if (forceMapCenter && map) {
            const center = map.getCenter();
            setWeatherFocusPoint(center, { refresh: false, sourceLabel: t('centre carte', 'centro mapa') });
        }

        const current = await getCurrentWeatherAtWaypoint(focus.lat, focus.lng);

        const dayOffsets = [1, 2, 3];
        const forecastEntries = [];

        for (const offset of dayOffsets) {
            const target = new Date();
            target.setUTCDate(target.getUTCDate() + offset);
            target.setUTCHours(12, 0, 0, 0);
            const slot = toDateAndHourUtc(target);
            const weather = await getWeatherAtDateHour(focus.lat, focus.lng, slot.date, slot.hour);
            forecastEntries.push({ slot, weather });
        }

        const nowImpact = computeWeatherImpactLabel(current);
        const nowWind = Number.isFinite(current?.windSpeed) ? `${current.windSpeed.toFixed(1)} kn` : 'N/A';
        const nowWave = Number.isFinite(current?.waveHeight) ? `${current.waveHeight.toFixed(1)} m` : 'N/A';
        const nowTemp = Number.isFinite(current?.temperature) ? `${current.temperature.toFixed(1)}°C` : 'N/A';
        const nowPressure = Number.isFinite(current?.pressure) ? `${current.pressure.toFixed(0)} hPa` : 'N/A';
        const nowGust = Number.isFinite(current?.windGust) ? `${current.windGust.toFixed(1)} kn` : 'N/A';

        const weatherSeries = [current, ...forecastEntries.map(entry => entry.weather)];
        const tempEvolution = buildWeatherMetricEvolutionSummary(weatherSeries.map(entry => entry?.temperature), {
            unit: '°C',
            decimals: 1,
            riseThreshold: 0.8,
            fallThreshold: -0.8
        });
        const pressureEvolution = buildWeatherMetricEvolutionSummary(weatherSeries.map(entry => entry?.pressure), {
            unit: ' hPa',
            decimals: 0,
            riseThreshold: 1.2,
            fallThreshold: -1.2
        });

        const forecastHtml = forecastEntries.map(entry => {
            const impact = computeWeatherImpactLabel(entry.weather);
            const wind = Number.isFinite(entry.weather?.windSpeed) ? `${entry.weather.windSpeed.toFixed(1)} kn` : 'N/A';
            const wave = Number.isFinite(entry.weather?.waveHeight) ? `${entry.weather.waveHeight.toFixed(1)} m` : 'N/A';
            const dir = Number.isFinite(entry.weather?.windDirection) ? `${Math.round(entry.weather.windDirection)}° ${degreesToCardinalFr(entry.weather.windDirection)}` : 'N/A';
            const rain = Number.isFinite(entry.weather?.precipitation) ? `${entry.weather.precipitation.toFixed(1)} mm` : 'N/A';
            const temp = Number.isFinite(entry.weather?.temperature) ? `${entry.weather.temperature.toFixed(1)}°C` : 'N/A';
            const pressure = Number.isFinite(entry.weather?.pressure) ? `${entry.weather.pressure.toFixed(0)} hPa` : 'N/A';
            return `<div class="${impact.className}"><strong>${entry.slot.date} · 12:00 UTC</strong><br>${t('Impact nav', 'Impacto nav')}: ${impact.text}<br>${t('Temp', 'Temp')}: ${temp} · ${t('Pression', 'Presión')}: ${pressure}<br>${t('Vent', 'Viento')}: ${wind} (${dir}) · ${t('Rafales', 'Ráfagas')}: ${Number.isFinite(entry.weather?.windGust) ? `${entry.weather.windGust.toFixed(1)} kn` : 'N/A'}<br>${t('Houle', 'Oleaje')}: ${wave} · ${t('Pluie', 'Lluvia')}: ${rain}</div>`;
        }).join('');

        list.innerHTML =
            `<div class="${nowImpact.className}"><strong>${t('Conditions actuelles', 'Condiciones actuales')}</strong><br>${t('Impact nav', 'Impacto nav')}: ${nowImpact.text}<br>${t('Temp', 'Temp')}: ${nowTemp} · ${t('Pression', 'Presión')}: ${nowPressure}<br>${t('Vent', 'Viento')}: ${nowWind} · ${t('Rafales', 'Ráfagas')}: ${nowGust}<br>${t('Houle', 'Oleaje')}: ${nowWave}</div>` +
            `<div class="weather-card"><strong>${t('Évolution actuelle → J+3', 'Evolución actual → D+3')}</strong><br>${t('Température', 'Temperatura')}: ${tempEvolution}<br>${t('Pression', 'Presión')}: ${pressureEvolution}</div>` +
            forecastHtml;

        status.textContent = `${t('Météo', 'Meteo')}: ${focus.source} (${focus.lat.toFixed(4)}, ${focus.lng.toFixed(4)})`;
    } catch (_error) {
        status.textContent = t('Météo: impossible de récupérer les prévisions.', 'Meteo: no se pueden recuperar las previsiones.');
        list.innerHTML = '';
    }
}

// =====================
// INIT MAP
// =====================

document.addEventListener('DOMContentLoaded', async function() {
    const savedMapView = loadSavedMapView();
    const initialLat = savedMapView?.lat ?? 41.3851;
    const initialLng = savedMapView?.lng ?? 2.1734;
    const initialZoom = savedMapView?.zoom ?? 8;
    map = L.map('map').setView([initialLat, initialLng], initialZoom);
    initializeLanguageSwitcher();

    standardTileLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '© OpenStreetMap',
        crossOrigin: true
    });

    satelliteTileLayer = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
        attribution: 'Tiles © Esri',
        crossOrigin: true
    });

    marineDepthLayer = L.tileLayer('https://services.arcgisonline.com/ArcGIS/rest/services/Ocean/World_Ocean_Base/MapServer/tile/{z}/{y}/{x}', {
        attribution: 'Bathymétrie © Esri, GEBCO, NOAA',
        opacity: 0.85,
        maxNativeZoom: 10,
        errorTileUrl: TRANSPARENT_TILE_DATA_URI,
        crossOrigin: true
    });

    marineHazardLayer = L.tileLayer('https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png', {
        attribution: 'Dangers maritimes © OpenSeaMap',
        opacity: 0.95,
        maxNativeZoom: 18,
        errorTileUrl: TRANSPARENT_TILE_DATA_URI,
        crossOrigin: true
    });

    const openWeatherTileAppId = getStoredOpenWeatherTileAppId();
    isobarLayer = createIsobarOverlayLayer(openWeatherTileAppId);

    const layerLabels = getLayerControlLabels();
    baseLayerControl = L.control.layers(
        {
            [layerLabels.standard]: standardTileLayer,
            [layerLabels.satellite]: satelliteTileLayer
        },
        {
            [layerLabels.marineDepth]: marineDepthLayer,
            [layerLabels.marineHazard]: marineHazardLayer,
            [layerLabels.isobars]: isobarLayer
        },
        { position: 'topright', collapsed: false }
    ).addTo(map);

    activeBaseLayer = standardTileLayer.addTo(map);

    function setMapStyle(style) {
        if (activeBaseLayer) map.removeLayer(activeBaseLayer);
        activeBaseLayer = style === 'satellite' ? satelliteTileLayer : standardTileLayer;
        activeBaseLayer.addTo(map);
    }

    createNauticalScaleControl();

    // Setup event listeners after map is ready
    map.on('click', function(e) {
        if (isAuthGateLocked()) {
            setCloudStatus(t('Accès verrouillé: authentifie-toi (email/mot de passe).', 'Acceso bloqueado: autentícate (email/contraseña).'), true);
            return;
        }

        if (weatherPointerPlacementMode && activeTabName === 'weather') {
            setWeatherFocusPoint(e.latlng, { refresh: true, sourceLabel: t('pointeur carte', 'puntero mapa') });
            setWeatherPointerPlacementMode(false);
            return;
        }

        if (measureModeEnabled) {
            addMeasurePoint(e.latlng);
            return;
        }

        addUserWaypoint(e.latlng);
    });

    map.on('moveend zoomend', () => {
        syncWaypointPhotoMarkersInView();
        persistMapView();
    });

    // =====================
    // DATE & TIME INPUT
    // =====================

    updateDepartureDateTimeInput();
    document.getElementById("departureDateTimeInput").addEventListener("change", function(e) {
        setDepartureFromDateTimeInput(e.target.value);
    });

    const savedStyle = normalizeMapStyle(localStorage.getItem(MAP_STYLE_STORAGE_KEY));
    setMapStyle(savedStyle);

    map.on('baselayerchange', function(e) {
        const style = e.layer === satelliteTileLayer ? 'satellite' : 'standard';
        activeBaseLayer = style === 'satellite' ? satelliteTileLayer : standardTileLayer;
        localStorage.setItem(MAP_STYLE_STORAGE_KEY, style);
    });

    const routingTabBtn = document.getElementById('routingTabBtn');
    const routesTabBtn = document.getElementById('routesTabBtn');
    const cloudTabBtn = document.getElementById('cloudTabBtn');
    const navLogTabBtn = document.getElementById('navLogTabBtn');
    const engineTabBtn = document.getElementById('engineTabBtn');
    const weatherTabBtn = document.getElementById('weatherTabBtn');
    const arrivalTabBtn = document.getElementById('arrivalTabBtn');
    const waypointTabBtn = document.getElementById('waypointTabBtn');
    const maintenanceTabBtn = document.getElementById('maintenanceTabBtn');
    const routingTab = document.getElementById('routingTab');
    const routesTab = document.getElementById('routesTab');
    const cloudTab = document.getElementById('cloudTab');
    const navLogTab = document.getElementById('navLogTab');
    const engineTab = document.getElementById('engineTab');
    const weatherTab = document.getElementById('weatherTab');
    const arrivalTab = document.getElementById('arrivalTab');
    const waypointTab = document.getElementById('waypointTab');
    const maintenanceTab = document.getElementById('maintenanceTab');
    const mapContainer = document.getElementById('map');
    const maintenanceMapPanel = document.getElementById('maintenanceMapPanel');
    const maintenanceInvoicePreviewPanel = document.getElementById('maintenanceInvoicePreviewPanel');

    function setMaintenanceMapMode(enabled) {
        const shouldShowMaintenanceCanvas = enabled && activeMaintenanceSubtab === 'tasks';
        const shouldShowInvoicePreview = enabled && activeMaintenanceSubtab === 'expenses' && !!maintenanceInvoicePreviewUrl;
        const shouldHideMap = shouldShowMaintenanceCanvas || shouldShowInvoicePreview;

        if (mapContainer) {
            mapContainer.style.display = shouldHideMap ? 'none' : '';
        }
        if (maintenanceMapPanel) {
            maintenanceMapPanel.style.display = shouldShowMaintenanceCanvas ? 'block' : 'none';
        }
        if (maintenanceInvoicePreviewPanel) {
            maintenanceInvoicePreviewPanel.style.display = shouldShowInvoicePreview ? 'block' : 'none';
        }

        if (shouldShowMaintenanceCanvas) {
            renderMaintenanceBoard();
            return;
        }

        window.setTimeout(() => {
            if (map) {
                map.invalidateSize();
            }
        }, 80);
    }

    window.addEventListener('ceibo:maintenance-subtab-changed', () => {
        setMaintenanceMapMode(activeTabName === 'maintenance');
    });

    function activateTab(tabName) {
        if (isAuthGateLocked() && tabName !== 'cloud') {
            setCloudStatus(t('Accès verrouillé: authentifie-toi (email/mot de passe).', 'Acceso bloqueado: autentícate (email/contraseña).'), true);
            tabName = 'cloud';
        }

        activeTabName = tabName;
        const isRouting = tabName === 'routing';
        const isRoutes = tabName === 'routes';
        const isCloud = tabName === 'cloud';
        const isNavLog = tabName === 'navlog';
        const isEngine = tabName === 'engine';
        const isWeather = tabName === 'weather';
        const isArrival = tabName === 'arrival';
        const isWaypoint = tabName === 'waypoint';
        const isMaintenance = tabName === 'maintenance';

        routingTabBtn.classList.toggle('active', isRouting);
        routesTabBtn.classList.toggle('active', isRoutes);
        cloudTabBtn.classList.toggle('active', isCloud);
        navLogTabBtn.classList.toggle('active', isNavLog);
        engineTabBtn.classList.toggle('active', isEngine);
        weatherTabBtn.classList.toggle('active', isWeather);
        arrivalTabBtn.classList.toggle('active', isArrival);
        waypointTabBtn.classList.toggle('active', isWaypoint);
        maintenanceTabBtn.classList.toggle('active', isMaintenance);

        routingTab.classList.toggle('active', isRouting);
        routesTab.classList.toggle('active', isRoutes);
        cloudTab.classList.toggle('active', isCloud);
        navLogTab.classList.toggle('active', isNavLog);
        engineTab.classList.toggle('active', isEngine);
        weatherTab.classList.toggle('active', isWeather);
        arrivalTab.classList.toggle('active', isArrival);
        waypointTab.classList.toggle('active', isWaypoint);
        maintenanceTab.classList.toggle('active', isMaintenance);
        setMaintenanceMapMode(isMaintenance);

        if (isWeather) {
            refreshWeatherOutlook();
        }

        if (!isWeather) {
            setWeatherPointerPlacementMode(false);
        }
    }

    routingTabBtn.addEventListener('click', () => activateTab('routing'));
    routesTabBtn.addEventListener('click', () => activateTab('routes'));
    cloudTabBtn.addEventListener('click', () => activateTab('cloud'));
    navLogTabBtn.addEventListener('click', () => activateTab('navlog'));
    engineTabBtn.addEventListener('click', () => activateTab('engine'));
    weatherTabBtn.addEventListener('click', () => activateTab('weather'));
    arrivalTabBtn.addEventListener('click', () => activateTab('arrival'));
    waypointTabBtn.addEventListener('click', () => activateTab('waypoint'));
    maintenanceTabBtn.addEventListener('click', () => activateTab('maintenance'));
    setMaintenanceMapMode(false);

    document.getElementById("tackingTimeInput").value = tackingTimeHours;
    document.getElementById("tackingTimeInput").addEventListener("change", function(e) {
        tackingTimeHours = parseFloat(e.target.value);
    });

    document.getElementById("sailModeSelect").value = sailMode;
    document.getElementById("sailModeSelect").addEventListener("change", function(e) {
        sailMode = e.target.value;
    });

    const autoWpSpacingInput = document.getElementById('autoWpSpacingInput');
    if (autoWpSpacingInput) {
        autoWpSpacingInput.value = Number.isFinite(autoWpMinSpacingNm) ? String(autoWpMinSpacingNm) : 'off';
        autoWpSpacingInput.addEventListener('change', function(e) {
            const value = String(e.target.value || 'off');
            if (value === 'off') {
                autoWpMinSpacingNm = null;
                return;
            }

            const parsed = parseFloat(value);
            if (!Number.isFinite(parsed)) {
                autoWpMinSpacingNm = null;
                e.target.value = 'off';
                return;
            }

            autoWpMinSpacingNm = Math.max(2, Math.min(50, parsed));
            e.target.value = String(autoWpMinSpacingNm);
        });
    }

    const forecastWindowDaysSelect = document.getElementById('forecastWindowDaysSelect');
    if (forecastWindowDaysSelect) {
        forecastWindowDaysSelect.value = String(forecastWindowDays);
        forecastWindowDaysSelect.addEventListener('change', e => {
            const parsed = parseInt(String(e.target.value || '3'), 10);
            forecastWindowDays = Number.isFinite(parsed) ? Math.max(2, Math.min(7, parsed)) : 3;
            e.target.value = String(forecastWindowDays);
        });
    }

    document.getElementById("computeBtn").addEventListener("click", computeRoute);
    const suggestDepartureBtn = document.getElementById('suggestDepartureBtn');
    if (suggestDepartureBtn) {
        suggestDepartureBtn.addEventListener('click', suggestBestDeparture);
    }

    const suggestAiRoutesBtn = document.getElementById('suggestAiRoutesBtn');
    if (suggestAiRoutesBtn) {
        suggestAiRoutesBtn.addEventListener('click', suggestAiRouteOptions);
    }

    const aiRouteSuggestions = document.getElementById('aiRouteSuggestions');
    if (aiRouteSuggestions) {
        aiRouteSuggestions.addEventListener('click', event => {
            const target = event.target;
            if (!(target instanceof HTMLElement)) return;
            const indexRaw = target.getAttribute('data-ai-route-index');
            if (indexRaw === null) return;
            const index = parseInt(indexRaw, 10);
            if (!Number.isFinite(index)) return;
            applyAiRouteCandidate(index);
        });
    }

    const departureSuggestionInfo = document.getElementById('departureSuggestionInfo');
    if (departureSuggestionInfo) {
        departureSuggestionInfo.addEventListener('click', () => {
            applyLastDepartureSuggestion();
        });
    }
    document.getElementById("recenterBtn").addEventListener("click", recenterOnRoute);
    const deleteSelectedWpBtn = document.getElementById('deleteSelectedWpBtn');
    if (deleteSelectedWpBtn) {
        deleteSelectedWpBtn.addEventListener('click', () => {
            deleteUserWaypointAtIndex(selectedUserWaypointIndex);
        });
    }
    const measureToggleBtn = document.getElementById('measureToggleBtn');
    if (measureToggleBtn) {
        measureToggleBtn.addEventListener('click', () => setMeasureMode(!measureModeEnabled));
    }

    const measureClearBtn = document.getElementById('measureClearBtn');
    if (measureClearBtn) {
        measureClearBtn.addEventListener('click', () => clearMeasureTool());
    }

    const analyzeArrivalBtn = document.getElementById('analyzeArrivalBtn');
    if (analyzeArrivalBtn) {
        analyzeArrivalBtn.addEventListener('click', analyzeArrivalZone);
    }

    const waypointPhotoInput = document.getElementById('waypointPhotoInput');
    if (waypointPhotoInput) {
        waypointPhotoInput.addEventListener('change', handleWaypointPhotoInputChange);
    }

    const waypointQuickCaptureBtn = document.getElementById('waypointQuickCaptureBtn');
    const waypointQuickCaptureInput = document.getElementById('waypointQuickCaptureInput');
    if (waypointQuickCaptureBtn && waypointQuickCaptureInput) {
        waypointQuickCaptureBtn.addEventListener('click', () => waypointQuickCaptureInput.click());
        waypointQuickCaptureInput.addEventListener('change', handleQuickWaypointCaptureChange);
    }

    const saveWaypointPhotoBtn = document.getElementById('saveWaypointPhotoBtn');
    if (saveWaypointPhotoBtn) {
        saveWaypointPhotoBtn.addEventListener('click', saveWaypointPhotoEntry);
    }

    const cancelWaypointPhotoEditBtn = document.getElementById('cancelWaypointPhotoEditBtn');
    if (cancelWaypointPhotoEditBtn) {
        cancelWaypointPhotoEditBtn.addEventListener('click', cancelWaypointPhotoEdit);
    }

    const startNavLogBtn = document.getElementById('startNavLogBtn');
    if (startNavLogBtn) {
        startNavLogBtn.addEventListener('click', startNavigationLogging);
    }

    const stopNavLogBtn = document.getElementById('stopNavLogBtn');
    if (stopNavLogBtn) {
        stopNavLogBtn.addEventListener('click', stopNavigationLogging);
    }

    const requestMotionPermissionBtn = document.getElementById('requestMotionPermissionBtn');
    if (requestMotionPermissionBtn) {
        requestMotionPermissionBtn.addEventListener('click', requestMotionPermissionIfNeeded);
    }

    const clearNavLogBtn = document.getElementById('clearNavLogBtn');
    if (clearNavLogBtn) {
        clearNavLogBtn.addEventListener('click', clearNavigationLogbook);
    }

    const addManualNavLogBtn = document.getElementById('addManualNavLogBtn');
    if (addManualNavLogBtn) {
        addManualNavLogBtn.addEventListener('click', addManualNavigationLogEntry);
    }

    const saveEngineLogBtn = document.getElementById('saveEngineLogBtn');
    if (saveEngineLogBtn) {
        saveEngineLogBtn.addEventListener('click', addEngineLogEntryFromForm);
    }

    const clearEngineLogBtn = document.getElementById('clearEngineLogBtn');
    if (clearEngineLogBtn) {
        clearEngineLogBtn.addEventListener('click', clearEngineLogbook);
    }

    const useMapCenterWeatherBtn = document.getElementById('useMapCenterWeatherBtn');
    if (useMapCenterWeatherBtn) {
        useMapCenterWeatherBtn.addEventListener('click', () => {
            if (!map) return;
            const center = map.getCenter();
            setWeatherFocusPoint(center, { refresh: true, sourceLabel: t('centre carte', 'centro mapa') });
        });
    }

    const placeWeatherPointerBtn = document.getElementById('placeWeatherPointerBtn');
    if (placeWeatherPointerBtn) {
        placeWeatherPointerBtn.addEventListener('click', () => {
            activateTab('weather');
            setWeatherPointerPlacementMode(true);
        });
    }

    const refreshWeatherOutlookBtn = document.getElementById('refreshWeatherOutlookBtn');
    if (refreshWeatherOutlookBtn) {
        refreshWeatherOutlookBtn.addEventListener('click', () => refreshWeatherOutlook());
    }

    const owmApiKeyInput = document.getElementById('owmApiKeyInput');
    const owmApiKeyStatus = document.getElementById('owmApiKeyStatus');
    const weatherApiConfigSection = document.getElementById('weatherApiConfigSection');
    const weatherApiConfigSummary = document.getElementById('weatherApiConfigSummary');
    const toggleWeatherApiConfigBtn = document.getElementById('toggleWeatherApiConfigBtn');

    function setWeatherApiConfigVisibility(showConfig, summaryText = 'API météo connectée.') {
        if (weatherApiConfigSection) weatherApiConfigSection.style.display = showConfig ? '' : 'none';
        if (weatherApiConfigSummary) {
            weatherApiConfigSummary.textContent = summaryText;
            weatherApiConfigSummary.style.display = showConfig ? 'none' : '';
        }
        if (toggleWeatherApiConfigBtn) {
            toggleWeatherApiConfigBtn.style.display = showConfig ? '' : '';
            toggleWeatherApiConfigBtn.textContent = showConfig ? t('Masquer API météo', 'Ocultar API meteo') : t('Afficher API météo', 'Mostrar API meteo');
        }
    }

    if (toggleWeatherApiConfigBtn) {
        toggleWeatherApiConfigBtn.addEventListener('click', () => {
            const isShown = weatherApiConfigSection?.style.display !== 'none';
            setWeatherApiConfigVisibility(!isShown);
        });
    }

    if (owmApiKeyInput) {
        owmApiKeyInput.value = getStoredOpenWeatherTileAppId();
    }

    setWeatherApiConfigVisibility(true);

    const storedOwmKey = getStoredOpenWeatherTileAppId();
    if (storedOwmKey) {
        if (owmApiKeyStatus) owmApiKeyStatus.textContent = t('Clé OWM: validation en cours...', 'Clave OWM: validación en curso...');
        testOpenWeatherApiKey(storedOwmKey).then(result => {
            if (owmApiKeyStatus) {
                owmApiKeyStatus.textContent = result.ok
                    ? `${t('Clé OWM', 'Clave OWM')}: ✅ ${result.message}`
                    : `${t('Clé OWM', 'Clave OWM')}: ❌ ${result.message}`;
            }
            if (result.ok) {
                setWeatherApiConfigVisibility(false, t('API météo connectée (isobares actives).', 'API meteo conectada (isobaras activas).'));
            }
        }).catch(() => {
            if (owmApiKeyStatus) owmApiKeyStatus.textContent = t('Clé OWM: test impossible.', 'Clave OWM: prueba imposible.');
        });
    }

    const testOwmApiKeyBtn = document.getElementById('testOwmApiKeyBtn');
    if (testOwmApiKeyBtn) {
        testOwmApiKeyBtn.addEventListener('click', async () => {
            const keyValue = String(owmApiKeyInput?.value || '').trim();
            if (owmApiKeyStatus) owmApiKeyStatus.textContent = t('Clé OWM: test en cours...', 'Clave OWM: prueba en curso...');

            const result = await testOpenWeatherApiKey(keyValue);
            if (owmApiKeyStatus) {
                owmApiKeyStatus.textContent = result.ok
                    ? `${t('Clé OWM', 'Clave OWM')}: ✅ ${result.message}`
                    : `${t('Clé OWM', 'Clave OWM')}: ❌ ${result.message}`;
            }

            if (result.ok) {
                setWeatherApiConfigVisibility(false, t('API météo connectée (isobares actives).', 'API meteo conectada (isobaras activas).'));
            } else {
                setWeatherApiConfigVisibility(true);
            }
        });
    }

    const saveOwmApiKeyBtn = document.getElementById('saveOwmApiKeyBtn');
    if (saveOwmApiKeyBtn) {
        saveOwmApiKeyBtn.addEventListener('click', () => {
            const keyValue = String(owmApiKeyInput?.value || '').trim();
            if (!keyValue) {
                alert(t('Renseigne une clé API OpenWeatherMap.', 'Introduce una clave API de OpenWeatherMap.'));
                return;
            }
            localStorage.setItem(OWM_TILE_APPID_STORAGE_KEY, keyValue);
            if (owmApiKeyStatus) owmApiKeyStatus.textContent = t('Clé OWM: enregistrée (non testée).', 'Clave OWM: guardada (no probada).');
            alert(t('Clé OWM enregistrée. Recharge la page pour activer la couche isobares.', 'Clave OWM guardada. Recarga la página para activar la capa de isobaras.'));
        });
    }

    const clearOwmApiKeyBtn = document.getElementById('clearOwmApiKeyBtn');
    if (clearOwmApiKeyBtn) {
        clearOwmApiKeyBtn.addEventListener('click', () => {
            localStorage.removeItem(OWM_TILE_APPID_STORAGE_KEY);
            if (owmApiKeyInput) owmApiKeyInput.value = '';
            if (owmApiKeyStatus) owmApiKeyStatus.textContent = t('Clé OWM: supprimée.', 'Clave OWM: eliminada.');
            setWeatherApiConfigVisibility(true);
            alert(t('Clé OWM supprimée. Recharge la page.', 'Clave OWM eliminada. Recarga la página.'));
        });
    }

    const cloudEmailSignInBtn = document.getElementById('cloudEmailSignInBtn');
    if (cloudEmailSignInBtn) {
        cloudEmailSignInBtn.addEventListener('click', async () => {
            if (!cloudClient) {
                const config = readCloudConfigFromForm();
                const connected = await connectCloud(config);
                if (!connected) return;
            }

            const { email, password } = readCloudUserCredentials();
            if (!email || !password) {
                setCloudAuthStatus(t('Renseigne email + mot de passe.', 'Introduce email y contraseña.'), true);
                return;
            }

            try {
                const { error } = await cloudClient.auth.signInWithPassword({ email, password });
                if (error) throw error;
                setCloudAuthStatus(t(`Connexion email OK (${email})`, `Conexión email OK (${email})`));
                activateTab('routes');
            } catch (error) {
                setCloudAuthStatus(t(`Connexion email impossible: ${formatCloudError(error)}`, `Conexión email imposible: ${formatCloudError(error)}`), true);
            }
        });
    }

    const cloudEmailSignUpBtn = document.getElementById('cloudEmailSignUpBtn');
    if (cloudEmailSignUpBtn) {
        cloudEmailSignUpBtn.addEventListener('click', async () => {
            if (!cloudClient) {
                const config = readCloudConfigFromForm();
                const connected = await connectCloud(config);
                if (!connected) return;
            }

            const { email, password } = readCloudUserCredentials();
            if (!email || !password) {
                setCloudAuthStatus(t('Renseigne email + mot de passe.', 'Introduce email y contraseña.'), true);
                return;
            }

            if (password.length < 8) {
                setCloudAuthStatus(t('Mot de passe trop court (minimum 8 caractères).', 'Contraseña demasiado corta (mínimo 8 caracteres).'), true);
                return;
            }

            try {
                const { data, error } = await cloudClient.auth.signUp({ email, password });
                if (error) throw error;

                if (data?.user && !data?.session) {
                    setCloudAuthStatus(t(`Compte créé (${email}). Vérifie l'email de confirmation.`, `Cuenta creada (${email}). Revisa el email de confirmación.`));
                } else {
                    setCloudAuthStatus(t(`Compte créé et connecté (${email}).`, `Cuenta creada y conectada (${email}).`));
                    activateTab('routes');
                }
            } catch (error) {
                setCloudAuthStatus(t(`Création compte impossible: ${formatCloudError(error)}`, `Creación de cuenta imposible: ${formatCloudError(error)}`), true);
            }
        });
    }

    const cloudSignOutBtn = document.getElementById('cloudSignOutBtn');
    if (cloudSignOutBtn) {
        cloudSignOutBtn.addEventListener('click', async () => {
            if (!cloudClient) return;
            try {
                await cloudClient.auth.signOut();
                cloudAuthUser = null;
                updateCloudAuthUi();
            } catch (error) {
                setCloudAuthStatus(t(`Déconnexion impossible: ${formatCloudError(error)}`, `Desconexión imposible: ${formatCloudError(error)}`), true);
            }
        });
    }

    const aiTrafficCloseBtn = document.getElementById('aiTrafficCloseBtn');
    if (aiTrafficCloseBtn) {
        aiTrafficCloseBtn.addEventListener('click', () => {
            if (aiTrafficAutoHideTimer) {
                clearTimeout(aiTrafficAutoHideTimer);
                aiTrafficAutoHideTimer = null;
            }
            hideAiTrafficOverlay();
        });
    }

    updateMeasureInfo();
    updateSelectedWaypointInfo();
    setMeasureMode(false);

    document.getElementById("resetBtn").addEventListener("click", () => {

        routePoints = [];
        markers = [];
        selectedUserWaypointIndex = -1;
        currentLoadedRouteIndex = -1;

        if (routeLayer) map.removeLayer(routeLayer);
        if (windLayer) map.removeLayer(windLayer);

        map.eachLayer(layer => {
            if (layer instanceof L.Marker) map.removeLayer(layer);
        });
        clearWaypointWindDirectionLayers();
        clearWaveDirectionSegmentLayers();
        clearGeneratedWaypointMarkers();
        clearArrivalPoiMarkers();
        clearMeasureTool();
        setMeasureMode(false);
        waypointPassageSlots.clear();
        lastRouteBounds = null;
        lastComputedReportData = null;

        document.getElementById("info").innerHTML = "";
        const weatherContainer = document.getElementById('waypointWeatherInfo');
        if (weatherContainer) weatherContainer.innerHTML = '';
        const windLegend = document.getElementById('windSpeedLegend');
        if (windLegend) windLegend.innerHTML = '';
        const suggestionBox = document.getElementById('departureSuggestionInfo');
        lastDepartureSuggestion = null;
        if (suggestionBox) {
            suggestionBox.textContent = t('Suggestion départ: en attente', 'Sugerencia salida: en espera');
            suggestionBox.classList.remove('suggestion-clickable');
        }
        const aiInfo = document.getElementById('aiRouteSuggestionInfo');
        const aiList = document.getElementById('aiRouteSuggestions');
        lastAiRouteCandidates = [];
        if (aiInfo) aiInfo.textContent = t('Routes IA: en attente', 'Rutas IA: en espera');
        if (aiList) aiList.innerHTML = '';
        const arrivalSummary = document.getElementById('arrivalSummary');
        if (arrivalSummary) arrivalSummary.textContent = t('Analyse mouillage: en attente', 'Análisis fondeo: en espera');
        const anchorageContainer = document.getElementById('anchorageRecommendations');
        if (anchorageContainer) anchorageContainer.innerHTML = '';
        const restaurants = document.getElementById('nearbyRestaurants');
        if (restaurants) restaurants.innerHTML = '';
        const shops = document.getElementById('nearbyShops');
        if (shops) shops.innerHTML = '';
        updateSelectedWaypointInfo();
        syncWaypointPhotoMarkersInView();
    });

    // Saved routes UI
    document.getElementById('saveRouteBtn').addEventListener('click', () => { saveRoute(); });
    document.getElementById('loadRouteBtn').addEventListener('click', () => {
        const sel = document.getElementById('savedRoutesSelect');
        loadRoute(sel.selectedIndex);
    });
    document.getElementById('savedRoutesSelect').addEventListener('change', e => {
        const index = Number(e.target.selectedIndex);
        if (Number.isFinite(index) && index >= 0) {
            loadRoute(index);
        }
    });
    const reverseRouteBtn = document.getElementById('reverseRouteBtn');
    if (reverseRouteBtn) {
        reverseRouteBtn.addEventListener('click', () => {
            reverseCurrentRoute();
        });
    }
    document.getElementById('deleteRouteBtn').addEventListener('click', () => {
        const sel = document.getElementById('savedRoutesSelect');
        deleteRoute(sel.selectedIndex);
    });
    document.getElementById('exportRouteBtn').addEventListener('click', () => {
        const sel = document.getElementById('savedRoutesSelect');
        exportRoute(sel.selectedIndex);
    });
    document.getElementById('exportRouteGpxBtn').addEventListener('click', () => {
        const sel = document.getElementById('savedRoutesSelect');
        exportRouteGpx(sel.selectedIndex);
    });
    const exportVoyagePdfBtn = document.getElementById('exportVoyagePdfBtn');
    if (exportVoyagePdfBtn) {
        exportVoyagePdfBtn.addEventListener('click', exportVoyagePdfReport);
    }
    document.getElementById('importRouteInput').addEventListener('change', handleImport);

    const cloudRefreshBtn = document.getElementById('cloudRefreshBtn');
    if (cloudRefreshBtn) {
        cloudRefreshBtn.addEventListener('click', async () => {
            if (!isCloudReady()) {
                setCloudStatus(t('Cloud non connecté', 'Nube no conectada'), true);
                return;
            }

            try {
                await autoPullRoutesFromCloud('manual');
            } catch (error) {
                setCloudStatus(t(`Rafraîchissement cloud impossible: ${formatCloudError(error)}`, `Actualización nube imposible: ${formatCloudError(error)}`), true);
            }
        });
    }

    window.addEventListener('focus', () => {
        autoPullRoutesFromCloud('silent');
    });

    document.addEventListener('visibilitychange', () => {
        if (document.visibilityState === 'visible') {
            autoPullRoutesFromCloud('silent');
        }
    });

    resetWaypointPhotoFormValues();
    setWaypointPhotoEditMode(null);
    initializeMaintenanceFeature();
    updateCloudAuthUi();
    updateCloudDataSourceStatus('initialisation', 0, 0);
    await applyAuthGateState({ clearWhenLocked: true });

    const storedCloudConfig = loadCloudConfigFromStorage();
    updateCloudFormFromConfig(storedCloudConfig);
    if (storedCloudConfig) {
        await connectCloud(storedCloudConfig, { silent: true });
    } else {
        const hiddenConfig = readCloudConfigFromForm();
        if (hiddenConfig?.url && hiddenConfig?.anonKey && hiddenConfig?.projectKey) {
            await connectCloud(hiddenConfig, { silent: true });
        } else {
            setCloudStatus(t('Mode local (pas de cloud configuré)', 'Modo local (nube no configurada)'));
        }
    }

    if (cloudAuthUser) {
        activateTab('routes');
    }

    await applyAuthGateState({ clearWhenLocked: true });

    refreshWeatherOutlook();
});

// =====================
// COMPUTE ROUTE
// =====================

async function computeRoute() {

    if (routePoints.length < 2) return;

    clearWaypointWindDirectionLayers();
    clearWaveDirectionSegmentLayers();
    clearGeneratedWaypointMarkers();
    waypointPassageSlots.clear();

    let fullRoute = [];
    let totalTime = 0;
    let totalDistance = 0;
    let segmentsInfo = [];
    const waypointPassageWeather = [];
    const routeSegmentsForDraw = [];
    let generatedAutoWaypointCount = 0;
    let generatedWaypointOrdinal = 1;

    const departureDateTime = new Date(`${departureDate}T${departureTime}:00`);
    if (Number.isNaN(departureDateTime.getTime())) {
        alert(t('Date/heure de départ invalide', 'Fecha/hora de salida inválida'));
        return;
    }

    for (let i = 0; i < routePoints.length - 1; i++) {

        const userStart = { lat: routePoints[i].lat, lon: routePoints[i].lng };
        const userEnd = { lat: routePoints[i + 1].lat, lon: routePoints[i + 1].lng };
        const avoidLandIsActive = Number.isFinite(autoWpMinSpacingNm);
        const coastalLegPoints = avoidLandIsActive
            ? await buildCoastalBypassWaypoints(userStart, userEnd, COASTAL_CLEARANCE_NM, autoWpMinSpacingNm)
            : [userStart, userEnd];

        if (avoidLandIsActive && !coastalLegPoints) {
            alert(`Impossible de contourner la terre sur le segment ${i + 1}. Essaie un espacement WP plus faible (5 ou 8 NM) ou ajoute un waypoint manuel.`);
            return;
        }

        if (!Array.isArray(coastalLegPoints) || coastalLegPoints.length < 2) continue;

        if (coastalLegPoints.length > 2) {
            generatedAutoWaypointCount += Math.max(0, coastalLegPoints.length - 2);
        }

        const pointMetas = coastalLegPoints.map((point, pointIndex) => {
            if (pointIndex === 0) {
                return {
                    type: 'user',
                    label: `WP ${i + 1}`,
                    icon: '📍',
                    marker: markers[i] || null,
                    latlng: { lat: point.lat, lng: point.lon }
                };
            }

            if (pointIndex === coastalLegPoints.length - 1) {
                return {
                    type: 'user',
                    label: `WP ${i + 2}`,
                    icon: '📍',
                    marker: markers[i + 1] || null,
                    latlng: { lat: point.lat, lng: point.lon }
                };
            }

            const generatedIndex = generatedWaypointOrdinal++;
            const label = `WP auto ${generatedIndex}`;
            const latlng = { lat: point.lat, lng: point.lon };
            const marker = createGeneratedWaypointMarker(latlng, label, `A${generatedIndex}`);
            generatedWaypointMarkers.push(marker);

            return {
                type: 'generated',
                label,
                icon: `A${generatedIndex}`,
                marker,
                latlng
            };
        });

        const toPointKey = (point) => {
            const lat = Number(point?.lat);
            const lon = Number.isFinite(point?.lon) ? Number(point.lon) : Number(point?.lng);
            if (!Number.isFinite(lat) || !Number.isFinite(lon)) return '';
            return `${lat.toFixed(6)}:${lon.toFixed(6)}`;
        };

        const pointMetaByKey = new Map();
        coastalLegPoints.forEach((point, idx) => {
            const key = toPointKey(point);
            if (!key) return;
            pointMetaByKey.set(key, pointMetas[idx]);
        });

        const legStartDateTime = new Date(departureDateTime.getTime() + totalTime * 3600 * 1000);
        const legStartSlot = toDateAndHourUtc(legStartDateTime);
        const legStartWeather = await getWeatherAtDateHour(userStart.lat, userStart.lon, legStartSlot.date, legStartSlot.hour);
        const effectiveSpeed = estimateEffectiveSpeedForLegSplit(userStart, userEnd, legStartWeather);
        const maxDistanceForWeatherNm = Math.max(4, effectiveSpeed * AUTO_WEATHER_SPLIT_MAX_HOURS);
        const weatherLegPoints = densifyPolylineForWeather(coastalLegPoints, maxDistanceForWeatherNm);
        if (!Array.isArray(weatherLegPoints) || weatherLegPoints.length < 2) continue;

        if (!avoidLandIsActive && weatherLegPoints.length > 2) {
            generatedAutoWaypointCount += Math.max(0, weatherLegPoints.length - 2);

            for (let weatherPointIndex = 1; weatherPointIndex < weatherLegPoints.length - 1; weatherPointIndex++) {
                const weatherPoint = weatherLegPoints[weatherPointIndex];
                const key = toPointKey(weatherPoint);
                if (!key || pointMetaByKey.has(key)) continue;

                const generatedIndex = generatedWaypointOrdinal++;
                const label = `WP météo ${generatedIndex}`;
                const latlng = { lat: weatherPoint.lat, lng: weatherPoint.lon };
                const marker = createGeneratedWaypointMarker(latlng, label, `M${generatedIndex}`);
                generatedWaypointMarkers.push(marker);

                pointMetaByKey.set(key, {
                    type: 'generated-weather',
                    label,
                    icon: `M${generatedIndex}`,
                    marker,
                    latlng
                });
            }
        }

        let activeDisplaySegment = null;

        for (let legIndex = 0; legIndex < weatherLegPoints.length - 1; legIndex++) {
            const startPoint = weatherLegPoints[legIndex];
            const endPoint = weatherLegPoints[legIndex + 1];
            const startMeta = pointMetaByKey.get(toPointKey(startPoint)) || null;

            const passageDateTime = new Date(departureDateTime.getTime() + totalTime * 3600 * 1000);
            const passageSlot = toDateAndHourUtc(passageDateTime);
            const weatherAtPassage = await getWeatherAtDateHour(
                startPoint.lat,
                startPoint.lon,
                passageSlot.date,
                passageSlot.hour
            );

            if (startMeta?.marker) {
                startMeta.marker._ceiboPassageSlot = passageSlot;
                startMeta.marker._ceiboLabel = startMeta.label;
            }

            const wind = {
                speed: Number.isFinite(weatherAtPassage.windSpeed) ? weatherAtPassage.windSpeed : 10,
                direction: Number.isFinite(weatherAtPassage.windDirection) ? weatherAtPassage.windDirection : 0
            };

            if (legIndex === 0) {
                waypointPassageWeather.push({
                    waypointIndex: i,
                    weather: weatherAtPassage,
                    slot: passageSlot
                });
                waypointPassageSlots.set(i, passageSlot);
            }

            drawWind(startPoint.lat, startPoint.lon, wind.direction);

            const isMotorSegment = wind.speed < MOTOR_WIND_THRESHOLD_KN;

            const rawSegment = isMotorSegment
                ? {
                    type: 'motor',
                    distance: distanceNm(startPoint.lat, startPoint.lon, endPoint.lat, endPoint.lon),
                    speed: MOTOR_SPEED_KN,
                    bearing: getBearing(startPoint, endPoint),
                    timeHours: distanceNm(startPoint.lat, startPoint.lon, endPoint.lat, endPoint.lon) / MOTOR_SPEED_KN,
                    points: [startPoint, endPoint]
                }
                : routeSegment(
                    startPoint,
                    endPoint,
                    wind.direction,
                    wind.speed,
                    tackingTimeHours
                );

            const twa = isMotorSegment ? null : computeTWA(rawSegment.bearing, wind.direction);
            const sailSetup = getSailRecommendation({
                isMotorSegment,
                tws: wind.speed,
                twa,
                sailModeValue: sailMode
            });

            const sailComment = getSailComment({
                sailSetup,
                sailModeValue: sailMode,
                tws: wind.speed,
                twa,
                isMotorSegment
            });

            const sailFactor = getSailPerformanceFactor({
                isMotorSegment,
                sailModeValue: sailMode,
                tws: wind.speed,
                twa,
                sailSetup
            });

            let segment = isMotorSegment
                ? rawSegment
                : {
                    ...rawSegment,
                    speed: rawSegment.speed * sailFactor,
                    timeHours: rawSegment.timeHours / sailFactor
                };

            let segmentLatLngs = buildSegmentLatLngs(
                { lat: startPoint.lat, lng: startPoint.lon },
                { lat: endPoint.lat, lng: endPoint.lon },
                segment
            );

            if (Number.isFinite(autoWpMinSpacingNm) && polylineLatLngCrossesLand(segmentLatLngs)) {
                const directDistance = distanceNm(startPoint.lat, startPoint.lon, endPoint.lat, endPoint.lon);
                const safeSpeed = Math.max(3, Number.isFinite(segment.speed) ? segment.speed : 6);
                const safeDirectSegment = {
                    type: 'sail-safe',
                    distance: directDistance,
                    speed: safeSpeed,
                    bearing: getBearing(startPoint, endPoint),
                    timeHours: directDistance / safeSpeed,
                    points: [startPoint, endPoint]
                };

                segment = safeDirectSegment;
                segmentLatLngs = buildSegmentLatLngs(
                    { lat: startPoint.lat, lng: startPoint.lon },
                    { lat: endPoint.lat, lng: endPoint.lon },
                    segment
                );

                if (polylineLatLngCrossesLand(segmentLatLngs)) {
                    alert(`Segment ${segmentsInfo.length + 1} invalide: la trajectoire coupe la terre. Ajoute un waypoint manuel ou baisse l'espacement WP auto.`);
                    return;
                }
            }

            routeSegmentsForDraw.push({
                latlngs: segmentLatLngs,
                windSpeed: wind.speed,
                mode: segment.type,
                waveHeight: weatherAtPassage.waveHeight,
                waveDirection: weatherAtPassage.waveDirection,
                segmentNumber: routeSegmentsForDraw.length + 1,
                sailSetup,
                sailComment,
                departureHour: passageSlot.hour
            });

            fullRoute = fullRoute.concat(segment.points);
            totalTime += segment.timeHours;
            totalDistance += segment.distance;

            const segmentArrivalDateTime = new Date(passageDateTime.getTime() + segment.timeHours * 3600 * 1000);

            if (startMeta || !activeDisplaySegment) {
                activeDisplaySegment = {
                    number: segmentsInfo.length + 1,
                    startType: startMeta?.type || 'user',
                    startLabel: startMeta?.label || `WP ${i + 1}`,
                    startIcon: startMeta?.icon || '📍',
                    startMarkerRef: startMeta?.marker || null,
                    startLatLng: startMeta?.latlng || { lat: startPoint.lat, lng: startPoint.lon },
                    departureLabel: formatWeekdayHourUtc(passageDateTime),
                    arrivalLabel: formatWeekdayHourUtc(segmentArrivalDateTime),
                    distance: '0.00',
                    time: '0.00',
                    speed: '0.00',
                    bearing: Math.round(segment.bearing),
                    windSpeed: '0.0',
                    windDirection: Math.round(wind.direction),
                    type: segment.type,
                    sailSetup,
                    sailComment,
                    departureHour: passageSlot.hour
                };
                segmentsInfo.push(activeDisplaySegment);
            }

            const previousDistance = Number(activeDisplaySegment.distance) || 0;
            const previousTime = Number(activeDisplaySegment.time) || 0;
            const aggregatedDistance = previousDistance + segment.distance;
            const aggregatedTime = previousTime + segment.timeHours;
            activeDisplaySegment.distance = aggregatedDistance.toFixed(2);
            activeDisplaySegment.time = aggregatedTime.toFixed(2);
            activeDisplaySegment.speed = (aggregatedTime > 0 ? (aggregatedDistance / aggregatedTime) : segment.speed).toFixed(2);
            activeDisplaySegment.arrivalLabel = formatWeekdayHourUtc(segmentArrivalDateTime);
            activeDisplaySegment.windSpeed = Math.max(Number(activeDisplaySegment.windSpeed) || 0, wind.speed).toFixed(1);
            activeDisplaySegment.windDirection = Math.round(wind.direction);
            const bearingStart = {
                lat: activeDisplaySegment.startLatLng?.lat,
                lon: activeDisplaySegment.startLatLng?.lon ?? activeDisplaySegment.startLatLng?.lng
            };
            const bearingEnd = {
                lat: endPoint?.lat,
                lon: endPoint?.lon ?? endPoint?.lng
            };
            const updatedBearing = getBearing(bearingStart, bearingEnd);
            if (Number.isFinite(updatedBearing)) {
                activeDisplaySegment.bearing = Math.round(updatedBearing);
            }

            if (activeDisplaySegment.type !== segment.type) {
                activeDisplaySegment.type = 'mixed';
            }

            if (activeDisplaySegment.sailSetup !== sailSetup) {
                if (activeDisplaySegment.type === 'mixed') {
                    activeDisplaySegment.sailSetup = 'Mixte';
                } else if (activeDisplaySegment.type === 'motor') {
                    activeDisplaySegment.sailSetup = 'Moteur';
                }
            }
        }
    }

    const arrivalDateTime = new Date(departureDateTime.getTime() + totalTime * 3600 * 1000);
    const arrivalSlot = toDateAndHourUtc(arrivalDateTime);
    const arrivalWeatherAtPassage = await getWeatherAtDateHour(
        routePoints[routePoints.length - 1].lat,
        routePoints[routePoints.length - 1].lng,
        arrivalSlot.date,
        arrivalSlot.hour
    );

    waypointPassageWeather.push({
        waypointIndex: routePoints.length - 1,
        weather: arrivalWeatherAtPassage,
        slot: arrivalSlot
    });
    waypointPassageSlots.set(routePoints.length - 1, arrivalSlot);

    drawWaypointWindDirections(waypointPassageWeather);

    drawRouteByWindSpeed(routeSegmentsForDraw);
    drawStrongWaveDirections(routeSegmentsForDraw);

    let segmentsHtml = `<table id="segmentsTable" style="width:100%; border-collapse:collapse; margin-top:8px; font-size:8px;"><tr style="border-bottom:1px solid #ccc;"><th style="text-align:left; padding:2px; width:22px;">WP</th><th style="text-align:left; padding:2px; width:14px;">Seg</th><th style="text-align:right; padding:2px;">${t('Départ', 'Salida')}</th><th style="text-align:right; padding:2px;">${t('Arrivée', 'Llegada')}</th><th style="text-align:right; padding:2px;">${t('Cap', 'Rumbo')}</th><th style="text-align:right; padding:2px;">${t('Dist.', 'Dist.')}</th><th style="text-align:right; padding:2px;">${t('Temps', 'Tiempo')}</th><th style="text-align:right; padding:2px;">${t('Vit.', 'Vel.')}</th><th style="text-align:right; padding:2px;">${t('V.V', 'V.V')}</th><th style="text-align:right; padding:2px;">${t('D.V', 'D.V')}</th><th style="text-align:left; padding:2px;">${t('Voiles', 'Velas')}</th></tr>`;
    
    segmentsInfo.forEach((seg, segIndex) => {
        segmentsHtml += `<tr class="segment-row" data-seg-index="${segIndex}" style="border-bottom:1px solid #eee; cursor:pointer;"><td style="padding:2px; width:22px;" title="${seg.startLabel}">${seg.startIcon}</td><td style="padding:2px; width:14px;">${seg.number}</td><td style="text-align:right; padding:2px;">${seg.departureLabel}</td><td style="text-align:right; padding:2px;">${seg.arrivalLabel}</td><td style="text-align:right; padding:2px;">${seg.bearing}°</td><td style="text-align:right; padding:2px;">${seg.distance} nm</td><td style="text-align:right; padding:2px;">${seg.time} h</td><td style="text-align:right; padding:2px;">${seg.speed} kn</td><td style="text-align:right; padding:2px;">${seg.windSpeed} kn</td><td style="text-align:right; padding:2px;">${seg.windDirection}°</td><td style="padding:2px;">${seg.sailSetup}</td></tr>`;
    });

    segmentsHtml += '</table>';

    const pressureSummary = buildPressureEvolutionSummary(waypointPassageWeather);
    const weatherUpdatedAt = formatUtcDateTime(lastWeatherUpdateAt);

    const waypointRowsForReport = waypointPassageWeather
        .map(entry => {
            const wpIndex = Number(entry.waypointIndex);
            const isArrival = wpIndex === routePoints.length - 1;
            const icon = isArrival ? '🏁' : '📍';
            const label = isArrival ? `Arrivée (WP ${wpIndex + 1})` : `WP ${wpIndex + 1}`;
            const weather = entry.weather || {};
            return {
                waypointIndex: wpIndex,
                label: `${icon} ${label}`,
                passageLabel: formatLocalSlotLabel(entry.slot),
                windSpeed: Number.isFinite(weather.windSpeed) ? `${weather.windSpeed.toFixed(1)} kn` : 'N/A',
                windDirection: Number.isFinite(weather.windDirection) ? `${Math.round(weather.windDirection)}° ${degreesToCardinalFr(weather.windDirection)}` : 'N/A',
                pressure: Number.isFinite(weather.pressure) ? `${weather.pressure.toFixed(0)} hPa` : 'N/A',
                waveHeight: Number.isFinite(weather.waveHeight) ? `${weather.waveHeight.toFixed(1)} m` : 'N/A',
                summary: weatherCodeToLabel(weather.weatherCode)
            };
        })
        .sort((a, b) => a.waypointIndex - b.waypointIndex)
        .map(({ waypointIndex, ...rest }) => rest);

    const routeNameInput = document.getElementById('routeNameInput');
    const routeName = routeNameInput?.value?.trim() || 'Route CEIBO';

    const routePolylineForReport = [];
    routeSegmentsForDraw.forEach(seg => {
        const latlngs = Array.isArray(seg?.latlngs) ? seg.latlngs : [];
        latlngs.forEach(pair => {
            const lat = Number(pair?.[0]);
            const lng = Number(pair?.[1]);
            if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;

            const previous = routePolylineForReport[routePolylineForReport.length - 1];
            if (previous && previous.lat === lat && previous.lng === lng) return;
            routePolylineForReport.push({ lat, lng });
        });
    });

    const routeWaypointsForReport = [];
    segmentsInfo.forEach(seg => {
        const lat = Number(seg?.startLatLng?.lat);
        const lng = Number(seg?.startLatLng?.lng);
        if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;
        routeWaypointsForReport.push({
            lat,
            lng,
            label: seg.startLabel,
            icon: seg.startIcon
        });
    });

    const finalPoint = routePoints[routePoints.length - 1];
    if (finalPoint) {
        routeWaypointsForReport.push({
            lat: Number(finalPoint.lat),
            lng: Number(finalPoint.lng),
            label: `Arrivée (WP ${routePoints.length})`,
            icon: '🏁'
        });
    }

    lastComputedReportData = {
        routeName,
        computedAt: new Date().toISOString(),
        departureIso: departureDateTime.toISOString(),
        arrivalIso: arrivalDateTime.toISOString(),
        weatherUpdatedAt: lastWeatherUpdateAt,
        metrics: {
            totalDistanceNm: totalDistance.toFixed(2),
            totalTimeHours: totalTime,
            totalTimeLabel: formatDurationHours(totalTime),
            segmentCount: segmentsInfo.length,
            generatedWaypointCount: generatedAutoWaypointCount,
            pressureSummary
        },
        segments: segmentsInfo.map(seg => ({
            number: seg.number,
            startIcon: seg.startIcon,
            startLabel: seg.startLabel,
            departureLabel: seg.departureLabel,
            arrivalLabel: seg.arrivalLabel,
            bearing: seg.bearing,
            distance: seg.distance,
            time: seg.time,
            speed: seg.speed,
            sailSetup: seg.sailSetup
        })),
        waypoints: waypointRowsForReport,
        routeVector: {
            polyline: routePolylineForReport,
            waypoints: routeWaypointsForReport
        }
    };

    document.getElementById("info").innerHTML =
        `<div class="segment-summary">
            <strong>${t('Segments', 'Segmentos')}: ${routePoints.length - 1}</strong><br>
            <strong>${t('Distance totale', 'Distancia total')}: ${totalDistance.toFixed(2)} nm</strong><br>
            <strong>${t('Temps total', 'Tiempo total')}: ${totalTime.toFixed(2)} h</strong><br>
            <strong>${t('WP auto générés', 'WP auto generados')}: ${generatedAutoWaypointCount}</strong><br>
            <strong>${t('Météo (dernière MAJ)', 'Meteo (última ACT)')}: ${weatherUpdatedAt}</strong><br>
            <strong>${t('Évolution pression', 'Evolución presión')}: ${pressureSummary}</strong>
        </div>` +
        segmentsHtml;

    const segmentRows = document.querySelectorAll('#segmentsTable .segment-row');
    segmentRows.forEach(row => {
        row.addEventListener('click', async () => {
            const rawSegIndex = row.getAttribute('data-seg-index');

            const segIndex = Number(rawSegIndex);
            const seg = segmentsInfo[segIndex];
            if (!seg?.startLatLng) return;

            const latlng = seg.startLatLng;
            map.panTo(latlng, { animate: true, duration: 0.45 });

            const marker = seg.startMarkerRef;
            if (!marker) return;
            await openWaypointWeatherPopup(marker);
        });
    });

    renderWindSpeedLegend();

    updateArrivalPointWeather(totalTime);
}

// =====================
// GET WIND
// =====================

async function getWind(lat, lon) {

    const url = `https://api.open-meteo.com/v1/forecast?latitude=${lat}&longitude=${lon}&start_date=${departureDate}&end_date=${departureDate}&hourly=windspeed_10m,winddirection_10m&windspeed_unit=kn&timezone=UTC`;

    const response = await fetch(url);
    const data = await response.json();

    // Extraire l'heure de départ (format HH:MM)
    const hourIndex = parseInt(departureTime.split(':')[0]);
    const windSpeed = data.hourly.windspeed_10m[hourIndex] || 10;
    const windDirection = data.hourly.winddirection_10m[hourIndex] || 0;

    return {
        speed: windSpeed,
        direction: windDirection
    };
}

function toDateAndHourUtc(dateObj) {
    const iso = dateObj.toISOString();
    const date = iso.slice(0, 10);
    const hour = `${String(dateObj.getUTCHours()).padStart(2, '0')}:00`;
    return { date, hour };
}

function formatWeekdayHourUtc(dateObj) {
    if (!(dateObj instanceof Date) || Number.isNaN(dateObj.getTime())) return 'N/A';

    const rounded = new Date(dateObj.getTime());
    const minutes = rounded.getMinutes();
    const roundedMinutes = Math.round(minutes / 10) * 10;
    rounded.setMinutes(roundedMinutes, 0, 0);

    const weekday = rounded.toLocaleDateString(getCurrentLocale(), { weekday: 'long' });
    const day = rounded.getDate();
    const hour = String(rounded.getHours()).padStart(2, '0');
    const minute = String(rounded.getMinutes()).padStart(2, '0');

    return `${weekday} ${day} ${hour}:${minute}`;
}

function formatLocalSlotLabel(slot) {
    if (!slot?.date || !slot?.hour) return 'N/A';
    const dateObj = new Date(`${slot.date}T${slot.hour}:00Z`);
    if (Number.isNaN(dateObj.getTime())) return 'N/A';
    return formatWeekdayHourUtc(dateObj);
}

function buildPressureEvolutionSummary(waypointPassageWeather) {
    const pressureSeries = waypointPassageWeather
        .map(entry => entry.weather?.pressure)
        .filter(value => Number.isFinite(value));

    if (pressureSeries.length < 2) return t('Données insuffisantes', 'Datos insuficientes');

    const firstPressure = pressureSeries[0];
    const lastPressure = pressureSeries[pressureSeries.length - 1];
    const delta = lastPressure - firstPressure;
    const roundedDelta = `${delta >= 0 ? '+' : ''}${delta.toFixed(1)} hPa`;

    let trend = t('stable', 'estable');
    if (delta > 0.8) trend = t('hausse', 'subida');
    if (delta < -0.8) trend = t('baisse', 'bajada');

    return `${firstPressure.toFixed(0)} → ${lastPressure.toFixed(0)} hPa (${roundedDelta}, ${trend})`;
}

function buildWeatherMetricEvolutionSummary(values, {
    unit = '',
    decimals = 1,
    riseThreshold = 0.8,
    fallThreshold = -0.8
} = {}) {
    const series = Array.isArray(values) ? values.filter(value => Number.isFinite(value)) : [];
    if (series.length < 2) return t('Données insuffisantes', 'Datos insuficientes');

    const first = series[0];
    const last = series[series.length - 1];
    const delta = last - first;
    const sign = delta >= 0 ? '+' : '';

    let trend = t('stable', 'estable');
    if (delta > riseThreshold) trend = t('hausse', 'subida');
    if (delta < fallThreshold) trend = t('baisse', 'bajada');

    return `${first.toFixed(decimals)} → ${last.toFixed(decimals)}${unit} (${sign}${delta.toFixed(decimals)}${unit}, ${trend})`;
}

function weatherCodeToLabel(code) {
    const labels = {
        0: t('Ciel dégagé', 'Cielo despejado'),
        1: t('Peu nuageux', 'Poco nuboso'),
        2: t('Partiellement nuageux', 'Parcialmente nuboso'),
        3: t('Couvert', 'Cubierto'),
        45: t('Brouillard', 'Niebla'),
        48: t('Brouillard givrant', 'Niebla helada'),
        51: t('Bruine légère', 'Llovizna ligera'),
        53: t('Bruine modérée', 'Llovizna moderada'),
        55: t('Bruine forte', 'Llovizna fuerte'),
        61: t('Pluie faible', 'Lluvia débil'),
        63: t('Pluie modérée', 'Lluvia moderada'),
        65: t('Pluie forte', 'Lluvia fuerte'),
        71: t('Neige faible', 'Nieve débil'),
        73: t('Neige modérée', 'Nieve moderada'),
        75: t('Neige forte', 'Nieve fuerte'),
        80: t('Averses faibles', 'Chubascos débiles'),
        81: t('Averses modérées', 'Chubascos moderados'),
        82: t('Averses fortes', 'Chubascos fuertes'),
        95: t('Orage', 'Tormenta')
    };

    return labels[code] || t('Conditions variables', 'Condiciones variables');
}

function degreesToCardinalFr(degrees) {
    if (!Number.isFinite(degrees)) return 'N/A';
    const directions = currentLanguage === 'es'
        ? ['Norte', 'Noreste', 'Este', 'Sureste', 'Sur', 'Suroeste', 'Oeste', 'Noroeste']
        : ['Nord', 'Nord-Est', 'Est', 'Sud-Est', 'Sud', 'Sud-Ouest', 'Ouest', 'Nord-Ouest'];
    const normalized = ((degrees % 360) + 360) % 360;
    const index = Math.round(normalized / 45) % 8;
    return directions[index];
}

function getWeatherCacheKey(lat, lon) {
    const roundedLat = Number(lat).toFixed(4);
    const roundedLon = Number(lon).toFixed(4);
    return `${roundedLat},${roundedLon}|${departureDate}|${departureTime}`;
}

function getWeatherCacheKeyAtDateHour(lat, lon, date, hour) {
    const roundedLat = Number(lat).toFixed(4);
    const roundedLon = Number(lon).toFixed(4);
    return `${roundedLat},${roundedLon}|${date}|${hour}`;
}

async function fetchJsonWithRetry(url, { retries = 2, timeoutMs = 9000 } = {}) {
    let lastError = null;

    for (let attempt = 0; attempt <= retries; attempt++) {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), timeoutMs);

        try {
            const response = await fetch(url, { signal: controller.signal });
            clearTimeout(timeout);

            if (!response.ok) {
                lastError = new Error(`HTTP ${response.status}`);
            } else {
                return await response.json();
            }
        } catch (error) {
            clearTimeout(timeout);
            lastError = error;
        }

        if (attempt < retries) {
            await new Promise(resolve => setTimeout(resolve, 350 * (attempt + 1)));
        }
    }

    return null;
}

async function getWeatherAtDateHour(lat, lon, date, hour) {
    const cacheKey = getWeatherCacheKeyAtDateHour(lat, lon, date, hour);
    if (weatherCache.has(cacheKey)) return weatherCache.get(cacheKey);

    const forecastUrl = `https://api.open-meteo.com/v1/forecast?latitude=${lat}&longitude=${lon}&start_date=${date}&end_date=${date}&hourly=temperature_2m,windspeed_10m,winddirection_10m,windgusts_10m,precipitation,weather_code,surface_pressure&windspeed_unit=kn&timezone=UTC`;
    const marineUrl = `https://marine-api.open-meteo.com/v1/marine?latitude=${lat}&longitude=${lon}&start_date=${date}&end_date=${date}&hourly=wave_height,wave_direction,wave_period&timezone=UTC`;

    const [data, marineData] = await Promise.all([
        fetchJsonWithRetry(forecastUrl, { retries: 2, timeoutMs: 9000 }),
        fetchJsonWithRetry(marineUrl, { retries: 1, timeoutMs: 9000 })
    ]);

    const parsedHour = parseInt(String(hour || '12:00').split(':')[0], 10);
    const hourIndex = Number.isFinite(parsedHour) ? Math.max(0, Math.min(23, parsedHour)) : 12;

    const fetchedAt = new Date().toISOString();

    const weather = {
        temperature: data?.hourly?.temperature_2m?.[hourIndex] ?? 20,
        windSpeed: data?.hourly?.windspeed_10m?.[hourIndex] ?? 10,
        windGust: data?.hourly?.windgusts_10m?.[hourIndex] ?? 12,
        windDirection: data?.hourly?.winddirection_10m?.[hourIndex] ?? 0,
        precipitation: data?.hourly?.precipitation?.[hourIndex] ?? 0,
        pressure: data?.hourly?.surface_pressure?.[hourIndex] ?? 1015,
        waveHeight: marineData?.hourly?.wave_height?.[hourIndex],
        waveDirection: marineData?.hourly?.wave_direction?.[hourIndex],
        wavePeriod: marineData?.hourly?.wave_period?.[hourIndex],
        weatherCode: data?.hourly?.weather_code?.[hourIndex] ?? 2,
        updatedAt: fetchedAt
    };

    lastWeatherUpdateAt = fetchedAt;

    weatherCache.set(cacheKey, weather);
    return weather;
}

async function getWeatherAtWaypoint(lat, lon) {
    return getWeatherAtDateHour(lat, lon, departureDate, departureTime);
}

function getWaypointSlotByMarker(marker) {
    if (marker?._ceiboPassageSlot) return marker._ceiboPassageSlot;

    const index = markers.indexOf(marker);
    if (index === -1) return null;
    return waypointPassageSlots.get(index) || null;
}

async function getWeatherForMarker(marker) {
    const current = marker.getLatLng();
    const slot = getWaypointSlotByMarker(marker);

    if (slot) {
        const weather = await getWeatherAtDateHour(current.lat, current.lng, slot.date, slot.hour);
        return {
            weather,
            referenceLabel: formatLocalSlotLabel(slot)
        };
    }

    const weather = await getWeatherAtWaypoint(current.lat, current.lng);
    const fallbackSlot = { date: departureDate, hour: departureTime };
    return {
        weather,
        referenceLabel: formatLocalSlotLabel(fallbackSlot)
    };
}

async function getCurrentWeatherAtWaypoint(lat, lon) {
    const now = new Date();
    const { date, hour } = toDateAndHourUtc(now);
    return getWeatherAtDateHour(lat, lon, date, hour);
}

function formatWeatherTooltipContent(weather, referenceLabel) {
    const temp = Number.isFinite(weather.temperature) ? `${weather.temperature.toFixed(1)}°C` : 'N/A';
    const windSpeed = Number.isFinite(weather.windSpeed) ? `${weather.windSpeed.toFixed(1)} kn` : 'N/A';
    const windGust = Number.isFinite(weather.windGust) ? `${weather.windGust.toFixed(1)} kn` : 'N/A';
    const windDirection = Number.isFinite(weather.windDirection) ? `${Math.round(weather.windDirection)}°` : 'N/A';
    const windCardinal = degreesToCardinalFr(weather.windDirection);
    const precipitation = Number.isFinite(weather.precipitation) ? `${weather.precipitation.toFixed(1)} mm` : 'N/A';
    const pressure = Number.isFinite(weather.pressure) ? `${weather.pressure.toFixed(0)} hPa` : 'N/A';
    const waveHeight = Number.isFinite(weather.waveHeight) ? `${weather.waveHeight.toFixed(1)} m` : 'N/A';
    const wavePeriod = Number.isFinite(weather.wavePeriod) ? `${weather.wavePeriod.toFixed(1)} s` : 'N/A';
    const waveDirection = Number.isFinite(weather.waveDirection) ? `${Math.round(weather.waveDirection)}°` : 'N/A';
    const waveCardinal = degreesToCardinalFr(weather.waveDirection);
    const seaComfort = getSeaComfortLevel(weather);
    const passageRef = referenceLabel || formatLocalSlotLabel({ date: departureDate, hour: departureTime });
    const summary = weatherCodeToLabel(weather.weatherCode);

    return `<strong>${t('Météo', 'Meteo')}</strong><br>${summary}<br>${t('Temp', 'Temp')}: ${temp}<br>${t('Vent', 'Viento')}: ${windSpeed} (${windDirection}, ${windCardinal})<br>${t('Rafales', 'Ráfagas')}: ${windGust}<br>${t('Pluie', 'Lluvia')}: ${precipitation}<br>${t('Pression', 'Presión')}: ${pressure}<br>${t('Houle', 'Oleaje')}: ${waveHeight} · ${wavePeriod} · ${waveDirection} (${waveCardinal})<br>${seaComfort}<br>${t('Passage WP', 'Paso WP')}: ${passageRef}`;
}

function formatUtcDateTime(isoString) {
    if (!isoString) return 'N/A';
    const date = new Date(isoString);
    if (Number.isNaN(date.getTime())) return 'N/A';

    return `${date.toLocaleDateString(getCurrentLocale())} ${date.toLocaleTimeString(getCurrentLocale(), {
        hour: '2-digit',
        minute: '2-digit'
    })}`;
}

function renderWaypointWeatherInfo(marker, weather, referenceLabel) {
    const weatherContainer = document.getElementById('waypointWeatherInfo');
    if (!weatherContainer) return;

    const current = marker.getLatLng();
    const index = markers.indexOf(marker);
    const waypointLabel = index !== -1 ? `Waypoint ${index + 1}` : 'Waypoint';

    const temp = Number.isFinite(weather.temperature) ? `${weather.temperature.toFixed(1)}°C` : 'N/A';
    const windSpeed = Number.isFinite(weather.windSpeed) ? `${weather.windSpeed.toFixed(1)} kn` : 'N/A';
    const windGust = Number.isFinite(weather.windGust) ? `${weather.windGust.toFixed(1)} kn` : 'N/A';
    const windDirectionDeg = Number.isFinite(weather.windDirection) ? `${Math.round(weather.windDirection)}°` : 'N/A';
    const windDirectionCardinal = degreesToCardinalFr(weather.windDirection);
    const precipitation = Number.isFinite(weather.precipitation) ? `${weather.precipitation.toFixed(1)} mm` : 'N/A';
    const pressure = Number.isFinite(weather.pressure) ? `${weather.pressure.toFixed(0)} hPa` : 'N/A';
    const waveHeight = Number.isFinite(weather.waveHeight) ? `${weather.waveHeight.toFixed(1)} m` : 'N/A';
    const wavePeriod = Number.isFinite(weather.wavePeriod) ? `${weather.wavePeriod.toFixed(1)} s` : 'N/A';
    const waveDirectionDeg = Number.isFinite(weather.waveDirection) ? `${Math.round(weather.waveDirection)}°` : 'N/A';
    const waveDirectionCardinal = degreesToCardinalFr(weather.waveDirection);
    const seaComfort = getSeaComfortLevel(weather);
    const passageRef = referenceLabel || formatLocalSlotLabel({ date: departureDate, hour: departureTime });
    const summary = weatherCodeToLabel(weather.weatherCode);

    weatherContainer.innerHTML =
        `<strong>${waypointLabel}</strong><br>` +
        `${current.lat.toFixed(4)}, ${current.lng.toFixed(4)}<br>` +
        `<strong>${summary}</strong><br>` +
        `${t('Température', 'Temperatura')}: ${temp}<br>` +
        `${t('Vent', 'Viento')}: ${windSpeed} — ${windDirectionDeg} (${windDirectionCardinal})<br>` +
        `${t('Rafales', 'Ráfagas')}: ${windGust}<br>` +
        `${t('Pluie', 'Lluvia')}: ${precipitation}<br>` +
        `${t('Pression', 'Presión')}: ${pressure}<br>` +
        `${t('Houle', 'Oleaje')}: ${waveHeight} · ${wavePeriod} · ${waveDirectionDeg} (${waveDirectionCardinal})<br>` +
        `${seaComfort}<br>` +
        `${t('Passage WP', 'Paso WP')}: ${passageRef}`;
}

function formatWaypointPopupContent(marker, weather, referenceLabel) {
    const current = marker.getLatLng();
    const index = markers.indexOf(marker);
    const waypointLabel = marker?._ceiboLabel || (index !== -1 ? `Waypoint ${index + 1}` : 'Waypoint');

    return `<strong>${waypointLabel}</strong><br>${current.lat.toFixed(4)}, ${current.lng.toFixed(4)}<br>${formatWeatherTooltipContent(weather, referenceLabel).replace('<strong>Météo</strong><br>', '')}`;
}

async function openWaypointWeatherPopup(marker) {
    const markerLabel = marker?._ceiboLabel || 'Waypoint';
    marker.bindPopup(`<strong>${markerLabel}</strong><br>${t('Chargement météo...', 'Cargando meteo...')}`, { maxWidth: 340, autoPan: false });
    marker.openPopup();

    try {
        const result = await getWeatherForMarker(marker);
        marker.setPopupContent(formatWaypointPopupContent(marker, result.weather, result.referenceLabel));
        marker.openPopup();
    } catch (error) {
        marker.setPopupContent(`<strong>${t('Waypoint', 'Waypoint')}</strong><br>${t('Météo indisponible', 'Meteo no disponible')}`);
        marker.openPopup();
    }
}

function createWaypointMarker(latlng) {
    const marker = L.marker(latlng, { draggable: true }).addTo(map);

    marker.on('dragend', function() {
        const index = markers.indexOf(marker);
        if (index !== -1) {
            routePoints[index] = marker.getLatLng();
            selectUserWaypoint(marker);
            invalidateComputedRouteDisplay();
        }
    });

    marker.on('mouseover', async function() {
        marker.bindTooltip(t('Chargement météo...', 'Cargando meteo...'), { direction: 'top', opacity: 0.95 });
        marker.openTooltip();

        try {
            const result = await getWeatherForMarker(marker);
            marker.setTooltipContent(formatWeatherTooltipContent(result.weather, result.referenceLabel));
            marker.openTooltip();
        } catch (error) {
            marker.setTooltipContent(t('Météo indisponible', 'Meteo no disponible'));
            marker.openTooltip();
        }
    });

    marker.on('mouseout', function() {
        marker.closeTooltip();
    });

    marker.on('click', async function() {
        selectUserWaypoint(marker);
        try {
            const result = await getWeatherForMarker(marker);
            renderWaypointWeatherInfo(marker, result.weather, result.referenceLabel);
        } catch (error) {
            const weatherContainer = document.getElementById('waypointWeatherInfo');
            if (weatherContainer) {
                weatherContainer.innerHTML = `<strong>${t('Waypoint', 'Waypoint')}</strong><br>${t('Météo indisponible', 'Meteo no disponible')}`;
            }
        }
    });

    marker.on('contextmenu', function() {
        const index = markers.indexOf(marker);
        deleteUserWaypointAtIndex(index);
    });

    return marker;
}

function clearGeneratedWaypointMarkers() {
    generatedWaypointMarkers.forEach(marker => {
        if (map.hasLayer(marker)) map.removeLayer(marker);
    });
    generatedWaypointMarkers = [];
}

function createGeneratedWaypointMarker(latlng, label, badgeText) {
    const generatedIcon = L.divIcon({
        className: 'generated-waypoint-map-icon',
        html: `<div class="generated-waypoint-map-icon__dot">${badgeText}</div>`,
        iconSize: [24, 20],
        iconAnchor: [12, 10]
    });

    const marker = L.marker(latlng, {
        icon: generatedIcon,
        keyboard: false,
        zIndexOffset: 300
    }).addTo(map);

    marker._ceiboLabel = label;

    marker.on('mouseover', async function() {
        marker.bindTooltip(t('Chargement météo...', 'Cargando meteo...'), { direction: 'top', opacity: 0.95 });
        marker.openTooltip();

        try {
            const result = await getWeatherForMarker(marker);
            marker.setTooltipContent(formatWeatherTooltipContent(result.weather, result.referenceLabel));
            marker.openTooltip();
        } catch (error) {
            marker.setTooltipContent(t('Météo indisponible', 'Meteo no disponible'));
            marker.openTooltip();
        }
    });

    marker.on('mouseout', function() {
        marker.closeTooltip();
    });

    marker.on('click', async function() {
        await openWaypointWeatherPopup(marker);
        try {
            const result = await getWeatherForMarker(marker);
            renderWaypointWeatherInfo(marker, result.weather, result.referenceLabel);
        } catch (error) {
            const weatherContainer = document.getElementById('waypointWeatherInfo');
            if (weatherContainer) {
                weatherContainer.innerHTML = `<strong>${label}</strong><br>${t('Météo indisponible', 'Meteo no disponible')}`;
            }
        }
    });

    return marker;
}

function clearWaypointWindDirectionLayers() {
    waypointWindDirectionLayers.forEach(layer => {
        if (map.hasLayer(layer)) map.removeLayer(layer);
    });
    waypointWindDirectionLayers = [];
}

function drawWaypointWindDirections(waypointPassageWeather) {
    clearWaypointWindDirectionLayers();

    waypointPassageWeather.forEach(entry => {
        const marker = markers[entry.waypointIndex];
        const windDirection = entry.weather?.windDirection;

        if (!marker || !Number.isFinite(windDirection)) return;

        const iconDirection = (windDirection - 90 + 360) % 360;
        const sourceCardinal = degreesToCardinalFr(windDirection);
        const windSpeed = entry.weather?.windSpeed;
        const windSpeedLabel = Number.isFinite(windSpeed) ? `${windSpeed.toFixed(1)} kn` : 'N/A';
        const icon = L.divIcon({
            className: 'wind-direction-waypoint',
            html: `<div class="wind-direction-waypoint__arrow" style="transform: rotate(${iconDirection}deg);">➤</div>`,
            iconSize: [16, 16],
            iconAnchor: [8, 8]
        });

        const arrowLayer = L.marker(marker.getLatLng(), {
            icon,
            interactive: true,
            keyboard: false,
            zIndexOffset: 1000
        }).addTo(map);

        arrowLayer.bindTooltip(`${t('Vent de', 'Viento de')} ${sourceCardinal} · ${windSpeedLabel}`, {
            direction: 'top',
            opacity: 0.95
        });

        waypointWindDirectionLayers.push(arrowLayer);
    });
}

async function updateArrivalPointWeather(totalTimeHours) {
    if (!markers.length) return;

    const arrivalMarker = markers[markers.length - 1];
    const destination = arrivalMarker.getLatLng();
    const departureDateTime = new Date(`${departureDate}T${departureTime}:00`);

    if (Number.isNaN(departureDateTime.getTime())) return;

    const arrivalDateTime = new Date(departureDateTime.getTime() + totalTimeHours * 3600 * 1000);
    const arrivalSlot = toDateAndHourUtc(arrivalDateTime);

    try {
        const [currentWeather, arrivalWeather] = await Promise.all([
            getCurrentWeatherAtWaypoint(destination.lat, destination.lng),
            getWeatherAtDateHour(destination.lat, destination.lng, arrivalSlot.date, arrivalSlot.hour)
        ]);

        const now = new Date();
        const currentRef = formatWeekdayHourUtc(now);
        const arrivalRef = formatLocalSlotLabel(arrivalSlot);

        const currentLine = formatWeatherTooltipContent(currentWeather, currentRef);
        const arrivalLine = formatWeatherTooltipContent(arrivalWeather, arrivalRef);

        const popupContent =
            `<strong>${t("Point d'arrivée", 'Punto de llegada')}</strong><br>` +
            `<strong>${t('Météo actuelle', 'Meteo actual')}</strong><br>${currentLine.replace('<strong>Météo</strong><br>', '').replace('<strong>Meteo</strong><br>', '')}<br><br>` +
            `<strong>${t('Météo prévue', 'Meteo prevista')} (${arrivalRef})</strong><br>${arrivalLine.replace('<strong>Météo</strong><br>', '').replace('<strong>Meteo</strong><br>', '')}`;

        arrivalMarker.bindPopup(popupContent, { maxWidth: 320 });
        arrivalMarker.openPopup();
    } catch (error) {
        arrivalMarker.bindPopup(`<strong>${t("Point d'arrivée", 'Punto de llegada')}</strong><br>${t('Météo indisponible', 'Meteo no disponible')}`, { maxWidth: 300 });
    }
}

// =====================
// DRAW ROUTE
// =====================

function drawRoute(points) {

    if (routeLayer) map.removeLayer(routeLayer);

    const latlngs = points.map(p => [p.lat, p.lon ?? p.lng]);

    routeLayer = L.polyline(latlngs, {
        color: 'orange',
        weight: 3
    }).addTo(map);

    routeLayer.on('click', function(e) {
        if (e?.originalEvent) L.DomEvent.stop(e.originalEvent);

        const insertIndex = getLogicalInsertionIndexFromRouteClick(e.latlng);
        addUserWaypoint(e.latlng, { insertIndex });
    });
}

function buildSegmentLatLngs(start, end, segment) {
    const segmentPoints = Array.isArray(segment?.points) ? segment.points : [];
    const latlngs = [[start.lat, start.lng]];

    segmentPoints.forEach(point => {
        const lat = point.lat;
        const lon = point.lon ?? point.lng;
        if (Number.isFinite(lat) && Number.isFinite(lon)) {
            const previous = latlngs[latlngs.length - 1];
            if (!previous || previous[0] !== lat || previous[1] !== lon) {
                latlngs.push([lat, lon]);
            }
        }
    });

    const endLatLng = [end.lat, end.lng];
    const last = latlngs[latlngs.length - 1];
    if (!last || last[0] !== endLatLng[0] || last[1] !== endLatLng[1]) {
        latlngs.push(endLatLng);
    }

    return latlngs;
}

function getWindSpeedColor(speed) {
    if (!Number.isFinite(speed)) return '#9e9e9e';
    if (speed < 8) return '#2ecc71';
    if (speed < 14) return '#f1c40f';
    if (speed < 20) return '#e67e22';
    return '#e74c3c';
}

function getRouteSegmentColor(segment) {
    if (segment?.mode === 'motor') return '#ff4fa3';
    return getWindSpeedColor(segment?.windSpeed);
}

function drawRouteByWindSpeed(routeSegments) {
    if (routeLayer) map.removeLayer(routeLayer);

    const polylines = routeSegments
        .filter(seg => Array.isArray(seg.latlngs) && seg.latlngs.length >= 2)
        .map(seg => {
            const polyline = L.polyline(seg.latlngs, {
                color: getRouteSegmentColor(seg),
                weight: 4,
                opacity: 0.95
            });

            const popupText =
                `<strong>${t('Segment', 'Segmento')} ${seg.segmentNumber}</strong><br>` +
                `${t('Départ', 'Salida')}: ${seg.departureHour} UTC<br>` +
                `${t('Réglage voiles', 'Ajuste velas')}: ${seg.sailSetup}<br>` +
                `${seg.sailComment}`;

            polyline.bindPopup(popupText, { maxWidth: 340 });

            polyline.on('click', function(e) {
                if (e?.originalEvent) L.DomEvent.stop(e.originalEvent);

                const segmentNumber = Number(seg?.segmentNumber);
                const insertIndex = Number.isInteger(segmentNumber) && segmentNumber >= 1
                    ? Math.min(Math.max(segmentNumber, 1), routePoints.length)
                    : getLogicalInsertionIndexFromRouteClick(e.latlng);

                addUserWaypoint(e.latlng, { insertIndex });
            });

            return polyline;
        });

    routeLayer = L.layerGroup(polylines).addTo(map);

    if (polylines.length > 0) {
        const bounds = L.featureGroup(polylines).getBounds();
        if (bounds.isValid()) {
            lastRouteBounds = bounds;
            map.fitBounds(bounds, { padding: [30, 30] });
        }
    }
}

function recenterOnRoute() {
    if (!lastRouteBounds || !lastRouteBounds.isValid()) return;
    map.fitBounds(lastRouteBounds, { padding: [30, 30] });
}

function clearWaveDirectionSegmentLayers() {
    waveDirectionSegmentLayers.forEach(layer => {
        if (map.hasLayer(layer)) map.removeLayer(layer);
    });
    waveDirectionSegmentLayers = [];
}

function getSegmentMidpoint(latlngs) {
    if (!Array.isArray(latlngs) || latlngs.length < 2) return null;
    const middleIndex = Math.floor((latlngs.length - 1) / 2);
    const a = latlngs[middleIndex];
    const b = latlngs[middleIndex + 1] || a;
    return {
        lat: (a[0] + b[0]) / 2,
        lng: (a[1] + b[1]) / 2
    };
}

function drawStrongWaveDirections(routeSegments) {
    clearWaveDirectionSegmentLayers();

    routeSegments.forEach(seg => {
        const waveHeight = seg?.waveHeight;
        const waveDirection = seg?.waveDirection;

        if (!Number.isFinite(waveHeight) || waveHeight < STRONG_WAVE_THRESHOLD_M) return;
        if (!Number.isFinite(waveDirection)) return;

        const mid = getSegmentMidpoint(seg.latlngs);
        if (!mid) return;

        const iconDirection = (waveDirection - 90 + 360) % 360;
        const sourceCardinal = degreesToCardinalFr(waveDirection);
        const icon = L.divIcon({
            className: 'wave-direction-segment',
            html: `<div class="wave-direction-segment__arrow" style="transform: rotate(${iconDirection}deg);">➵</div>`,
            iconSize: [16, 16],
            iconAnchor: [8, 8]
        });

        const waveLayer = L.marker([mid.lat, mid.lng], {
            icon,
            interactive: true,
            keyboard: false,
            zIndexOffset: 900
        }).addTo(map);

        waveLayer.bindTooltip(`Houle forte de ${sourceCardinal} · ${waveHeight.toFixed(1)} m`, {
            direction: 'top',
            opacity: 0.95
        });

        waveDirectionSegmentLayers.push(waveLayer);
    });
}

function renderWindSpeedLegend() {
    const legend = document.getElementById('windSpeedLegend');
    if (!legend) return;

    legend.innerHTML =
    `<strong>${t('Légende vent (route)', 'Leyenda viento (ruta)')}</strong>` +
    `<div class="wind-legend-row"><span class="wind-legend-swatch" style="background:#ff4fa3"></span><span>${t('Moteur (&lt; 5 kn) · 7 kn', 'Motor (&lt; 5 kn) · 7 kn')}</span></div>` +
        '<div class="wind-legend-row"><span class="wind-legend-swatch" style="background:#2ecc71"></span><span>&lt; 8 kn</span></div>' +
        '<div class="wind-legend-row"><span class="wind-legend-swatch" style="background:#f1c40f"></span><span>8–14 kn</span></div>' +
        '<div class="wind-legend-row"><span class="wind-legend-swatch" style="background:#e67e22"></span><span>14–20 kn</span></div>' +
        '<div class="wind-legend-row"><span class="wind-legend-swatch" style="background:#e74c3c"></span><span>&gt; 20 kn</span></div>';
}

// =====================
// DRAW WIND ARROW
// =====================

function drawWind(lat, lon, direction) {

    if (windLayer) map.removeLayer(windLayer);

    const length = 0.3;

    const endLat = lat + length * Math.cos((direction - 180) * Math.PI/180);
    const endLon = lon + length * Math.sin((direction - 180) * Math.PI/180);

    windLayer = L.polyline(
        [[lat, lon], [endLat, endLon]],
        {color: 'blue', weight: 3}
    ).addTo(map);
}

// =====================
// SAVE / LOAD ROUTES
// =====================

function sanitizeSavedRoute(route, fallbackIndex = 0) {
    const nowIso = new Date().toISOString();
    const name = String(route?.name || `Route ${fallbackIndex + 1}`).trim() || `Route ${fallbackIndex + 1}`;
    const date = String(route?.date || departureDate || nowIso.slice(0, 10));
    const time = normalizeHourTime(route?.time || departureTime || '12:00');
    const tack = Number(route?.tackingTimeHours);
    const tacking = Number.isFinite(tack) && tack > 0 ? tack : 0.5;
    const points = Array.isArray(route?.points)
        ? route.points
            .map(pt => ({
                lat: Number(pt?.lat),
                lon: Number(pt?.lon ?? pt?.lng)
            }))
            .filter(pt => Number.isFinite(pt.lat) && Number.isFinite(pt.lon))
        : [];

    return {
        name,
        date,
        time,
        tackingTimeHours: tacking,
        points,
        createdAt: String(route?.createdAt || nowIso),
        updatedAt: String(route?.updatedAt || route?.createdAt || nowIso)
    };
}

function loadRoutesFromLocalStorage() {
    try {
        const raw = localStorage.getItem(SAVED_ROUTES_STORAGE_KEY);
        const parsed = raw ? JSON.parse(raw) : [];
        if (!Array.isArray(parsed)) return [];
        return parsed.map((route, index) => sanitizeSavedRoute(route, index));
    } catch (_error) {
        return [];
    }
}

function getSavedRoutes() {
    return Array.isArray(savedRoutesCache) ? savedRoutesCache : [];
}

function setSavedRoutes(list) {
    const normalized = Array.isArray(list)
        ? list.map((route, index) => sanitizeSavedRoute(route, index))
        : [];
    savedRoutesCache = normalized;
    localStorage.setItem(SAVED_ROUTES_STORAGE_KEY, JSON.stringify(normalized));
}

function loadCloudConfigFromStorage() {
    try {
        const raw = localStorage.getItem(CLOUD_CONFIG_STORAGE_KEY);
        if (!raw) return null;
        const parsed = JSON.parse(raw);
        const url = String(parsed?.url || '').trim();
        const anonKey = String(parsed?.anonKey || '').trim();
        const projectKey = String(parsed?.projectKey || '').trim();
        if (!url || !anonKey || !projectKey) return null;
        return { url, anonKey, projectKey };
    } catch (_error) {
        return null;
    }
}

function saveCloudConfigToStorage(config) {
    if (!config?.url || !config?.anonKey || !config?.projectKey) return;
    localStorage.setItem(CLOUD_CONFIG_STORAGE_KEY, JSON.stringify(config));
}

function setCloudStatus(message, isError = false) {
    const status = document.getElementById('cloudStatus');
    if (!status) return;
    status.textContent = message;
    status.style.color = isError ? '#ff8f8f' : '';
}

function updateCloudDataSourceStatus(sourceLabel, routeCount = null, photoCount = null) {
    const status = document.getElementById('cloudDataSourceStatus');
    if (!status) return;

    const safeSource = String(sourceLabel || 'inconnu');
    const sourceLabelMap = {
        'verrouillé (auth requise)': t('verrouillé (auth requise)', 'bloqueado (auth requerida)'),
        'attente authentification': t('attente authentification', 'esperando autenticación'),
        'cache local': t('cache local', 'caché local'),
        'cloud': t('cloud', 'nube'),
        'cache local (fallback)': t('cache local (fallback)', 'caché local (fallback)'),
        'indisponible': t('indisponible', 'no disponible'),
        'initialisation': t('initialisation', 'inicialización'),
        'cache local (cloud vide)': t('cache local (cloud vide)', 'caché local (nube vacía)'),
        'local (non synchronisé)': t('local (non synchronisé)', 'local (no sincronizado)'),
        'cache local (synchro en échec)': t('cache local (synchro en échec)', 'caché local (sincronización fallida)')
    };
    const safeSourceLocalized = sourceLabelMap[safeSource] || safeSource;
    const routesLabel = Number.isFinite(routeCount) ? routeCount : getSavedRoutes().length;
    const photosLabel = Number.isFinite(photoCount) ? photoCount : waypointPhotoEntries.length;
    status.textContent = `${t('Données routes/photos/maintenance', 'Datos rutas/fotos/mantenimiento')}: ${safeSourceLocalized} · ${t('routes', 'rutas')}: ${routesLabel} · ${t('photos', 'fotos')}: ${photosLabel} · ${t('schémas', 'esquemas')}: ${maintenanceBoards.length} · ${t('dépenses', 'gastos')}: ${maintenanceExpenses.length} · ${t('fournisseurs', 'proveedores')}: ${maintenanceSuppliers.length}`;
}

function formatCloudError(error) {
    if (!error) return 'erreur inconnue';
    const parts = [];
    if (error.code) parts.push(String(error.code));
    if (error.status) parts.push(`HTTP ${error.status}`);
    if (error.message) parts.push(String(error.message));
    const detail = error.details || error.hint;
    if (detail) parts.push(String(detail));
    return parts.join(' · ') || String(error);
}

function isCloudReady() {
    if (!cloudConnected || !cloudClient || !cloudConfig?.projectKey) return false;
    if (isAuthRequiredRuntime() && !cloudAuthUser) return false;
    return true;
}

async function autoPullRoutesFromCloud(trigger = 'auto') {
    if (!isCloudReady() || cloudAutoPullInFlight) return false;
    cloudAutoPullInFlight = true;

    try {
        const routes = await pullRoutesFromCloud();
        refreshSavedList();
        if (trigger !== 'silent') {
            setCloudStatus(t(`Cloud synchro auto · ${routes.length} route(s)`, `Nube sincronización auto · ${routes.length} ruta(s)`));
        }
        return true;
    } catch (error) {
        setCloudStatus(t(`Synchro auto cloud impossible: ${formatCloudError(error)}`, `Sincronización auto nube imposible: ${formatCloudError(error)}`), true);
        return false;
    } finally {
        cloudAutoPullInFlight = false;
    }
}

function stopCloudAutoSync() {
    if (!cloudAutoPullTimer) return;
    clearInterval(cloudAutoPullTimer);
    cloudAutoPullTimer = null;
}

function startCloudAutoSync() {
    stopCloudAutoSync();
    if (!isCloudReady()) return;
    cloudAutoPullTimer = setInterval(() => {
        autoPullRoutesFromCloud('silent');
    }, CLOUD_AUTO_PULL_INTERVAL_MS);
}

async function pullRoutesFromCloud() {
    if (!isCloudReady()) return getSavedRoutes();

    const localRoutesBeforePull = [...getSavedRoutes()];

    const { data, error } = await cloudClient
        .from(CLOUD_TABLE_NAME)
        .select('routes')
        .eq('project_key', cloudConfig.projectKey)
        .maybeSingle();

    if (error) throw error;

    const rawPayload = data?.routes;
    const hasCloudPayload = rawPayload !== undefined && rawPayload !== null;

    if (!hasCloudPayload) {
        updateCloudDataSourceStatus('cache local (cloud vide)', localRoutesBeforePull.length, waypointPhotoEntries.length);
        return localRoutesBeforePull;
    }

    let rawRoutes = [];
    let rawWaypointPhotos = null;
    let rawMaintenanceBoards = null;
    let rawMaintenanceExpenses = null;
    let rawMaintenanceSuppliers = null;
    let rawNavLogEntries = null;
    let rawEngineLogEntries = null;

    if (Array.isArray(rawPayload)) {
        rawRoutes = rawPayload;
    } else if (rawPayload && typeof rawPayload === 'object') {
        rawRoutes = Array.isArray(rawPayload.routes) ? rawPayload.routes : [];
        rawWaypointPhotos = Array.isArray(rawPayload.waypointPhotos) ? rawPayload.waypointPhotos : [];
        rawMaintenanceBoards = Array.isArray(rawPayload.maintenanceBoards) ? rawPayload.maintenanceBoards : null;
        rawMaintenanceExpenses = Array.isArray(rawPayload.maintenanceExpenses) ? rawPayload.maintenanceExpenses : null;
        rawMaintenanceSuppliers = Array.isArray(rawPayload.maintenanceSuppliers) ? rawPayload.maintenanceSuppliers : null;
        rawNavLogEntries = Array.isArray(rawPayload.navLogEntries) ? rawPayload.navLogEntries : [];
        rawEngineLogEntries = Array.isArray(rawPayload.engineLogEntries) ? rawPayload.engineLogEntries : [];
    }

    const cloudRoutes = rawRoutes.map((route, index) => sanitizeSavedRoute(route, index));
    setSavedRoutes(cloudRoutes);

    if (Array.isArray(rawWaypointPhotos)) {
        setWaypointPhotoEntries(rawWaypointPhotos, { persistLocal: true, refreshUi: true });
    }

    if (Array.isArray(rawMaintenanceBoards)) {
        setMaintenanceBoards(rawMaintenanceBoards, { persistLocal: true, refreshUi: true, syncCloud: false });
    }

    if (Array.isArray(rawMaintenanceExpenses)) {
        setMaintenanceExpenses(rawMaintenanceExpenses, { persistLocal: true, refreshUi: true, syncCloud: false });
    }

    if (Array.isArray(rawMaintenanceSuppliers)) {
        setMaintenanceSuppliers(rawMaintenanceSuppliers, { persistLocal: true, refreshUi: true, syncCloud: false });
    }

    if (Array.isArray(rawNavLogEntries)) {
        navLogEntries = rawNavLogEntries;
        saveArrayToStorage(NAV_LOG_STORAGE_KEY, navLogEntries);
        renderNavLogList();
    }

    if (Array.isArray(rawEngineLogEntries)) {
        engineLogEntries = rawEngineLogEntries;
        saveArrayToStorage(ENGINE_LOG_STORAGE_KEY, engineLogEntries);
        renderEngineLogList();
    }

    updateCloudDataSourceStatus('cloud', cloudRoutes.length, waypointPhotoEntries.length);

    return cloudRoutes;
}

async function pushRoutesToCloud() {
    if (!isCloudReady()) return false;
    const payload = {
        version: 5,
        routes: getSavedRoutes(),
        waypointPhotos: waypointPhotoEntries,
        maintenanceBoards,
        maintenanceExpenses,
        maintenanceSuppliers,
        navLogEntries,
        engineLogEntries
    };

    const { error } = await cloudClient
        .from(CLOUD_TABLE_NAME)
        .upsert({
            project_key: cloudConfig.projectKey,
            routes: payload,
            updated_at: new Date().toISOString()
        }, {
            onConflict: 'project_key'
        });

    if (error) throw error;
    return true;
}

function updateCloudFormFromConfig(config) {
    const urlInput = document.getElementById('cloudUrlInput');
    const anonInput = document.getElementById('cloudAnonKeyInput');
    const projectInput = document.getElementById('cloudProjectKeyInput');
    if (urlInput && config?.url) urlInput.value = config.url;
    if (anonInput && config?.anonKey) anonInput.value = config.anonKey;
    if (projectInput && config?.projectKey) projectInput.value = config.projectKey;
}

function readCloudConfigFromForm() {
    const url = String(document.getElementById('cloudUrlInput')?.value || '').trim();
    const anonKey = String(document.getElementById('cloudAnonKeyInput')?.value || '').trim();
    const projectKey = String(document.getElementById('cloudProjectKeyInput')?.value || '').trim();
    return { url, anonKey, projectKey };
}

async function connectCloud(config, { silent = false } = {}) {
    if (!config?.url || !config?.anonKey || !config?.projectKey) {
        stopCloudAutoSync();
        if (cloudLogbookPushTimer) {
            clearTimeout(cloudLogbookPushTimer);
            cloudLogbookPushTimer = null;
        }
        unsubscribeCloudAuthSubscription();
        cloudClient = null;
        cloudConfig = null;
        cloudConnected = false;
        cloudAuthUser = null;
        updateCloudAuthUi();
        if (!silent) setCloudStatus(t('Mode local (paramètres cloud incomplets)', 'Modo local (parámetros nube incompletos)'));
        return false;
    }

    try {
        const isSameConfig =
            !!cloudClient &&
            !!cloudConfig &&
            cloudConfig.url === config.url &&
            cloudConfig.anonKey === config.anonKey &&
            cloudConfig.projectKey === config.projectKey;

        if (!isSameConfig) {
            unsubscribeCloudAuthSubscription();

            cloudClient = createClient(config.url, config.anonKey, {
                auth: {
                    persistSession: true,
                    autoRefreshToken: true,
                    detectSessionInUrl: true,
                    storageKey: `ceibo-supabase-${config.projectKey}`
                }
            });

            cloudAuthSubscription = cloudClient.auth.onAuthStateChange((_event, session) => {
                cloudAuthUser = session?.user || null;
                updateCloudAuthUi();
                void (async () => {
                    await enforceCloudWhitelistForCurrentUser();
                    await applyAuthGateState({ clearWhenLocked: true });
                })();
            });
        }

        cloudConfig = config;
        saveCloudConfigToStorage(config);
        cloudConnected = true;
        await refreshCloudAuthSession();
        startCloudAutoSync();

        if (isCloudReady()) {
            const routes = await pullRoutesFromCloud();
            refreshSavedList();
            setCloudStatus(t(`Cloud connecté · ${routes.length} route(s) partagée(s)`, `Nube conectada · ${routes.length} ruta(s) compartida(s)`));
        } else {
            setCloudStatus(t('Cloud connecté · authentification requise.', 'Nube conectada · autenticación requerida.'));
        }

        return true;
    } catch (error) {
        stopCloudAutoSync();
        if (cloudLogbookPushTimer) {
            clearTimeout(cloudLogbookPushTimer);
            cloudLogbookPushTimer = null;
        }
        unsubscribeCloudAuthSubscription();
        cloudClient = null;
        cloudConfig = null;
        cloudConnected = false;
        cloudAuthUser = null;
        updateCloudAuthUi();
        setCloudStatus(t(`Connexion cloud impossible: ${formatCloudError(error)}`, `Conexión nube imposible: ${formatCloudError(error)}`), true);
        return false;
    }
}

function refreshSavedList() {
    const sel = document.getElementById('savedRoutesSelect');
    if (!sel) return;
    const previousIndex = sel.selectedIndex;
    sel.innerHTML = '';
    const saved = getSavedRoutes();
    saved.forEach((r, i) => {
        const opt = document.createElement('option');
        opt.value = i;
        opt.text = `${r.name} (${r.date} ${r.time})`;
        sel.appendChild(opt);
    });

    if (saved.length === 0) {
        currentLoadedRouteIndex = -1;
        return;
    }

    if (Number.isInteger(currentLoadedRouteIndex) && currentLoadedRouteIndex >= 0 && currentLoadedRouteIndex < saved.length) {
        sel.selectedIndex = currentLoadedRouteIndex;
        return;
    }

    if (Number.isInteger(previousIndex) && previousIndex >= 0 && previousIndex < saved.length) {
        sel.selectedIndex = previousIndex;
    }
}

async function saveRoute() {
    if (routePoints.length === 0) return alert(t('Aucun waypoint à sauvegarder', 'No hay waypoints para guardar'));
    const nameInput = document.getElementById('routeNameInput');
    const rawName = (nameInput?.value || '').trim();
    const saved = [...getSavedRoutes()];
    const canUpdateLoadedRoute = Number.isInteger(currentLoadedRouteIndex)
        && currentLoadedRouteIndex >= 0
        && currentLoadedRouteIndex < saved.length;
    const fallbackName = `Route ${new Date().toLocaleString()}`;
    const existingName = canUpdateLoadedRoute ? String(saved[currentLoadedRouteIndex]?.name || '').trim() : '';
    const name = rawName || existingName || fallbackName;

    const payload = {
        name,
        date: departureDate,
        time: departureTime,
        tackingTimeHours,
        points: routePoints.map(p => ({ lat: p.lat, lon: p.lng ?? p.lon })) ,
        createdAt: canUpdateLoadedRoute
            ? (saved[currentLoadedRouteIndex]?.createdAt || new Date().toISOString())
            : new Date().toISOString(),
        updatedAt: new Date().toISOString()
    };

    if (canUpdateLoadedRoute) {
        saved[currentLoadedRouteIndex] = {
            ...saved[currentLoadedRouteIndex],
            ...payload
        };
    } else {
        saved.push(payload);
        currentLoadedRouteIndex = saved.length - 1;
    }

    setSavedRoutes(saved);
    updateCloudDataSourceStatus('local (non synchronisé)', saved.length, waypointPhotoEntries.length);

    if (isCloudReady()) {
        try {
            await pushRoutesToCloud();
            setCloudStatus(t(`Cloud synchronisé · ${saved.length} route(s)`, `Nube sincronizada · ${saved.length} ruta(s)`));
            updateCloudDataSourceStatus('cloud', saved.length, waypointPhotoEntries.length);
        } catch (error) {
            setCloudStatus(t(`Sauvegarde locale OK, synchro cloud échouée: ${formatCloudError(error)}`, `Guardado local OK, sincronización nube fallida: ${formatCloudError(error)}`), true);
            updateCloudDataSourceStatus('cache local (synchro en échec)', saved.length, waypointPhotoEntries.length);
        }
    }

    refreshSavedList();

    const sel = document.getElementById('savedRoutesSelect');
    if (sel && currentLoadedRouteIndex >= 0 && currentLoadedRouteIndex < saved.length) {
        sel.selectedIndex = currentLoadedRouteIndex;
    }

    alert(canUpdateLoadedRoute
        ? t(`Route mise à jour: ${name}`, `Ruta actualizada: ${name}`)
        : t(`Route sauvegardée: ${name}`, `Ruta guardada: ${name}`));
}

function clearCurrentRoute() {
    // remove markers
    markers.forEach(m => map.removeLayer(m));
    markers = [];
    routePoints = [];
    selectedUserWaypointIndex = -1;
    currentLoadedRouteIndex = -1;
    waypointPassageSlots.clear();
    lastRouteBounds = null;
    lastComputedReportData = null;
    clearWaypointWindDirectionLayers();
    clearWaveDirectionSegmentLayers();
    clearGeneratedWaypointMarkers();
    clearArrivalPoiMarkers();
    if (routeLayer) { map.removeLayer(routeLayer); routeLayer = null; }
    const weatherContainer = document.getElementById('waypointWeatherInfo');
    if (weatherContainer) weatherContainer.innerHTML = '';
    const windLegend = document.getElementById('windSpeedLegend');
    if (windLegend) windLegend.innerHTML = '';
    const suggestionBox = document.getElementById('departureSuggestionInfo');
    lastDepartureSuggestion = null;
    if (suggestionBox) {
        suggestionBox.textContent = t('Suggestion départ: en attente', 'Sugerencia salida: en espera');
        suggestionBox.classList.remove('suggestion-clickable');
    }
    const aiInfo = document.getElementById('aiRouteSuggestionInfo');
    const aiList = document.getElementById('aiRouteSuggestions');
    lastAiRouteCandidates = [];
    if (aiInfo) aiInfo.textContent = t('Routes IA: en attente', 'Rutas IA: en espera');
    if (aiList) aiList.innerHTML = '';
    const arrivalSummary = document.getElementById('arrivalSummary');
    if (arrivalSummary) arrivalSummary.textContent = t('Analyse mouillage: en attente', 'Análisis fondeo: en espera');
    const anchorageContainer = document.getElementById('anchorageRecommendations');
    if (anchorageContainer) anchorageContainer.innerHTML = '';
    const restaurants = document.getElementById('nearbyRestaurants');
    if (restaurants) restaurants.innerHTML = '';
    const shops = document.getElementById('nearbyShops');
    if (shops) shops.innerHTML = '';
    updateSelectedWaypointInfo();
}

function reverseCurrentRoute() {
    if (!Array.isArray(routePoints) || routePoints.length < 2) {
        alert(t(
            'Il faut au moins 2 waypoints pour créer une route retour',
            'Se necesitan al menos 2 waypoints para crear una ruta de retorno'
        ));
        return;
    }

    const reversedPoints = routePoints
        .slice()
        .reverse()
        .map(point => ({ lat: Number(point.lat), lng: Number(point.lng ?? point.lon) }))
        .filter(point => Number.isFinite(point.lat) && Number.isFinite(point.lng));

    if (reversedPoints.length < 2) {
        alert(t(
            'Impossible d\'inverser cette route (waypoints invalides)',
            'No se puede invertir esta ruta (waypoints no válidos)'
        ));
        return;
    }

    markers.forEach(marker => {
        if (marker && map?.hasLayer(marker)) {
            map.removeLayer(marker);
        }
    });
    markers = [];
    routePoints = [];
    selectedUserWaypointIndex = -1;

    reversedPoints.forEach(point => {
        addUserWaypoint(L.latLng(point.lat, point.lng), { select: false, invalidate: false });
    });

    const routeNameInput = document.getElementById('routeNameInput');
    if (routeNameInput) {
        const baseName = String(routeNameInput.value || '').trim();
        if (!baseName) {
            routeNameInput.value = t('Route RETOUR', 'Ruta RETOUR');
        } else if (!/\bretour\b/i.test(baseName)) {
            routeNameInput.value = `${baseName} RETOUR`;
        }
    }

    invalidateComputedRouteDisplay();
    drawRoute(routePoints);
    updateSelectedWaypointInfo();
}

function loadRoute(index) {
    const saved = getSavedRoutes();
    if (!saved || !saved[index]) return alert(t('Aucune route sélectionnée', 'Ninguna ruta seleccionada'));
    const r = saved[index];

    clearCurrentRoute();
    waypointPassageSlots.clear();
    lastRouteBounds = null;

    r.points.forEach(pt => {
        const lat = pt.lat;
        const lon = pt.lon;
        const marker = createWaypointMarker([lat, lon]);
        markers.push(marker);
        routePoints.push(marker.getLatLng());
    });

    selectedUserWaypointIndex = -1;
    currentLoadedRouteIndex = index;
    updateSelectedWaypointInfo();

    const nameInput = document.getElementById('routeNameInput');
    if (nameInput) nameInput.value = r.name || '';

    // restore UI values (keep current selected date)
    departureTime = normalizeHourTime(r.time);
    updateDepartureDateTimeInput();
    document.getElementById('tackingTimeInput').value = r.tackingTimeHours;
    tackingTimeHours = r.tackingTimeHours;

    // draw polyline between waypoints
    drawRoute(routePoints);

    // force recenter after loading from Routes tab
    if (lastRouteBounds && lastRouteBounds.isValid()) {
        recenterOnRoute();
    } else if (routePoints.length > 0) {
        map.setView(routePoints[0], Math.max(map.getZoom(), 10));
    }
}

function deleteRoute(index) {
    const saved = [...getSavedRoutes()];
    if (!saved || !saved[index]) return;
    saved.splice(index, 1);

    if (currentLoadedRouteIndex === index) {
        currentLoadedRouteIndex = -1;
    } else if (currentLoadedRouteIndex > index) {
        currentLoadedRouteIndex -= 1;
    }

    setSavedRoutes(saved);

    const finalize = () => refreshSavedList();
    if (isCloudReady()) {
        pushRoutesToCloud()
            .then(() => setCloudStatus(t(`Cloud synchronisé · ${saved.length} route(s)`, `Nube sincronizada · ${saved.length} ruta(s)`)))
            .catch(error => setCloudStatus(t(`Suppression locale OK, synchro cloud échouée: ${formatCloudError(error)}`, `Eliminación local OK, sincronización nube fallida: ${formatCloudError(error)}`), true))
            .finally(finalize);
        return;
    }

    finalize();
}

function exportRoute(index) {
    const saved = getSavedRoutes();
    if (!saved || !saved[index]) return alert(t('Aucune route sélectionnée', 'Ninguna ruta seleccionada'));
    const data = JSON.stringify(saved[index], null, 2);
    const blob = new Blob([data], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${saved[index].name.replace(/[^a-z0-9\-]/gi,'_')}.json`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
}

function escapeXml(value) {
    return String(value)
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&apos;');
}

function routeToGpx(route) {
    const routeName = escapeXml(route.name || 'Route');
    const points = (route.points || [])
        .map(pt => {
            const lat = Number(pt.lat);
            const lon = Number(pt.lon ?? pt.lng);
            if (!Number.isFinite(lat) || !Number.isFinite(lon)) return '';
            return `    <rtept lat="${lat}" lon="${lon}"></rtept>`;
        })
        .filter(Boolean)
        .join('\n');

    return `<?xml version="1.0" encoding="UTF-8"?>\n<gpx version="1.1" creator="CEIBO Router" xmlns="http://www.topografix.com/GPX/1/1">\n  <metadata>\n    <name>${routeName}</name>\n  </metadata>\n  <rte>\n    <name>${routeName}</name>\n${points}\n  </rte>\n</gpx>`;
}

function exportRouteGpx(index) {
    const saved = getSavedRoutes();
    if (!saved || !saved[index]) return alert(t('Aucune route sélectionnée', 'Ninguna ruta seleccionada'));

    const gpx = routeToGpx(saved[index]);
    const blob = new Blob([gpx], { type: 'application/gpx+xml' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${saved[index].name.replace(/[^a-z0-9\-]/gi,'_')}.gpx`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
}

function parseGpxToRoute(gpxText, fileName = 'Route importée') {
    const parser = new DOMParser();
    const xml = parser.parseFromString(gpxText, 'application/xml');
    const parserError = xml.querySelector('parsererror');
    if (parserError) throw new Error('GPX invalide');

    const routeNameNode =
        xml.querySelector('metadata > name') ||
        xml.querySelector('rte > name') ||
        xml.querySelector('trk > name');

    const routeName = routeNameNode?.textContent?.trim() || fileName.replace(/\.gpx$/i, '');

    const rtePts = Array.from(xml.getElementsByTagName('rtept'));
    const trkPts = Array.from(xml.getElementsByTagName('trkpt'));
    const wpts = Array.from(xml.getElementsByTagName('wpt'));
    const pointNodes = rtePts.length ? rtePts : (trkPts.length ? trkPts : wpts);

    const points = pointNodes
        .map(node => ({
            lat: Number(node.getAttribute('lat')),
            lon: Number(node.getAttribute('lon'))
        }))
        .filter(pt => Number.isFinite(pt.lat) && Number.isFinite(pt.lon));

    if (points.length === 0) throw new Error('Aucun point GPX trouvé');

    return {
        name: routeName,
        date: departureDate,
        time: departureTime,
        tackingTimeHours,
        points,
        createdAt: new Date().toISOString()
    };
}

function handleImport(e) {
    const file = e.target.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = async function(evt) {
        try {
            const content = String(evt.target.result || '');
            const saved = [...getSavedRoutes()];

            const isLikelyGpx = file.name.toLowerCase().endsWith('.gpx') || content.trim().startsWith('<');

            if (isLikelyGpx) {
                const route = parseGpxToRoute(content, file.name);
                saved.push(route);
            } else {
                const obj = JSON.parse(content);
                if (Array.isArray(obj)) {
                    obj.forEach(o => saved.push(o));
                } else {
                    saved.push(obj);
                }
            }

            setSavedRoutes(saved);
            if (isCloudReady()) {
                try {
                    await pushRoutesToCloud();
                    setCloudStatus(t(`Cloud synchronisé · ${saved.length} route(s)`, `Nube sincronizada · ${saved.length} ruta(s)`));
                } catch (error) {
                    setCloudStatus(t(`Import local OK, synchro cloud échouée: ${formatCloudError(error)}`, `Importación local OK, sincronización nube fallida: ${formatCloudError(error)}`), true);
                }
            }
            refreshSavedList();
            alert(t('Import OK', 'Importación OK'));
        } catch (err) { alert(t('Fichier JSON/GPX invalide', 'Archivo JSON/GPX inválido')); }
    };
    reader.readAsText(file);
}