import { routeSegment, distanceNm, getBearing, computeTWA, movePoint } from './polarRouter.js';
import { feature as topojsonFeature } from 'https://cdn.jsdelivr.net/npm/topojson-client@3/+esm';
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

const APP_BUILD_VERSION = '20260306-22';

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
const CLOUD_ROUTES_TABLE = 'routes';
const CLOUD_ROUTE_POINTS_TABLE = 'route_points';
const CLOUD_WAYPOINT_PHOTOS_TABLE = 'waypoint_photos';
const CLOUD_MAINTENANCE_SCHEMAS_TABLE = 'maintenance_schemas';
const CLOUD_MAINTENANCE_PINS_TABLE = 'maintenance_pins';
const CLOUD_MAINTENANCE_SUPPLIERS_TABLE = 'maintenance_suppliers';
const CLOUD_MAINTENANCE_EXPENSES_TABLE = 'maintenance_expenses';
const CLOUD_NAV_LOG_TABLE = 'nav_log_entries';
const CLOUD_ENGINE_LOG_TABLE = 'engine_log';
const CLOUD_ALLOWED_USERS_TABLE = 'allowed_users';
const CLOUD_PROJECTS_TABLE = 'projects';
const CLOUD_OWNER_ADMIN_EMAILS = new Set(['max.patissier@gmail.com']);
const CLOUD_AUTO_PULL_INTERVAL_MS = 45000;
const CLOUD_LOGBOOK_PUSH_DEBOUNCE_MS = 1800;
const NAV_GPS_SAMPLE_INTERVAL_MS = 60 * 1000;
const GOOGLE_PHOTOS_CLIENT_ID_STORAGE_KEY = 'ceiboGooglePhotosClientIdV1';
let savedRoutesCache = [];
let cloudClient = null;
let cloudConfig = null;
let cloudConnected = false;
let cloudAuthUser = null;
let cloudUserProfile = null;
let cloudManagedUsers = [];
let cloudAllowedUsersHasNameColumn = true;
let routesCloudDirty = false;
let cloudResolvedProjectIdUuid = '';
let cloudAuthSubscription = null;
let cloudAutoPullTimer = null;
let cloudAutoPullInFlight = false;
let cloudLogbookPushTimer = null;
let cloudLogbookPushInFlight = false;
let cloudWhitelistCheckInFlight = false;
let cloudLastSeenUpdatedAtMs = 0;
let cloudDataSourceLabel = 'initialisation';
let cloudLastStatusMessage = '';
let navLogEntries = [];
let navWatchId = null;
let navLatestHeelDeg = null;
let navLatestSpeedKn = null;
let navMotionListenerBound = false;
let navGpsSampleTimerId = null;
let navGpsLatestFix = null;
let navGpsSessionStartMs = 0;
let navGpsSessionStartEntryIndex = 0;
let navGpsSessionHasSample = false;
let navGpsTraceLayerGroup = null;
let editingNavLogEntryId = null;
let engineLogEntries = [];
let editingEngineLogEntryId = null;
let logWorkspaceMode = 'none';
let lastAiRouteCandidates = [];
let aiTrafficEntries = [];
let aiTrafficAutoHideTimer = null;
let weatherFocusMarker = null;
let weatherFocusPoint = null;
let weatherPointerPlacementMode = false;
let mapWebOverlayElement = null;
let mapWebOverlayFrame = null;
let maintenanceBoards = [];
let selectedMaintenanceBoardId = null;
let activeMaintenanceAnnotationId = null;
let maintenanceSchemaManagerVisible = false;
let maintenanceExpenses = [];
let maintenanceSuppliers = [];
let maintenanceSupplierFormDraft = null;
let activeMaintenanceSubtab = 'tasks';
let maintenanceTesseractLoader = null;
let maintenancePdfJsLoader = null;
let maintenanceInvoicePreviewUrl = '';
let maintenanceInvoicePreviewType = '';
let maintenanceLastScannedText = '';
let selectedMaintenanceExpenseId = null;
let selectedMaintenanceSupplierId = null;
let activeMaintenanceExpensesView = 'list';
let activeRoutesSubtab = 'manage';
let routesSortOrder = 'asc';
let routesSearchTerm = '';
let waypointSearchTerm = '';
let activeCloudSubtab = 'account';
let cloudTableStatsRemoteCounts = null;
let cloudTableStatsRefreshPromise = null;
let cloudTableStatsLastRefreshAtMs = 0;
let protectedDataLoaded = false;
let googlePhotosAccessToken = '';
let googlePhotosTokenExpiryMs = 0;
let googlePhotosPickerItems = [];
let googlePhotosPickerNextPageToken = '';
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
const CLOUD_STATS_REFRESH_TTL_MS = 20000;
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
    return true;
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
    setElementText('#weatherTabBtn', t('Météo', 'Meteo'));
    setElementText('#arrivalTabBtn', t('Arrivée', 'Llegada'));
    setElementText('#waypointTabBtn', t('Waypoint', 'Waypoint'));
    setElementText('#maintenanceTabBtn', t('Maintenance', 'Mantenimiento'));

    setElementText('label[for="routeNameInput"]', t('Nom de la route:', 'Nombre de la ruta:'));
    setElementPlaceholder('#routeNameInput', t('Nom de la route', 'Nombre de la ruta'));
    setElementText('#saveRouteBtn', t('Sauvegarder', 'Guardar'));
    setElementText('#exportRouteBtn', t('Exporter JSON', 'Exportar JSON'));
    setElementText('#exportRouteGpxBtn', t('Exporter GPX', 'Exportar GPX'));
    setElementText('#resetBtn', t('Reset', 'Reiniciar'));
    setElementText('#reverseRouteBtn', t('Route retour', 'Ruta retorno'));
    setElementText('#exportVoyagePdfBtn', t('Exporter rapport PDF', 'Exportar informe PDF'));
    setElementText('#savedRoutesListLabel', t('Routes sauvegardées:', 'Rutas guardadas:'));
    const routeSearchInput = document.getElementById('routeSearchInput');
    if (routeSearchInput) routeSearchInput.placeholder = t('Rechercher une route...', 'Buscar una ruta...');
    setElementText('#routesManageSubtabBtn', t('Gestion', 'Gestión'));
    setElementText('#routesImportExportSubtabBtn', t('Import/Export', 'Importar/Exportar'));
    setElementText('#routesToolsSubtabBtn', t('Outils', 'Herramientas'));
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
    setElementText('#cloudMainTitle', t('V5 · Base de données partagée', 'V5 · Base de datos compartida'));
    setElementText('#cloudAccountSubtabBtn', t('Compte', 'Cuenta'));
    setElementText('#cloudPasswordSubtabBtn', t('Mot de passe', 'Contraseña'));
    setElementText('#cloudStatsSubtabBtn', t('Stats', 'Stats'));
    setElementText('#cloudUsersSubtabBtn', t('Utilisateurs', 'Usuarios'));
    setElementText('#cloudAccountTitle', t('Compte utilisateur (Email + mot de passe)', 'Cuenta de usuario (Email + contraseña)'));
    setElementText('#cloudPasswordTitle', t('Changer mon mot de passe', 'Cambiar mi contraseña'));
    setElementText('#cloudChangePasswordBtn', t('Mettre à jour le mot de passe', 'Actualizar contraseña'));
    setElementText('#cloudPasswordStatus', t('Mot de passe: en attente', 'Contraseña: en espera'));
    setElementText('#cloudUserCreateTitle', t('Créer un utilisateur autorisé', 'Crear usuario autorizado'));
    setElementText('#cloudAdminCreateUserBtn', t('Créer / mettre à jour user', 'Crear / actualizar usuario'));
    setElementText('#cloudUserManagementStatus', t('Gestion utilisateurs: connecte-toi avec un compte administrateur.', 'Gestión usuarios: inicia sesión con una cuenta administradora.'));
    setElementText('#cloudUserAdminHint', t('Note: chaque utilisateur créé ici doit ensuite utiliser « Créer compte » pour définir son mot de passe.', 'Nota: cada usuario creado aquí debe luego usar « Crear cuenta » para definir su contraseña.'));
    setElementPlaceholder('#cloudEmailInput', t('Email utilisateur', 'Email usuario'));
    setElementPlaceholder('#cloudUserPasswordInput', t('Mot de passe utilisateur', 'Contraseña usuario'));
    setElementPlaceholder('#cloudNewPasswordInput', t('Nouveau mot de passe', 'Nueva contraseña'));
    setElementPlaceholder('#cloudConfirmPasswordInput', t('Confirmer le nouveau mot de passe', 'Confirmar nueva contraseña'));
    setElementPlaceholder('#cloudAdminUserEmailInput', t('Email (clé unique)', 'Email (clave única)'));
    setElementPlaceholder('#cloudAdminUserNameInput', t('Nom', 'Nombre'));
    setElementText('#cloudAdminUserRoleInput option[value="utilisateur"]', t('utilisateur', 'usuario'));
    setElementText('#cloudAdminUserRoleInput option[value="administrateur"]', t('administrateur', 'administrador'));
    setElementText('#cloudEmailSignInBtn', t('Se connecter email', 'Conectar email'));
    setElementText('#cloudEmailSignUpBtn', t('Créer compte', 'Crear cuenta'));
    setElementText('#cloudSignOutBtn', t('Se déconnecter', 'Desconectar'));
    setElementText('#cloudAuthStatus', t('Utilisateur: non connecté', 'Usuario: no conectado'));
    setElementText('#cloudStatus', t('Mode local (pas de cloud configuré)', 'Modo local (nube no configurada)'));
    setElementText('#cloudDataSourceStatus', t('Données routes/photos: en attente', 'Datos rutas/fotos: en espera'));
    setElementText('#cloudAutoSyncInfo', t('Routes + photos waypoint + maintenance: synchronisation cloud automatique.', 'Rutas + fotos waypoint + mantenimiento: sincronización nube automática.'));
    setElementText('#cloudStatsTitle', t('Enregistrements par table utilisée', 'Registros por tabla usada'));
    setElementText('#cloudStatsSourceLabel', t('Source', 'Origen'));
    setElementText('#cloudStatsProjectsLabel', t('Projet (projects)', 'Proyecto (projects)'));
    setElementText('#cloudStatsAllowedUsersLabel', t('Utilisateurs autorisés (allowed_users)', 'Usuarios autorizados (allowed_users)'));
    setElementText('#cloudStatsRoutesLabel', t('Routes sauvegardées (routes)', 'Rutas guardadas (routes)'));
    setElementText('#cloudStatsRoutePointsLabel', t('Points de route (route_points)', 'Puntos de ruta (route_points)'));
    setElementText('#cloudStatsPhotosLabel', t('Photos waypoint (waypoint_photos)', 'Fotos waypoint (waypoint_photos)'));
    setElementText('#cloudStatsMaintenanceSchemasLabel', t('Schémas maintenance (maintenance_schemas)', 'Esquemas mantenimiento (maintenance_schemas)'));
    setElementText('#cloudStatsMaintenancePinsLabel', t('Pastilles maintenance (maintenance_pins)', 'Marcadores mantenimiento (maintenance_pins)'));
    setElementText('#cloudStatsSuppliersLabel', t('Fournisseurs (maintenance_suppliers)', 'Proveedores (maintenance_suppliers)'));
    setElementText('#cloudStatsExpensesLabel', t('Dépenses/factures (maintenance_expenses)', 'Gastos/facturas (maintenance_expenses)'));
    setElementText('#cloudStatsNavLabel', t('Journal navigation (nav_log_entries)', 'Diario navegación (nav_log_entries)'));
    setElementText('#cloudStatsEngineLabel', t('Journal moteur (engine_log)', 'Diario motor (engine_log)'));
    setElementText('#cloudStatsTotalLabel', t('Total enregistrements', 'Total registros'));
    setElementText('#cloudStatsStorageLabel', t('Taille utilisée', 'Tamaño usado'));
    setElementText('#cloudStatsQuotaLabel', t('Quota utilisé (500 Mo)', 'Cuota usada (500 MB)'));

    setElementText('#startNavLogBtn', t('Démarrer log GPS', 'Iniciar log GPS'));
    setElementText('#stopNavLogBtn', t('Arrêter log GPS', 'Detener log GPS'));
    setElementText('#requestMotionPermissionBtn', t('Activer capteur inclinaison', 'Activar sensor inclinación'));
    setElementText('#clearNavLogBtn', t('Effacer journal nav', 'Borrar diario nav'));
    setElementText('#navLogOpenCreateBtn', t('Ajouter', 'Añadir'));
    setElementText('#addManualNavLogBtn', t('Enregistrer entrée', 'Guardar entrada'));
    setElementText('#cancelNavLogEditBtn', t('Annuler', 'Cancelar'));
    setElementText('#logWorkspaceTitle', t('Saisie journal', 'Edición diario'));
    setElementText('#logWorkspacePlaceholder', t('Clique sur Ajouter pour ouvrir le formulaire', 'Haz clic en Añadir para abrir el formulario'));
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
    setElementText('#navLogListLabel', t('Entrées navigation:', 'Entradas navegación:'));

    setElementText('#saveEngineLogBtn', t('Ajouter entrée moteur', 'Añadir entrada motor'));
    setElementText('#engineLogOpenCreateBtn', t('Ajouter', 'Añadir'));
    setElementText('#cancelEngineLogEditBtn', t('Annuler modification', 'Cancelar edición'));
    setElementText('label[for="engineHoursInput"]', t('Compteur moteur (h):', 'Contador motor (h):'));
    setElementText('label[for="fuelAddedInput"]', t('Carburant ajouté (L):', 'Combustible añadido (L):'));
    setElementText('label[for="engineLogNoteInput"]', t('Note:', 'Nota:'));
    setElementText('#engineLogListLabel', t('Historique moteur:', 'Historial motor:'));

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
    setElementText('#waypointImportGooglePhotosBtn', t('Google Photos', 'Google Photos'));
    setElementText('#googlePhotosModalTitle', t('Importer depuis Google Photos', 'Importar desde Google Photos'));
    setElementText('#googlePhotosCloseBtn', t('Fermer', 'Cerrar'));
    setElementText('#googlePhotosLoadMoreBtn', t('Charger plus', 'Cargar mas'));
    setElementText('#waypointPhotoStatus', t("Coordonnées: en attente d'une photo", 'Coordenadas: esperando una foto'));
    setElementText('#waypointGoogleMapLink', t('Ouvrir dans Google Maps', 'Abrir en Google Maps'));
    setElementText('label[for="waypointPlaceNameInput"]', t('Nom du lieu:', 'Nombre del lugar:'));
    setElementText('label[for="waypointCommentInput"]', t('Commentaire:', 'Comentario:'));
    setElementText('label[for="waypointRatingInput"]', t('Note globale:', 'Nota global:'));
    setElementText('label[for="waypointCleanlinessInput"]', t('Propreté:', 'Limpieza:'));
    setElementText('#waypointTab .waypoint-protection-item > label', t('Protection (rose des vents):', 'Protección (rosa de vientos):'));
    setElementText('label[for="waypointDepthInput"]', t('Profondeur:', 'Profundidad:'));
    setElementText('label[for="waypointBottomTypeInput"]', t('Type de fond:', 'Tipo de fondo:'));
    setElementText('#waypointSavedAnchoragesDockLabel', t('Mouillages enregistrés:', 'Fondeos guardados:'));
    setElementPlaceholder('#waypointSearchInput', t('Recherche tags/commentaires...', 'Buscar tags/comentarios...'));

    setElementText('label[for="maintenanceSchemaNameInput"]', t('Nom du schéma:', 'Nombre del esquema:'));
    setElementPlaceholder('#maintenanceSchemaNameInput', t('Ex: Compartiment moteur', 'Ej: Compartimento motor'));
    setElementText('#maintenanceTasksSubtabBtn', t('Tâches', 'Tareas'));
    setElementText('#maintenanceExpensesSubtabBtn', t('Dépenses & factures', 'Gastos y facturas'));
    setElementText('#maintenanceSuppliersSubtabBtn', t('Fournisseurs', 'Proveedores'));
    setElementText('#maintenanceEngineSubtabBtn', t('Moteur', 'Motor'));
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
    setElementText('label[for="maintenanceInvoiceInput"]', t('Uploader facture/documents (images/PDF):', 'Subir factura/documentos (imágenes/PDF):'));
    setElementText('#maintenanceExpenseListTabBtn', t('Liste factures', 'Lista facturas'));
    setElementText('#maintenanceExpenseAddTabBtn', t('Ajouter facture', 'Añadir factura'));
    setElementText('#maintenanceScanInvoiceBtn', t('Scanner facture', 'Escanear factura'));
    setElementText('#maintenanceInvoiceScanStatus', t('Scan facture: en attente', 'Escaneo factura: en espera'));
    setElementText('#maintenanceSupplierSuggestionsLabel', t('Suggestions fournisseur:', 'Sugerencias proveedor:'));
    setElementText('#maintenanceInvoiceReviewTitle', t('Copie rapide depuis l\'aperçu PDF', 'Copia rápida desde la vista previa PDF'));
    setElementText('#maintenanceInvoiceReviewHint', t('1) Sélectionne du texte dans l’aperçu à droite 2) copie (⌘C) 3) colle dans le champ choisi.', '1) Selecciona texto en la vista previa derecha 2) copia (⌘C) 3) pega en el campo elegido.'));
    setElementText('label[for="maintenanceManualPasteTargetSelect"]', t('Champ de destination:', 'Campo de destino:'));
    setElementText('#maintenancePasteSelectedTextBtn', t('Coller texte copié dans ce champ', 'Pegar texto copiado en este campo'));
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
            { value: 'expenseAiComment', label: t('Commentaire facture', 'Comentario factura') },
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
    setElementText('#maintenanceExpensePaymentStatusSelect option[value="new"]', t('Nouvelle / à payer', 'Nueva / pendiente'));
    setElementText('#maintenanceExpensePaymentStatusSelect option[value="pending"]', t('À payer', 'Pendiente'));
    setElementText('#maintenanceExpensePaymentStatusSelect option[value="planned"]', t('À prévoir', 'A prever'));
    setElementText('#maintenanceExpensePaymentStatusSelect option[value="paid"]', t('Payé', 'Pagado'));
    setElementText('label[for="maintenanceExpenseSupplierInput"]', t('Fournisseur:', 'Proveedor:'));
    setElementText('label[for="maintenanceExpenseSupplierIbanInput"]', t('IBAN fournisseur:', 'IBAN proveedor:'));
    setElementText('label[for="maintenanceExpenseLinesInput"]', t('Lignes de travaux/produits (une ligne = libellé ; quantité ; prix ; total):', 'Líneas de trabajos/productos (una línea = concepto ; cantidad ; precio ; total):'));
    setElementText('label[for="maintenanceExpenseNoteInput"]', t('Note:', 'Nota:'));
    setElementText('label[for="maintenanceExpenseAiCommentInput"]', t('Commentaire facture:', 'Comentario factura:'));
    setElementPlaceholder('#maintenanceExpenseAiCommentInput', t('Observations sur la facture', 'Observaciones sobre la factura'));
    setElementText('#maintenanceAddExpenseBtn', t('Ajouter dépense', 'Añadir gasto'));
    setElementText('#maintenanceExpensesListLabel', t('Dépenses:', 'Gastos:'));
    setElementText('label[for="maintenanceSupplierNameInput"]', t('Nom fournisseur:', 'Nombre proveedor:'));
    setElementText('label[for="maintenanceSupplierContactInput"]', t('Contact:', 'Contacto:'));
    setElementText('label[for="maintenanceSupplierPhoneInput"]', t('Téléphone urgence:', 'Teléfono urgencia:'));
    setElementText('label[for="maintenanceSupplierIbanInput"]', t('IBAN:', 'IBAN:'));
    setElementText('label[for="maintenanceSupplierNoteInput"]', t('Note:', 'Nota:'));
    setElementText('#maintenanceSupplierNewBtn', t('Nouveau fournisseur', 'Nuevo proveedor'));
    setElementText('#maintenanceAddSupplierBtn', t('Ajouter fournisseur', 'Añadir proveedor'));
    setElementText('#maintenanceUpdateSupplierBtn', t('Mettre à jour fournisseur', 'Actualizar proveedor'));
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
    renderCloudStatsTable();
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
    setMaintenanceSuppliers([], { refreshUi: true });
    navLogEntries = [];
    renderNavLogList();
    engineLogEntries = [];
    renderEngineLogList();
    clearCurrentRoute();
    updateCloudDataSourceStatus('verrouillé (auth requise)', 0, waypointPhotoEntries.length);
}

async function applyAuthGateState({ clearWhenLocked = true } = {}) {
    const locked = isAuthGateLocked();
    const isInitialProtectedLoad = !protectedDataLoaded;
    setProtectedTabsEnabled(!locked);
    document.body.classList.toggle('auth-locked', locked);

    if (locked) {
        if (activeTabName !== 'cloud') {
            const cloudBtn = document.getElementById('cloudTabBtn');
            if (cloudBtn) cloudBtn.click();
        }

        if (clearWhenLocked) {
            clearProtectedUiData();
            protectedDataLoaded = false;
        }

        setActiveCloudSubtab('account');

        setCloudStatus(
            t(
                'Accès verrouillé: seul l\'onglet Cloud est accessible (connexion / création de compte).',
                'Acceso bloqueado: solo la pestaña Cloud es accesible (conexión / creación de cuenta).'
            ),
            true
        );
        return;
    }

    if (!protectedDataLoaded) {
        if (isAuthRequiredRuntime()) {
            // Hydrate local caches first to avoid blanking maintenance data
            // if the cloud payload does not include maintenance fields.
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
            updateCloudDataSourceStatus('cache local (auth ok)', getSavedRoutes().length, waypointPhotoEntries.length);
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
            const shouldHydrateMaintenanceFromCloud = isAuthRequiredRuntime() && isInitialProtectedLoad;
            const routes = await pullRoutesFromCloud({ allowMaintenanceOverwrite: shouldHydrateMaintenanceFromCloud });
            refreshSavedList();
            setCloudStatus(t(`Cloud connecté · ${routes.length} route(s) partagée(s)`, `Nube conectada · ${routes.length} ruta(s) compartida(s)`));
            updateCloudDataSourceStatus('cloud', routes.length, waypointPhotoEntries.length);
        } catch (error) {
            console.error('[CEIBO] Cloud pull failed', error);
            setCloudStatus(`Récupération cloud impossible: ${formatCloudError(error)}`, true);
            updateCloudDataSourceStatus('indisponible', 0, waypointPhotoEntries.length);
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

function parseDateInputFlexible(dateInput) {
    if (dateInput instanceof Date) {
        return Number.isNaN(dateInput.getTime()) ? null : new Date(dateInput.getTime());
    }

    if (dateInput === null || dateInput === undefined) return null;

    const raw = String(dateInput).trim();
    if (!raw) return null;

    const localNaiveMatch = raw.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2}))?$/);
    if (localNaiveMatch) {
        const [, year, month, day, hour, minute, second = '00'] = localNaiveMatch;
        const dateObj = new Date(
            Number(year),
            Number(month) - 1,
            Number(day),
            Number(hour),
            Number(minute),
            Number(second),
            0
        );
        return Number.isNaN(dateObj.getTime()) ? null : dateObj;
    }

    const parsed = new Date(raw);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
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
            return { ok: false, message: t('Timeout OpenWeatherMap (9s).', 'Timeout OpenWeatherMap (9s).') };
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
    const latitude = Number(lat);
    const longitude = Number(lng);
    if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
        return 'https://www.google.com/maps';
    }

    const query = `${latitude.toFixed(6)},${longitude.toFixed(6)}`;
    return `https://maps.google.com/maps?q=${encodeURIComponent(query)}&hl=fr&z=14&output=embed`;
}

function ensureMapWebOverlay() {
    if (mapWebOverlayElement && mapWebOverlayFrame) {
        return { overlay: mapWebOverlayElement, frame: mapWebOverlayFrame };
    }

    const mapContainer = document.getElementById('map');
    if (!mapContainer) return { overlay: null, frame: null };

    const overlay = document.createElement('div');
    overlay.className = 'map-web-overlay';
    overlay.style.display = 'none';

    const header = document.createElement('div');
    header.className = 'map-web-overlay__header';

    const title = document.createElement('strong');
    title.textContent = t('Aperçu Google', 'Vista Google');
    header.appendChild(title);

    const closeBtn = document.createElement('button');
    closeBtn.type = 'button';
    closeBtn.className = 'map-web-overlay__close';
    closeBtn.textContent = t('Fermer', 'Cerrar');
    closeBtn.addEventListener('click', () => {
        overlay.style.display = 'none';
        if (mapWebOverlayFrame) {
            mapWebOverlayFrame.src = 'about:blank';
        }
    });
    header.appendChild(closeBtn);

    const frame = document.createElement('iframe');
    frame.className = 'map-web-overlay__frame';
    frame.setAttribute('referrerpolicy', 'no-referrer-when-downgrade');
    frame.setAttribute('loading', 'eager');

    overlay.appendChild(header);
    overlay.appendChild(frame);
    mapContainer.appendChild(overlay);

    mapWebOverlayElement = overlay;
    mapWebOverlayFrame = frame;
    return { overlay, frame };
}

function openUrlInMapOverlay(url) {
    const safeUrl = String(url || '').trim();
    if (!safeUrl) return;

    const { overlay, frame } = ensureMapWebOverlay();
    if (!overlay || !frame) return;

    overlay.style.display = 'flex';
    frame.src = safeUrl;
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
    googleLink.onclick = (event) => {
        event.preventDefault();
        openUrlInMapOverlay(googleLink.href);
    };

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

    const creatorEmail = normalizeEmailForCompare(
        entry?.creatorEmail || ''
    );
    const creatorName = String(
        entry?.creatorName || ''
    ).trim();

    const legacyDepthRating = clampStarRating(entry?.depth);
    const derivedDepth = normalizeDepthMeters(Number(entry?.depthMeters ?? 0) || legacyDepthRating * 5);

    return {
        id: String(entry?.id || generateClientUuid()),
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
        creatorEmail,
        creatorName,
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
    return setWaypointPhotoStorageList(waypointPhotoEntries);
}

async function syncWaypointPhotosToCloud() {
    if (!isCloudReady()) return false;

    const synced = await pushWaypointPhotosToCloudV2(waypointPhotoEntries);
    if (!synced) {
        throw new Error('Synchronisation waypoint_photos refusée (cloud indisponible ou utilisateur non connecté).');
    }

    routesCloudDirty = false;
    cloudLastSeenUpdatedAtMs = Math.max(cloudLastSeenUpdatedAtMs, Date.now());
    setCloudStatus(`Cloud synchronisé · ${getSavedRoutes().length} route(s) + ${waypointPhotoEntries.length} photo(s)`);
    return true;
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

function getStoredGooglePhotosClientId() {
    return String(localStorage.getItem(GOOGLE_PHOTOS_CLIENT_ID_STORAGE_KEY) || '').trim();
}

function setStoredGooglePhotosClientId(clientId) {
    const safeValue = String(clientId || '').trim();
    if (!safeValue) {
        localStorage.removeItem(GOOGLE_PHOTOS_CLIENT_ID_STORAGE_KEY);
        return;
    }
    localStorage.setItem(GOOGLE_PHOTOS_CLIENT_ID_STORAGE_KEY, safeValue);
}

function ensureGooglePhotosClientId() {
    let clientId = getStoredGooglePhotosClientId();
    if (clientId) return clientId;

    const entered = window.prompt(t(
        'Entre ton Google OAuth Client ID (Web) pour Google Photos:',
        'Introduce tu Google OAuth Client ID (Web) para Google Photos:'
    ));
    clientId = String(entered || '').trim();
    if (!clientId) return '';
    setStoredGooglePhotosClientId(clientId);
    return clientId;
}

function buildGooglePhotosOAuthHelpMessage(error) {
    const raw = String(error?.message || error || '');
    const lower = raw.toLowerCase();
    const origin = String(window.location.origin || '');

    if (
        lower.includes('generaloauthflow') ||
        lower.includes('invalid_client') ||
        lower.includes('origin') ||
        lower.includes('redirect_uri') ||
        lower.includes('unauthorized') ||
        lower.includes('access_denied') ||
        lower.includes('idpiframe')
    ) {
        return t(
            `Vérifie Google Cloud: 1) OAuth Client ID de type Web, 2) origine autorisée = ${origin}, 3) API Google Photos Library activée, 4) écran de consentement configuré + ton email dans les testeurs si app en mode test.`,
            `Verifica Google Cloud: 1) OAuth Client ID tipo Web, 2) origen autorizado = ${origin}, 3) API Google Photos Library activada, 4) pantalla de consentimiento configurada + tu email en testers si la app está en modo prueba.`
        );
    }

    if (lower.includes('popup_closed')) {
        return t('Fenêtre Google fermée avant validation.', 'Ventana de Google cerrada antes de validar.');
    }

    if (lower.includes('popup_failed_to_open')) {
        return t('Popup bloquée: autorise les popups pour ce site.', 'Popup bloqueada: permite popups para este sitio.');
    }

    return t('Vérifie la configuration OAuth Google Photos.', 'Verifica la configuración OAuth de Google Photos.');
}

function setGooglePhotosPickerStatus(message, isError = false) {
    const status = document.getElementById('googlePhotosPickerStatus');
    if (!status) return;
    status.textContent = String(message || '');
    status.style.color = isError ? '#ffadad' : '';
}

function closeGooglePhotosPickerModal() {
    const modal = document.getElementById('googlePhotosPickerModal');
    if (modal) modal.style.display = 'none';
}

function renderGooglePhotosPickerGrid(items) {
    const grid = document.getElementById('googlePhotosPickerGrid');
    if (!grid) return;

    if (!Array.isArray(items) || !items.length) {
        grid.innerHTML = `<div class="google-photos-modal__empty">${t('Aucune photo trouvée.', 'No se encontraron fotos.')}</div>`;
        return;
    }

    grid.innerHTML = items.map((item, index) => {
        const thumb = `${String(item.baseUrl || '')}=w260-h260-c`;
        const name = escapeHtml(String(item.filename || `Photo ${index + 1}`));
        const when = item.mediaMetadata?.creationTime
            ? new Date(item.mediaMetadata.creationTime).toLocaleDateString(getCurrentLocale())
            : '';
        return `<button type="button" class="google-photos-item" data-photo-index="${index}" title="${name}">
            <img src="${thumb}" alt="${name}">
            <span>${name}</span>
            <small>${escapeHtml(when)}</small>
        </button>`;
    }).join('');
}

async function requestGooglePhotosAccessToken({ forceConsent = false } = {}) {
    if (googlePhotosAccessToken && Date.now() < googlePhotosTokenExpiryMs - 5000) {
        return googlePhotosAccessToken;
    }

    const clientId = ensureGooglePhotosClientId();
    if (!clientId) throw new Error(t('Client ID Google manquant.', 'Falta Google Client ID.'));

    const oauth2 = window.google?.accounts?.oauth2;
    if (!oauth2?.initTokenClient) {
        throw new Error(t('SDK Google non chargé.', 'SDK de Google no cargado.'));
    }

    return new Promise((resolve, reject) => {
        const tokenClient = oauth2.initTokenClient({
            client_id: clientId,
            scope: 'https://www.googleapis.com/auth/photoslibrary.readonly',
            callback: (response) => {
                if (!response || response.error || !response.access_token) {
                    reject(new Error(String(response?.error || 'oauth_failed')));
                    return;
                }

                const expiresInSec = Math.max(30, Number(response.expires_in || 3600));
                googlePhotosAccessToken = String(response.access_token);
                googlePhotosTokenExpiryMs = Date.now() + (expiresInSec * 1000);
                resolve(googlePhotosAccessToken);
            },
            error_callback: (oauthError) => {
                const reason = String(oauthError?.type || oauthError?.message || 'oauth_popup_error');
                reject(new Error(reason));
            }
        });

        tokenClient.requestAccessToken({ prompt: forceConsent ? 'consent' : '' });
    });
}

async function fetchGooglePhotosMediaItems(pageToken = '') {
    const token = await requestGooglePhotosAccessToken();
    const body = {
        pageSize: 36,
        ...(pageToken ? { pageToken } : {})
    };

    const response = await fetch('https://photoslibrary.googleapis.com/v1/mediaItems:search', {
        method: 'POST',
        headers: {
            Authorization: `Bearer ${token}`,
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(body)
    });

    if (!response.ok) {
        const details = await response.text();
        throw new Error(`Google Photos ${response.status}: ${details.slice(0, 180)}`);
    }

    const data = await response.json();
    const items = Array.isArray(data?.mediaItems)
        ? data.mediaItems.filter(item => item?.mimeType?.startsWith('image/') && item?.baseUrl)
        : [];

    return {
        items,
        nextPageToken: String(data?.nextPageToken || '')
    };
}

async function importGooglePhotoItemIntoWaypoint(item) {
    if (!item?.baseUrl) throw new Error('photo_invalid');

    setGooglePhotosPickerStatus(t('Import de la photo Google en cours...', 'Importando foto de Google...'));

    const imageUrl = `${String(item.baseUrl)}=w2000-h2000`;
    const response = await fetch(imageUrl);
    if (!response.ok) {
        throw new Error(`download_${response.status}`);
    }

    const blob = await response.blob();
    const mimeType = String(blob.type || 'image/jpeg');
    const fileName = String(item.filename || `google-photo-${Date.now()}.jpg`);
    const file = new File([blob], fileName, { type: mimeType });

    await handleWaypointPhotoInputChange({ target: { files: [file] } });
    closeGooglePhotosPickerModal();
    setGooglePhotosPickerStatus('');

    const status = document.getElementById('waypointPhotoStatus');
    if (status) {
        status.textContent = t('Photo Google importée: complète les infos puis sauvegarde le waypoint.', 'Foto Google importada: completa datos y guarda el waypoint.');
    }
}

async function openGooglePhotosPickerModal() {
    const modal = document.getElementById('googlePhotosPickerModal');
    const loadMoreBtn = document.getElementById('googlePhotosLoadMoreBtn');
    if (!modal) return;

    modal.style.display = 'flex';
    setGooglePhotosPickerStatus(t('Connexion Google Photos...', 'Conectando con Google Photos...'));

    try {
        const { items, nextPageToken } = await fetchGooglePhotosMediaItems('');
        googlePhotosPickerItems = items;
        googlePhotosPickerNextPageToken = nextPageToken;
        renderGooglePhotosPickerGrid(googlePhotosPickerItems);

        if (loadMoreBtn) {
            loadMoreBtn.style.display = googlePhotosPickerNextPageToken ? '' : 'none';
        }

        setGooglePhotosPickerStatus(t(`Photos disponibles: ${items.length}`, `Fotos disponibles: ${items.length}`));
    } catch (error) {
        const reason = String(error?.message || error);
        const help = buildGooglePhotosOAuthHelpMessage(error);
        setGooglePhotosPickerStatus(`${t('Connexion Google Photos impossible', 'No se puede conectar con Google Photos')}: ${reason}. ${help}`, true);
        const loadMore = document.getElementById('googlePhotosLoadMoreBtn');
        if (loadMore) loadMore.style.display = 'none';
    }
}

function estimateDataUrlSizeBytes(dataUrl) {
    const raw = String(dataUrl || '');
    const commaIndex = raw.indexOf(',');
    if (commaIndex === -1) return Math.max(0, raw.length);

    const meta = raw.slice(0, commaIndex).toLowerCase();
    const payload = raw.slice(commaIndex + 1);

    if (!meta.includes(';base64')) {
        try {
            return new TextEncoder().encode(decodeURIComponent(payload)).length;
        } catch (_error) {
            return Math.max(0, payload.length);
        }
    }

    const trimmed = payload.replace(/\s/g, '');
    const padding = trimmed.endsWith('==') ? 2 : (trimmed.endsWith('=') ? 1 : 0);
    return Math.max(0, Math.floor((trimmed.length * 3) / 4) - padding);
}

async function prepareDocumentForStorage(file, options = {}) {
    const maxSide = Number(options.maxSide) > 0 ? Number(options.maxSide) : 1280;
    const quality = Number(options.quality) > 0 ? Number(options.quality) : 0.55;
    const originalMime = String(file?.type || '').toLowerCase();
    const isImage = originalMime.startsWith('image/');

    if (!isImage) {
        const dataUrl = await imageFileToDataUrl(file);
        return {
            dataUrl,
            mimeType: originalMime,
            sizeBytes: Math.max(0, Number(file?.size || 0) || 0)
        };
    }

    const dataUrl = await imageFileToCompressedDataUrl(file, maxSide, quality);
    return {
        dataUrl,
        mimeType: 'image/jpeg',
        sizeBytes: estimateDataUrlSizeBytes(dataUrl)
    };
}

function buildWaypointPhotoPopupContent(entry, weatherHtml = '') {
    const title = entry.placeName ? escapeHtml(entry.placeName) : t('Mouillage noté', 'Fondeo valorado');
    const creator = String(entry?.creatorName || '').trim();
    const creatorLine = creator ? `<div>${t('Créateur', 'Creador')}: ${escapeHtml(creator)}</div>` : '';
    const comment = entry.comment ? `<div style="margin-top:6px;">${escapeHtml(entry.comment)}</div>` : '';
    const bottom = entry.bottomType ? `<div>${t('Fond', 'Fondo')}: ${escapeHtml(entry.bottomType)}</div>` : '';
    const image = entry.imageDataUrl
        ? `<img src="${entry.imageDataUrl}" alt="${t('Photo mouillage', 'Foto fondeo')}" style="margin-top:6px; width:100%; max-width:180px; border-radius:8px;">`
        : '';

    return `<strong>${title}</strong><br>` +
        `${entry.lat.toFixed(5)}, ${entry.lng.toFixed(5)}<br>` +
        `${t('Global', 'Global')}: ${starsLabel(entry.rating)} · ${t('Propreté', 'Limpieza')}: ${starsLabel(entry.cleanliness)} · ${t('Profondeur', 'Profundidad')}: ${entry.depthMeters} m<br>` +
        `${t('Protection', 'Protección')}: ${escapeHtml(formatProtectionList(entry.protection))}` +
        `${creatorLine}` +
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

    if (isCloudReady()) {
        syncWaypointPhotosToCloud()
            .catch(error => setCloudStatus(`Photos locales OK, synchro cloud échouée: ${formatCloudError(error)}`, true));
    }

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
    const container = document.getElementById('waypointPhotoListDock');
    const dockTitle = document.getElementById('waypointSavedAnchoragesDockLabel');
    if (!container) return;

    const searchTerm = waypointSearchTerm.trim().toLowerCase();
    const filteredEntries = searchTerm
        ? waypointPhotoEntries.filter(entry => {
            const searchBlob = [
                entry.placeName,
                entry.comment,
                entry.bottomType,
                entry.creatorName,
                entry.creatorEmail,
                formatProtectionList(entry.protection),
                Number.isFinite(entry.lat) ? entry.lat.toFixed(4) : '',
                Number.isFinite(entry.lng) ? entry.lng.toFixed(4) : ''
            ].join(' ').toLowerCase();

            return searchBlob.includes(searchTerm);
        })
        : waypointPhotoEntries;

    if (dockTitle) {
        const countLabel = searchTerm
            ? `${filteredEntries.length}/${waypointPhotoEntries.length}`
            : `${waypointPhotoEntries.length}`;
        dockTitle.textContent = `${t('Mouillages enregistrés', 'Fondeos guardados')}: ${countLabel}`;
    }

    if (!waypointPhotoEntries.length) {
        container.innerHTML = `<div class="arrival-list__item">${t('Aucun mouillage enregistré.', 'No hay fondeos guardados.')}</div>`;
        return;
    }

    if (!filteredEntries.length) {
        container.innerHTML = `<div class="arrival-list__item">${t('Aucun mouillage trouvé pour cette recherche.', 'No hay fondeos para esta búsqueda.')}</div>`;
        return;
    }

    container.innerHTML = filteredEntries.map(entry => {
        const title = entry.placeName ? escapeHtml(entry.placeName) : t('Mouillage sans nom', 'Fondeo sin nombre');
        const creator = String(entry?.creatorName || '').trim();
        const creatorLine = creator ? `<div>${t('Créateur', 'Creador')}: ${escapeHtml(creator)}</div>` : '';
        const bottom = entry.bottomType ? `<div>${t('Fond', 'Fondo')}: ${escapeHtml(entry.bottomType)}</div>` : '';
        const comment = entry.comment ? `<div style="margin-top:4px;">${escapeHtml(entry.comment)}</div>` : '';
        const image = entry.imageDataUrl ? `<img class="waypoint-photo-card__img" src="${entry.imageDataUrl}" alt="${t('Photo mouillage', 'Foto fondeo')}">` : '';

        return `<div class="waypoint-photo-card">
            <div><strong>${title}</strong></div>
            <div><strong>${starsLabel(entry.rating)}</strong> · ${entry.lat.toFixed(4)}, ${entry.lng.toFixed(4)}</div>
            <div>${t('Propreté', 'Limpieza')} ${starsLabel(entry.cleanliness)} · ${t('Profondeur', 'Profundidad')} ${entry.depthMeters} m</div>
            <div>${t('Protection', 'Protección')} ${escapeHtml(formatProtectionList(entry.protection))}</div>
            ${creatorLine}
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
            id: generateClientUuid(),
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
            creatorEmail: getCreatorPatch().creatorEmail,
            creatorName: getCreatorPatch().creatorName,
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

        if (isCloudReady()) {
            try {
                await syncWaypointPhotosToCloud();
            } catch (error) {
                waypointPhotoEntries.shift();
                persistWaypointPhotoEntries();
                renderWaypointPhotoList();
                syncWaypointPhotoMarkersInView();
                alert(t('Photo locale créée puis annulée: écriture cloud impossible. Vérifie la connexion cloud et réessaie.', 'Foto local creada y luego cancelada: escritura en nube imposible. Verifica la conexión cloud y vuelve a intentar.'));
                return;
            }
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

        waypointPhotoEntries[editIndex] = ensureCreatorOnEditedRecord({
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
        });

        const persisted = persistWaypointPhotoEntries();
        if (!persisted) {
            waypointPhotoEntries[editIndex] = previousEntry;
            alert(t('Enregistrement impossible: stockage local saturé. Essaie une image plus légère ou supprime des anciennes photos.', 'No se puede guardar: almacenamiento local lleno. Prueba una imagen más ligera o elimina fotos antiguas.'));
            return;
        }

        if (isCloudReady()) {
            try {
                await syncWaypointPhotosToCloud();
            } catch (error) {
                waypointPhotoEntries[editIndex] = previousEntry;
                persistWaypointPhotoEntries();
                renderWaypointPhotoList();
                syncWaypointPhotoMarkersInView();
                alert(t('Modification locale annulée: écriture cloud impossible. Vérifie la connexion cloud et réessaie.', 'Modificación local cancelada: escritura en nube imposible. Verifica la conexión cloud y vuelve a intentar.'));
                return;
            }
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
        id: generateClientUuid(),
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
        creatorEmail: getCreatorPatch().creatorEmail,
        creatorName: getCreatorPatch().creatorName,
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

    if (isCloudReady()) {
        try {
            await syncWaypointPhotosToCloud();
        } catch (error) {
            waypointPhotoEntries.shift();
            persistWaypointPhotoEntries();
            renderWaypointPhotoList();
            syncWaypointPhotoMarkersInView();
            alert(t('Enregistrement local annulé: écriture cloud impossible. Vérifie la connexion cloud et réessaie.', 'Guardado local cancelado: escritura en nube imposible. Verifica la conexión cloud y vuelve a intentar.'));
            return;
        }
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

    const isShopList = containerId === 'nearbyShops';
    const kind = isShopList ? 'shop' : 'restaurant';

    const clamp = (value, min, max) => Math.max(min, Math.min(max, value));

    const buildGoogleSearchUrl = (item) => {
        const name = String(item?.name || '').trim();
        const lat = Number(item?.lat);
        const lon = Number(item?.lon);
        const hasCoords = Number.isFinite(lat) && Number.isFinite(lon);
        const coords = hasCoords ? `${lat.toFixed(6)},${lon.toFixed(6)}` : '';
        const query = [name, coords].filter(Boolean).join(', ');
        const safeQuery = query || name || coords || '';
        return `https://maps.google.com/maps?q=${encodeURIComponent(safeQuery)}&hl=fr&z=14&output=embed`;
    };

    const formatAmenityScore = (item, amenityKind) => {
        const distance = Number(item?.distanceNm);
        const tags = item?.tags || {};
        let score = 86;

        if (Number.isFinite(distance)) {
            score -= distance * 9;
        }

        if (tags.opening_hours) score += 4;
        if (tags.phone) score += 3;
        if (tags.website) score += 4;
        if (tags['addr:street']) score += 2;
        if (tags.wheelchair === 'yes') score += 2;

        if (amenityKind === 'restaurant') {
            if (tags.cuisine) score += 6;
            if (tags.takeaway === 'yes') score += 2;
            if (tags.delivery === 'yes') score += 2;
            if (tags.outdoor_seating === 'yes') score += 1;
        } else {
            if (tags.shop) score += 4;
            if (tags.brand) score += 3;
            if (tags.organic === 'yes') score += 2;
        }

        return clamp(Math.round(score), 45, 99);
    };

    const buildAmenitySummary = (item, amenityKind) => {
        const tags = item?.tags || {};
        const summaryParts = [];

        if (amenityKind === 'restaurant') {
            const cuisine = String(tags.cuisine || '').trim();
            if (cuisine) {
                summaryParts.push(`${t('Cuisine', 'Cocina')}: ${cuisine.replaceAll(';', ', ')}`);
            }
            if (tags.takeaway === 'yes') summaryParts.push(t('À emporter', 'Para llevar'));
            if (tags.delivery === 'yes') summaryParts.push(t('Livraison', 'Entrega'));
        } else {
            const shopType = String(tags.shop || '').trim();
            if (shopType) summaryParts.push(`${t('Type', 'Tipo')}: ${shopType}`);
            const brand = String(tags.brand || '').trim();
            if (brand) summaryParts.push(`${t('Enseigne', 'Marca')}: ${brand}`);
        }

        const openingHours = String(tags.opening_hours || '').trim();
        if (openingHours) {
            summaryParts.push(`${t('Horaires', 'Horario')}: ${openingHours}`);
        }

        if (summaryParts.length === 0) {
            return t('Infos limitées (OSM)', 'Info limitada (OSM)');
        }

        return summaryParts.slice(0, 2).join(' · ');
    };

    container.innerHTML = `<div class="arrival-list">${items.map(item =>
        `<div class="arrival-list__item">
            <div class="arrival-list__head">
                <strong>${escapeHtml(item.name)}</strong>
                <span class="arrival-list__score">${t('Score', 'Puntuacion')}: ${formatAmenityScore(item, kind)}/100</span>
            </div>
            <div class="arrival-list__meta">${item.distanceNm.toFixed(2)} nm · ${escapeHtml(buildAmenitySummary(item, kind))}</div>
            <a class="arrival-list__link" href="${buildGoogleSearchUrl(item)}">${t('Voir sur Google', 'Ver en Google')}</a>
        </div>`
    ).join('')}</div>`;

    container.querySelectorAll('.arrival-list__link').forEach(link => {
        link.addEventListener('click', event => {
            event.preventDefault();
            const href = String(link.getAttribute('href') || '').trim();
            if (!href) return;
            openUrlInMapOverlay(href);
        });
    });
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

    beginAiTrafficSession(t('Analyse zone arrivée', 'Analisis zona llegada'));

    const button = document.getElementById('analyzeArrivalBtn');
    const summary = document.getElementById('arrivalSummary');
    if (button) {
        button.disabled = true;
        button.textContent = t('Analyse en cours...', 'Análisis en curso...');
    }
    if (summary) summary.textContent = t('Analyse mouillage: récupération des données...', 'Análisis fondeo: recuperando datos...');

    clearArrivalPoiMarkers();
    pushAiTrafficLog(t('Nettoyage des marqueurs d\'arrivée précédents', 'Limpieza de marcadores de llegada anteriores'));

    try {
        const destination = routePoints[routePoints.length - 1];
        const arrivalTime = getArrivalReferenceDateTime();
        pushAiTrafficLog(t(
            `Destination ciblée: ${Number(destination.lat).toFixed(4)}, ${Number(destination.lng).toFixed(4)}`,
            `Destino objetivo: ${Number(destination.lat).toFixed(4)}, ${Number(destination.lng).toFixed(4)}`
        ));

        pushAiTrafficLog(t('Recherche mouillages à proximité (Overpass)...', 'Busqueda de fondeos cercanos (Overpass)...'));
        const anchorages = await fetchNearbyAnchorages(destination.lat, destination.lng);
        pushAiTrafficLog(t(`Mouillages trouvés: ${anchorages.length}`, `Fondeos encontrados: ${anchorages.length}`));

        pushAiTrafficLog(t('Recherche restaurants proches...', 'Busqueda de restaurantes cercanos...'));
        const restaurants = await fetchNearbyAmenityList(destination.lat, destination.lng, 'restaurant');
        pushAiTrafficLog(t(`Restaurants trouvés: ${restaurants.length}`, `Restaurantes encontrados: ${restaurants.length}`));

        pushAiTrafficLog(t('Recherche magasins proches...', 'Busqueda de tiendas cercanas...'));
        const shops = await fetchNearbyAmenityList(destination.lat, destination.lng, 'shop');
        pushAiTrafficLog(t(`Magasins trouvés: ${shops.length}`, `Tiendas encontradas: ${shops.length}`));

        pushAiTrafficLog(t('Scoring météo des mouillages...', 'Puntuacion meteo de los fondeos...'));
        const recommendations = await scoreAnchoragesForArrival(anchorages, arrivalTime);
        pushAiTrafficLog(t(
            `Top recommandations calculées: ${recommendations.length}`,
            `Top recomendaciones calculadas: ${recommendations.length}`
        ));

        recommendations.forEach(item => addArrivalPoiMarker(item.lat, item.lon, item.name, 'anchorage'));
        restaurants.forEach(item => addArrivalPoiMarker(item.lat, item.lon, item.name, 'restaurant'));
        shops.forEach(item => addArrivalPoiMarker(item.lat, item.lon, item.name, 'shop'));
        pushAiTrafficLog(t('Marqueurs arrivée affichés sur la carte', 'Marcadores de llegada mostrados en el mapa'));

        renderAnchorageRecommendations(recommendations);
        renderNearbyList('nearbyRestaurants', restaurants, t('Aucun restaurant proche trouvé', 'No se encontraron restaurantes cercanos'));
        renderNearbyList('nearbyShops', shops, t('Aucun magasin proche trouvé', 'No se encontraron tiendas cercanas'));
        pushAiTrafficLog(t('Listes arrivée mises à jour', 'Listas de llegada actualizadas'));

        if (summary) {
            if (recommendations.length) {
                summary.innerHTML = `<strong>${t('Top mouillage:', 'Mejor fondeo:')}</strong> ${escapeHtml(recommendations[0].name)} · ${recommendations[0].distanceNm.toFixed(2)} nm ${t('de l\'arrivée', 'de la llegada')}`;
                pushAiTrafficLog(t(
                    `Top mouillage: ${recommendations[0].name} (${recommendations[0].distanceNm.toFixed(2)} nm)`,
                    `Mejor fondeo: ${recommendations[0].name} (${recommendations[0].distanceNm.toFixed(2)} nm)`
                ));
            } else {
                summary.textContent = t('Analyse mouillage: aucun mouillage adapté trouvé à proximité.', 'Análisis fondeo: no se encontró un fondeo adecuado cerca.');
                pushAiTrafficLog(t('Aucun mouillage adapté trouvé', 'No se encontro fondeo adecuado'));
            }
        }
        endAiTrafficSession(t('Analyse arrivée terminée', 'Analisis llegada terminado'));
    } catch (_error) {
        pushAiTrafficLog(t('Erreur pendant l\'analyse arrivée', 'Error durante el analisis de llegada'));
        if (summary) summary.textContent = t('Analyse mouillage: erreur de récupération des données.', 'Análisis fondeo: error al recuperar datos.');
        endAiTrafficSession(t('Analyse arrivée terminée en erreur', 'Analisis llegada terminado con error'));
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
    const dateObj = parseDateInputFlexible(dateInput);
    if (!dateObj) return 'N/A';
    return dateObj.toLocaleString(getCurrentLocale(), {
        day: '2-digit',
        month: '2-digit',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
    });
}

function loadArrayFromStorage(storageKey) {
    const raw = localStorage.getItem(storageKey);
    if (raw === null) return [];

    let parsed;
    try {
        parsed = JSON.parse(raw);
    } catch (error) {
        throw new Error(`Stockage local invalide pour ${storageKey}: ${String(error?.message || error)}`);
    }

    if (!Array.isArray(parsed)) {
        throw new Error(`Stockage local invalide pour ${storageKey}: tableau attendu.`);
    }

    return parsed;
}

function saveArrayToStorage(storageKey, data) {
    if (!Array.isArray(data)) {
        throw new Error(`Impossible de sauvegarder ${storageKey}: tableau attendu.`);
    }
    localStorage.setItem(storageKey, JSON.stringify(data));
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

function toTimestampMs(value) {
    const ts = Date.parse(String(value || ''));
    return Number.isFinite(ts) ? ts : 0;
}

function mergeByIdPreferNewest(localList, remoteList, sanitizeFn) {
    const mergedById = new Map();

    const upsert = (entry) => {
        const sanitized = sanitizeFn(entry);
        const id = String(sanitized?.id || '').trim();
        if (!id) return;

        const previous = mergedById.get(id);
        if (!previous) {
            mergedById.set(id, sanitized);
            return;
        }

        const prevTs = toTimestampMs(previous.updatedAt);
        const nextTs = toTimestampMs(sanitized.updatedAt);
        if (nextTs >= prevTs) {
            mergedById.set(id, sanitized);
        }
    };

    (Array.isArray(remoteList) ? remoteList : []).forEach(upsert);
    (Array.isArray(localList) ? localList : []).forEach(upsert);

    return Array.from(mergedById.values());
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

function sanitizeMaintenanceInvoiceDocument(entry, fallbackIndex = 0) {
    const name = String(entry?.name || entry?.invoiceName || '').trim();
    const dataUrl = String(entry?.dataUrl || entry?.invoiceDataUrl || '');
    const mimeType = String(entry?.mimeType || entry?.invoiceMimeType || '').trim().toLowerCase();
    const sizeBytes = Math.max(0, Number(entry?.sizeBytes || entry?.invoiceSizeBytes || 0) || 0);

    return {
        id: String(entry?.id || `expense-doc-${Date.now()}-${fallbackIndex}`),
        name,
        dataUrl,
        mimeType,
        sizeBytes
    };
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

    const rawDocuments = Array.isArray(entry?.invoiceDocuments) ? entry.invoiceDocuments : [];
    let invoiceDocuments = rawDocuments
        .map((doc, docIndex) => sanitizeMaintenanceInvoiceDocument(doc, docIndex))
        .filter(doc => doc.dataUrl);

    if (!invoiceDocuments.length && entry?.invoiceDataUrl) {
        invoiceDocuments = [sanitizeMaintenanceInvoiceDocument({
            id: `expense-doc-legacy-${Date.now()}-${fallbackIndex}`,
            name: entry?.invoiceName || 'facture',
            dataUrl: entry?.invoiceDataUrl,
            mimeType: entry?.invoiceMimeType,
            sizeBytes: entry?.invoiceSizeBytes
        }, 0)];
    }

    const primaryInvoice = invoiceDocuments[0] || null;

    return {
        id: String(entry?.id || generateClientUuid()),
        invoiceName: String(entry?.invoiceName || primaryInvoice?.name || '').trim(),
        invoiceDataUrl: String(entry?.invoiceDataUrl || primaryInvoice?.dataUrl || ''),
        invoiceMimeType: String(entry?.invoiceMimeType || primaryInvoice?.mimeType || '').trim(),
        invoiceSizeBytes: Math.max(0, Number(entry?.invoiceSizeBytes || primaryInvoice?.sizeBytes || 0) || 0),
        invoiceDocuments,
        date: String(entry?.date || new Date().toISOString().slice(0, 10)),
        supplierName: String(entry?.supplierName || '').trim(),
        supplierIban: String(entry?.supplierIban || '').trim(),
        payer: (() => {
            const rawPayer = String(entry?.payer || 'PATISSIER').toUpperCase();
            if (rawPayer === 'KLENIK') return 'KLENIK';
            if (rawPayer === 'OTRO') return 'OTRO';
            return 'PATISSIER';
        })(),
        paymentStatus: (() => {
            const rawStatus = String(entry?.paymentStatus || 'pending');
            if (rawStatus === 'partial') return 'planned';
            return ['new', 'pending', 'planned', 'paid'].includes(rawStatus) ? rawStatus : 'pending';
        })(),
        totalAmount: toFiniteAmount(entry?.totalAmount),
        currency: String(entry?.currency || 'EUR').trim().toUpperCase() || 'EUR',
        lines,
        note: String(entry?.note || '').trim(),
        aiComment: String(entry?.aiComment || '').trim(),
        postComment: String(entry?.postComment || '').trim(),
        scannedText: String(entry?.scannedText || ''),
        creatorEmail: normalizeEmailForCompare(entry?.creatorEmail || ''),
        creatorName: String(entry?.creatorName || '').trim(),
        createdAt: String(entry?.createdAt || new Date().toISOString()),
        updatedAt: String(entry?.updatedAt || new Date().toISOString())
    };
}

function sanitizeMaintenanceSupplier(entry, fallbackIndex = 0) {
    const rawDocuments = Array.isArray(entry?.documents) ? entry.documents : [];
    const documents = rawDocuments
        .map((doc, docIndex) => sanitizeMaintenanceInvoiceDocument(doc, docIndex))
        .filter(doc => doc.dataUrl);

    return {
        id: String(entry?.id || generateClientUuid()),
        name: String(entry?.name || '').trim(),
        contact: String(entry?.contact || '').trim(),
        emergencyPhone: String(entry?.emergencyPhone || '').trim(),
        iban: String(entry?.iban || '').trim(),
        note: String(entry?.note || '').trim(),
        documents,
        creatorEmail: normalizeEmailForCompare(entry?.creatorEmail || ''),
        creatorName: String(entry?.creatorName || '').trim(),
        createdAt: String(entry?.createdAt || new Date().toISOString()),
        updatedAt: String(entry?.updatedAt || new Date().toISOString())
    };
}

function generateClientUuid() {
    if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
        return crypto.randomUUID();
    }

    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, char => {
        const random = Math.floor(Math.random() * 16);
        const value = char === 'x' ? random : ((random & 0x3) | 0x8);
        return value.toString(16);
    });
}

function loadMaintenanceExpenses() {
    setMaintenanceExpenses([], { refreshUi: false });
}

function setMaintenanceExpenses(list, { refreshUi = true } = {}) {
    maintenanceExpenses = (Array.isArray(list) ? list : [])
        .map((entry, index) => sanitizeMaintenanceExpense(entry, index))
        .sort((a, b) => String(b.date || '').localeCompare(String(a.date || '')));

    if (selectedMaintenanceExpenseId && !maintenanceExpenses.some(item => item.id === selectedMaintenanceExpenseId)) {
        selectedMaintenanceExpenseId = null;
    }

    if (refreshUi) {
        renderMaintenanceExpenses();
    }
}

function buildMaintenanceExpenseFromCloudRow(row, index = 0) {
    return sanitizeMaintenanceExpense({
        id: String(row?.id || generateClientUuid()),
        invoiceName: String(row?.invoice_name || ''),
        invoiceDataUrl: String(row?.invoice_data_url || ''),
        invoiceMimeType: String(row?.invoice_mime_type || ''),
        invoiceSizeBytes: Math.max(0, Number(row?.invoice_size_bytes || 0) || 0),
        invoiceDocuments: Array.isArray(row?.invoice_documents) ? row.invoice_documents : [],
        date: String(row?.expense_date || new Date().toISOString().slice(0, 10)),
        supplierName: String(row?.supplier_name || ''),
        supplierIban: String(row?.supplier_iban || ''),
        payer: String(row?.payer || 'PATISSIER'),
        paymentStatus: String(row?.payment_status || 'pending'),
        totalAmount: toFiniteAmount(row?.total_amount),
        currency: String(row?.currency || 'EUR'),
        lines: Array.isArray(row?.lines) ? row.lines : [],
        note: String(row?.note || ''),
        aiComment: String(row?.ai_comment || ''),
        postComment: String(row?.post_comment || ''),
        scannedText: String(row?.scanned_text || ''),
        creatorEmail: String(row?.creator_email || ''),
        creatorName: String(row?.creator_name || ''),
        createdAt: String(row?.created_at || new Date().toISOString()),
        updatedAt: String(row?.updated_at || new Date().toISOString())
    }, index);
}

function buildMaintenanceExpenseCloudPayload(entry, projectIdUuid, creatorEmail) {
    const safe = sanitizeMaintenanceExpense(entry, 0);
    return {
        id: String(safe.id || generateClientUuid()),
        project_id: projectIdUuid,
        creator_email: creatorEmail,
        creator_name: safe.creatorName || null,
        invoice_name: safe.invoiceName || '',
        invoice_data_url: safe.invoiceDataUrl || '',
        invoice_mime_type: safe.invoiceMimeType || '',
        invoice_size_bytes: Math.max(0, Number(safe.invoiceSizeBytes || 0) || 0),
        invoice_documents: Array.isArray(safe.invoiceDocuments) ? safe.invoiceDocuments : [],
        expense_date: String(safe.date || new Date().toISOString().slice(0, 10)),
        supplier_name: safe.supplierName || '',
        supplier_iban: safe.supplierIban || '',
        payer: ['PATISSIER', 'KLENIK', 'OTRO'].includes(String(safe.payer || '').toUpperCase())
            ? String(safe.payer || '').toUpperCase()
            : 'PATISSIER',
        payment_status: ['new', 'pending', 'planned', 'paid'].includes(safe.paymentStatus) ? safe.paymentStatus : 'pending',
        total_amount: toFiniteAmount(safe.totalAmount),
        currency: String(safe.currency || 'EUR').trim().toUpperCase() || 'EUR',
        lines: Array.isArray(safe.lines) ? safe.lines : [],
        note: safe.note || '',
        ai_comment: safe.aiComment || '',
        post_comment: safe.postComment || '',
        scanned_text: safe.scannedText || '',
        created_at: safe.createdAt || new Date().toISOString(),
        updated_at: new Date().toISOString()
    };
}

async function pullMaintenanceExpensesFromCloudV2() {
    if (!isCloudReady()) {
        throw new Error('Cloud non pret: impossible de lire maintenance_expenses.');
    }

    const creatorEmail = getCurrentCloudUserEmail();
    if (!creatorEmail) {
        throw new Error('creator_email manquant: impossible de lire maintenance_expenses.');
    }

    const projectIdUuid = await resolveCloudProjectIdUuid();

    let query = cloudClient
        .from(CLOUD_MAINTENANCE_EXPENSES_TABLE)
        .select('*')
        .eq('creator_email', creatorEmail)
        .order('expense_date', { ascending: false })
        .order('created_at', { ascending: false });

    if (isUuidString(projectIdUuid)) {
        query = query.eq('project_id', projectIdUuid);
    }

    const { data: rows, error } = await query;
    if (error) throw error;

    return (Array.isArray(rows) ? rows : [])
        .map((row, index) => buildMaintenanceExpenseFromCloudRow(row, index))
        .sort((a, b) => String(b.date || '').localeCompare(String(a.date || '')));
}

async function insertMaintenanceExpenseInCloud(entry) {
    if (!isCloudReady()) {
        throw new Error('Cloud non pret: impossible d\'inserer maintenance_expenses.');
    }

    const creatorEmail = getCurrentCloudUserEmail();
    if (!creatorEmail) {
        throw new Error('creator_email manquant: impossible d\'inserer maintenance_expenses.');
    }

    let projectIdUuid = await resolveCloudProjectIdUuid();
    if (!projectIdUuid) {
        throw new Error('project_id introuvable pour maintenance_expenses.');
    }
    projectIdUuid = await ensureCloudProjectRow(projectIdUuid);

    const payload = buildMaintenanceExpenseCloudPayload(entry, projectIdUuid, creatorEmail);
    const { error } = await cloudClient
        .from(CLOUD_MAINTENANCE_EXPENSES_TABLE)
        .insert(payload);
    if (error) throw error;

    return buildMaintenanceExpenseFromCloudRow(payload, 0);
}

async function updateMaintenanceExpenseById(expenseId, updater, { refreshUi = true } = {}) {
    const targetId = String(expenseId || '');
    if (!targetId) return false;

    const expenseIndex = maintenanceExpenses.findIndex(item => item.id === targetId);
    if (expenseIndex < 0) return false;

    const currentExpense = sanitizeMaintenanceExpense(maintenanceExpenses[expenseIndex], expenseIndex);
    const currentDocuments = Array.isArray(currentExpense.invoiceDocuments)
        ? currentExpense.invoiceDocuments.map((doc, docIndex) => sanitizeMaintenanceInvoiceDocument(doc, docIndex)).filter(doc => doc.dataUrl)
        : [];

    const patch = typeof updater === 'function'
        ? updater({ ...currentExpense, invoiceDocuments: currentDocuments })
        : updater;

    if (!patch || typeof patch !== 'object') return false;

    const mergedExpense = {
        ...currentExpense,
        ...patch,
        id: currentExpense.id,
        updatedAt: new Date().toISOString()
    };

    const sanitizedExpense = sanitizeMaintenanceExpense(mergedExpense, expenseIndex);

    if (!isCloudReady()) {
        throw new Error('Cloud non pret: impossible de mettre a jour maintenance_expenses.');
    }

    const creatorEmail = getCurrentCloudUserEmail();
    if (!creatorEmail) {
        throw new Error('creator_email manquant: impossible de mettre a jour maintenance_expenses.');
    }

    let projectIdUuid = await resolveCloudProjectIdUuid();
    if (!projectIdUuid) {
        throw new Error('project_id introuvable pour maintenance_expenses.');
    }
    projectIdUuid = await ensureCloudProjectRow(projectIdUuid);

    const payload = buildMaintenanceExpenseCloudPayload(sanitizedExpense, projectIdUuid, creatorEmail);
    const { error } = await cloudClient
        .from(CLOUD_MAINTENANCE_EXPENSES_TABLE)
        .update({
            invoice_name: payload.invoice_name,
            invoice_data_url: payload.invoice_data_url,
            invoice_mime_type: payload.invoice_mime_type,
            invoice_size_bytes: payload.invoice_size_bytes,
            invoice_documents: payload.invoice_documents,
            expense_date: payload.expense_date,
            supplier_name: payload.supplier_name,
            supplier_iban: payload.supplier_iban,
            payer: payload.payer,
            payment_status: payload.payment_status,
            total_amount: payload.total_amount,
            currency: payload.currency,
            lines: payload.lines,
            note: payload.note,
            ai_comment: payload.ai_comment,
            post_comment: payload.post_comment,
            scanned_text: payload.scanned_text,
            creator_name: payload.creator_name,
            updated_at: payload.updated_at
        })
        .eq('id', targetId)
        .eq('project_id', projectIdUuid)
        .eq('creator_email', creatorEmail);
    if (error) throw error;

    maintenanceExpenses = maintenanceExpenses
        .map((item, index) => (index === expenseIndex ? sanitizedExpense : sanitizeMaintenanceExpense(item, index)))
        .sort((a, b) => String(b.date || '').localeCompare(String(a.date || '')));

    if (refreshUi) {
        renderMaintenanceExpenses();
    }

    return true;
}

async function deleteMaintenanceExpenseById(expenseId) {
    const targetId = String(expenseId || '').trim();
    if (!targetId) {
        throw new Error('expenseId manquant: suppression impossible.');
    }

    if (!isCloudReady()) {
        throw new Error('Cloud non pret: impossible de supprimer maintenance_expenses.');
    }

    const creatorEmail = getCurrentCloudUserEmail();
    if (!creatorEmail) {
        throw new Error('creator_email manquant: impossible de supprimer maintenance_expenses.');
    }

    let projectIdUuid = await resolveCloudProjectIdUuid();
    if (!projectIdUuid) {
        throw new Error('project_id introuvable pour maintenance_expenses.');
    }
    projectIdUuid = await ensureCloudProjectRow(projectIdUuid);

    const { error } = await cloudClient
        .from(CLOUD_MAINTENANCE_EXPENSES_TABLE)
        .delete()
        .eq('id', targetId)
        .eq('project_id', projectIdUuid)
        .eq('creator_email', creatorEmail);
    if (error) throw error;

    maintenanceExpenses = maintenanceExpenses.filter(item => item.id !== targetId);
    if (selectedMaintenanceExpenseId === targetId) {
        selectedMaintenanceExpenseId = null;
    }
    renderMaintenanceExpenses();
}

function loadMaintenanceSuppliers() {
    setMaintenanceSuppliers([], { refreshUi: false });
}

function getMaintenanceSupplierDraftKey() {
    return selectedMaintenanceSupplierId || '__new__';
}

function clearMaintenanceSupplierDraft() {
    maintenanceSupplierFormDraft = null;
}

function isMaintenanceSupplierEditorFocused() {
    const detailPanel = document.getElementById('maintenanceExpenseDetailPanel');
    const activeElement = document.activeElement;
    if (!detailPanel || !activeElement) return false;
    if (detailPanel.dataset.detailMode !== 'supplier') return false;
    return detailPanel.contains(activeElement);
}

function setMaintenanceSuppliers(list, { refreshUi = true } = {}) {
    maintenanceSuppliers = (Array.isArray(list) ? list : [])
        .map((entry, index) => sanitizeMaintenanceSupplier(entry, index))
        .filter(entry => entry.name)
        .sort((a, b) => a.name.localeCompare(b.name));

    if (selectedMaintenanceSupplierId && !maintenanceSuppliers.some(item => item.id === selectedMaintenanceSupplierId)) {
        selectedMaintenanceSupplierId = null;
        clearMaintenanceSupplierDraft();
    }

    if (refreshUi) {
        renderMaintenanceSuppliers();
    }
}

function buildMaintenanceSupplierFromCloudRow(row, index = 0) {
    return sanitizeMaintenanceSupplier({
        id: String(row?.id || generateClientUuid()),
        name: String(row?.name || ''),
        contact: String(row?.contact || ''),
        emergencyPhone: String(row?.emergency_phone || ''),
        iban: String(row?.iban || ''),
        note: String(row?.note || ''),
        documents: Array.isArray(row?.documents) ? row.documents : [],
        creatorEmail: String(row?.creator_email || ''),
        creatorName: String(row?.creator_name || ''),
        createdAt: String(row?.created_at || new Date().toISOString()),
        updatedAt: String(row?.updated_at || new Date().toISOString())
    }, index);
}

function buildMaintenanceSupplierCloudPayload(entry, projectIdUuid, creatorEmail) {
    const safe = sanitizeMaintenanceSupplier(entry, 0);
    return {
        id: String(safe.id || generateClientUuid()),
        project_id: projectIdUuid,
        creator_email: creatorEmail,
        creator_name: safe.creatorName || null,
        name: safe.name,
        contact: safe.contact || '',
        emergency_phone: safe.emergencyPhone || '',
        iban: safe.iban || '',
        note: safe.note || '',
        documents: Array.isArray(safe.documents) ? safe.documents : [],
        created_at: safe.createdAt || new Date().toISOString(),
        updated_at: new Date().toISOString()
    };
}

function removeMaintenanceSupplierFromState(supplierId, { refreshUi = true } = {}) {
    const targetId = String(supplierId || '').trim();
    if (!targetId) return;

    maintenanceSuppliers = maintenanceSuppliers.filter(item => item.id !== targetId);
    if (selectedMaintenanceSupplierId === targetId) {
        selectedMaintenanceSupplierId = null;
    }

    setMaintenanceSuppliers(maintenanceSuppliers, { refreshUi });
}

function getMissingPostgrestColumn(error) {
    const code = String(error?.code || '');
    const message = String(error?.message || '');
    if (code !== 'PGRST204') return '';
    const match = message.match(/Could not find the '([^']+)' column/i);
    return String(match?.[1] || '').trim();
}

async function insertSupplierPayloadWithSchemaTolerance(basePayload) {
    let payload = { ...basePayload };
    for (let attempt = 0; attempt < 8; attempt++) {
        const { error } = await cloudClient
            .from(CLOUD_MAINTENANCE_SUPPLIERS_TABLE)
            .insert(payload);

        if (!error) return payload;

        const missingColumn = getMissingPostgrestColumn(error);
        if (!missingColumn || !(missingColumn in payload)) throw error;

        console.warn(`[SUPPLIERS] Missing DB column ignored on insert: ${missingColumn}`);
        delete payload[missingColumn];
    }

    throw new Error('INSERT maintenance_suppliers impossible: trop de colonnes manquantes.');
}

async function updateSupplierPayloadWithSchemaTolerance(targetId, projectIdUuid, creatorEmail, basePayload) {
    let payload = { ...basePayload };
    for (let attempt = 0; attempt < 8; attempt++) {
        const { error } = await cloudClient
            .from(CLOUD_MAINTENANCE_SUPPLIERS_TABLE)
            .update(payload)
            .eq('id', targetId)
            .eq('project_id', projectIdUuid)
            .eq('creator_email', creatorEmail);

        if (!error) return payload;

        const missingColumn = getMissingPostgrestColumn(error);
        if (!missingColumn || !(missingColumn in payload)) throw error;

        console.warn(`[SUPPLIERS] Missing DB column ignored on update: ${missingColumn}`);
        delete payload[missingColumn];
    }

    throw new Error('UPDATE maintenance_suppliers impossible: trop de colonnes manquantes.');
}

async function pullMaintenanceSuppliersFromCloudV2() {
    if (!isCloudReady()) {
        throw new Error('Cloud non pret: impossible de lire maintenance_suppliers.');
    }

    const creatorEmail = getCurrentCloudUserEmail();
    if (!creatorEmail) {
        throw new Error('creator_email manquant: impossible de lire maintenance_suppliers.');
    }

    const projectIdUuid = await resolveCloudProjectIdUuid();

    let query = cloudClient
        .from(CLOUD_MAINTENANCE_SUPPLIERS_TABLE)
        .select('*')
        .eq('creator_email', creatorEmail)
        .order('name', { ascending: true });

    if (isUuidString(projectIdUuid)) {
        query = query.eq('project_id', projectIdUuid);
    }

    const { data: rows, error } = await query;
    if (error) throw error;

    return (Array.isArray(rows) ? rows : [])
        .map((row, index) => buildMaintenanceSupplierFromCloudRow(row, index))
        .filter(item => item.name)
        .sort((a, b) => a.name.localeCompare(b.name));
}

async function insertMaintenanceSupplierInCloud(entry) {
    if (!isCloudReady()) {
        throw new Error('Cloud non pret: impossible d\'inserer maintenance_suppliers.');
    }

    const creatorEmail = getCurrentCloudUserEmail();
    if (!creatorEmail) {
        throw new Error('creator_email manquant: impossible d\'inserer maintenance_suppliers.');
    }

    let projectIdUuid = await resolveCloudProjectIdUuid();
    if (!projectIdUuid) {
        throw new Error('project_id introuvable pour maintenance_suppliers.');
    }
    projectIdUuid = await ensureCloudProjectRow(projectIdUuid);

    const basePayload = buildMaintenanceSupplierCloudPayload(entry, projectIdUuid, creatorEmail);
    const insertedPayload = await insertSupplierPayloadWithSchemaTolerance(basePayload);

    return buildMaintenanceSupplierFromCloudRow(insertedPayload, 0);
}

async function updateMaintenanceSupplierById(supplierId, updater, { refreshUi = true } = {}) {
    const targetId = String(supplierId || '');
    if (!targetId) return false;

    const supplierIndex = maintenanceSuppliers.findIndex(item => item.id === targetId);
    if (supplierIndex < 0) return false;

    const currentSupplier = sanitizeMaintenanceSupplier(maintenanceSuppliers[supplierIndex], supplierIndex);
    const patch = typeof updater === 'function' ? updater({ ...currentSupplier }) : updater;
    if (!patch || typeof patch !== 'object') return false;

    const mergedSupplier = {
        ...currentSupplier,
        ...patch,
        id: currentSupplier.id,
        updatedAt: new Date().toISOString()
    };

    const sanitizedSupplier = sanitizeMaintenanceSupplier(mergedSupplier, supplierIndex);

    if (!isCloudReady()) {
        throw new Error('Cloud non pret: impossible de mettre a jour maintenance_suppliers.');
    }

    const creatorEmail = getCurrentCloudUserEmail();
    if (!creatorEmail) {
        throw new Error('creator_email manquant: impossible de mettre a jour maintenance_suppliers.');
    }

    let projectIdUuid = await resolveCloudProjectIdUuid();
    if (!projectIdUuid) {
        throw new Error('project_id introuvable pour maintenance_suppliers.');
    }
    projectIdUuid = await ensureCloudProjectRow(projectIdUuid);

    const payload = buildMaintenanceSupplierCloudPayload(sanitizedSupplier, projectIdUuid, creatorEmail);
    await updateSupplierPayloadWithSchemaTolerance(
        targetId,
        projectIdUuid,
        creatorEmail,
        {
            name: payload.name,
            contact: payload.contact,
            emergency_phone: payload.emergency_phone,
            iban: payload.iban,
            note: payload.note,
            documents: payload.documents,
            creator_name: payload.creator_name,
            updated_at: payload.updated_at
        }
    );

    maintenanceSuppliers = maintenanceSuppliers
        .map((item, index) => (index === supplierIndex ? sanitizedSupplier : sanitizeMaintenanceSupplier(item, index)))
        .filter(item => item.name)
        .sort((a, b) => a.name.localeCompare(b.name));

    if (refreshUi) {
        renderMaintenanceSuppliers();
    }

    return true;
}

async function deleteMaintenanceSupplierById(supplierId) {
    const targetId = String(supplierId || '').trim();
    if (!targetId) {
        throw new Error('supplierId manquant: suppression impossible.');
    }

    if (!isCloudReady()) {
        throw new Error('Cloud non pret: impossible de supprimer maintenance_suppliers.');
    }

    const creatorEmail = getCurrentCloudUserEmail();
    if (!creatorEmail) {
        throw new Error('creator_email manquant: impossible de supprimer maintenance_suppliers.');
    }

    let projectIdUuid = await resolveCloudProjectIdUuid();
    if (!projectIdUuid) {
        throw new Error('project_id introuvable pour maintenance_suppliers.');
    }
    projectIdUuid = await ensureCloudProjectRow(projectIdUuid);

    const { error } = await cloudClient
        .from(CLOUD_MAINTENANCE_SUPPLIERS_TABLE)
        .delete()
        .eq('id', targetId)
        .eq('project_id', projectIdUuid)
        .eq('creator_email', creatorEmail);
    if (error) throw error;

    removeMaintenanceSupplierFromState(targetId, { refreshUi: true });
}

function renderMaintenanceExpenses() {
    const container = document.getElementById('maintenanceExpensesList');
    if (!container) return;

    if (!maintenanceExpenses.length) {
        container.innerHTML = `<div class="maintenance-legend-empty">${t('Aucune dépense enregistrée.', 'No hay gastos registrados.')}</div>`;
        if (activeMaintenanceSubtab === 'expenses') {
            renderMaintenanceExpenseDetailPanel();
        }
        return;
    }

    container.innerHTML = '';
    const headerRow = document.createElement('div');
    headerRow.className = 'maintenance-expense-list-header';
    headerRow.innerHTML =
        `<span>${t('Date', 'Fecha')}</span>` +
        `<span>${t('Fournisseur', 'Proveedor')}</span>` +
        `<span>${t('Statut', 'Estado')}</span>` +
        `<span class="maintenance-expense-col--amount">${t('Montant', 'Importe')}</span>`;
    container.appendChild(headerRow);

    maintenanceExpenses.forEach((expense, index) => {
        const row = document.createElement('div');
        row.className = 'maintenance-expense-row';
        if (expense.id === selectedMaintenanceExpenseId) {
            row.classList.add('maintenance-expense-row--active');
        }

        const summaryBtn = document.createElement('button');
        summaryBtn.type = 'button';
        summaryBtn.className = 'maintenance-expense-summary-btn';
        summaryBtn.innerHTML =
            `<span class="maintenance-expense-col">${escapeHtml(expense.date || '—')}</span>` +
            `<span class="maintenance-expense-col">${escapeHtml(expense.supplierName || t('Fournisseur non renseigné', 'Proveedor no indicado'))}</span>` +
            `<span class="maintenance-expense-col">${escapeHtml(({
                new: t('Nouvelle / à payer', 'Nueva / pendiente'),
                pending: t('À payer', 'Pendiente'),
                planned: t('À prévoir', 'A prever'),
                paid: t('Payé', 'Pagado')
            })[expense.paymentStatus] || expense.paymentStatus || '—')}</span>` +
            `<span class="maintenance-expense-col maintenance-expense-col--amount">${expense.totalAmount.toFixed(2)} ${escapeHtml(expense.currency)}</span>`;
        summaryBtn.addEventListener('click', () => {
            selectedMaintenanceExpenseId = expense.id;
            renderMaintenanceExpenses();
        });
        row.appendChild(summaryBtn);

        const actions = document.createElement('div');
        actions.className = 'maintenance-card-actions';
        const deleteBtn = document.createElement('button');
        deleteBtn.type = 'button';
        deleteBtn.className = 'maintenance-delete-btn';
        deleteBtn.textContent = t('Supprimer', 'Eliminar');
        deleteBtn.addEventListener('click', async event => {
            event.stopPropagation();
            await deleteMaintenanceExpenseById(expense.id);
        });
        actions.appendChild(deleteBtn);
        row.appendChild(actions);

        container.appendChild(row);
    });

    if (activeMaintenanceSubtab === 'expenses') {
        renderMaintenanceExpenseDetailPanel();
    }
}

function renderMaintenanceExpenseDetailPanel() {
    const detailPanel = document.getElementById('maintenanceExpenseDetailPanel');
    const title = document.getElementById('maintenanceInvoicePreviewTitle');
    const imagePreview = document.getElementById('maintenanceInvoicePreviewImage');
    const pdfPreview = document.getElementById('maintenanceInvoicePreviewPdfContainer');
    const placeholder = document.getElementById('maintenanceInvoicePreviewPlaceholder');
    if (!detailPanel || !title || !imagePreview || !pdfPreview || !placeholder) return;

    title.style.display = 'flex';
    title.textContent = t('Détail facture', 'Detalle factura');
    imagePreview.style.display = 'none';
    imagePreview.removeAttribute('src');
    pdfPreview.style.display = 'none';
    pdfPreview.innerHTML = '';
    placeholder.style.display = 'none';

    const selectedExpense = maintenanceExpenses.find(item => item.id === selectedMaintenanceExpenseId) || null;
    if (!selectedExpense) {
        detailPanel.style.display = 'flex';
        detailPanel.innerHTML = `<div class="maintenance-detail-empty">${t('Clique sur une facture dans la liste pour voir le détail.', 'Haz clic en una factura de la lista para ver el detalle.')}</div>`;
        return;
    }

    const paymentLabelMap = {
        new: t('Nouvelle / à payer', 'Nueva / pendiente'),
        pending: t('À payer', 'Pendiente'),
        planned: t('À prévoir', 'A prever'),
        paid: t('Payé', 'Pagado')
    };
    const linesText = selectedExpense.lines.map(line => {
        const details = [line.label];
        if (line.quantity != null) details.push(`x${line.quantity}`);
        if (line.unitPrice != null) details.push(`${line.unitPrice.toFixed(2)}`);
        if (line.total != null) details.push(`= ${line.total.toFixed(2)}`);
        return `<div class="maintenance-expense-line">• ${escapeHtml(details.join(' '))}</div>`;
    }).join('');
    const aiCommentHtml = escapeHtml(selectedExpense.aiComment || '').replace(/\n/g, '<br>');
    const invoiceDocuments = Array.isArray(selectedExpense.invoiceDocuments)
        ? selectedExpense.invoiceDocuments.filter(doc => String(doc?.dataUrl || ''))
        : [];

    detailPanel.style.display = 'block';
    detailPanel.innerHTML =
        `<div class="maintenance-detail-meta"><strong>${escapeHtml(selectedExpense.supplierName || t('Fournisseur non renseigné', 'Proveedor no indicado'))}</strong></div>` +
        `<div class="maintenance-detail-meta">${escapeHtml(selectedExpense.date)} · ${selectedExpense.totalAmount.toFixed(2)} ${escapeHtml(selectedExpense.currency)}</div>` +
        `<div class="maintenance-detail-meta">${t('Payeur', 'Pagador')}: ${escapeHtml(selectedExpense.payer)} · ${t('Paiement', 'Pago')}: ${escapeHtml(paymentLabelMap[selectedExpense.paymentStatus] || selectedExpense.paymentStatus)}</div>` +
        `<div class="maintenance-detail-meta">${t('IBAN', 'IBAN')}: ${escapeHtml(selectedExpense.supplierIban || '—')}</div>` +
        (selectedExpense.note ? `<div class="maintenance-detail-block"><strong>${t('Note', 'Nota')}</strong>: ${escapeHtml(selectedExpense.note)}</div>` : '') +
        (selectedExpense.aiComment ? `<div class="maintenance-detail-block"><strong>${t('Commentaire facture', 'Comentario factura')}</strong>:<br>${aiCommentHtml}</div>` : '') +
        (linesText ? `<div class="maintenance-detail-block"><strong>${t('Lignes', 'Líneas')}:</strong>${linesText}</div>` : '');

    const editSection = document.createElement('div');
    editSection.className = 'maintenance-detail-block';

    const editTitle = document.createElement('strong');
    editTitle.textContent = t('Mettre à jour la facture', 'Actualizar factura');
    editSection.appendChild(editTitle);

    const editStatusSelect = document.createElement('select');
    editStatusSelect.style.width = '100%';
    editStatusSelect.style.marginTop = '6px';
    editStatusSelect.style.boxSizing = 'border-box';
    [
        { value: 'new', label: t('Nouvelle / à payer', 'Nueva / pendiente') },
        { value: 'pending', label: t('À payer', 'Pendiente') },
        { value: 'planned', label: t('À prévoir', 'A prever') },
        { value: 'paid', label: t('Payé', 'Pagado') }
    ].forEach(item => {
        const option = document.createElement('option');
        option.value = item.value;
        option.textContent = item.label;
        editStatusSelect.appendChild(option);
    });
    editStatusSelect.value = ['new', 'pending', 'planned', 'paid'].includes(selectedExpense.paymentStatus)
        ? selectedExpense.paymentStatus
        : 'pending';
    editSection.appendChild(editStatusSelect);

    const editDateInput = document.createElement('input');
    editDateInput.type = 'date';
    editDateInput.value = String(selectedExpense.date || '');
    editDateInput.style.width = '100%';
    editDateInput.style.marginTop = '6px';
    editDateInput.style.boxSizing = 'border-box';
    editSection.appendChild(editDateInput);

    const editSupplierInput = document.createElement('input');
    editSupplierInput.type = 'text';
    editSupplierInput.value = String(selectedExpense.supplierName || '');
    editSupplierInput.placeholder = t('Fournisseur', 'Proveedor');
    editSupplierInput.style.width = '100%';
    editSupplierInput.style.marginTop = '6px';
    editSupplierInput.style.boxSizing = 'border-box';
    editSection.appendChild(editSupplierInput);

    const editAmountInput = document.createElement('input');
    editAmountInput.type = 'number';
    editAmountInput.step = '0.01';
    editAmountInput.min = '0';
    editAmountInput.value = Number.isFinite(selectedExpense.totalAmount) ? selectedExpense.totalAmount.toFixed(2) : '0.00';
    editAmountInput.style.width = '100%';
    editAmountInput.style.marginTop = '6px';
    editAmountInput.style.boxSizing = 'border-box';
    editSection.appendChild(editAmountInput);

    const editAiCommentInput = document.createElement('textarea');
    editAiCommentInput.rows = 4;
    editAiCommentInput.value = String(selectedExpense.aiComment || '');
    editAiCommentInput.placeholder = t('Commentaire facture', 'Comentario factura');
    editAiCommentInput.style.width = '100%';
    editAiCommentInput.style.marginTop = '6px';
    editAiCommentInput.style.boxSizing = 'border-box';
    editSection.appendChild(editAiCommentInput);

    const saveEditBtn = document.createElement('button');
    saveEditBtn.type = 'button';
    saveEditBtn.className = 'maintenance-delete-btn';
    saveEditBtn.style.marginTop = '6px';
    saveEditBtn.textContent = t('Mettre à jour', 'Actualizar');
    saveEditBtn.addEventListener('click', async () => {
        const nextAmount = toFiniteAmount(editAmountInput.value);
        await updateMaintenanceExpenseById(selectedExpense.id, {
            paymentStatus: ['new', 'pending', 'planned', 'paid'].includes(editStatusSelect.value) ? editStatusSelect.value : 'pending',
            date: String(editDateInput.value || selectedExpense.date || '').trim(),
            supplierName: String(editSupplierInput.value || '').trim(),
            totalAmount: nextAmount > 0 ? nextAmount : selectedExpense.totalAmount,
            aiComment: String(editAiCommentInput.value || '').trim()
        }, { refreshUi: false });
        renderMaintenanceExpenses();
    });
    editSection.appendChild(saveEditBtn);

    detailPanel.appendChild(editSection);

    const docsSection = document.createElement('div');
    docsSection.className = 'maintenance-detail-block';
    docsSection.innerHTML = `<strong>${t('Pièces du dossier', 'Piezas del expediente')}</strong>`;

    if (invoiceDocuments.length) {
        const docsList = document.createElement('div');
        docsList.className = 'maintenance-doc-list';
        invoiceDocuments.forEach((doc, docIndex) => {
            const docCard = document.createElement('div');
            docCard.className = 'maintenance-doc-item';

            const docName = document.createElement('div');
            docName.className = 'maintenance-doc-name';
            docName.textContent = String(doc.name || `${t('Document', 'Documento')} ${docIndex + 1}`);
            docCard.appendChild(docName);

            const mimeType = String(doc.mimeType || '').toLowerCase();
            const docDataUrl = String(doc.dataUrl || '');
            const docFileName = String(doc.name || '');
            const isImageDoc = mimeType.startsWith('image/');
            const isPdfDoc = mimeType.includes('pdf') || /\.pdf$/i.test(docFileName);

            const previewBlock = document.createElement('div');
            previewBlock.className = 'maintenance-doc-preview';
            if (isImageDoc && docDataUrl) {
                const imagePreviewEl = document.createElement('img');
                imagePreviewEl.className = 'maintenance-doc-preview-image';
                imagePreviewEl.src = docDataUrl;
                imagePreviewEl.alt = docFileName || `${t('Aperçu du document', 'Vista previa del documento')} ${docIndex + 1}`;
                previewBlock.appendChild(imagePreviewEl);
            } else if (isPdfDoc && docDataUrl) {
                const pdfPreviewFrame = document.createElement('iframe');
                pdfPreviewFrame.className = 'maintenance-doc-preview-pdf';
                pdfPreviewFrame.src = `${docDataUrl}#toolbar=0&navpanes=0&scrollbar=0`;
                pdfPreviewFrame.title = docFileName || `${t('Aperçu PDF', 'Vista previa PDF')} ${docIndex + 1}`;
                previewBlock.appendChild(pdfPreviewFrame);
            } else {
                const fallbackPreviewText = document.createElement('div');
                fallbackPreviewText.className = 'maintenance-doc-preview-fallback';
                fallbackPreviewText.textContent = t('Aperçu indisponible', 'Vista previa no disponible');
                previewBlock.appendChild(fallbackPreviewText);
            }
            docCard.appendChild(previewBlock);

            const docActions = document.createElement('div');
            docActions.className = 'maintenance-doc-actions';

            const openLink = document.createElement('a');
            openLink.href = String(doc.dataUrl || '');
            openLink.target = '_blank';
            openLink.rel = 'noopener';
            openLink.textContent = t('Ouvrir', 'Abrir');
            docActions.appendChild(openLink);

            const downloadLink = document.createElement('a');
            downloadLink.href = String(doc.dataUrl || '');
            downloadLink.target = '_blank';
            downloadLink.rel = 'noopener';
            downloadLink.download = String(doc.name || `${t('Document', 'Documento')} ${docIndex + 1}`);
            downloadLink.textContent = t('Télécharger', 'Descargar');
            docActions.appendChild(downloadLink);

            const deleteDocBtn = document.createElement('button');
            deleteDocBtn.type = 'button';
            deleteDocBtn.className = 'maintenance-doc-remove-btn';
            deleteDocBtn.textContent = t('Supprimer', 'Eliminar');
            deleteDocBtn.addEventListener('click', async () => {
                await updateMaintenanceExpenseById(selectedExpense.id, currentExpense => {
                    const currentDocs = Array.isArray(currentExpense.invoiceDocuments)
                        ? currentExpense.invoiceDocuments.filter(item => String(item?.dataUrl || ''))
                        : [];
                    const nextDocs = currentDocs.filter(item => item.id !== doc.id);
                    const firstDoc = nextDocs[0] || null;

                    return {
                        invoiceDocuments: nextDocs,
                        invoiceName: String(firstDoc?.name || ''),
                        invoiceDataUrl: String(firstDoc?.dataUrl || ''),
                        invoiceMimeType: String(firstDoc?.mimeType || ''),
                        invoiceSizeBytes: Math.max(0, Number(firstDoc?.sizeBytes || 0) || 0)
                    };
                });
            });
            docActions.appendChild(deleteDocBtn);

            docCard.appendChild(docActions);
            docsList.appendChild(docCard);
        });
        docsSection.appendChild(docsList);
    } else {
        const emptyText = document.createElement('div');
        emptyText.className = 'maintenance-legend-empty';
        emptyText.textContent = t('Aucune pièce dans ce dossier.', 'No hay piezas en este expediente.');
        docsSection.appendChild(emptyText);
    }

    const addDocsInput = document.createElement('input');
    addDocsInput.type = 'file';
    addDocsInput.multiple = true;
    addDocsInput.accept = 'image/*,.pdf';
    addDocsInput.style.marginTop = '8px';
    addDocsInput.style.width = '100%';
    docsSection.appendChild(addDocsInput);

    const addDocsBtn = document.createElement('button');
    addDocsBtn.type = 'button';
    addDocsBtn.className = 'maintenance-delete-btn';
    addDocsBtn.style.marginTop = '6px';
    addDocsBtn.textContent = t('Ajouter des fichiers au dossier', 'Añadir archivos al expediente');
    addDocsBtn.addEventListener('click', async () => {
        const files = Array.from(addDocsInput.files || []);
        if (!files.length) return;

        const newDocs = [];
        for (let fileIndex = 0; fileIndex < files.length; fileIndex++) {
            const file = files[fileIndex];
            try {
                const preparedDoc = await prepareDocumentForStorage(file);
                newDocs.push(sanitizeMaintenanceInvoiceDocument({
                    id: `expense-doc-${Date.now()}-${fileIndex}`,
                    name: file.name,
                    dataUrl: preparedDoc.dataUrl,
                    mimeType: preparedDoc.mimeType,
                    sizeBytes: preparedDoc.sizeBytes
                }, fileIndex));
            } catch (_error) {
                continue;
            }
        }

        if (!newDocs.length) return;

        await updateMaintenanceExpenseById(selectedExpense.id, currentExpense => {
            const currentDocs = Array.isArray(currentExpense.invoiceDocuments)
                ? currentExpense.invoiceDocuments.filter(item => String(item?.dataUrl || ''))
                : [];
            const nextDocs = [...currentDocs, ...newDocs];
            const firstDoc = nextDocs[0] || null;

            return {
                invoiceDocuments: nextDocs,
                invoiceName: String(firstDoc?.name || currentExpense.invoiceName || ''),
                invoiceDataUrl: String(firstDoc?.dataUrl || currentExpense.invoiceDataUrl || ''),
                invoiceMimeType: String(firstDoc?.mimeType || currentExpense.invoiceMimeType || ''),
                invoiceSizeBytes: Math.max(0, Number(firstDoc?.sizeBytes || currentExpense.invoiceSizeBytes || 0) || 0)
            };
        });

        addDocsInput.value = '';
    });
    docsSection.appendChild(addDocsBtn);
    detailPanel.appendChild(docsSection);

    const postCommentLabel = document.createElement('div');
    postCommentLabel.className = 'maintenance-detail-block';
    postCommentLabel.innerHTML = `<strong>${t('Commentaire suivi', 'Comentario de seguimiento')}</strong>`;
    detailPanel.appendChild(postCommentLabel);

    const postCommentInput = document.createElement('textarea');
    postCommentInput.rows = 3;
    postCommentInput.value = String(selectedExpense.postComment || '');
    postCommentInput.placeholder = t('Ajoute un commentaire après enregistrement', 'Añade un comentario después del registro');
    postCommentInput.style.width = '100%';
    postCommentInput.style.marginTop = '6px';
    postCommentInput.style.boxSizing = 'border-box';
    detailPanel.appendChild(postCommentInput);

    const savePostCommentBtn = document.createElement('button');
    savePostCommentBtn.type = 'button';
    savePostCommentBtn.className = 'maintenance-delete-btn';
    savePostCommentBtn.style.marginTop = '6px';
    savePostCommentBtn.textContent = t('Enregistrer commentaire suivi', 'Guardar comentario de seguimiento');
    savePostCommentBtn.addEventListener('click', async () => {
        await updateMaintenanceExpenseById(selectedExpense.id, {
            postComment: String(postCommentInput.value || '').trim()
        }, { refreshUi: false });
        renderMaintenanceExpenseDetailPanel();
    });
    detailPanel.appendChild(savePostCommentBtn);
}

function renderMaintenanceSuppliers() {
    const container = document.getElementById('maintenanceSuppliersList');
    if (!container) return;

    if (!maintenanceSuppliers.length) {
        container.innerHTML = `<div class="maintenance-legend-empty">${t('Aucun fournisseur enregistré.', 'Ningún proveedor registrado.')}</div>`;
        if (activeMaintenanceSubtab === 'suppliers') {
            renderMaintenanceSupplierDetailPanel();
        }
        return;
    }

    container.innerHTML = '';
    const headerRow = document.createElement('div');
    headerRow.className = 'maintenance-supplier-list-header';
    headerRow.innerHTML =
        `<span>${t('Nom', 'Nombre')}</span>` +
        `<span>${t('Contact', 'Contacto')}</span>` +
        `<span>${t('Urgence', 'Urgencia')}</span>`;
    container.appendChild(headerRow);

    maintenanceSuppliers.forEach((supplier, index) => {
        const card = document.createElement('div');
        card.className = 'maintenance-supplier-row';
        if (supplier.id === selectedMaintenanceSupplierId) {
            card.classList.add('maintenance-supplier-row--active');
        }

        const summaryBtn = document.createElement('button');
        summaryBtn.type = 'button';
        summaryBtn.className = 'maintenance-supplier-summary-btn';
        summaryBtn.innerHTML =
            `<span class="maintenance-expense-col">${escapeHtml(supplier.name || `${t('Fournisseur', 'Proveedor')} ${index + 1}`)}</span>` +
            `<span class="maintenance-expense-col">${escapeHtml(supplier.contact || '—')}</span>` +
            `<span class="maintenance-expense-col">${escapeHtml(supplier.emergencyPhone || '—')}</span>`;
        summaryBtn.addEventListener('click', () => {
            clearMaintenanceSupplierDraft();
            selectedMaintenanceSupplierId = supplier.id;
            renderMaintenanceSuppliers();
        });
        card.appendChild(summaryBtn);

        const actions = document.createElement('div');
        actions.className = 'maintenance-card-actions';
        const deleteBtn = document.createElement('button');
        deleteBtn.type = 'button';
        deleteBtn.className = 'maintenance-delete-btn';
        deleteBtn.textContent = t('Supprimer', 'Eliminar');
        deleteBtn.addEventListener('click', async () => {
            await deleteMaintenanceSupplierById(supplier.id);
        });
        actions.appendChild(deleteBtn);
        card.appendChild(actions);

        container.appendChild(card);
    });

    if (activeMaintenanceSubtab === 'suppliers') {
        renderMaintenanceSupplierDetailPanel();
    }
}

function renderMaintenanceSupplierDetailPanel() {
    const detailPanel = document.getElementById('maintenanceExpenseDetailPanel');
    const title = document.getElementById('maintenanceInvoicePreviewTitle');
    const imagePreview = document.getElementById('maintenanceInvoicePreviewImage');
    const pdfPreview = document.getElementById('maintenanceInvoicePreviewPdfContainer');
    const placeholder = document.getElementById('maintenanceInvoicePreviewPlaceholder');
    if (!detailPanel || !title || !imagePreview || !pdfPreview || !placeholder) return;

    title.style.display = 'flex';
    title.textContent = t('Détail fournisseur', 'Detalle proveedor');
    imagePreview.style.display = 'none';
    imagePreview.removeAttribute('src');
    pdfPreview.style.display = 'none';
    pdfPreview.innerHTML = '';
    placeholder.style.display = 'none';

    const selectedSupplier = maintenanceSuppliers.find(item => item.id === selectedMaintenanceSupplierId) || null;
    const draftKey = selectedSupplier?.id || '__new__';
    const draftValues = maintenanceSupplierFormDraft?.key === draftKey
        ? maintenanceSupplierFormDraft.values
        : null;

    detailPanel.dataset.detailMode = 'supplier';
    detailPanel.style.display = 'block';
    detailPanel.innerHTML = '';

    const modeLabel = document.createElement('div');
    modeLabel.className = 'maintenance-detail-meta';
    modeLabel.innerHTML = `<strong>${selectedSupplier ? t('Modification fournisseur', 'Edición proveedor') : t('Nouveau fournisseur', 'Nuevo proveedor')}</strong>`;
    detailPanel.appendChild(modeLabel);

    const nameInput = document.createElement('input');
    nameInput.type = 'text';
    nameInput.placeholder = t('Nom fournisseur', 'Nombre proveedor');
    nameInput.value = String(draftValues?.name ?? selectedSupplier?.name ?? '');
    nameInput.style.width = '100%';
    nameInput.style.marginTop = '6px';
    nameInput.style.boxSizing = 'border-box';
    detailPanel.appendChild(nameInput);

    const contactInput = document.createElement('input');
    contactInput.type = 'text';
    contactInput.placeholder = t('Contact', 'Contacto');
    contactInput.value = String(draftValues?.contact ?? selectedSupplier?.contact ?? '');
    contactInput.style.width = '100%';
    contactInput.style.marginTop = '6px';
    contactInput.style.boxSizing = 'border-box';
    detailPanel.appendChild(contactInput);

    const phoneInput = document.createElement('input');
    phoneInput.type = 'text';
    phoneInput.placeholder = t('Téléphone urgence', 'Teléfono urgencia');
    phoneInput.value = String(draftValues?.emergencyPhone ?? selectedSupplier?.emergencyPhone ?? '');
    phoneInput.style.width = '100%';
    phoneInput.style.marginTop = '6px';
    phoneInput.style.boxSizing = 'border-box';
    detailPanel.appendChild(phoneInput);

    const ibanInput = document.createElement('input');
    ibanInput.type = 'text';
    ibanInput.placeholder = 'IBAN';
    ibanInput.value = String(draftValues?.iban ?? selectedSupplier?.iban ?? '');
    ibanInput.style.width = '100%';
    ibanInput.style.marginTop = '6px';
    ibanInput.style.boxSizing = 'border-box';
    detailPanel.appendChild(ibanInput);

    const noteInput = document.createElement('textarea');
    noteInput.rows = 2;
    noteInput.placeholder = t('Note', 'Nota');
    noteInput.value = String(draftValues?.note ?? selectedSupplier?.note ?? '');
    noteInput.style.width = '100%';
    noteInput.style.marginTop = '6px';
    noteInput.style.boxSizing = 'border-box';
    detailPanel.appendChild(noteInput);

    const attachInput = document.createElement('input');
    attachInput.type = 'file';
    attachInput.multiple = true;
    attachInput.accept = 'image/*,.pdf';
    attachInput.style.width = '100%';
    attachInput.style.marginTop = '6px';
    attachInput.style.boxSizing = 'border-box';
    detailPanel.appendChild(attachInput);

    const syncSupplierDraft = () => {
        maintenanceSupplierFormDraft = {
            key: draftKey,
            values: {
                name: String(nameInput.value || ''),
                contact: String(contactInput.value || ''),
                emergencyPhone: String(phoneInput.value || ''),
                iban: String(ibanInput.value || ''),
                note: String(noteInput.value || '')
            }
        };
    };

    [nameInput, contactInput, phoneInput, ibanInput, noteInput].forEach(input => {
        input.addEventListener('input', syncSupplierDraft);
    });

    const actionRow = document.createElement('div');
    actionRow.className = 'button-row';

    const addBtn = document.createElement('button');
    addBtn.type = 'button';
    addBtn.style.flex = '1';
    addBtn.textContent = t('Ajouter fournisseur', 'Añadir proveedor');
    addBtn.addEventListener('click', async () => {
        const name = String(nameInput.value || '').trim();
        if (!name) return;

        const files = Array.from(attachInput.files || []);
        const documents = [];
        for (let index = 0; index < files.length; index++) {
            const file = files[index];
            const preparedDoc = await prepareDocumentForStorage(file);
            documents.push(sanitizeMaintenanceInvoiceDocument({
                id: `supplier-doc-${Date.now()}-${index}`,
                name: file.name,
                dataUrl: preparedDoc.dataUrl,
                mimeType: preparedDoc.mimeType,
                sizeBytes: preparedDoc.sizeBytes
            }, index));
        }

        const entry = sanitizeMaintenanceSupplier({
            id: generateClientUuid(),
            name,
            contact: contactInput.value,
            emergencyPhone: phoneInput.value,
            iban: ibanInput.value,
            note: noteInput.value,
            documents,
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString()
        }, maintenanceSuppliers.length);

        const inserted = await insertMaintenanceSupplierInCloud(entry);
        clearMaintenanceSupplierDraft();
        selectedMaintenanceSupplierId = inserted.id;
        setMaintenanceSuppliers([...maintenanceSuppliers, inserted], { refreshUi: true });
    });
    actionRow.appendChild(addBtn);

    const updateBtn = document.createElement('button');
    updateBtn.type = 'button';
    updateBtn.style.flex = '1';
    updateBtn.textContent = t('Mettre à jour fournisseur', 'Actualizar proveedor');
    updateBtn.disabled = !selectedSupplier;
    updateBtn.addEventListener('click', async () => {
        if (!selectedSupplier) return;
        const name = String(nameInput.value || '').trim();
        if (!name) return;

        const newDocuments = [];
        const files = Array.from(attachInput.files || []);
        for (let index = 0; index < files.length; index++) {
            const file = files[index];
            const preparedDoc = await prepareDocumentForStorage(file);
            newDocuments.push(sanitizeMaintenanceInvoiceDocument({
                id: `supplier-doc-${Date.now()}-${index}`,
                name: file.name,
                dataUrl: preparedDoc.dataUrl,
                mimeType: preparedDoc.mimeType,
                sizeBytes: preparedDoc.sizeBytes
            }, index));
        }

        await updateMaintenanceSupplierById(selectedSupplier.id, current => ({
            name,
            contact: String(contactInput.value || '').trim(),
            emergencyPhone: String(phoneInput.value || '').trim(),
            iban: String(ibanInput.value || '').trim(),
            note: String(noteInput.value || '').trim(),
            documents: [...(Array.isArray(current.documents) ? current.documents : []), ...newDocuments]
        }));
        clearMaintenanceSupplierDraft();
    });
    actionRow.appendChild(updateBtn);
    detailPanel.appendChild(actionRow);

    const documents = Array.isArray(selectedSupplier?.documents)
        ? selectedSupplier.documents.filter(item => String(item?.dataUrl || ''))
        : [];

    const docsSection = document.createElement('div');
    docsSection.className = 'maintenance-detail-block';
    docsSection.innerHTML = `<strong>${t('Pièces jointes fournisseur', 'Adjuntos proveedor')}</strong>`;

    if (!documents.length) {
        const emptyText = document.createElement('div');
        emptyText.className = 'maintenance-legend-empty';
        emptyText.textContent = t('Aucune pièce jointe.', 'Sin adjuntos.');
        docsSection.appendChild(emptyText);
    } else {
        const docsList = document.createElement('div');
        docsList.className = 'maintenance-doc-list';
        documents.forEach((doc, docIndex) => {
            const docCard = document.createElement('div');
            docCard.className = 'maintenance-doc-item';

            const docName = document.createElement('div');
            docName.className = 'maintenance-doc-name';
            docName.textContent = String(doc.name || `${t('Document', 'Documento')} ${docIndex + 1}`);
            docCard.appendChild(docName);

            const docActions = document.createElement('div');
            docActions.className = 'maintenance-doc-actions';

            const openLink = document.createElement('a');
            openLink.href = String(doc.dataUrl || '');
            openLink.target = '_blank';
            openLink.rel = 'noopener';
            openLink.textContent = t('Ouvrir', 'Abrir');
            docActions.appendChild(openLink);

            const downloadLink = document.createElement('a');
            downloadLink.href = String(doc.dataUrl || '');
            downloadLink.target = '_blank';
            downloadLink.rel = 'noopener';
            downloadLink.download = String(doc.name || `${t('Document', 'Documento')} ${docIndex + 1}`);
            downloadLink.textContent = t('Télécharger', 'Descargar');
            docActions.appendChild(downloadLink);

            const deleteDocBtn = document.createElement('button');
            deleteDocBtn.type = 'button';
            deleteDocBtn.className = 'maintenance-doc-remove-btn';
            deleteDocBtn.textContent = t('Supprimer', 'Eliminar');
            deleteDocBtn.disabled = !selectedSupplier;
            deleteDocBtn.addEventListener('click', async () => {
                if (!selectedSupplier) return;
                await updateMaintenanceSupplierById(selectedSupplier.id, current => ({
                    documents: (Array.isArray(current.documents) ? current.documents : []).filter(item => item.id !== doc.id)
                }));
            });
            docActions.appendChild(deleteDocBtn);

            docCard.appendChild(docActions);
            docsList.appendChild(docCard);
        });
        docsSection.appendChild(docsList);
    }

    detailPanel.appendChild(docsSection);
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
        creatorEmail: normalizeEmailForCompare(board?.creatorEmail || ''),
        creatorName: String(board?.creatorName || '').trim(),
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
            .then(() => setCloudStatus(`Cloud synchronisé · ${getSavedRoutes().length} route(s) + ${waypointPhotoEntries.length} photo(s) + ${maintenanceBoards.length} schéma(s)`));
    }

    if (refreshUi) {
        renderMaintenanceBoard();
    }
}

function persistMaintenanceBoards({ syncCloud = true } = {}) {
    saveArrayToStorage(MAINTENANCE_BOARDS_STORAGE_KEY, maintenanceBoards);

    if (syncCloud && isCloudReady()) {
        pushRoutesToCloud()
            .then(() => setCloudStatus(`Cloud synchronisé · ${getSavedRoutes().length} route(s) + ${waypointPhotoEntries.length} photo(s) + ${maintenanceBoards.length} schéma(s)`));
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
    activeMaintenanceSubtab = ['tasks', 'expenses', 'suppliers', 'engine'].includes(tabKey) ? tabKey : 'tasks';

    const tabBtnMap = {
        tasks: document.getElementById('maintenanceTasksSubtabBtn'),
        expenses: document.getElementById('maintenanceExpensesSubtabBtn'),
        suppliers: document.getElementById('maintenanceSuppliersSubtabBtn'),
        engine: document.getElementById('maintenanceEngineSubtabBtn')
    };
    const panelMap = {
        tasks: document.getElementById('maintenanceTasksPanel'),
        expenses: document.getElementById('maintenanceExpensesPanel'),
        suppliers: document.getElementById('maintenanceSuppliersPanel'),
        engine: document.getElementById('engineTab')
    };

    Object.entries(tabBtnMap).forEach(([key, node]) => {
        if (!node) return;
        node.classList.toggle('active', key === activeMaintenanceSubtab);
    });

    Object.entries(panelMap).forEach(([key, node]) => {
        if (!node) return;
        node.classList.toggle('active', key === activeMaintenanceSubtab);
    });

    if (activeMaintenanceSubtab === 'expenses') {
        setActiveMaintenanceExpensesView('list');
    }

    window.dispatchEvent(new CustomEvent('ceibo:maintenance-subtab-changed'));
}

function setActiveMaintenanceExpensesView(viewKey) {
    activeMaintenanceExpensesView = ['list', 'add'].includes(viewKey) ? viewKey : 'list';

    const listBtn = document.getElementById('maintenanceExpenseListTabBtn');
    const addBtn = document.getElementById('maintenanceExpenseAddTabBtn');
    const listPanel = document.getElementById('maintenanceExpenseListPanel');
    const addPanel = document.getElementById('maintenanceExpenseAddPanel');

    if (listBtn) listBtn.classList.toggle('active', activeMaintenanceExpensesView === 'list');
    if (addBtn) addBtn.classList.toggle('active', activeMaintenanceExpensesView === 'add');
    if (listPanel) listPanel.classList.toggle('active', activeMaintenanceExpensesView === 'list');
    if (addPanel) addPanel.classList.toggle('active', activeMaintenanceExpensesView === 'add');
}

function setActiveRoutesSubtab(tabKey) {
    activeRoutesSubtab = ['manage', 'importexport', 'tools'].includes(tabKey) ? tabKey : 'manage';

    const tabBtnMap = {
        manage: document.getElementById('routesManageSubtabBtn'),
        importexport: document.getElementById('routesImportExportSubtabBtn'),
        tools: document.getElementById('routesToolsSubtabBtn')
    };
    const panelMap = {
        manage: document.getElementById('routesManagePanel'),
        importexport: document.getElementById('routesImportExportPanel'),
        tools: document.getElementById('routesToolsPanel')
    };

    Object.entries(tabBtnMap).forEach(([key, node]) => {
        if (!node) return;
        node.classList.toggle('active', key === activeRoutesSubtab);
    });

    Object.entries(panelMap).forEach(([key, node]) => {
        if (!node) return;
        node.classList.toggle('active', key === activeRoutesSubtab);
    });
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
    const engineSubtabBtn = document.getElementById('maintenanceEngineSubtabBtn');
    const expenseListTabBtn = document.getElementById('maintenanceExpenseListTabBtn');
    const expenseAddTabBtn = document.getElementById('maintenanceExpenseAddTabBtn');
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
    const invoicePreviewImage = document.getElementById('maintenanceInvoicePreviewImage');
    const invoicePreviewPdfContainer = document.getElementById('maintenanceInvoicePreviewPdfContainer');
    const invoicePreviewPlaceholder = document.getElementById('maintenanceInvoicePreviewPlaceholder');
    const invoicePreviewTitle = document.getElementById('maintenanceInvoicePreviewTitle');
    const maintenanceExpenseDetailPanel = document.getElementById('maintenanceExpenseDetailPanel');
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
    const supplierNewBtn = document.getElementById('maintenanceSupplierNewBtn');

    if (!tasksSubtabBtn || !expensesSubtabBtn || !suppliersSubtabBtn || !schemaNameInput || !schemaInput || !addBtn || !deleteBtn || !schemaSelect || !toggleSchemaManagerBtn || !pinColorInput || !taskStatusInput || !legendInput || !canvas || !image || !invoiceInput || !scanInvoiceBtn || !invoiceScanStatus || !expenseDateInput || !expenseTotalInput || !expenseCurrencyInput || !expensePayerSelect || !expensePaymentStatusSelect || !expenseSupplierInput || !expenseSupplierIbanInput || !expenseLinesInput || !expenseNoteInput || !expenseAiCommentInput || !addExpenseBtn) return;

    const applyClipboardTextToTarget = (text) => {
        const value = String(text || '').trim();
        if (!value) return false;

        const target = String(manualPasteTargetSelect?.value || 'expenseSupplier');
        if (target === 'expenseSupplier') {
            expenseSupplierInput.value = value;
            return true;
        }
        if (target === 'expenseIban') {
            expenseSupplierIbanInput.value = normalizeIbanValue(value) || value;
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
        if (maintenanceExpenseDetailPanel) {
            maintenanceExpenseDetailPanel.style.display = 'none';
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
            renderMaintenanceExpenseDetailPanel();
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
        if (file) {
            selectedMaintenanceExpenseId = null;
            renderMaintenanceExpenses();
        }
        updateInvoicePreview(file);
        if (invoiceReviewPanel) {
            invoiceReviewPanel.style.display = file ? 'block' : 'none';
        }
    });

    const applySupplierSelection = (name) => {
        const selectedName = String(name || '').trim();
        if (!selectedName) return;
        expenseSupplierInput.value = selectedName;

        const existingSupplier = findMaintenanceSupplierByName(selectedName);
        if (existingSupplier) {
            if (existingSupplier.iban) {
                expenseSupplierIbanInput.value = existingSupplier.iban;
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
    setActiveMaintenanceExpensesView('list');

    loadMaintenanceBoards();
    loadMaintenanceExpenses();
    loadMaintenanceSuppliers();
    renderMaintenanceBoard();
    renderMaintenanceExpenses();
    renderMaintenanceSuppliers();

    tasksSubtabBtn.addEventListener('click', () => setActiveMaintenanceSubtab('tasks'));
    expensesSubtabBtn.addEventListener('click', () => setActiveMaintenanceSubtab('expenses'));
    suppliersSubtabBtn.addEventListener('click', () => setActiveMaintenanceSubtab('suppliers'));
    if (engineSubtabBtn) {
        engineSubtabBtn.addEventListener('click', () => setActiveMaintenanceSubtab('engine'));
    }
    if (expenseListTabBtn) {
        expenseListTabBtn.addEventListener('click', () => setActiveMaintenanceExpensesView('list'));
    }
    if (expenseAddTabBtn) {
        expenseAddTabBtn.addEventListener('click', () => setActiveMaintenanceExpensesView('add'));
    }

    toggleSchemaManagerBtn.addEventListener('click', () => {
        setMaintenanceSchemaManagerVisibility(!maintenanceSchemaManagerVisible);
    });

    schemaSelect.addEventListener('change', () => {
        selectedMaintenanceBoardId = String(schemaSelect.value || '');
        activeMaintenanceAnnotationId = null;
        renderMaintenanceBoard();
    });

    addBtn.addEventListener('click', async () => {
        const file = schemaInput.files?.[0];
        if (!file || !file.type.startsWith('image/')) {
            setMaintenanceStatus(t('Choisis une image valide pour ajouter un schéma.', 'Elige una imagen válida para añadir un esquema.'), true);
            return;
        }

        try {
            const imageDataUrl = await imageFileToCompressedDataUrl(file, 1400, 0.62);
            const boardName = String(schemaNameInput.value || '').trim() || `${t('Schéma', 'Esquema')} ${maintenanceBoards.length + 1}`;
            const newBoard = sanitizeMaintenanceBoard({
                id: `maintenance-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
                name: boardName,
                imageDataUrl,
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
        } catch (_error) {
            setMaintenanceStatus(t('Impossible de lire/comprimer cette image.', 'No se puede leer/comprimir esta imagen.'), true);
        }
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
                } else {
                    expenseSupplierIbanInput.value = iban;
                }
            }
            if (Number.isFinite(total) && total > 0) {
                expenseTotalInput.value = total.toFixed(2);
            }

            if (hasReliableSupplier && matchedSupplier) {
                if (!iban && matchedSupplier.iban) {
                    expenseSupplierIbanInput.value = matchedSupplier.iban;
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

    addExpenseBtn.addEventListener('click', async () => {
        const totalAmount = toFiniteAmount(expenseTotalInput.value);
        if (!Number.isFinite(totalAmount) || totalAmount <= 0) {
            invoiceScanStatus.textContent = t('Renseigne un montant total valide.', 'Introduce un importe total válido.');
            return;
        }

        const invoiceFiles = Array.from(invoiceInput.files || []);
        const invoiceDocuments = [];

        for (let fileIndex = 0; fileIndex < invoiceFiles.length; fileIndex++) {
            const file = invoiceFiles[fileIndex];
            try {
                const preparedDoc = await prepareDocumentForStorage(file);
                invoiceDocuments.push(sanitizeMaintenanceInvoiceDocument({
                    id: `expense-doc-${Date.now()}-${fileIndex}`,
                    name: file.name,
                    dataUrl: preparedDoc.dataUrl,
                    mimeType: preparedDoc.mimeType,
                    sizeBytes: preparedDoc.sizeBytes
                }, fileIndex));
            } catch (_error) {
                invoiceScanStatus.textContent = `${t('Impossible de lire le document', 'No se puede leer el documento')}: ${file.name}`;
                return;
            }
        }

        const firstInvoiceDocument = invoiceDocuments[0] || null;

        const lines = parseExpenseLinesText(expenseLinesInput.value);
        const entry = sanitizeMaintenanceExpense({
            id: generateClientUuid(),
            invoiceName: firstInvoiceDocument?.name || '',
            invoiceDataUrl: firstInvoiceDocument?.dataUrl || '',
            invoiceMimeType: firstInvoiceDocument?.mimeType || '',
            invoiceSizeBytes: firstInvoiceDocument?.sizeBytes || 0,
            invoiceDocuments,
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

        const insertedExpense = await insertMaintenanceExpenseInCloud(entry);
        selectedMaintenanceExpenseId = insertedExpense.id;
        setMaintenanceExpenses([insertedExpense, ...maintenanceExpenses], { refreshUi: true });
        setActiveMaintenanceExpensesView('list');

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
        if (invoiceDocuments.length > 1) {
            invoiceScanStatus.textContent = t(
                `Dépense ajoutée. ${invoiceDocuments.length} documents archivés.`,
                `Gasto añadido. ${invoiceDocuments.length} documentos archivados.`
            );
        } else if (invoiceDocuments.length === 1) {
            invoiceScanStatus.textContent = t('Dépense ajoutée. Facture archivée avec succès.', 'Gasto añadido. Factura archivada con éxito.');
        } else {
            invoiceScanStatus.textContent = t('Dépense ajoutée.', 'Gasto añadido.');
        }
    });

    if (supplierNewBtn) {
        supplierNewBtn.addEventListener('click', () => {
            clearMaintenanceSupplierDraft();
            selectedMaintenanceSupplierId = null;
            renderMaintenanceSuppliers();
        });
    }
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
        const displayName = String(cloudUserProfile?.name || '').trim();
        const role = normalizeCloudUserRole(cloudUserProfile?.role);
        const roleLabel = role === 'administrateur'
            ? t('administrateur', 'administrador')
            : t('utilisateur', 'usuario');
        const nameChunk = displayName ? ` · ${displayName}` : '';
        setCloudAuthStatus(t(`Utilisateur: connecté (${label}${nameChunk}) · rôle: ${roleLabel}`, `Usuario: conectado (${label}${nameChunk}) · rol: ${roleLabel}`));
    } else {
        setCloudAuthStatus(t('Utilisateur: non connecté', 'Usuario: no conectado'));
    }

    // Keep sign-in/up clickable to allow recovering from a failed cloud bootstrap.
    if (emailSignInBtn) emailSignInBtn.disabled = !!cloudAuthUser;
    if (emailSignUpBtn) emailSignUpBtn.disabled = !!cloudAuthUser;
    if (signOutBtn) signOutBtn.disabled = !cloudAuthUser;

    const locked = isAuthGateLocked();
    const cloudRefreshBtn = document.getElementById('cloudRefreshBtn');
    const cloudAccountSubtabBtn = document.getElementById('cloudAccountSubtabBtn');
    const cloudPasswordSubtabBtn = document.getElementById('cloudPasswordSubtabBtn');
    const cloudStatsSubtabBtn = document.getElementById('cloudStatsSubtabBtn');
    const cloudUsersSubtabBtn = document.getElementById('cloudUsersSubtabBtn');
    const cloudChangePasswordBtn = document.getElementById('cloudChangePasswordBtn');
    const cloudAdminCreateUserBtn = document.getElementById('cloudAdminCreateUserBtn');

    const setVisible = (node, visible) => {
        if (!node) return;
        node.style.display = visible ? '' : 'none';
    };

    if (cloudRefreshBtn) cloudRefreshBtn.disabled = locked;
    if (cloudAccountSubtabBtn) cloudAccountSubtabBtn.disabled = false;
    if (cloudPasswordSubtabBtn) cloudPasswordSubtabBtn.disabled = locked;
    if (cloudStatsSubtabBtn) cloudStatsSubtabBtn.disabled = locked;
    if (cloudUsersSubtabBtn) cloudUsersSubtabBtn.disabled = locked;
    if (cloudChangePasswordBtn) cloudChangePasswordBtn.disabled = locked || !cloudAuthUser;
    if (cloudAdminCreateUserBtn) cloudAdminCreateUserBtn.disabled = locked || !isCloudAdmin();

    if (locked && activeCloudSubtab !== 'account') {
        setActiveCloudSubtab('account');
    }

    // Non logge: only account/login-signup area is available in Cloud.
    setVisible(cloudRefreshBtn, !locked);
    setVisible(signOutBtn, !locked);
    setVisible(cloudPasswordSubtabBtn, !locked);
    setVisible(cloudStatsSubtabBtn, !locked);
    setVisible(cloudUsersSubtabBtn, !locked);

    renderCloudUsersList();
    renderCloudStatsTable();
}

async function loadCloudUserProfile() {
    const email = getCurrentCloudUserEmail();
    if (!cloudClient || !email) {
        cloudUserProfile = null;
        return null;
    }

    try {
        const { data, error } = await cloudClient
            .from(CLOUD_ALLOWED_USERS_TABLE)
            .select('*')
            .eq('email', email)
            .limit(1)
            .maybeSingle();

        if (error) {
            const code = String(error?.code || '');
            const message = String(error?.message || '').toLowerCase();
            if (code === '42P01' || message.includes('does not exist')) {
                cloudUserProfile = {
                    email,
                    name: getCurrentCloudUserDisplayName(),
                    role: isForcedCloudAdminEmail(email) ? 'administrateur' : 'utilisateur'
                };
                return cloudUserProfile;
            }
            cloudUserProfile = {
                email,
                name: getCurrentCloudUserDisplayName(),
                role: isForcedCloudAdminEmail(email) ? 'administrateur' : 'utilisateur'
            };
            return cloudUserProfile;
        }

        const role = isForcedCloudAdminEmail(email)
            ? 'administrateur'
            : normalizeCloudUserRole(data?.role);
        const name = String(data?.name || cloudAuthUser?.user_metadata?.full_name || '').trim();

        cloudUserProfile = {
            email,
            name,
            role
        };

        return cloudUserProfile;
    } catch (_error) {
        cloudUserProfile = {
            email,
            name: getCurrentCloudUserDisplayName(),
            role: isForcedCloudAdminEmail(email) ? 'administrateur' : 'utilisateur'
        };
        return cloudUserProfile;
    }
}

async function fetchCloudManagedUsers() {
    if (!cloudClient) {
        cloudManagedUsers = [];
        return [];
    }

    try {
        const { data, error } = await cloudClient
            .from(CLOUD_ALLOWED_USERS_TABLE)
            .select('*')
            .order('email', { ascending: true });

        if (error) throw error;

        cloudManagedUsers = (Array.isArray(data) ? data : [])
            .map((row, index) => sanitizeManagedCloudUser(row, index))
            .filter(row => row.email)
            .sort((a, b) => a.email.localeCompare(b.email));
    } catch (error) {
        const code = String(error?.code || '');
        const message = String(error?.message || '').toLowerCase();
        if (code === '42P01' || message.includes('does not exist')) {
            cloudManagedUsers = [];
            setCloudUserManagementStatus(
                t(`Table ${CLOUD_ALLOWED_USERS_TABLE} introuvable.`, `Tabla ${CLOUD_ALLOWED_USERS_TABLE} no encontrada.`),
                true
            );
            return [];
        }

        setCloudUserManagementStatus(
            t(`Chargement utilisateurs impossible: ${formatCloudError(error)}`, `Carga usuarios imposible: ${formatCloudError(error)}`),
            true
        );
        return cloudManagedUsers;
    }

    return cloudManagedUsers;
}

function renderCloudUsersList() {
    const container = document.getElementById('cloudUsersList');
    const createBtn = document.getElementById('cloudAdminCreateUserBtn');
    if (!container) return;

    const canManage = !!cloudClient && !!cloudAuthUser && isCloudAdmin();

    if (createBtn) createBtn.disabled = !canManage;

    if (!cloudAuthUser) {
        container.innerHTML = `<div class="arrival-list__item">${t('Connecte-toi pour voir les utilisateurs.', 'Inicia sesión para ver usuarios.')}</div>`;
        return;
    }

    if (!canManage) {
        container.innerHTML = `<div class="arrival-list__item">${t('Accès réservé aux administrateurs.', 'Acceso reservado a administradores.')}</div>`;
        return;
    }

    if (!cloudManagedUsers.length) {
        container.innerHTML = `<div class="arrival-list__item">${t('Aucun utilisateur autorisé.', 'No hay usuarios autorizados.')}</div>`;
        return;
    }

    container.innerHTML = cloudManagedUsers.map((user, index) => {
        const safeName = escapeHtml(user.name || '');
        const safeEmail = escapeHtml(user.email);
        const safeRole = escapeHtml(user.role);
        return `<div class="cloud-user-row" data-user-index="${index}">
            <div class="cloud-user-row__line"><strong>${safeEmail}</strong></div>
            <div class="cloud-user-row__line">
                <input type="text" class="cloud-user-name-input" value="${safeName}" placeholder="${escapeHtml(t('Nom', 'Nombre'))}">
                <select class="cloud-user-role-input">
                    <option value="utilisateur" ${user.role === 'utilisateur' ? 'selected' : ''}>${t('utilisateur', 'usuario')}</option>
                    <option value="administrateur" ${user.role === 'administrateur' ? 'selected' : ''}>${t('administrateur', 'administrador')}</option>
                </select>
            </div>
            <div class="button-row">
                <button type="button" class="cloud-user-save-btn" style="flex:1;">${t('Mettre à jour', 'Actualizar')}</button>
                <button type="button" class="cloud-user-delete-btn" style="flex:1;">${t('Supprimer', 'Eliminar')}</button>
            </div>
        </div>`;
    }).join('');

    container.querySelectorAll('.cloud-user-save-btn').forEach(button => {
        button.addEventListener('click', async () => {
            const row = button.closest('.cloud-user-row');
            if (!row) return;
            const index = Number(row.getAttribute('data-user-index'));
            if (!Number.isInteger(index) || index < 0 || index >= cloudManagedUsers.length) return;

            const target = cloudManagedUsers[index];
            const nameInput = row.querySelector('.cloud-user-name-input');
            const roleInput = row.querySelector('.cloud-user-role-input');
            const name = String(nameInput?.value || '').trim();
            const role = normalizeCloudUserRole(roleInput?.value || 'utilisateur');

            try {
                await upsertAllowedUserRecord({ email: target.email, name, role });

                setCloudUserManagementStatus(t(`Utilisateur mis à jour: ${target.email}`, `Usuario actualizado: ${target.email}`));
                await loadCloudUserProfile();
                await fetchCloudManagedUsers();
                renderCloudUsersList();
                updateCloudAuthUi();
            } catch (error) {
                setCloudUserManagementStatus(
                    t(`Mise à jour impossible: ${formatCloudError(error)}`, `Actualización imposible: ${formatCloudError(error)}`),
                    true
                );
            }
        });
    });

    container.querySelectorAll('.cloud-user-delete-btn').forEach(button => {
        button.addEventListener('click', async () => {
            const row = button.closest('.cloud-user-row');
            if (!row) return;
            const index = Number(row.getAttribute('data-user-index'));
            if (!Number.isInteger(index) || index < 0 || index >= cloudManagedUsers.length) return;

            const target = cloudManagedUsers[index];
            const confirmed = window.confirm(t(
                `Supprimer l'utilisateur autorisé ${target.email} ?`,
                `Eliminar el usuario autorizado ${target.email}?`
            ));
            if (!confirmed) return;

            try {
                const { error } = await cloudClient
                    .from(CLOUD_ALLOWED_USERS_TABLE)
                    .delete()
                    .eq('email', target.email);

                if (error) throw error;

                setCloudUserManagementStatus(t(`Utilisateur supprimé: ${target.email}`, `Usuario eliminado: ${target.email}`));
                await loadCloudUserProfile();
                await fetchCloudManagedUsers();
                renderCloudUsersList();
                updateCloudAuthUi();
            } catch (error) {
                setCloudUserManagementStatus(
                    t(`Suppression impossible: ${formatCloudError(error)}`, `Eliminación imposible: ${formatCloudError(error)}`),
                    true
                );
            }
        });
    });
}

function readCloudUserCredentials() {
    const email = String(document.getElementById('cloudEmailInput')?.value || '').trim().toLowerCase();
    const password = String(document.getElementById('cloudUserPasswordInput')?.value || '').trim();
    return { email, password };
}

function normalizeEmailForCompare(value) {
    return String(value || '').trim().toLowerCase();
}

function isForcedCloudAdminEmail(email) {
    return CLOUD_OWNER_ADMIN_EMAILS.has(normalizeEmailForCompare(email));
}

function isMissingAllowedUsersNameColumnError(error) {
    const code = String(error?.code || '').toUpperCase();
    const message = String(error?.message || '').toLowerCase();
    if (code !== 'PGRST204') return false;
    return message.includes("could not find the 'name' column")
        || (message.includes('allowed_users') && message.includes("'name'"));
}

async function upsertAllowedUserRecord({ email, name, role }) {
    const basePayload = {
        email: normalizeEmailForCompare(email),
        role: normalizeCloudUserRole(role),
        updated_at: new Date().toISOString()
    };

    const payloadWithName = {
        ...basePayload,
        name: String(name || '').trim()
    };

    const payloadWithoutName = { ...basePayload };

    const firstPayload = cloudAllowedUsersHasNameColumn ? payloadWithName : payloadWithoutName;
    const { error: firstError } = await cloudClient
        .from(CLOUD_ALLOWED_USERS_TABLE)
        .upsert(firstPayload, { onConflict: 'email' });

    if (!firstError) return;

    if (cloudAllowedUsersHasNameColumn && isMissingAllowedUsersNameColumnError(firstError)) {
        cloudAllowedUsersHasNameColumn = false;
        const { error: retryError } = await cloudClient
            .from(CLOUD_ALLOWED_USERS_TABLE)
            .upsert(payloadWithoutName, { onConflict: 'email' });
        if (retryError) throw retryError;
        return;
    }

    throw firstError;
}

function normalizeCloudUserRole(value) {
    const raw = String(value || '').trim().toLowerCase();
    if (raw === 'admin' || raw === 'administrateur' || raw === 'administrador') {
        return 'administrateur';
    }
    return 'utilisateur';
}

function getCurrentCloudUserEmail() {
    return normalizeEmailForCompare(cloudAuthUser?.email || cloudUserProfile?.email || '');
}

function getCurrentCloudUserDisplayName() {
    const profileName = String(cloudUserProfile?.name || '').trim();
    if (profileName) return profileName;

    const metadataName = String(cloudAuthUser?.user_metadata?.full_name || '').trim();
    if (metadataName) return metadataName;

    const email = getCurrentCloudUserEmail();
    if (email.includes('@')) return email.split('@')[0];
    return email || t('Utilisateur', 'Usuario');
}

function isCloudAdmin() {
    const currentEmail = getCurrentCloudUserEmail();
    if (isForcedCloudAdminEmail(currentEmail)) return true;
    return normalizeCloudUserRole(cloudUserProfile?.role) === 'administrateur';
}

function sanitizeManagedCloudUser(entry, fallbackIndex = 0) {
    const email = normalizeEmailForCompare(entry?.email || '');
    const name = String(entry?.name || '').trim();
    const role = isForcedCloudAdminEmail(email)
        ? 'administrateur'
        : normalizeCloudUserRole(entry?.role);

    return {
        id: String(entry?.id || `allowed-user-${fallbackIndex}`),
        email,
        name,
        role,
        createdAt: String(entry?.created_at || entry?.createdAt || ''),
        updatedAt: String(entry?.updated_at || entry?.updatedAt || '')
    };
}

function getCreatorPatch() {
    const creatorEmail = getCurrentCloudUserEmail();
    const creatorName = getCurrentCloudUserDisplayName();
    if (!creatorEmail) {
        return { creatorEmail: '', creatorName: '' };
    }
    return { creatorEmail, creatorName };
}

function addCreatorToNewRecord(entry) {
    return entry && typeof entry === 'object' ? { ...entry } : {};
}

function ensureCreatorOnEditedRecord(entry) {
    return entry && typeof entry === 'object' ? { ...entry } : {};
}

function setCloudUserManagementStatus(message, isError = false) {
    const node = document.getElementById('cloudUserManagementStatus');
    if (!node) return;
    node.textContent = String(message || '');
    node.style.color = isError ? '#ff8f8f' : '';
}

function setCloudPasswordStatus(message, isError = false) {
    const node = document.getElementById('cloudPasswordStatus');
    if (!node) return;
    node.textContent = String(message || '');
    node.style.color = isError ? '#ff8f8f' : '';
}

function setActiveCloudSubtab(tabKey) {
    const requested = ['account', 'password', 'stats', 'users'].includes(String(tabKey || '')) ? String(tabKey) : 'account';
    activeCloudSubtab = isAuthGateLocked() ? 'account' : requested;

    const accountBtn = document.getElementById('cloudAccountSubtabBtn');
    const passwordBtn = document.getElementById('cloudPasswordSubtabBtn');
    const statsBtn = document.getElementById('cloudStatsSubtabBtn');
    const usersBtn = document.getElementById('cloudUsersSubtabBtn');
    const accountPanel = document.getElementById('cloudAccountPanel');
    const passwordPanel = document.getElementById('cloudPasswordPanel');
    const statsPanel = document.getElementById('cloudStatsPanel');
    const usersPanel = document.getElementById('cloudUsersPanel');

    if (accountBtn) accountBtn.classList.toggle('active', activeCloudSubtab === 'account');
    if (passwordBtn) passwordBtn.classList.toggle('active', activeCloudSubtab === 'password');
    if (statsBtn) statsBtn.classList.toggle('active', activeCloudSubtab === 'stats');
    if (usersBtn) usersBtn.classList.toggle('active', activeCloudSubtab === 'users');
    if (accountPanel) accountPanel.classList.toggle('active', activeCloudSubtab === 'account');
    if (passwordPanel) passwordPanel.classList.toggle('active', activeCloudSubtab === 'password');
    if (statsPanel) statsPanel.classList.toggle('active', activeCloudSubtab === 'stats');
    if (usersPanel) usersPanel.classList.toggle('active', activeCloudSubtab === 'users');
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
        cloudUserProfile = null;
        cloudManagedUsers = [];
        updateCloudAuthUi();
        await applyAuthGateState({ clearWhenLocked: true });
        return;
    }

    try {
        const { data, error } = await cloudClient.auth.getSession();
        if (error) throw error;
        cloudAuthUser = data?.session?.user || null;
        if (cloudAuthUser) {
            await loadCloudUserProfile();
            if (isCloudAdmin()) {
                await fetchCloudManagedUsers();
            }
        } else {
            cloudUserProfile = null;
            cloudManagedUsers = [];
        }
        updateCloudAuthUi();
        await enforceCloudWhitelistForCurrentUser();
        await applyAuthGateState({ clearWhenLocked: true });
    } catch (_error) {
        cloudAuthUser = null;
        cloudUserProfile = null;
        cloudManagedUsers = [];
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
    if (!isCloudReady()) {
        const error = new Error('Cloud non prêt pour synchro journal (auth/connexion manquante).');
        console.error('[CEIBO] Nav/Log cloud sync unavailable', error);
        setCloudStatus(t('Synchro logs impossible: cloud non prêt.', 'Sincronización logs imposible: nube no lista.'), true);
        throw error;
    }

    if (cloudLogbookPushTimer) {
        clearTimeout(cloudLogbookPushTimer);
    }

    cloudLogbookPushTimer = setTimeout(async () => {
        cloudLogbookPushTimer = null;
        if (!isCloudReady()) {
            const error = new Error('Cloud devenu indisponible avant synchro journal.');
            console.error('[CEIBO] Nav/Log cloud sync unavailable', error);
            throw error;
        }

        cloudLogbookPushInFlight = true;
        try {
            // Push NAV first so edited watch data is committed before any cloud pull can overwrite local state.
            await pushNavLogEntriesToCloudTable();
            await pushRoutesToCloud();
            setCloudStatus(t(`Cloud synchro auto · ${getSavedRoutes().length} route(s) · logs OK`, `Nube sincronización auto · ${getSavedRoutes().length} ruta(s) · logs OK`));
        } finally {
            cloudLogbookPushInFlight = false;
        }
    }, CLOUD_LOGBOOK_PUSH_DEBOUNCE_MS);
}

function sanitizeNavLogEntry(entry, fallbackIndex = 0) {
    if (!entry || typeof entry !== 'object') return null;

    const readValue = (...keys) => {
        for (const key of keys) {
            if (Object.prototype.hasOwnProperty.call(entry, key) && entry[key] !== undefined && entry[key] !== null) {
                return entry[key];
            }
        }
        return null;
    };

    const id = String(entry.id || `${Date.now()}-${fallbackIndex}-${Math.random().toString(36).slice(2, 7)}`);
    const timestamp = String(
        readValue('timestamp', 'created_at', 'updated_at') || new Date().toISOString()
    );
    const watchTimeIsoRaw = readValue('watchTimeIso', 'watch_time_iso');
    const watchTimeIso = watchTimeIsoRaw ? String(watchTimeIsoRaw) : null;

    const toFiniteOrNull = value => {
        const n = Number(value);
        return Number.isFinite(n) ? n : null;
    };

    return {
        id,
        timestamp,
        watchTimeIso,
        lat: toFiniteOrNull(readValue('lat')),
        lng: toFiniteOrNull(readValue('lng', 'lon')),
        speedKn: toFiniteOrNull(readValue('speedKn', 'speed_kn')),
        heelDeg: toFiniteOrNull(readValue('heelDeg', 'heel_deg')),
        source: String(readValue('source') || 'manual'),
        watchCrew: String(readValue('watchCrew', 'watch_crew') || ''),
        headingDeg: toFiniteOrNull(readValue('headingDeg', 'heading_deg')),
        windDirectionDeg: toFiniteOrNull(readValue('windDirectionDeg', 'wind_direction_deg')),
        windSpeedKn: toFiniteOrNull(readValue('windSpeedKn', 'wind_speed_kn')),
        seaState: String(readValue('seaState', 'sea_state') || ''),
        sailConfig: String(readValue('sailConfig', 'sail_config') || ''),
        barometerHpa: toFiniteOrNull(readValue('barometerHpa', 'barometer_hpa')),
        logDistanceNm: toFiniteOrNull(readValue('logDistanceNm', 'log_distance_nm')),
        events: String(readValue('events') || ''),
        creatorEmail: normalizeEmailForCompare(readValue('creatorEmail', 'creator_email') || ''),
        creatorName: String(readValue('creatorName', 'creator_name') || '').trim()
    };
}

function sanitizeNavLogEntriesList(list) {
    if (!Array.isArray(list)) return [];
    return list
        .map((entry, index) => sanitizeNavLogEntry(entry, index))
        .filter(Boolean)
        .sort((a, b) => {
            const aTs = toTimestampMs(a?.timestamp);
            const bTs = toTimestampMs(b?.timestamp);
            return aTs - bTs;
        });
}

function sanitizeEngineLogEntry(entry, fallbackIndex = 0) {
    if (!entry || typeof entry !== 'object') return null;

    const readValue = (...keys) => {
        for (const key of keys) {
            if (Object.prototype.hasOwnProperty.call(entry, key) && entry[key] !== undefined && entry[key] !== null) {
                return entry[key];
            }
        }
        return null;
    };

    const toFiniteOrNull = value => {
        const n = Number(value);
        return Number.isFinite(n) ? n : null;
    };

    const id = String(readValue('id') || `${Date.now()}-${fallbackIndex}-${Math.random().toString(36).slice(2, 7)}`);
    const timestamp = String(readValue('timestamp', 'created_at', 'updated_at') || new Date().toISOString());

    const hours = toFiniteOrNull(readValue('hours'));
    if (!Number.isFinite(hours)) return null;

    return {
        id,
        timestamp,
        hours,
        fuelAddedL: Math.max(0, toFiniteOrNull(readValue('fuelAddedL', 'fuel_added_l')) ?? 0),
        note: String(readValue('note') || '').trim(),
        creatorEmail: normalizeEmailForCompare(readValue('creatorEmail', 'creator_email') || ''),
        creatorName: String(readValue('creatorName', 'creator_name') || '').trim()
    };
}

function sanitizeEngineLogEntriesList(list) {
    if (!Array.isArray(list)) return [];
    return list
        .map((entry, index) => sanitizeEngineLogEntry(entry, index))
        .filter(Boolean)
        .sort((a, b) => {
            const aTs = toTimestampMs(a?.timestamp);
            const bTs = toTimestampMs(b?.timestamp);
            return aTs - bTs;
        });
}

async function pushEngineLogEntriesToCloudTable() {
    if (!isCloudReady()) {
        throw new Error('Cloud non prêt: impossible d\'insérer engine_log.');
    }

    const creatorEmail = getCurrentCloudUserEmail();
    if (!creatorEmail) {
        throw new Error('creator_email manquant: impossible d\'insérer engine_log.');
    }

    let resolvedProjectIdUuid = await resolveCloudProjectIdUuid();
    if (!resolvedProjectIdUuid) {
        throw new Error('project_id introuvable pour engine_log');
    }
    resolvedProjectIdUuid = await ensureCloudProjectRow(resolvedProjectIdUuid);

    const safeEntries = sanitizeEngineLogEntriesList(engineLogEntries);
    if (safeEntries.length === 0) return true;

    const { data: existingRows, error: existingError } = await cloudClient
        .from(CLOUD_ENGINE_LOG_TABLE)
        .select('id')
        .eq('project_id', resolvedProjectIdUuid)
        .eq('creator_email', creatorEmail);

    if (existingError) throw existingError;

    const existingIds = new Set(
        (Array.isArray(existingRows) ? existingRows : [])
            .map(row => String(row?.id || '').trim())
            .filter(Boolean)
    );

    const nowIso = new Date().toISOString();

    const insertPayload = safeEntries
        .filter(entry => !existingIds.has(String(entry.id || '').trim()))
        .map(entry => ({
            id: String(entry.id),
            project_id: resolvedProjectIdUuid,
            creator_email: creatorEmail,
            creator_name: entry.creatorName || null,
            timestamp: entry.timestamp || nowIso,
            hours: entry.hours,
            fuel_added_l: Number.isFinite(entry.fuelAddedL) ? entry.fuelAddedL : 0,
            note: entry.note || '',
            created_at: entry.timestamp || nowIso,
            updated_at: nowIso
        }));

    if (insertPayload.length > 0) {
        const { error: insertError } = await cloudClient
            .from(CLOUD_ENGINE_LOG_TABLE)
            .insert(insertPayload);
        if (insertError) throw insertError;
    }

    const rowsToUpdate = safeEntries.filter(entry => existingIds.has(String(entry.id || '').trim()));
    for (const entry of rowsToUpdate) {
        const { error: updateError } = await cloudClient
            .from(CLOUD_ENGINE_LOG_TABLE)
            .update({
                creator_name: entry.creatorName || null,
                timestamp: entry.timestamp || nowIso,
                hours: entry.hours,
                fuel_added_l: Number.isFinite(entry.fuelAddedL) ? entry.fuelAddedL : 0,
                note: entry.note || '',
                updated_at: nowIso
            })
            .eq('id', String(entry.id))
            .eq('project_id', resolvedProjectIdUuid)
            .eq('creator_email', creatorEmail);

        if (updateError) throw updateError;
    }

    return true;
}

async function pullEngineLogEntriesFromCloudTable() {
    if (!isCloudReady()) {
        throw new Error('Cloud non prêt: impossible de lire engine_log.');
    }

    const creatorEmail = getCurrentCloudUserEmail();
    if (!creatorEmail) {
        throw new Error('creator_email manquant: impossible de lire engine_log.');
    }

    const resolvedProjectIdUuid = await resolveCloudProjectIdUuid();

    let query = cloudClient
        .from(CLOUD_ENGINE_LOG_TABLE)
        .select('*')
        .eq('creator_email', creatorEmail)
        .order('timestamp', { ascending: true });

    if (isUuidString(resolvedProjectIdUuid)) {
        query = query.eq('project_id', resolvedProjectIdUuid);
    }

    const { data, error } = await query;
    if (error) throw error;

    return sanitizeEngineLogEntriesList(data || []);
}

async function pushNavLogEntriesToCloudTable() {
    if (!isCloudReady()) {
        throw new Error('Cloud non prêt: impossible d\'insérer nav_log_entries.');
    }

    const creatorEmail = getCurrentCloudUserEmail();
    if (!creatorEmail) {
        throw new Error('creator_email manquant: impossible d\'insérer nav_log_entries.');
    }

    let resolvedProjectIdUuid = await resolveCloudProjectIdUuid();
    if (!resolvedProjectIdUuid) {
        throw new Error('project_id introuvable pour nav_log_entries');
    }
    resolvedProjectIdUuid = await ensureCloudProjectRow(resolvedProjectIdUuid);

    const sanitizedEntries = sanitizeNavLogEntriesList(navLogEntries);
    const updatedAt = new Date().toISOString();

    const rowPayloadSnake = sanitizedEntries.map(entry => ({
        project_id: resolvedProjectIdUuid,
        watch_time_iso: entry.watchTimeIso,
        lat: entry.lat,
        lng: entry.lng,
        speed_kn: entry.speedKn,
        heel_deg: entry.heelDeg,
        source: entry.source,
        watch_crew: entry.watchCrew,
        heading_deg: entry.headingDeg,
        wind_direction_deg: entry.windDirectionDeg,
        wind_speed_kn: entry.windSpeedKn,
        sea_state: entry.seaState,
        sail_config: entry.sailConfig,
        barometer_hpa: entry.barometerHpa,
        log_distance_nm: entry.logDistanceNm,
        events: entry.events,
        creator_email: creatorEmail,
        creator_name: entry.creatorName || null,
        created_at: entry.timestamp || updatedAt,
        updated_at: updatedAt
    }));

    // Strict mode: only row-per-entry persistence (snake_case schema).
    const { error: deleteByCreatorError } = await cloudClient
        .from(CLOUD_NAV_LOG_TABLE)
        .delete()
        .eq('project_id', resolvedProjectIdUuid)
        .eq('creator_email', creatorEmail);

    if (deleteByCreatorError) throw deleteByCreatorError;

    if (rowPayloadSnake.length === 0) return true;

    const { error: insertSnakeError } = await cloudClient
        .from(CLOUD_NAV_LOG_TABLE)
        .insert(rowPayloadSnake);

    if (insertSnakeError) throw insertSnakeError;
    return true;
}

async function pullNavLogEntriesFromCloudTable() {
    if (!isCloudReady()) {
        throw new Error('Cloud non prêt: impossible de lire nav_log_entries.');
    }

    const creatorEmail = getCurrentCloudUserEmail();
    if (!creatorEmail) {
        throw new Error('creator_email manquant: impossible de lire nav_log_entries.');
    }

    const resolvedProjectIdUuid = await resolveCloudProjectIdUuid();

    let query = cloudClient
        .from(CLOUD_NAV_LOG_TABLE)
        .select('*')
        .eq('creator_email', creatorEmail);

    if (isUuidString(resolvedProjectIdUuid)) {
        query = query.eq('project_id', resolvedProjectIdUuid);
    }

    const { data, error } = await query;

    if (error) throw error;
    if (!Array.isArray(data) || data.length === 0) return [];

    const singleRow = data.length === 1 ? data[0] : null;
    if (singleRow) {
        if (Array.isArray(singleRow.entries)) {
            return sanitizeNavLogEntriesList(singleRow.entries);
        }
        if (Array.isArray(singleRow.nav_log_entries)) {
            return sanitizeNavLogEntriesList(singleRow.nav_log_entries);
        }
        if (singleRow.payload && Array.isArray(singleRow.payload.entries)) {
            return sanitizeNavLogEntriesList(singleRow.payload.entries);
        }
    }

    // Multi-row layout: each row represents one nav entry.
    return sanitizeNavLogEntriesList(data);
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

    const watchDate = watchTimeInput?.value
        ? parseDateInputFlexible(`${watchTimeInput.value}:00`)
        : new Date();
    const isoDate = watchDate ? watchDate.toISOString() : new Date().toISOString();

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
        renderNavGpsTraceOnMap();
        return;
    }

    container.innerHTML = '';

    const headerRow = document.createElement('div');
    headerRow.className = 'log-list-header';
    headerRow.innerHTML =
        `<span>${t('Date', 'Fecha')}</span>` +
        `<span>${t('Créateur', 'Creador')}</span>` +
        `<span>${t('Quart', 'Guardia')}</span>` +
        `<span>${t('Loch', 'Corredera')}</span>`;
    container.appendChild(headerRow);

    navLogEntries
        .slice()
        .reverse()
        .forEach(item => {
            const watchTime = item?.watchTimeIso ? formatDateTimeFr(item.watchTimeIso) : formatDateTimeFr(item?.timestamp);
            const creatorName = String(item?.creatorName || '').trim();
            const creatorLabel = creatorName || '[creatorName manquant]';
            const watchCrewLabel = String(item?.watchCrew || '').trim() || '[quart manquant]';
            const loch = Number.isFinite(item?.logDistanceNm) ? `${item.logDistanceNm.toFixed(1)} NM` : '[loch manquant]';
            const row = document.createElement('div');
            row.className = 'log-list-row';
            if (editingNavLogEntryId && String(item?.id || '') === editingNavLogEntryId) {
                row.classList.add('log-list-row--active');
            }

            const summaryRow = document.createElement('div');
            summaryRow.className = 'log-list-summary';
            summaryRow.innerHTML =
                `<span class="log-list-col">${escapeHtml(watchTime)}</span>` +
                `<span class="log-list-col">${escapeHtml(creatorLabel)}</span>` +
                `<span class="log-list-col">${escapeHtml(watchCrewLabel)}</span>` +
                `<span class="log-list-col">${escapeHtml(loch)}</span>`;
            summaryRow.addEventListener('click', () => {
                startEditNavLogEntry(String(item?.id || ''));
            });

            row.appendChild(summaryRow);
            container.appendChild(row);
        });

    drawHeelSpeedChart();
    renderNavGpsTraceOnMap();
}

function getNavGpsTraceEntries() {
    return (Array.isArray(navLogEntries) ? navLogEntries : [])
        .filter(item => item?.source === 'gps-watch' && Number.isFinite(item?.lat) && Number.isFinite(item?.lng));
}

function ensureNavGpsTraceLayerGroup() {
    if (!map) return null;
    if (!navGpsTraceLayerGroup) {
        navGpsTraceLayerGroup = L.layerGroup();
    }
    if (!map.hasLayer(navGpsTraceLayerGroup)) {
        navGpsTraceLayerGroup.addTo(map);
    }
    return navGpsTraceLayerGroup;
}

function getNavHeelTraceColor(heelDeg) {
    const heel = Math.abs(Number(heelDeg) || 0);
    if (heel >= 25) return '#ff4d4d';
    if (heel >= 15) return '#ffb648';
    return '#34c759';
}

function renderNavGpsTraceOnMap() {
    if (!map) return;

    const traceEntries = getNavGpsTraceEntries();
    const layerGroup = ensureNavGpsTraceLayerGroup();
    if (!layerGroup) return;

    layerGroup.clearLayers();
    if (!traceEntries.length) return;

    const latlngs = traceEntries.map(item => [item.lat, item.lng]);
    const polyline = L.polyline(latlngs, {
        color: '#5ac8fa',
        weight: 3,
        opacity: 0.9
    });
    layerGroup.addLayer(polyline);

    // Keep markers lightweight: show heel points only on recent samples.
    const heelMarkers = traceEntries.slice(-300);
    heelMarkers.forEach(entry => {
        const heel = Number.isFinite(entry?.heelDeg) ? entry.heelDeg : null;
        const marker = L.circleMarker([entry.lat, entry.lng], {
            radius: 4,
            color: '#0b1f2e',
            weight: 1,
            fillColor: getNavHeelTraceColor(heel),
            fillOpacity: 0.9
        });

        const timeLabel = formatDateTimeFr(entry?.timestamp);
        const heelLabel = Number.isFinite(heel) ? `${heel.toFixed(1)}°` : t('N/A', 'N/A');
        marker.bindPopup(`${t('Trace GPS', 'Traza GPS')}<br>${timeLabel}<br>${t('Inclinaison', 'Inclinación')}: ${heelLabel}`);
        layerGroup.addLayer(marker);
    });
}

function resetNavLogEditorForm() {
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

    if (watchTimeInput) watchTimeInput.value = toLocalDateTimeInputValue(new Date());
    if (watchCrewInput) watchCrewInput.value = '';
    if (watchHeadingInput) watchHeadingInput.value = '';
    if (watchWindDirInput) watchWindDirInput.value = '';
    if (watchWindSpeedInput) watchWindSpeedInput.value = '';
    if (watchSeaStateInput) watchSeaStateInput.value = 'calme';
    if (watchSailConfigInput) watchSailConfigInput.value = '';
    if (watchBarometerInput) watchBarometerInput.value = '';
    if (watchLogNmInput) watchLogNmInput.value = '';
    if (watchEventsInput) watchEventsInput.value = '';
}

function cancelNavLogEdit() {
    editingNavLogEntryId = null;
    resetNavLogEditorForm();
    if (logWorkspaceMode === 'nav') {
        logWorkspaceMode = 'none';
    }
    renderNavLogList();
    renderLogWorkspacePanel();
}

function startEditNavLogEntry(entryId) {
    const targetId = String(entryId || '');
    const entry = navLogEntries.find(item => String(item?.id || '') === targetId);
    if (!entry) return;

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

    if (!watchTimeInput || !watchCrewInput || !watchHeadingInput || !watchWindDirInput || !watchWindSpeedInput || !watchSeaStateInput || !watchSailConfigInput || !watchBarometerInput || !watchLogNmInput || !watchEventsInput) {
        return;
    }

    editingNavLogEntryId = targetId;
    const entryDate = parseDateInputFlexible(entry?.watchTimeIso || entry?.timestamp || Date.now());
    watchTimeInput.value = toLocalDateTimeInputValue(entryDate || new Date());
    watchCrewInput.value = String(entry?.watchCrew || '');
    watchHeadingInput.value = Number.isFinite(entry?.headingDeg) ? String(entry.headingDeg) : '';
    watchWindDirInput.value = Number.isFinite(entry?.windDirectionDeg) ? String(entry.windDirectionDeg) : '';
    watchWindSpeedInput.value = Number.isFinite(entry?.windSpeedKn) ? String(entry.windSpeedKn) : '';
    watchSeaStateInput.value = String(entry?.seaState || 'calme');
    watchSailConfigInput.value = String(entry?.sailConfig || '');
    watchBarometerInput.value = Number.isFinite(entry?.barometerHpa) ? String(entry.barometerHpa) : '';
    watchLogNmInput.value = Number.isFinite(entry?.logDistanceNm) ? String(entry.logDistanceNm) : '';
    watchEventsInput.value = String(entry?.events || '');

    logWorkspaceMode = 'nav';
    renderNavLogList();
    renderLogWorkspacePanel();
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
        events,
        creatorEmail: getCreatorPatch().creatorEmail,
        creatorName: getCreatorPatch().creatorName
    });

    if (navLogEntries.length > 1200) {
        navLogEntries = navLogEntries.slice(navLogEntries.length - 1200);
    }

    saveNavLogEntries();
    renderNavLogList();
}

function captureNavGpsSample(sampleSource = 'gps-watch') {
    if (!navGpsLatestFix || !Number.isFinite(navGpsLatestFix.lat) || !Number.isFinite(navGpsLatestFix.lng)) {
        return false;
    }

    appendNavLogEntry({
        lat: navGpsLatestFix.lat,
        lng: navGpsLatestFix.lng,
        speedKn: navGpsLatestFix.speedKn,
        heelDeg: Number.isFinite(navLatestHeelDeg) ? navLatestHeelDeg : null,
        source: sampleSource
    });

    navGpsSessionHasSample = true;
    return true;
}

function appendAutoNavSessionSummaryEntry() {
    const sessionEntries = (Array.isArray(navLogEntries) ? navLogEntries.slice(navGpsSessionStartEntryIndex) : [])
        .filter(item => item?.source === 'gps-watch' && Number.isFinite(item?.lat) && Number.isFinite(item?.lng));

    if (!sessionEntries.length) return null;

    let distance = 0;
    for (let index = 1; index < sessionEntries.length; index++) {
        const previous = sessionEntries[index - 1];
        const current = sessionEntries[index];
        distance += distanceNm(previous, current);
    }

    const speedValues = sessionEntries.map(item => item?.speedKn).filter(value => Number.isFinite(value));
    const heelValues = sessionEntries.map(item => item?.heelDeg).filter(value => Number.isFinite(value));
    const averageSpeed = speedValues.length ? speedValues.reduce((sum, value) => sum + value, 0) / speedValues.length : null;
    const averageHeel = heelValues.length ? heelValues.reduce((sum, value) => sum + Math.abs(value), 0) / heelValues.length : null;
    const durationMinutes = navGpsSessionStartMs > 0 ? Math.max(0, (Date.now() - navGpsSessionStartMs) / 60000) : null;

    const summaryText = t(
        `Session GPS auto: ${sessionEntries.length} points · durée ${durationMinutes ? durationMinutes.toFixed(1) : '0.0'} min · distance ${distance.toFixed(2)} NM · vitesse moy ${Number.isFinite(averageSpeed) ? averageSpeed.toFixed(1) : 'N/A'} kn · inclinaison moy ${Number.isFinite(averageHeel) ? averageHeel.toFixed(1) : 'N/A'}°`,
        `Sesión GPS auto: ${sessionEntries.length} puntos · duración ${durationMinutes ? durationMinutes.toFixed(1) : '0.0'} min · distancia ${distance.toFixed(2)} NM · velocidad media ${Number.isFinite(averageSpeed) ? averageSpeed.toFixed(1) : 'N/A'} kn · inclinación media ${Number.isFinite(averageHeel) ? averageHeel.toFixed(1) : 'N/A'}°`
    );

    appendNavLogEntry({
        lat: null,
        lng: null,
        speedKn: averageSpeed,
        heelDeg: averageHeel,
        source: 'manual',
        watchTimeIso: new Date().toISOString(),
        watchCrew: t('AUTO GPS', 'AUTO GPS'),
        logDistanceNm: distance,
        events: summaryText
    });

    return {
        points: sessionEntries.length,
        distance,
        durationMinutes
    };
}

function addManualNavigationLogEntry(event) {
    if (event?.preventDefault) event.preventDefault();
    if (event?.stopPropagation) event.stopPropagation();

    const manualData = getManualNavFormData();

    if (editingNavLogEntryId) {
        let updated = false;
        navLogEntries = navLogEntries.map(entry => {
            if (String(entry?.id || '') !== editingNavLogEntryId) return entry;
            updated = true;
            return {
                ...entry,
                ...manualData,
                source: 'manual',
                speedKn: Number.isFinite(navLatestSpeedKn) ? navLatestSpeedKn : entry?.speedKn ?? null,
                heelDeg: Number.isFinite(navLatestHeelDeg) ? navLatestHeelDeg : entry?.heelDeg ?? null,
                creatorEmail: entry?.creatorEmail || getCreatorPatch().creatorEmail,
                creatorName: entry?.creatorName || getCreatorPatch().creatorName
            };
        });

        if (updated) {
            saveNavLogEntries();
            renderNavLogList();
            setNavLogStatus(t('Entrée journal mise à jour.', 'Entrada de diario actualizada.'));
            alert(t('Mise à jour du log de navigation confirmée.', 'Actualizacion del log de navegacion confirmada.'));
            // Keep nav editor visible after an update to avoid closing the workspace unexpectedly.
            logWorkspaceMode = 'nav';
            renderLogWorkspacePanel();
        }

        return;
    } else {
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

    editingNavLogEntryId = null;
    logWorkspaceMode = 'none';
    resetNavLogEditorForm();
    renderLogWorkspacePanel();
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

    navGpsLatestFix = null;
    navGpsSessionStartMs = Date.now();
    navGpsSessionStartEntryIndex = navLogEntries.length;
    navGpsSessionHasSample = false;

    if (navGpsSampleTimerId !== null) {
        window.clearInterval(navGpsSampleTimerId);
        navGpsSampleTimerId = null;
    }

    navGpsSampleTimerId = window.setInterval(() => {
        const saved = captureNavGpsSample('gps-watch');
        if (saved) {
            setNavLogStatus(t(`Log GPS actif · point sauvegardé (${NAV_GPS_SAMPLE_INTERVAL_MS / 1000}s) · ${navLogEntries.length} point(s)`, `Log GPS activo · punto guardado (${NAV_GPS_SAMPLE_INTERVAL_MS / 1000}s) · ${navLogEntries.length} punto(s)`));
        }
    }, NAV_GPS_SAMPLE_INTERVAL_MS);

    navWatchId = navigator.geolocation.watchPosition(
        position => {
            const latitude = Number(position?.coords?.latitude);
            const longitude = Number(position?.coords?.longitude);
            const speedMs = Number(position?.coords?.speed);
            const speedKn = Number.isFinite(speedMs) ? speedMs * 1.943844 : null;
            navLatestSpeedKn = speedKn;

            if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return;

            navGpsLatestFix = {
                lat: latitude,
                lng: longitude,
                speedKn,
                fixTimeMs: Date.now()
            };

            if (!navGpsSessionHasSample) {
                captureNavGpsSample('gps-watch');
            }

            setNavLogStatus(t(`Log GPS actif · fix reçu · enregistrement chaque minute · ${navLogEntries.length} point(s)`, `Log GPS activo · fix recibido · guardado cada minuto · ${navLogEntries.length} punto(s)`));
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

function stopNavigationLogging(options = {}) {
    const { createAutoSessionSummary = true } = options;

    if (navWatchId !== null && navigator.geolocation) {
        navigator.geolocation.clearWatch(navWatchId);
        navWatchId = null;
    }

    if (navGpsSampleTimerId !== null) {
        window.clearInterval(navGpsSampleTimerId);
        navGpsSampleTimerId = null;
    }

    if (createAutoSessionSummary) {
        captureNavGpsSample('gps-watch');
    }

    let summary = null;
    if (createAutoSessionSummary) {
        summary = appendAutoNavSessionSummaryEntry();
    }

    if (summary) {
        setNavLogStatus(t(
            `Log GPS arrêté · session enregistrée (${summary.points} points, ${summary.distance.toFixed(2)} NM).`,
            `Log GPS detenido · sesión guardada (${summary.points} puntos, ${summary.distance.toFixed(2)} NM).`
        ));
    } else {
        setNavLogStatus(t(`Log GPS arrêté · ${navLogEntries.length} point(s) enregistrés.`, `Log GPS detenido · ${navLogEntries.length} punto(s) guardado(s).`));
    }

    navGpsLatestFix = null;
    navGpsSessionStartMs = 0;
    navGpsSessionStartEntryIndex = navLogEntries.length;
    navGpsSessionHasSample = false;
}

function clearNavigationLogbook() {
    stopNavigationLogging({ createAutoSessionSummary: false });
    navLogEntries = [];
    editingNavLogEntryId = null;
    logWorkspaceMode = 'none';
    resetNavLogEditorForm();
    saveNavLogEntries();
    renderNavLogList();
    renderLogWorkspacePanel();
    setNavLogStatus(t('Journal navigation effacé.', 'Diario de navegación borrado.'));
}

function loadNavigationLogbook() {
    navLogEntries = loadArrayFromStorage(NAV_LOG_STORAGE_KEY);
    editingNavLogEntryId = null;
    logWorkspaceMode = 'none';
    const watchTimeInput = document.getElementById('watchTimeInput');
    if (watchTimeInput) {
        watchTimeInput.value = toLocalDateTimeInputValue(new Date());
    }
    renderNavLogList();
    renderLogWorkspacePanel();
}

function openNavLogCreateForm() {
    editingNavLogEntryId = null;
    resetNavLogEditorForm();
    logWorkspaceMode = 'nav';
    renderNavLogList();
    renderLogWorkspacePanel();
}

function openEngineLogCreateForm() {
    editingEngineLogEntryId = null;
    const hoursInput = document.getElementById('engineHoursInput');
    const fuelInput = document.getElementById('fuelAddedInput');
    const noteInput = document.getElementById('engineLogNoteInput');
    if (hoursInput) hoursInput.value = '';
    if (fuelInput) fuelInput.value = '';
    if (noteInput) noteInput.value = '';
    refreshEngineHoursInputGuard();
    logWorkspaceMode = 'engine';
    updateEngineLogFormMode();
    renderEngineLogList();
    renderLogWorkspacePanel();
}

function getEngineLogPreviousEntry(entryId) {
    const safeEntries = sanitizeEngineLogEntriesList(engineLogEntries);
    if (!safeEntries.length) return null;

    const targetId = String(entryId || '').trim();
    if (!targetId) {
        return safeEntries[safeEntries.length - 1] || null;
    }

    const currentIndex = safeEntries.findIndex(entry => String(entry?.id || '') === targetId);
    if (currentIndex <= 0) return null;
    return safeEntries[currentIndex - 1] || null;
}

function refreshEngineHoursInputGuard() {
    const hoursInput = document.getElementById('engineHoursInput');
    if (!hoursInput) return;

    const previousEntry = getEngineLogPreviousEntry(editingEngineLogEntryId);
    if (!previousEntry || !Number.isFinite(previousEntry.hours)) {
        hoursInput.removeAttribute('min');
        hoursInput.title = '';
        return;
    }

    // HTML min cannot express strict ">"; strict check is enforced in JS on save.
    hoursInput.min = String(previousEntry.hours);
    hoursInput.title = t(
        `Doit être supérieur à ${previousEntry.hours.toFixed(1)} h`,
        `Debe ser superior a ${previousEntry.hours.toFixed(1)} h`
    );
}

function saveEngineLogEntries() {
    scheduleCloudLogbookPush();
}

function renderLogWorkspacePanel() {
    const panel = document.getElementById('logWorkspacePanel');
    const title = document.getElementById('logWorkspaceTitle');
    const placeholder = document.getElementById('logWorkspacePlaceholder');
    const navPanel = document.getElementById('navLogEditorPanel');
    const enginePanel = document.getElementById('engineLogEditorPanel');
    if (!panel || !title || !placeholder || !navPanel || !enginePanel) return;

    const navVisible = logWorkspaceMode === 'nav';
    const engineVisible = logWorkspaceMode === 'engine';

    navPanel.style.display = navVisible ? 'block' : 'none';
    enginePanel.style.display = engineVisible ? 'block' : 'none';
    placeholder.style.display = navVisible || engineVisible ? 'none' : 'flex';

    if (navVisible) {
        title.textContent = editingNavLogEntryId
            ? t('Modifier entrée de navigation', 'Editar entrada de navegación')
            : t('Créer entrée de navigation', 'Crear entrada de navegación');
    } else if (engineVisible) {
        title.textContent = editingEngineLogEntryId
            ? t('Modifier entrée moteur', 'Editar entrada de motor')
            : t('Créer entrée moteur', 'Crear entrada de motor');
    } else {
        title.textContent = t('Saisie journal', 'Edición diario');
    }
}

function updateEngineLogFormMode() {
    const saveBtn = document.getElementById('saveEngineLogBtn');
    const cancelBtn = document.getElementById('cancelEngineLogEditBtn');

    if (saveBtn) {
        saveBtn.textContent = editingEngineLogEntryId
            ? t('Mettre à jour entrée moteur', 'Actualizar entrada motor')
            : t('Ajouter entrée moteur', 'Añadir entrada motor');
    }

    if (cancelBtn) {
        cancelBtn.style.display = editingEngineLogEntryId ? '' : 'none';
    }
}

function cancelEngineLogEdit() {
    editingEngineLogEntryId = null;

    const hoursInput = document.getElementById('engineHoursInput');
    const fuelInput = document.getElementById('fuelAddedInput');
    const noteInput = document.getElementById('engineLogNoteInput');
    if (hoursInput) hoursInput.value = '';
    if (fuelInput) fuelInput.value = '';
    if (noteInput) noteInput.value = '';
    refreshEngineHoursInputGuard();

    if (logWorkspaceMode === 'engine') {
        logWorkspaceMode = 'none';
    }

    updateEngineLogFormMode();
    renderEngineLogList();
    renderLogWorkspacePanel();
}

function startEditEngineLogEntry(entryId) {
    const targetId = String(entryId || '');
    const entry = engineLogEntries.find(item => String(item?.id || '') === targetId);
    if (!entry) return;

    const hoursInput = document.getElementById('engineHoursInput');
    const fuelInput = document.getElementById('fuelAddedInput');
    const noteInput = document.getElementById('engineLogNoteInput');
    if (!hoursInput || !fuelInput || !noteInput) return;

    editingEngineLogEntryId = targetId;
    hoursInput.value = Number.isFinite(entry?.hours) ? String(entry.hours) : '';
    fuelInput.value = Number.isFinite(entry?.fuelAddedL) ? String(entry.fuelAddedL) : '';
    noteInput.value = String(entry?.note || '');
    refreshEngineHoursInputGuard();
    logWorkspaceMode = 'engine';

    updateEngineLogFormMode();
    renderEngineLogList();
    renderLogWorkspacePanel();
}

function renderEngineLogList() {
    const container = document.getElementById('engineLogList');
    if (!container) return;

    if (!Array.isArray(engineLogEntries) || engineLogEntries.length === 0) {
        container.innerHTML = `<div class="log-card">${t('Aucune entrée moteur pour le moment.', 'No hay entradas de motor por ahora.')}</div>`;
        return;
    }

    container.innerHTML = '';

    const headerRow = document.createElement('div');
    headerRow.className = 'log-list-header';
    headerRow.innerHTML =
        `<span>${t('Date', 'Fecha')}</span>` +
        `<span>${t('Auteur', 'Autor')}</span>` +
        `<span>${t('Compteur', 'Contador')}</span>`;
    container.appendChild(headerRow);

    engineLogEntries
        .slice()
        .reverse()
        .forEach(entry => {
            const creator = String(entry?.creatorName || '').trim() || String(entry?.creatorEmail || '').trim() || 'N/A';
            const hours = Number.isFinite(entry?.hours) ? `${entry.hours.toFixed(1)} h` : 'N/A';

            const row = document.createElement('div');
            row.className = 'log-list-row';
            if (editingEngineLogEntryId && String(entry?.id || '') === editingEngineLogEntryId) {
                row.classList.add('log-list-row--active');
            }

            const summaryRow = document.createElement('div');
            summaryRow.className = 'log-list-summary';
            summaryRow.innerHTML =
                `<span class="log-list-col">${escapeHtml(formatDateTimeFr(entry?.timestamp))}</span>` +
                `<span class="log-list-col">${escapeHtml(creator)}</span>` +
                `<span class="log-list-col">${escapeHtml(hours)}</span>`;
            summaryRow.addEventListener('click', () => {
                startEditEngineLogEntry(String(entry?.id || ''));
            });

            row.appendChild(summaryRow);
            container.appendChild(row);
        });
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

    const previousEntry = getEngineLogPreviousEntry(editingEngineLogEntryId);
    if (previousEntry && Number.isFinite(previousEntry.hours) && !(hours > previousEntry.hours)) {
        alert(t(
            `Le compteur moteur doit être strictement supérieur à l'entrée précédente (${previousEntry.hours.toFixed(1)} h).`,
            `El contador motor debe ser estrictamente superior a la entrada anterior (${previousEntry.hours.toFixed(1)} h).`
        ));
        return;
    }

    const nextValues = {
        hours,
        fuelAddedL: Number.isFinite(fuelAddedL) ? Math.max(0, fuelAddedL) : 0,
        note
    };

    if (editingEngineLogEntryId) {
        let updated = false;
        engineLogEntries = engineLogEntries.map(entry => {
            if (String(entry?.id || '') !== editingEngineLogEntryId) return entry;
            updated = true;
            return ensureCreatorOnEditedRecord({
                ...entry,
                ...nextValues
            });
        });

        if (!updated) {
            editingEngineLogEntryId = null;
        }
    } else {
        engineLogEntries.push({
            id: `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
            timestamp: new Date().toISOString(),
            creatorEmail: getCreatorPatch().creatorEmail,
            creatorName: getCreatorPatch().creatorName,
            ...nextValues
        });

        if (engineLogEntries.length > 800) {
            engineLogEntries = engineLogEntries.slice(engineLogEntries.length - 800);
        }
    }

    saveEngineLogEntries();
    renderEngineLogList();
    cancelEngineLogEdit();
}

function loadEngineLogbook() {
    engineLogEntries = [];
    renderEngineLogList();
    cancelEngineLogEdit();
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
    document.title = `Gestion du CEIBO · v${APP_BUILD_VERSION}`;
    const buildVersionBadge = document.getElementById('buildVersionBadge');
    if (buildVersionBadge) {
        buildVersionBadge.textContent = `v${APP_BUILD_VERSION}`;
    }
    console.info(`[CEIBO] Front build ${APP_BUILD_VERSION}`);

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
            setCloudStatus(
                t(
                    'Accès verrouillé: seul l\'onglet Cloud est accessible (connexion / création de compte).',
                    'Acceso bloqueado: solo la pestaña Cloud es accesible (conexión / creación de cuenta).'
                ),
                true
            );
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
    const weatherTabBtn = document.getElementById('weatherTabBtn');
    const arrivalTabBtn = document.getElementById('arrivalTabBtn');
    const waypointTabBtn = document.getElementById('waypointTabBtn');
    const maintenanceTabBtn = document.getElementById('maintenanceTabBtn');
    const routingTab = document.getElementById('routingTab');
    const routesTab = document.getElementById('routesTab');
    const cloudTab = document.getElementById('cloudTab');
    const navLogTab = document.getElementById('navLogTab');
    const weatherTab = document.getElementById('weatherTab');
    const arrivalTab = document.getElementById('arrivalTab');
    const waypointTab = document.getElementById('waypointTab');
    const maintenanceTab = document.getElementById('maintenanceTab');
    const mapContainer = document.getElementById('mapWorkspace');
    const waypointDockPanel = document.getElementById('waypointDockPanel');
    const maintenanceMapPanel = document.getElementById('maintenanceMapPanel');
    const maintenanceInvoicePreviewPanel = document.getElementById('maintenanceInvoicePreviewPanel');
    const logWorkspacePanel = document.getElementById('logWorkspacePanel');

    function setMaintenanceMapMode(enabled) {
        const shouldShowMaintenanceCanvas = enabled && activeMaintenanceSubtab === 'tasks';
        const shouldShowInvoicePreview = enabled && (activeMaintenanceSubtab === 'expenses' || activeMaintenanceSubtab === 'suppliers');
        const shouldShowLogWorkspace = activeTabName === 'navlog' || (enabled && activeMaintenanceSubtab === 'engine');
        const shouldHideMap = shouldShowMaintenanceCanvas || shouldShowInvoicePreview || shouldShowLogWorkspace;

        if (mapContainer) {
            mapContainer.style.display = shouldHideMap ? 'none' : '';
        }
        if (maintenanceMapPanel) {
            maintenanceMapPanel.style.display = shouldShowMaintenanceCanvas ? 'block' : 'none';
        }
        if (maintenanceInvoicePreviewPanel) {
            maintenanceInvoicePreviewPanel.style.display = shouldShowInvoicePreview ? 'block' : 'none';
        }
        if (logWorkspacePanel) {
            logWorkspacePanel.style.display = shouldShowLogWorkspace ? 'block' : 'none';
        }

        if (shouldShowMaintenanceCanvas) {
            renderMaintenanceBoard();
            return;
        }

        if (shouldShowInvoicePreview) {
            if (activeMaintenanceSubtab === 'suppliers') {
                renderMaintenanceSupplierDetailPanel();
            } else {
                renderMaintenanceExpenseDetailPanel();
            }
        }

        if (shouldShowLogWorkspace) {
            renderLogWorkspacePanel();
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
            setCloudStatus(
                t(
                    'Accès verrouillé: seul l\'onglet Cloud est accessible (connexion / création de compte).',
                    'Acceso bloqueado: solo la pestaña Cloud es accesible (conexión / creación de cuenta).'
                ),
                true
            );
            tabName = 'cloud';
        }

        activeTabName = tabName;
        const isRouting = tabName === 'routing';
        const isRoutes = tabName === 'routes';
        const isCloud = tabName === 'cloud';
        const isNavLog = tabName === 'navlog';
        const isWeather = tabName === 'weather';
        const isArrival = tabName === 'arrival';
        const isWaypoint = tabName === 'waypoint';
        const isMaintenance = tabName === 'maintenance';

        routingTabBtn.classList.toggle('active', isRouting);
        routesTabBtn.classList.toggle('active', isRoutes);
        cloudTabBtn.classList.toggle('active', isCloud);
        navLogTabBtn.classList.toggle('active', isNavLog);
        weatherTabBtn.classList.toggle('active', isWeather);
        arrivalTabBtn.classList.toggle('active', isArrival);
        waypointTabBtn.classList.toggle('active', isWaypoint);
        maintenanceTabBtn.classList.toggle('active', isMaintenance);

        routingTab.classList.toggle('active', isRouting);
        routesTab.classList.toggle('active', isRoutes);
        cloudTab.classList.toggle('active', isCloud);
        navLogTab.classList.toggle('active', isNavLog);
        weatherTab.classList.toggle('active', isWeather);
        arrivalTab.classList.toggle('active', isArrival);
        waypointTab.classList.toggle('active', isWaypoint);
        maintenanceTab.classList.toggle('active', isMaintenance);
        setMaintenanceMapMode(isMaintenance);

        if (waypointDockPanel) {
            waypointDockPanel.classList.toggle('is-visible', isWaypoint && !isMaintenance);
        }

        if (isWaypoint) {
            renderWaypointPhotoList();
        }

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
    const waypointImportGooglePhotosBtn = document.getElementById('waypointImportGooglePhotosBtn');
    const googlePhotosCloseBtn = document.getElementById('googlePhotosCloseBtn');
    const googlePhotosLoadMoreBtn = document.getElementById('googlePhotosLoadMoreBtn');
    const googlePhotosPickerGrid = document.getElementById('googlePhotosPickerGrid');
    const googlePhotosPickerModal = document.getElementById('googlePhotosPickerModal');
    if (waypointQuickCaptureBtn && waypointQuickCaptureInput) {
        waypointQuickCaptureBtn.addEventListener('click', () => waypointQuickCaptureInput.click());
        waypointQuickCaptureInput.addEventListener('change', handleQuickWaypointCaptureChange);
    }

    if (waypointImportGooglePhotosBtn) {
        waypointImportGooglePhotosBtn.addEventListener('click', () => {
            void openGooglePhotosPickerModal();
        });
    }

    if (googlePhotosCloseBtn) {
        googlePhotosCloseBtn.addEventListener('click', () => closeGooglePhotosPickerModal());
    }

    if (googlePhotosPickerModal) {
        googlePhotosPickerModal.addEventListener('click', event => {
            if (event.target === googlePhotosPickerModal) {
                closeGooglePhotosPickerModal();
            }
        });
    }

    if (googlePhotosLoadMoreBtn) {
        googlePhotosLoadMoreBtn.addEventListener('click', async () => {
            if (!googlePhotosPickerNextPageToken) return;
            googlePhotosLoadMoreBtn.disabled = true;
            setGooglePhotosPickerStatus(t('Chargement des photos...', 'Cargando fotos...'));

            try {
                const { items, nextPageToken } = await fetchGooglePhotosMediaItems(googlePhotosPickerNextPageToken);
                googlePhotosPickerItems = [...googlePhotosPickerItems, ...items];
                googlePhotosPickerNextPageToken = nextPageToken;
                renderGooglePhotosPickerGrid(googlePhotosPickerItems);
                googlePhotosLoadMoreBtn.style.display = googlePhotosPickerNextPageToken ? '' : 'none';
                setGooglePhotosPickerStatus(t(`Photos disponibles: ${googlePhotosPickerItems.length}`, `Fotos disponibles: ${googlePhotosPickerItems.length}`));
            } catch (error) {
                setGooglePhotosPickerStatus(`${t('Chargement impossible', 'Carga imposible')}: ${String(error?.message || error)}`, true);
            } finally {
                googlePhotosLoadMoreBtn.disabled = false;
            }
        });
    }

    if (googlePhotosPickerGrid) {
        googlePhotosPickerGrid.addEventListener('click', event => {
            const target = event.target;
            if (!(target instanceof HTMLElement)) return;
            const button = target.closest('.google-photos-item');
            if (!button) return;

            const indexRaw = button.getAttribute('data-photo-index');
            const index = Number(indexRaw);
            if (!Number.isInteger(index) || index < 0 || index >= googlePhotosPickerItems.length) return;

            void importGooglePhotoItemIntoWaypoint(googlePhotosPickerItems[index]).catch(error => {
                setGooglePhotosPickerStatus(`${t('Import photo impossible', 'Importacion de foto imposible')}: ${String(error?.message || error)}`, true);
            });
        });
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

    const navLogOpenCreateBtn = document.getElementById('navLogOpenCreateBtn');
    if (navLogOpenCreateBtn) {
        navLogOpenCreateBtn.addEventListener('click', openNavLogCreateForm);
    }

    const addManualNavLogBtn = document.getElementById('addManualNavLogBtn');
    if (addManualNavLogBtn) {
        addManualNavLogBtn.addEventListener('click', addManualNavigationLogEntry);
    }

    const cancelNavLogEditBtn = document.getElementById('cancelNavLogEditBtn');
    if (cancelNavLogEditBtn) {
        cancelNavLogEditBtn.addEventListener('click', cancelNavLogEdit);
    }

    const saveEngineLogBtn = document.getElementById('saveEngineLogBtn');
    if (saveEngineLogBtn) {
        saveEngineLogBtn.addEventListener('click', addEngineLogEntryFromForm);
    }

    const engineLogOpenCreateBtn = document.getElementById('engineLogOpenCreateBtn');
    if (engineLogOpenCreateBtn) {
        engineLogOpenCreateBtn.addEventListener('click', openEngineLogCreateForm);
    }

    const cancelEngineLogEditBtn = document.getElementById('cancelEngineLogEditBtn');
    if (cancelEngineLogEditBtn) {
        cancelEngineLogEditBtn.addEventListener('click', cancelEngineLogEdit);
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

    setWeatherApiConfigVisibility(false, t('API météo connectée.', 'API meteo conectada.'));

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
    const cloudAccountSubtabBtn = document.getElementById('cloudAccountSubtabBtn');
    if (cloudAccountSubtabBtn) {
        cloudAccountSubtabBtn.addEventListener('click', () => setActiveCloudSubtab('account'));
    }

    const cloudPasswordSubtabBtn = document.getElementById('cloudPasswordSubtabBtn');
    if (cloudPasswordSubtabBtn) {
        cloudPasswordSubtabBtn.addEventListener('click', () => setActiveCloudSubtab('password'));
    }

    const cloudStatsSubtabBtn = document.getElementById('cloudStatsSubtabBtn');
    if (cloudStatsSubtabBtn) {
        cloudStatsSubtabBtn.addEventListener('click', () => {
            setActiveCloudSubtab('stats');
            void refreshCloudStatsTableCounts({ force: true });
        });
    }

    const cloudUsersSubtabBtn = document.getElementById('cloudUsersSubtabBtn');
    if (cloudUsersSubtabBtn) {
        cloudUsersSubtabBtn.addEventListener('click', async () => {
            setActiveCloudSubtab('users');
            if (cloudClient && cloudAuthUser && isCloudAdmin()) {
                await fetchCloudManagedUsers();
                renderCloudUsersList();
            }
        });
    }

    const cloudChangePasswordBtn = document.getElementById('cloudChangePasswordBtn');
    if (cloudChangePasswordBtn) {
        cloudChangePasswordBtn.addEventListener('click', async () => {
            if (!cloudClient || !cloudAuthUser) {
                setCloudPasswordStatus(t('Connecte-toi pour changer ton mot de passe.', 'Inicia sesión para cambiar tu contraseña.'), true);
                return;
            }

            const nextPassword = String(document.getElementById('cloudNewPasswordInput')?.value || '').trim();
            const confirmPassword = String(document.getElementById('cloudConfirmPasswordInput')?.value || '').trim();

            if (!nextPassword || !confirmPassword) {
                setCloudPasswordStatus(t('Renseigne les 2 champs mot de passe.', 'Completa los 2 campos de contraseña.'), true);
                return;
            }

            if (nextPassword.length < 8) {
                setCloudPasswordStatus(t('Mot de passe trop court (minimum 8 caractères).', 'Contraseña demasiado corta (mínimo 8 caracteres).'), true);
                return;
            }

            if (nextPassword !== confirmPassword) {
                setCloudPasswordStatus(t('Les mots de passe ne correspondent pas.', 'Las contraseñas no coinciden.'), true);
                return;
            }

            try {
                const { error } = await cloudClient.auth.updateUser({ password: nextPassword });
                if (error) throw error;
                const newPasswordInput = document.getElementById('cloudNewPasswordInput');
                const confirmPasswordInput = document.getElementById('cloudConfirmPasswordInput');
                if (newPasswordInput) newPasswordInput.value = '';
                if (confirmPasswordInput) confirmPasswordInput.value = '';
                setCloudPasswordStatus(t('Mot de passe mis à jour.', 'Contraseña actualizada.'));
            } catch (error) {
                setCloudPasswordStatus(t(`Mise à jour impossible: ${formatCloudError(error)}`, `Actualización imposible: ${formatCloudError(error)}`), true);
            }
        });
    }

    const cloudAdminCreateUserBtn = document.getElementById('cloudAdminCreateUserBtn');
    if (cloudAdminCreateUserBtn) {
        cloudAdminCreateUserBtn.addEventListener('click', async () => {
            if (!cloudClient || !cloudAuthUser) {
                setCloudUserManagementStatus(t('Connecte-toi d\'abord.', 'Conéctate primero.'), true);
                return;
            }
            if (!isCloudAdmin()) {
                setCloudUserManagementStatus(t('Action réservée aux administrateurs.', 'Acción reservada a administradores.'), true);
                return;
            }

            const email = normalizeEmailForCompare(document.getElementById('cloudAdminUserEmailInput')?.value || '');
            const name = String(document.getElementById('cloudAdminUserNameInput')?.value || '').trim();
            const role = normalizeCloudUserRole(document.getElementById('cloudAdminUserRoleInput')?.value || 'utilisateur');

            if (!email || !email.includes('@')) {
                setCloudUserManagementStatus(t('Email invalide.', 'Email inválido.'), true);
                return;
            }

            try {
                await upsertAllowedUserRecord({ email, name, role });

                setCloudUserManagementStatus(t(`Utilisateur enregistré: ${email}`, `Usuario guardado: ${email}`));
                await loadCloudUserProfile();
                await fetchCloudManagedUsers();
                renderCloudUsersList();

                const emailInput = document.getElementById('cloudAdminUserEmailInput');
                const nameInput = document.getElementById('cloudAdminUserNameInput');
                if (emailInput) emailInput.value = '';
                if (nameInput) nameInput.value = '';
            } catch (error) {
                setCloudUserManagementStatus(t(`Création impossible: ${formatCloudError(error)}`, `Creación imposible: ${formatCloudError(error)}`), true);
            }
        });
    }

    if (cloudEmailSignInBtn) {
        cloudEmailSignInBtn.addEventListener('click', async () => {
            setCloudAuthStatus(t('Connexion en cours...', 'Conexión en curso...'));

            if (!cloudClient) {
                const config = readCloudConfigFromForm();
                const connected = await connectCloud(config);
                if (!connected) {
                    setCloudAuthStatus(buildCloudConnectFailureMessage(config), true);
                    return;
                }
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
                await loadCloudUserProfile();
                if (isCloudAdmin()) {
                    await fetchCloudManagedUsers();
                }
                updateCloudAuthUi();
                activateTab('routes');
            } catch (error) {
                setCloudAuthStatus(t(`Connexion email impossible: ${formatCloudError(error)}`, `Conexión email imposible: ${formatCloudError(error)}`), true);
            }
        });
    }

    const cloudEmailSignUpBtn = document.getElementById('cloudEmailSignUpBtn');
    if (cloudEmailSignUpBtn) {
        cloudEmailSignUpBtn.addEventListener('click', async () => {
            setCloudAuthStatus(t('Création compte en cours...', 'Creación cuenta en curso...'));

            if (!cloudClient) {
                const config = readCloudConfigFromForm();
                const connected = await connectCloud(config);
                if (!connected) {
                    setCloudAuthStatus(buildCloudConnectFailureMessage(config), true);
                    return;
                }
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

            const emailVerdict = await checkCloudEmailAllowed(email);
            if (!emailVerdict.allowed) {
                setCloudAuthStatus(t(`Email non autorisé (${email}). Demande à un administrateur de te créer dans ${CLOUD_ALLOWED_USERS_TABLE}.`, `Email no autorizado (${email}). Pide a un administrador que te cree en ${CLOUD_ALLOWED_USERS_TABLE}.`), true);
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
                cloudUserProfile = null;
                cloudManagedUsers = [];
                updateCloudAuthUi();
            } catch (error) {
                setCloudAuthStatus(t(`Déconnexion impossible: ${formatCloudError(error)}`, `Desconexión imposible: ${formatCloudError(error)}`), true);
            }
        });
    }

    setActiveCloudSubtab('account');
    renderCloudUsersList();

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
        const routeNameInput = document.getElementById('routeNameInput');
        if (routeNameInput) routeNameInput.value = '';
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
    const reverseRouteBtn = document.getElementById('reverseRouteBtn');
    if (reverseRouteBtn) {
        reverseRouteBtn.addEventListener('click', () => {
            reverseCurrentRoute();
        });
    }
    document.getElementById('exportRouteBtn').addEventListener('click', () => {
        if (currentLoadedRouteIndex >= 0) {
            exportRoute(currentLoadedRouteIndex);
        } else {
            alert(t('Aucune route chargée', 'No hay ruta cargada'));
        }
    });
    document.getElementById('exportRouteGpxBtn').addEventListener('click', () => {
        if (currentLoadedRouteIndex >= 0) {
            exportRouteGpx(currentLoadedRouteIndex);
        } else {
            alert(t('Aucune route chargée', 'No hay ruta cargada'));
        }
    });
    const exportVoyagePdfBtn = document.getElementById('exportVoyagePdfBtn');
    if (exportVoyagePdfBtn) {
        exportVoyagePdfBtn.addEventListener('click', exportVoyagePdfReport);
    }
    document.getElementById('importRouteInput').addEventListener('change', handleImport);

    // Routes subtabs
    const routesManageSubtabBtn = document.getElementById('routesManageSubtabBtn');
    const routesImportExportSubtabBtn = document.getElementById('routesImportExportSubtabBtn');
    const routesToolsSubtabBtn = document.getElementById('routesToolsSubtabBtn');
    if (routesManageSubtabBtn) {
        routesManageSubtabBtn.addEventListener('click', () => setActiveRoutesSubtab('manage'));
    }
    if (routesImportExportSubtabBtn) {
        routesImportExportSubtabBtn.addEventListener('click', () => setActiveRoutesSubtab('importexport'));
    }
    if (routesToolsSubtabBtn) {
        routesToolsSubtabBtn.addEventListener('click', () => setActiveRoutesSubtab('tools'));
    }
    setActiveRoutesSubtab('manage');

    // Route search input
    const routeSearchInput = document.getElementById('routeSearchInput');
    if (routeSearchInput) {
        routeSearchInput.addEventListener('input', (e) => {
            routesSearchTerm = e.target.value;
            refreshSavedList();
        });
    }

    const waypointSearchInput = document.getElementById('waypointSearchInput');
    if (waypointSearchInput) {
        waypointSearchInput.addEventListener('input', (e) => {
            waypointSearchTerm = String(e.target?.value || '');
            renderWaypointPhotoList();
        });
    }

    const cloudRefreshBtn = document.getElementById('cloudRefreshBtn');
    if (cloudRefreshBtn) {
        cloudRefreshBtn.addEventListener('click', async () => {
            if (!isCloudReady()) {
                setCloudStatus(t('Cloud non connecté', 'Nube no conectada'), true);
                return;
            }

            try {
                await autoPullRoutesFromCloud('manual');
                void refreshCloudStatsTableCounts({ force: true });
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

    // Auto-save distance after routing
    if (routePoints.length > 1 && Number.isInteger(currentLoadedRouteIndex) && currentLoadedRouteIndex >= 0) {
        const allSavedRoutes = getSavedRoutes();
        if (currentLoadedRouteIndex < allSavedRoutes.length) {
            const routeToUpdate = allSavedRoutes[currentLoadedRouteIndex];
            if (routeToUpdate && Number.isFinite(totalDistance)) {
                // Update the route with the computed distance
                routeToUpdate.totalDistanceNm = Number(totalDistance.toFixed(2));
                routeToUpdate.updatedAt = new Date().toISOString();
                setSavedRoutes(allSavedRoutes);
                
                // Refresh the UI to show updated distance
                setTimeout(() => refreshSavedList(), 100);
                
                updateCloudDataSourceStatus('local (non synchronisé)', allSavedRoutes.length, waypointPhotoEntries.length);
                
                // Sync to cloud if available
                if (isCloudReady()) {
                    pushRoutesToCloud().catch(error => {
                        updateCloudDataSourceStatus('cache local (synchro en échec)', allSavedRoutes.length, waypointPhotoEntries.length);
                    });
                }
            }
        }
    }

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
    const parsedDistance = Number(route?.totalDistanceNm);
    const totalDistanceNm = Number.isFinite(parsedDistance) ? parsedDistance : null;

    return {
        name,
        date,
        time,
        tackingTimeHours: tacking,
        points,
        totalDistanceNm,
        creatorEmail: normalizeEmailForCompare(route?.creatorEmail || ''),
        creatorName: String(route?.creatorName || '').trim(),
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
    cloudLastStatusMessage = String(message || '');
    status.textContent = message;
    status.style.color = isError ? '#ff8f8f' : '';
    renderCloudStatsTable();
}

function getLocalizedCloudSourceLabel(sourceLabel) {
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
        'cache local (synchro en échec)': t('cache local (synchro en échec)', 'caché local (sincronización fallida)'),
        'cloud (routes v2)': t('cloud (routes v2)', 'nube (rutas v2)')
    };
    return sourceLabelMap[safeSource] || safeSource;
}

function estimateCloudPayloadSizeBytes() {
    const payload = {
        version: 5,
        routes: getSavedRoutes(),
        maintenanceBoards,
        maintenanceExpenses,
        maintenanceSuppliers,
        navLogEntries,
        engineLogEntries
    };

    try {
        return new TextEncoder().encode(JSON.stringify(payload)).length;
    } catch (_error) {
        return 0;
    }
}

function formatStorageSize(bytes) {
    const safeBytes = Math.max(0, Number(bytes) || 0);
    const kb = safeBytes / 1024;
    const mb = safeBytes / (1024 * 1024);

    if (mb >= 1) return `${mb.toFixed(2)} Mo`;
    if (kb >= 1) return `${kb.toFixed(1)} Ko`;
    return `${safeBytes} o`;
}

function isMissingCloudTableError(error) {
    const code = String(error?.code || '').toUpperCase();
    const message = String(error?.message || '').toLowerCase();
    return code === '42P01' || message.includes('does not exist');
}

function isMissingCloudColumnError(error) {
    const code = String(error?.code || '').toUpperCase();
    const message = String(error?.message || '').toLowerCase();
    return code === '42703' || message.includes('column') && message.includes('does not exist');
}

async function countCloudTableRows(tableName, options = {}) {
    if (!isCloudReady()) return null;

    let query = cloudClient
        .from(tableName)
        .select('*', { count: 'exact', head: true });

    if (options?.eq && typeof options.eq === 'object') {
        Object.entries(options.eq).forEach(([column, value]) => {
            query = query.eq(column, value);
        });
    }

    const { count, error } = await query;
    if (error) {
        if (isMissingCloudTableError(error)) return null;
        if (isMissingCloudColumnError(error)) return null;
        throw error;
    }

    return Number.isFinite(count) ? Number(count) : 0;
}

async function refreshCloudStatsTableCounts({ force = false } = {}) {
    if (!isCloudReady()) {
        cloudTableStatsRemoteCounts = null;
        renderCloudStatsTable();
        return null;
    }

    const now = Date.now();
    if (!force && cloudTableStatsRemoteCounts && (now - cloudTableStatsLastRefreshAtMs) < CLOUD_STATS_REFRESH_TTL_MS) {
        return cloudTableStatsRemoteCounts;
    }

    if (cloudTableStatsRefreshPromise) {
        return cloudTableStatsRefreshPromise;
    }

    cloudTableStatsRefreshPromise = (async () => {
        try {
            const projectId = await resolveCloudProjectIdUuid();
            const creatorEmail = getCurrentCloudUserEmail();
            const creatorFilter = creatorEmail ? { eq: { creator_email: creatorEmail } } : {};
            const counts = {
                projects: null,
                allowedUsers: null,
                routes: null,
                routePoints: null,
                waypointPhotos: null,
                maintenanceSchemas: null,
                maintenancePins: null,
                maintenanceSuppliers: null,
                maintenanceExpenses: null,
                navLog: null,
                engineLog: null
            };

            counts.projects = await countCloudTableRows(CLOUD_PROJECTS_TABLE, projectId ? { eq: { id: projectId } } : {});
            counts.allowedUsers = await countCloudTableRows(CLOUD_ALLOWED_USERS_TABLE);
            counts.routes = await countCloudTableRows(CLOUD_ROUTES_TABLE, creatorFilter);
            // route_points is scoped through routes via route_id (no direct user linkage here).
            counts.routePoints = await countCloudTableRows(CLOUD_ROUTE_POINTS_TABLE);
            counts.waypointPhotos = await countCloudTableRows(CLOUD_WAYPOINT_PHOTOS_TABLE, creatorFilter);
            counts.maintenanceSchemas = await countCloudTableRows(CLOUD_MAINTENANCE_SCHEMAS_TABLE, creatorFilter);
            counts.maintenancePins = await countCloudTableRows(CLOUD_MAINTENANCE_PINS_TABLE, creatorFilter);
            counts.maintenanceSuppliers = await countCloudTableRows(CLOUD_MAINTENANCE_SUPPLIERS_TABLE, creatorFilter);
            counts.maintenanceExpenses = await countCloudTableRows(CLOUD_MAINTENANCE_EXPENSES_TABLE, creatorFilter);
            counts.navLog = await countCloudTableRows(CLOUD_NAV_LOG_TABLE, creatorFilter);
            counts.engineLog = await countCloudTableRows(CLOUD_ENGINE_LOG_TABLE, creatorFilter);

            cloudTableStatsRemoteCounts = counts;
            cloudTableStatsLastRefreshAtMs = Date.now();
            renderCloudStatsTable();
            return counts;
        } catch (_error) {
            // Keep local fallback stats if cloud counting fails.
            return null;
        } finally {
            cloudTableStatsRefreshPromise = null;
        }
    })();

    return cloudTableStatsRefreshPromise;
}

function renderCloudStatsTable() {
    const sourceValue = document.getElementById('cloudStatsSourceValue');
    if (!sourceValue) return;

    if (!isCloudReady()) {
        cloudTableStatsRemoteCounts = null;
    }

    const localFallbackCounts = {
        projects: cloudConfig?.projectKey ? 1 : 0,
        allowedUsers: Array.isArray(cloudManagedUsers) ? cloudManagedUsers.length : 0,
        routes: getSavedRoutes().length,
        routePoints: getSavedRoutes().reduce((acc, route) => {
            const points = Array.isArray(route?.points) ? route.points.length : 0;
            return acc + points;
        }, 0),
        waypointPhotos: waypointPhotoEntries.length,
        maintenanceSchemas: maintenanceBoards.length,
        maintenancePins: maintenanceBoards.reduce((acc, board) => {
            const pins = Array.isArray(board?.annotations) ? board.annotations.length : 0;
            return acc + pins;
        }, 0),
        maintenanceSuppliers: maintenanceSuppliers.length,
        maintenanceExpenses: maintenanceExpenses.length,
        navLog: navLogEntries.length,
        engineLog: engineLogEntries.length
    };

    const remoteCounts = cloudTableStatsRemoteCounts || {};
    const resolvedCounts = {
        projects: Number.isFinite(remoteCounts.projects) ? remoteCounts.projects : localFallbackCounts.projects,
        allowedUsers: Number.isFinite(remoteCounts.allowedUsers) ? remoteCounts.allowedUsers : localFallbackCounts.allowedUsers,
        routes: Number.isFinite(remoteCounts.routes) ? remoteCounts.routes : localFallbackCounts.routes,
        routePoints: Number.isFinite(remoteCounts.routePoints) ? remoteCounts.routePoints : localFallbackCounts.routePoints,
        waypointPhotos: Number.isFinite(remoteCounts.waypointPhotos) ? remoteCounts.waypointPhotos : localFallbackCounts.waypointPhotos,
        maintenanceSchemas: Number.isFinite(remoteCounts.maintenanceSchemas) ? remoteCounts.maintenanceSchemas : localFallbackCounts.maintenanceSchemas,
        maintenancePins: Number.isFinite(remoteCounts.maintenancePins) ? remoteCounts.maintenancePins : localFallbackCounts.maintenancePins,
        maintenanceSuppliers: Number.isFinite(remoteCounts.maintenanceSuppliers) ? remoteCounts.maintenanceSuppliers : localFallbackCounts.maintenanceSuppliers,
        maintenanceExpenses: Number.isFinite(remoteCounts.maintenanceExpenses) ? remoteCounts.maintenanceExpenses : localFallbackCounts.maintenanceExpenses,
        navLog: Number.isFinite(remoteCounts.navLog) ? remoteCounts.navLog : localFallbackCounts.navLog,
        engineLog: Number.isFinite(remoteCounts.engineLog) ? remoteCounts.engineLog : localFallbackCounts.engineLog
    };

    const totalCount = Object.values(resolvedCounts).reduce((sum, value) => sum + (Number.isFinite(value) ? value : 0), 0);
    const storageBytes = estimateCloudPayloadSizeBytes();
    const storageQuotaBytes = 500 * 1024 * 1024;
    const storageQuotaPercent = Math.min(999, (storageBytes / storageQuotaBytes) * 100);

    sourceValue.textContent = getLocalizedCloudSourceLabel(cloudDataSourceLabel);

    const setText = (id, value) => {
        const node = document.getElementById(id);
        if (node) node.textContent = String(value);
    };

    setText('cloudStatsProjectsValue', resolvedCounts.projects);
    setText('cloudStatsAllowedUsersValue', resolvedCounts.allowedUsers);
    setText('cloudStatsRoutesValue', resolvedCounts.routes);
    setText('cloudStatsRoutePointsValue', resolvedCounts.routePoints);
    setText('cloudStatsPhotosValue', resolvedCounts.waypointPhotos);
    setText('cloudStatsMaintenanceSchemasValue', resolvedCounts.maintenanceSchemas);
    setText('cloudStatsMaintenancePinsValue', resolvedCounts.maintenancePins);
    setText('cloudStatsSuppliersValue', resolvedCounts.maintenanceSuppliers);
    setText('cloudStatsExpensesValue', resolvedCounts.maintenanceExpenses);
    setText('cloudStatsNavValue', resolvedCounts.navLog);
    setText('cloudStatsEngineValue', resolvedCounts.engineLog);
    setText('cloudStatsTotalValue', totalCount);
    setText('cloudStatsStorageValue', formatStorageSize(storageBytes));
    setText('cloudStatsQuotaValue', `${storageQuotaPercent.toFixed(2)} %`);

    if (isCloudReady()) {
        void refreshCloudStatsTableCounts();
    }
}

function updateCloudDataSourceStatus(sourceLabel, routeCount = null, photoCount = null) {
    const status = document.getElementById('cloudDataSourceStatus');
    if (!status) return;

    cloudDataSourceLabel = String(sourceLabel || 'inconnu');
    const safeSourceLocalized = getLocalizedCloudSourceLabel(cloudDataSourceLabel);
    const routesLabel = Number.isFinite(routeCount) ? routeCount : getSavedRoutes().length;
    const photosLabel = Number.isFinite(photoCount) ? photoCount : waypointPhotoEntries.length;
    status.textContent = `${t('Données routes/photos/maintenance', 'Datos rutas/fotos/mantenimiento')}: ${safeSourceLocalized} · ${t('routes', 'rutas')}: ${routesLabel} · ${t('photos', 'fotos')}: ${photosLabel} · ${t('schémas', 'esquemas')}: ${maintenanceBoards.length} · ${t('dépenses', 'gastos')}: ${maintenanceExpenses.length} · ${t('fournisseurs', 'proveedores')}: ${maintenanceSuppliers.length}`;
    renderCloudStatsTable();
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
    if (!isCloudReady() || cloudAutoPullInFlight || cloudLogbookPushInFlight || cloudLogbookPushTimer) return false;
    cloudAutoPullInFlight = true;

    try {
        const routes = await pullRoutesFromCloud({
            allowMaintenanceOverwrite: trigger === 'manual'
        });
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

function buildRoutesFromCloudV2Rows(routeRows, pointRows) {
    const pointsByRouteId = new Map();

    (Array.isArray(pointRows) ? pointRows : []).forEach(point => {
        const routeId = String(point?.route_id || point?.routeId || '');
        if (!routeId) return;

        const lat = Number(point?.lat);
        const lon = Number(point?.lon ?? point?.lng);
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;

        if (!pointsByRouteId.has(routeId)) {
            pointsByRouteId.set(routeId, []);
        }

        pointsByRouteId.get(routeId).push({
            seq: Number(point?.seq),
            lat,
            lon
        });
    });

    return (Array.isArray(routeRows) ? routeRows : []).map((row, index) => {
        const routeId = String(row?.id || row?.route_id || row?.routeId || '');
        const points = (pointsByRouteId.get(routeId) || [])
            .sort((a, b) => {
                const aSeq = Number.isFinite(a.seq) ? a.seq : 0;
                const bSeq = Number.isFinite(b.seq) ? b.seq : 0;
                return aSeq - bSeq;
            })
            .map(point => ({ lat: point.lat, lon: point.lon }));

        return sanitizeSavedRoute({
            name: row?.name,
            date: row?.departure_date ?? row?.date,
            time: row?.departure_time ?? row?.time,
            tackingTimeHours: row?.tacking_time_hours ?? row?.tackingTimeHours,
            totalDistanceNm: row?.total_distance_nm ?? row?.totalDistanceNm,
            creatorEmail: row?.creator_email ?? row?.creatorEmail,
            creatorName: row?.creator_name ?? row?.creatorName,
            createdAt: row?.created_at ?? row?.createdAt,
            updatedAt: row?.updated_at ?? row?.updatedAt,
            points
        }, index);
    });
}

function extractMissingColumnName(error) {
    const message = String(error?.message || '');
    const details = String(error?.details || '');
    const hint = String(error?.hint || '');
    const combined = `${message}\n${details}\n${hint}`;

    const patterns = [
        /could not find the '([a-zA-Z0-9_]+)' column/i,
        /column\s+"?([a-zA-Z0-9_]+)"?\s+of\s+relation\s+"?[a-zA-Z0-9_]+"?\s+does not exist/i,
        /column\s+"?([a-zA-Z0-9_]+)"?\s+does not exist/i,
        /unknown column\s+"?([a-zA-Z0-9_]+)"?/i
    ];

    for (const pattern of patterns) {
        const match = combined.match(pattern);
        if (match && match[1]) return String(match[1]).trim();
    }

    return '';
}

function extractNotNullColumnName(error) {
    const code = String(error?.code || '').toUpperCase();
    const message = String(error?.message || '');
    const details = String(error?.details || '');
    const combined = `${message}\n${details}`;

    if (code !== '23502' && !/not-null|null value/i.test(combined)) {
        return '';
    }

    const patterns = [
        /null value in column\s+"?([a-zA-Z0-9_]+)"?\s+of\s+relation/i,
        /violates not-null constraint.*column\s+"?([a-zA-Z0-9_]+)"?/i
    ];

    for (const pattern of patterns) {
        const match = combined.match(pattern);
        if (match && match[1]) return String(match[1]).trim();
    }

    return '';
}

function isUuidString(value) {
    const raw = String(value || '').trim();
    return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(raw);
}

async function resolveCloudProjectIdUuid() {
    if (!isCloudReady()) return '';
    if (isUuidString(cloudResolvedProjectIdUuid)) return cloudResolvedProjectIdUuid;

    const projectKey = String(cloudConfig?.projectKey || '').trim();
    if (!projectKey) return '';

    if (isUuidString(projectKey)) {
        cloudResolvedProjectIdUuid = projectKey;
        return cloudResolvedProjectIdUuid;
    }

    const text = String(projectKey || 'ceibo-main');
    let h1 = 0x811c9dc5;
    let h2 = 0x01000193;
    let h3 = 0x9e3779b9;
    let h4 = 0x85ebca6b;

    for (let i = 0; i < text.length; i += 1) {
        const code = text.charCodeAt(i);
        h1 = Math.imul(h1 ^ code, 0x01000193) >>> 0;
        h2 = Math.imul(h2 ^ (code + i), 0x27d4eb2d) >>> 0;
        h3 = Math.imul(h3 ^ (code + i * 7), 0x165667b1) >>> 0;
        h4 = Math.imul(h4 ^ (code + i * 13), 0x9e3779b1) >>> 0;
    }

    const hex = [h1, h2, h3, h4]
        .map(n => n.toString(16).padStart(8, '0'))
        .join('')
        .split('');

    hex[12] = '4';
    hex[16] = ['8', '9', 'a', 'b'][parseInt(hex[16], 16) % 4];

    const normalized = hex.join('');
    cloudResolvedProjectIdUuid = `${normalized.slice(0, 8)}-${normalized.slice(8, 12)}-${normalized.slice(12, 16)}-${normalized.slice(16, 20)}-${normalized.slice(20, 32)}`;
    return cloudResolvedProjectIdUuid;
}

async function ensureCloudProjectRow(projectIdUuid) {
    const targetId = String(projectIdUuid || '').trim();
    if (!isUuidString(targetId)) {
        throw new Error('project_id UUID invalide');
    }

    const projectKey = String(cloudConfig?.projectKey || '').trim() || 'ceibo-main';

    const { data: existingRow, error: existingError } = await cloudClient
        .from(CLOUD_PROJECTS_TABLE)
        .select('id')
        .eq('id', targetId)
        .maybeSingle();

    if (!existingError && existingRow?.id) {
        return targetId;
    }

    if (existingError) {
        const code = String(existingError?.code || '').toUpperCase();
        const message = String(existingError?.message || '').toLowerCase();
        if (!(code === 'PGRST116' || message.includes('0 rows'))) {
            throw existingError;
        }
    }

    let insertPayload = {
        id: targetId,
        project_key: projectKey,
        name: projectKey
    };

    for (let attempt = 0; attempt < 12; attempt += 1) {
        const { error: insertError } = await cloudClient
            .from(CLOUD_PROJECTS_TABLE)
            .insert(insertPayload);

        if (!insertError) {
            return targetId;
        }

        const code = String(insertError?.code || '').toUpperCase();
        const message = String(insertError?.message || '').toLowerCase();
        if (code === '23505' || message.includes('duplicate key')) {
            return targetId;
        }

        const missingColumn = extractMissingColumnName(insertError);
        if (missingColumn && Object.prototype.hasOwnProperty.call(insertPayload, missingColumn)) {
            const { [missingColumn]: _removed, ...nextPayload } = insertPayload;
            insertPayload = nextPayload;
            continue;
        }

        const notNullColumn = extractNotNullColumnName(insertError);
        if (notNullColumn && !Object.prototype.hasOwnProperty.call(insertPayload, notNullColumn)) {
            if (notNullColumn === 'id') {
                insertPayload = { id: targetId, ...insertPayload };
                continue;
            }
            if (notNullColumn.includes('project')) {
                insertPayload = { [notNullColumn]: projectKey, ...insertPayload };
                continue;
            }
            if (notNullColumn.includes('name') || notNullColumn.includes('title') || notNullColumn.includes('label')) {
                insertPayload = { [notNullColumn]: projectKey, ...insertPayload };
                continue;
            }
        }

        throw insertError;
    }

    const { data: anyProjectRow, error: anyProjectError } = await cloudClient
        .from(CLOUD_PROJECTS_TABLE)
        .select('id')
        .limit(1)
        .maybeSingle();

    if (!anyProjectError && isUuidString(anyProjectRow?.id)) {
        cloudResolvedProjectIdUuid = String(anyProjectRow.id);
        return cloudResolvedProjectIdUuid;
    }

    throw new Error('Aucune ligne projects compatible pour satisfaire routes_project_id_fkey');
}

async function pullRoutesFromCloudV2() {
    if (!isCloudReady()) return [];
    const creatorEmail = getCurrentCloudUserEmail();
    if (!creatorEmail) return [];

    const { data: routeRows, error: routeError } = await cloudClient
        .from(CLOUD_ROUTES_TABLE)
        .select('*')
        .eq('creator_email', creatorEmail)
        .order('updated_at', { ascending: true });

    if (routeError) throw routeError;

    if (!Array.isArray(routeRows) || routeRows.length === 0) {
        return [];
    }

    const routeIds = routeRows
        .map(row => String(row?.id || ''))
        .filter(Boolean);

    if (routeIds.length === 0) {
        throw new Error('routes.id column missing or empty');
    }

    const { data: pointRows, error: pointError } = await cloudClient
        .from(CLOUD_ROUTE_POINTS_TABLE)
        .select('*')
        .in('route_id', routeIds)
        .order('seq', { ascending: true });

    if (pointError) throw pointError;

    return buildRoutesFromCloudV2Rows(routeRows, pointRows || []);
}

function parseWaypointProtectionFromCloud(rawProtection) {
    if (Array.isArray(rawProtection)) {
        return rawProtection.map(item => String(item || '').trim()).filter(Boolean);
    }

    if (typeof rawProtection === 'string') {
        const text = rawProtection.trim();
        if (!text) return [];

        try {
            const parsed = JSON.parse(text);
            if (Array.isArray(parsed)) {
                return parsed.map(item => String(item || '').trim()).filter(Boolean);
            }
        } catch (_error) {
            // keep CSV fallback below
        }

        return text.split(',').map(item => String(item || '').trim()).filter(Boolean);
    }

    return [];
}

function buildWaypointPhotosFromCloudRows(rows) {
    return (Array.isArray(rows) ? rows : [])
        .map(row => normalizeWaypointPhotoEntry({
            id: String(row?.id || ''),
            lat: row?.lat,
            lng: row?.lng,
            placeName: row?.place_name,
            imageDataUrl: row?.image_data_url,
            comment: row?.comment,
            rating: row?.rating,
            cleanliness: row?.cleanliness,
            protection: parseWaypointProtectionFromCloud(row?.protection),
            depthMeters: row?.depth_meters,
            bottomType: row?.bottom_type,
            creatorEmail: row?.creator_email,
            creatorName: row?.creator_name,
            createdAt: row?.created_at,
            updatedAt: row?.updated_at
        }))
        .filter(Boolean)
        .sort((a, b) => String(b.updatedAt || '').localeCompare(String(a.updatedAt || '')));
}

async function pullWaypointPhotosFromCloudV2() {
    if (!isCloudReady()) return [];
    const creatorEmail = getCurrentCloudUserEmail();
    if (!creatorEmail) return [];

    const { data: rows, error } = await cloudClient
        .from(CLOUD_WAYPOINT_PHOTOS_TABLE)
        .select('*')
        .eq('creator_email', creatorEmail)
        .order('updated_at', { ascending: false });

    if (error) throw error;
    return buildWaypointPhotosFromCloudRows(rows || []);
}

async function pushWaypointPhotosToCloudV2(entries) {
    if (!isCloudReady()) return false;

    const creatorEmail = getCurrentCloudUserEmail();
    if (!creatorEmail) return false;

    const safeEntries = (Array.isArray(entries) ? entries : [])
        .map(normalizeWaypointPhotoEntry)
        .filter(Boolean);

    let resolvedProjectIdUuid = await resolveCloudProjectIdUuid();
    if (!resolvedProjectIdUuid) {
        throw new Error('project_id introuvable pour waypoint_photos.');
    }
    resolvedProjectIdUuid = await ensureCloudProjectRow(resolvedProjectIdUuid);

    const { error: deleteError } = await cloudClient
        .from(CLOUD_WAYPOINT_PHOTOS_TABLE)
        .delete()
        .eq('creator_email', creatorEmail)
        .eq('project_id', resolvedProjectIdUuid);

    if (deleteError) throw deleteError;

    if (safeEntries.length === 0) {
        return true;
    }

    const nowIso = new Date().toISOString();
    const payload = safeEntries.map(entry => {
        const row = {
            lat: entry.lat,
            lng: entry.lng,
            place_name: entry.placeName || null,
            image_data_url: entry.imageDataUrl || null,
            comment: entry.comment || null,
            rating: entry.rating,
            cleanliness: entry.cleanliness,
            protection: Array.isArray(entry.protection) ? entry.protection : [],
            depth_meters: entry.depthMeters,
            bottom_type: entry.bottomType || null,
            creator_email: creatorEmail,
            creator_name: entry.creatorName || null,
            project_id: resolvedProjectIdUuid,
            created_at: entry.createdAt || nowIso,
            updated_at: entry.updatedAt || nowIso
        };

        if (isUuidString(entry.id)) {
            row.id = entry.id;
        }

        return row;
    });

    const { error: insertError } = await cloudClient
        .from(CLOUD_WAYPOINT_PHOTOS_TABLE)
        .insert(payload);

    if (insertError) throw insertError;
    return true;
}

function buildMaintenanceBoardsFromCloudRows(schemaRows, pinRows) {
    const safeSchemaRows = Array.isArray(schemaRows) ? schemaRows : [];
    const safePinRows = Array.isArray(pinRows) ? pinRows : [];
    const pinsBySchemaId = new Map();

    safePinRows.forEach((row, index) => {
        const schemaId = String(row?.schema_id || '').trim();
        if (!schemaId) return;
        if (!pinsBySchemaId.has(schemaId)) {
            pinsBySchemaId.set(schemaId, []);
        }

        const pin = {
            id: String(row?.id || `maintenance-ann-${Date.now()}-${index}`),
            xPercent: Number(row?.x_percent),
            yPercent: Number(row?.y_percent),
            colorKey: String(row?.color_key || 'red'),
            statusKey: String(row?.status_key || 'active'),
            legend: String(row?.legend || '').trim(),
            createdAt: String(row?.created_at || new Date().toISOString())
        };

        pinsBySchemaId.get(schemaId).push(pin);
    });

    return safeSchemaRows
        .map((row, index) => {
            const boardId = String(row?.id || `maintenance-${Date.now()}-${index}`);
            const board = sanitizeMaintenanceBoard({
                id: boardId,
                name: String(row?.name || ''),
                imageDataUrl: String(row?.image_data_url || ''),
                annotations: pinsBySchemaId.get(boardId) || [],
                creatorEmail: String(row?.creator_email || ''),
                creatorName: String(row?.creator_name || ''),
                createdAt: String(row?.created_at || new Date().toISOString()),
                updatedAt: String(row?.updated_at || new Date().toISOString())
            }, index);

            board.annotations.sort((a, b) => String(a.createdAt || '').localeCompare(String(b.createdAt || '')));
            return board;
        })
        .sort((a, b) => String(b.updatedAt || '').localeCompare(String(a.updatedAt || '')));
}

async function pullMaintenanceBoardsFromCloudV2() {
    if (!isCloudReady()) {
        throw new Error('Cloud non pret: impossible de lire maintenance_schemas/maintenance_pins.');
    }

    const creatorEmail = getCurrentCloudUserEmail();
    if (!creatorEmail) {
        throw new Error('creator_email manquant: impossible de lire maintenance_schemas/maintenance_pins.');
    }

    const resolvedProjectIdUuid = await resolveCloudProjectIdUuid();

    let schemasQuery = cloudClient
        .from(CLOUD_MAINTENANCE_SCHEMAS_TABLE)
        .select('*')
        .eq('creator_email', creatorEmail)
        .order('updated_at', { ascending: false });

    let pinsQuery = cloudClient
        .from(CLOUD_MAINTENANCE_PINS_TABLE)
        .select('*')
        .eq('creator_email', creatorEmail)
        .order('created_at', { ascending: true });

    if (isUuidString(resolvedProjectIdUuid)) {
        schemasQuery = schemasQuery.eq('project_id', resolvedProjectIdUuid);
        pinsQuery = pinsQuery.eq('project_id', resolvedProjectIdUuid);
    }

    const [{ data: schemaRows, error: schemaError }, { data: pinRows, error: pinError }] = await Promise.all([
        schemasQuery,
        pinsQuery
    ]);

    if (schemaError) throw schemaError;
    if (pinError) throw pinError;

    return buildMaintenanceBoardsFromCloudRows(schemaRows, pinRows);
}

async function pushMaintenanceBoardsToCloudV2(boards) {
    if (!isCloudReady()) {
        throw new Error('Cloud non pret: impossible d\'inserer maintenance_schemas/maintenance_pins.');
    }

    const creatorEmail = getCurrentCloudUserEmail();
    if (!creatorEmail) {
        throw new Error('creator_email manquant: impossible d\'inserer maintenance_schemas/maintenance_pins.');
    }

    let resolvedProjectIdUuid = await resolveCloudProjectIdUuid();
    if (!resolvedProjectIdUuid) {
        throw new Error('project_id introuvable pour maintenance_schemas/maintenance_pins.');
    }
    resolvedProjectIdUuid = await ensureCloudProjectRow(resolvedProjectIdUuid);

    const safeBoards = (Array.isArray(boards) ? boards : [])
        .map((board, index) => sanitizeMaintenanceBoard(board, index));
    const nowIso = new Date().toISOString();

    const { data: existingSchemaRows, error: existingSchemaError } = await cloudClient
        .from(CLOUD_MAINTENANCE_SCHEMAS_TABLE)
        .select('id')
        .eq('project_id', resolvedProjectIdUuid)
        .eq('creator_email', creatorEmail);
    if (existingSchemaError) throw existingSchemaError;

    const localSchemaIds = new Set(safeBoards.map(board => String(board.id || '').trim()).filter(Boolean));
    const remoteSchemaIds = new Set(
        (Array.isArray(existingSchemaRows) ? existingSchemaRows : [])
            .map(row => String(row?.id || '').trim())
            .filter(Boolean)
    );

    const schemaIdsToDelete = [...remoteSchemaIds].filter(schemaId => !localSchemaIds.has(schemaId));
    if (schemaIdsToDelete.length > 0) {
        // Never update schemas: deletion is explicit and cascades to pins.
        const { error: deleteSchemasError } = await cloudClient
            .from(CLOUD_MAINTENANCE_SCHEMAS_TABLE)
            .delete()
            .in('id', schemaIdsToDelete)
            .eq('project_id', resolvedProjectIdUuid)
            .eq('creator_email', creatorEmail);
        if (deleteSchemasError) throw deleteSchemasError;
    }

    const schemaPayload = safeBoards
        .filter(board => {
            const boardId = String(board.id || '').trim();
            return boardId && !remoteSchemaIds.has(boardId);
        })
        .map(board => ({
        id: String(board.id || `maintenance-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`),
        project_id: resolvedProjectIdUuid,
        creator_email: creatorEmail,
        creator_name: board.creatorName || null,
        name: String(board.name || '').trim(),
        image_data_url: String(board.imageDataUrl || ''),
        created_at: board.createdAt || nowIso,
        updated_at: board.updatedAt || nowIso
    }));

    if (schemaPayload.length > 0) {
        const { error: insertSchemasError } = await cloudClient
            .from(CLOUD_MAINTENANCE_SCHEMAS_TABLE)
            .insert(schemaPayload);
        if (insertSchemasError) throw insertSchemasError;
    }

    const localSchemaIdList = [...localSchemaIds];
    if (localSchemaIdList.length > 0) {
        // Pins remain editable: replace pins only for schemas currently in local state.
        const { error: deletePinsError } = await cloudClient
            .from(CLOUD_MAINTENANCE_PINS_TABLE)
            .delete()
            .eq('project_id', resolvedProjectIdUuid)
            .eq('creator_email', creatorEmail)
            .in('schema_id', localSchemaIdList);
        if (deletePinsError) throw deletePinsError;
    }

    const pinPayload = safeBoards.flatMap((board, boardIndex) => {
        const boardId = String(board.id || `maintenance-${Date.now()}-${boardIndex}`);
        return (Array.isArray(board.annotations) ? board.annotations : []).map((annotation, annotationIndex) => {
            const safeX = Number(annotation?.xPercent);
            const safeY = Number(annotation?.yPercent);
            return {
                id: String(annotation?.id || `maintenance-ann-${Date.now()}-${boardIndex}-${annotationIndex}`),
                schema_id: boardId,
                project_id: resolvedProjectIdUuid,
                creator_email: creatorEmail,
                creator_name: board.creatorName || null,
                x_percent: Number.isFinite(safeX) ? Math.max(0, Math.min(100, safeX)) : 0,
                y_percent: Number.isFinite(safeY) ? Math.max(0, Math.min(100, safeY)) : 0,
                color_key: getMaintenanceColorMeta(annotation?.colorKey).key,
                status_key: normalizeMaintenanceTaskStatus(annotation?.statusKey),
                legend: String(annotation?.legend || '').trim(),
                created_at: String(annotation?.createdAt || nowIso),
                updated_at: board.updatedAt || nowIso
            };
        });
    });

    if (!pinPayload.length) {
        return true;
    }

    const { error: insertPinsError } = await cloudClient
        .from(CLOUD_MAINTENANCE_PINS_TABLE)
        .insert(pinPayload);
    if (insertPinsError) throw insertPinsError;

    return true;
}

async function pushRoutesToCloudV2(routes) {
    if (!isCloudReady()) return false;

    const creatorEmail = getCurrentCloudUserEmail();
    if (!creatorEmail) return false;

    let resolvedProjectIdUuid = await resolveCloudProjectIdUuid();
    if (!resolvedProjectIdUuid) {
        throw new Error('project_id introuvable');
    }

    resolvedProjectIdUuid = await ensureCloudProjectRow(resolvedProjectIdUuid);

    const safeRoutes = Array.isArray(routes)
        ? routes.map((route, index) => sanitizeSavedRoute(route, index))
        : [];

    const { data: existingRouteRows, error: existingRouteError } = await cloudClient
        .from(CLOUD_ROUTES_TABLE)
        .select('id,name')
        .eq('creator_email', creatorEmail);

    if (existingRouteError) throw existingRouteError;

    const canonicalRouteName = value => String(value || '').trim().toLowerCase();
    const existingRows = Array.isArray(existingRouteRows) ? existingRouteRows : [];
    const existingByName = new Map();
    const duplicateRouteIdsToDelete = [];

    existingRows.forEach(row => {
        const id = String(row?.id || '').trim();
        if (!id) return;
        const key = canonicalRouteName(row?.name);
        if (!key) return;

        if (!existingByName.has(key)) {
            existingByName.set(key, {
                id,
                name: String(row?.name || '').trim()
            });
            return;
        }

        // Keep first match and clean duplicated names for deterministic update-by-name behavior.
        duplicateRouteIdsToDelete.push(id);
    });

    const localRouteNameKeys = new Set();

    for (const route of safeRoutes) {
        const routeName = String(route?.name || '').trim();
        if (!routeName) {
            throw new Error('Nom de route vide: synchronisation cloud refusée.');
        }
        const routeNameKey = canonicalRouteName(routeName);
        if (!routeNameKey) {
            throw new Error('Nom de route invalide: synchronisation cloud refusée.');
        }
        localRouteNameKeys.add(routeNameKey);

        const existingRow = existingByName.get(routeNameKey) || null;
        const routeId = existingRow?.id || generateClientUuid();
        const fallbackUpdatedAt = route.updatedAt || new Date().toISOString();
        const baseRoutePayload = {
            name: routeName,
            departure_date: route.date || null,
            departure_time: route.time || null,
            tacking_time_hours: Number.isFinite(route.tackingTimeHours) ? route.tackingTimeHours : null,
            total_distance_nm: Number.isFinite(route.totalDistanceNm) ? route.totalDistanceNm : null,
            creator_email: route.creatorEmail || creatorEmail,
            creator_name: route.creatorName || null,
            legacy_hash: null,
            legacy_payload: null,
            created_at: route.createdAt || fallbackUpdatedAt,
            updated_at: fallbackUpdatedAt
        };

        const routePayload = {
            id: routeId,
            project_id: resolvedProjectIdUuid,
            ...baseRoutePayload
        };

        if (existingRow) {
            const { error: updateRouteError } = await cloudClient
                .from(CLOUD_ROUTES_TABLE)
                .update({
                    project_id: routePayload.project_id,
                    ...baseRoutePayload
                })
                .eq('id', routeId);

            if (updateRouteError) throw updateRouteError;
        } else {
            const { error: insertRouteError } = await cloudClient
                .from(CLOUD_ROUTES_TABLE)
                .insert(routePayload);
            if (insertRouteError) throw insertRouteError;
            existingByName.set(routeNameKey, { id: routeId, name: routeName });
        }

        const { error: deleteExistingPointsError } = await cloudClient
            .from(CLOUD_ROUTE_POINTS_TABLE)
            .delete()
            .eq('route_id', routeId);

        if (deleteExistingPointsError) throw deleteExistingPointsError;

        const points = Array.isArray(route.points) ? route.points : [];
        if (!points.length) continue;

        const pointPayload = points
            .map((point, index) => {
                return {
                    route_id: routeId,
                    seq: index,
                    lat: Number(point?.lat),
                    lon: Number(point?.lon ?? point?.lng),
                    created_at: fallbackUpdatedAt
                };
            })
            .filter(point => Number.isFinite(point.lat) && Number.isFinite(point.lon));

        if (!pointPayload.length) continue;

        const pointsResult = await cloudClient
            .from(CLOUD_ROUTE_POINTS_TABLE)
            .insert(pointPayload);
        if (pointsResult.error) throw pointsResult.error;
    }

    const staleRouteIds = existingRows
        .filter(row => {
            const id = String(row?.id || '').trim();
            if (!id) return false;
            const key = canonicalRouteName(row?.name);
            return key && !localRouteNameKeys.has(key);
        })
        .map(row => String(row?.id || '').trim())
        .filter(Boolean);

    const routeIdsToDelete = [...new Set([...duplicateRouteIdsToDelete, ...staleRouteIds])];

    if (routeIdsToDelete.length > 0) {
        const { error: deleteStalePointsError } = await cloudClient
            .from(CLOUD_ROUTE_POINTS_TABLE)
            .delete()
            .in('route_id', routeIdsToDelete);
        if (deleteStalePointsError) throw deleteStalePointsError;

        const { error: deleteStaleRoutesError } = await cloudClient
            .from(CLOUD_ROUTES_TABLE)
            .delete()
            .in('id', routeIdsToDelete);
        if (deleteStaleRoutesError) throw deleteStaleRoutesError;
    }

    return true;
}

async function pullRoutesFromCloud(options = {}) {
    if (!isCloudReady()) return getSavedRoutes();

    void options;
    const cloudNavEntriesFromTable = await pullNavLogEntriesFromCloudTable();
    const cloudEngineEntriesFromTable = await pullEngineLogEntriesFromCloudTable();
    const cloudWaypointPhotosV2 = await pullWaypointPhotosFromCloudV2();
    const cloudMaintenanceBoardsV2 = await pullMaintenanceBoardsFromCloudV2();
    const cloudMaintenanceExpensesV2 = await pullMaintenanceExpensesFromCloudV2();
    const cloudMaintenanceSuppliersV2 = await pullMaintenanceSuppliersFromCloudV2();

    const localRoutesBeforePull = [...getSavedRoutes()];
    const cloudRoutesV2 = await pullRoutesFromCloudV2();
    const effectiveRoutes = Array.isArray(cloudRoutesV2) ? cloudRoutesV2 : [];

    if (routesCloudDirty && localRoutesBeforePull.length > 0 && effectiveRoutes.length === 0) {
        // Avoid wiping local unsynced routes with an older/empty cloud snapshot.
        updateCloudDataSourceStatus('cache local (synchro en échec)', localRoutesBeforePull.length, waypointPhotoEntries.length);
        return localRoutesBeforePull;
    }

    setSavedRoutes(effectiveRoutes);

    setWaypointPhotoEntries(cloudWaypointPhotosV2, { persistLocal: true, refreshUi: true });
    setMaintenanceBoards(cloudMaintenanceBoardsV2, { persistLocal: true, refreshUi: true, syncCloud: false });
    setMaintenanceExpenses(cloudMaintenanceExpensesV2, { refreshUi: true });
    const preserveSupplierEditor = isMaintenanceSupplierEditorFocused();
    setMaintenanceSuppliers(cloudMaintenanceSuppliersV2, { refreshUi: !preserveSupplierEditor });

    if (Array.isArray(cloudNavEntriesFromTable)) {
        navLogEntries = cloudNavEntriesFromTable;
        saveArrayToStorage(NAV_LOG_STORAGE_KEY, navLogEntries);
        renderNavLogList();
    }

    if (Array.isArray(cloudEngineEntriesFromTable)) {
        engineLogEntries = cloudEngineEntriesFromTable;
        renderEngineLogList();
    }

    updateCloudDataSourceStatus('cloud (routes v2)', effectiveRoutes.length, waypointPhotoEntries.length);

    return effectiveRoutes;
}

async function pushRoutesToCloud() {
    if (!isCloudReady()) return false;

    const routesSnapshot = getSavedRoutes();

    // Strict mode: if normalized tables fail, report sync failure instead of silently claiming success.
    await pushEngineLogEntriesToCloudTable();
    await pushRoutesToCloudV2(routesSnapshot);
    await pushWaypointPhotosToCloudV2(waypointPhotoEntries);
    await pushMaintenanceBoardsToCloudV2(maintenanceBoards);

    routesCloudDirty = false;
    cloudLastSeenUpdatedAtMs = Math.max(cloudLastSeenUpdatedAtMs, Date.now());
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

function getCloudStatusText() {
    return String(document.getElementById('cloudStatus')?.textContent || '').trim();
}

function buildCloudConnectFailureMessage(config) {
    if (!config?.url || !config?.anonKey || !config?.projectKey) {
        return t('Paramètres cloud incomplets (URL / clé anon / project key).', 'Parámetros nube incompletos (URL / clave anon / project key).');
    }

    const status = getCloudStatusText();
    if (status) {
        return `${t('Connexion cloud impossible', 'Conexión nube imposible')}: ${status}`;
    }

    return t('Connexion cloud impossible. Vérifie réseau, URL Supabase et clé anon.', 'Conexión nube imposible. Verifica red, URL Supabase y clave anon.');
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
        cloudTableStatsRemoteCounts = null;
        cloudTableStatsLastRefreshAtMs = 0;
        updateCloudAuthUi();
        setCloudAuthStatus(t('Paramètres cloud incomplets (URL / clé anon / project key).', 'Parámetros nube incompletos (URL / clave anon / project key).'), true);
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
            cloudResolvedProjectIdUuid = '';
            cloudTableStatsRemoteCounts = null;
            cloudTableStatsLastRefreshAtMs = 0;
        }

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
                void (async () => {
                    if (cloudAuthUser) {
                        await loadCloudUserProfile();
                        if (isCloudAdmin()) {
                            await fetchCloudManagedUsers();
                        } else {
                            cloudManagedUsers = [];
                        }
                    } else {
                        cloudUserProfile = null;
                        cloudManagedUsers = [];
                    }
                    updateCloudAuthUi();
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
            const routes = await pullRoutesFromCloud({ allowMaintenanceOverwrite: true });
            refreshSavedList();
            setCloudStatus(t(`Cloud connecté · ${routes.length} route(s) partagée(s)`, `Nube conectada · ${routes.length} ruta(s) compartida(s)`));
            void refreshCloudStatsTableCounts({ force: true });
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
        cloudTableStatsRemoteCounts = null;
        cloudTableStatsLastRefreshAtMs = 0;
        updateCloudAuthUi();
        setCloudAuthStatus(t(`Connexion cloud impossible: ${formatCloudError(error)}`, `Conexión nube imposible: ${formatCloudError(error)}`), true);
        setCloudStatus(t(`Connexion cloud impossible: ${formatCloudError(error)}`, `Conexión nube imposible: ${formatCloudError(error)}`), true);
        return false;
    }
}

function refreshSavedList() {
    const container = document.getElementById('savedRoutesList');
    if (!container) return;
    
    container.innerHTML = '';
    const saved = getSavedRoutes();
    
    if (saved.length === 0) {
        currentLoadedRouteIndex = -1;
        container.innerHTML = `<div style="padding:8px; opacity:0.7; font-size:12px;">${t('Aucune route sauvegardée', 'No hay rutas guardadas')}</div>`;
        return;
    }

    // Create indexed array with calculated distances
    const routesWithData = saved.map((r, i) => {
        let totalDistance = 0;
        const storedDistance = Number(r.totalDistanceNm);
        if (Number.isFinite(storedDistance)) {
            totalDistance = storedDistance;
        } else if (r.points && r.points.length > 1) {
            for (let j = 0; j < r.points.length - 1; j++) {
                const p1 = r.points[j];
                const p2 = r.points[j + 1];
                totalDistance += distanceNm(
                    Number(p1.lat),
                    Number(p1.lon ?? p1.lng),
                    Number(p2.lat),
                    Number(p2.lon ?? p2.lng)
                );
            }
        }
        return {
            route: r,
            index: i,
            distance: totalDistance
        };
    });

    // Filter by search term
    let filteredRoutes = routesWithData;
    if (routesSearchTerm.trim()) {
        const searchLower = routesSearchTerm.toLowerCase().trim();
        filteredRoutes = routesWithData.filter(item => 
            item.route.name.toLowerCase().includes(searchLower)
        );
    }

    // Sort by name
    filteredRoutes.sort((a, b) => {
        const nameA = a.route.name.toLowerCase();
        const nameB = b.route.name.toLowerCase();
        if (routesSortOrder === 'asc') {
            return nameA.localeCompare(nameB);
        } else {
            return nameB.localeCompare(nameA);
        }
    });

    // Header with sortable column
    const header = document.createElement('div');
    header.className = 'routes-list-header';
    const sortIcon = routesSortOrder === 'asc' ? '▲' : '▼';
    header.innerHTML = `
        <div class="routes-header-sortable" id="routesHeaderName">${t('Nom', 'Nombre')} ${sortIcon}</div>
        <div>${t('Distance', 'Distancia')}</div>
        <div></div>
    `;
    container.appendChild(header);

    // Add click handler for sorting
    const nameHeader = document.getElementById('routesHeaderName');
    if (nameHeader) {
        nameHeader.addEventListener('click', () => {
            routesSortOrder = routesSortOrder === 'asc' ? 'desc' : 'asc';
            refreshSavedList();
        });
    }

    if (filteredRoutes.length === 0) {
        container.innerHTML += `<div style="padding:8px; opacity:0.7; font-size:12px;">${t('Aucune route trouvée', 'No se encontraron rutas')}</div>`;
        return;
    }

    // Routes
    filteredRoutes.forEach(({ route, index, distance }) => {
        const row = document.createElement('div');
        row.className = 'route-row';
        if (index === currentLoadedRouteIndex) {
            row.classList.add('route-row--active');
        }

        const btn = document.createElement('div');
        btn.className = 'route-summary-btn';
        const creatorName = String(route?.creatorName || '').trim();
        const creatorMeta = creatorName
            ? `<div class="route-col__meta">${t('Créateur', 'Creador')}: ${escapeHtml(creatorName)}</div>`
            : '';
        btn.innerHTML = `
            <div class="route-col route-col--with-meta" title="${escapeHtml(route.name)}">${escapeHtml(route.name)}${creatorMeta}</div>
            <div class="route-col route-col--distance">${distance.toFixed(1)} NM</div>
            <div class="route-actions">
                <button type="button" class="route-action-btn route-action-btn--reroute" data-action="reroute" data-index="${index}" title="${t('Routage', 'Navegación')}">⛵</button>
                <button type="button" class="route-action-btn route-action-btn--delete" data-action="delete" data-index="${index}" title="${t('Supprimer', 'Eliminar')}">🗑️</button>
            </div>
        `;
        
        btn.addEventListener('click', async (e) => {
            const actionTarget = e.target.closest('[data-action]');
            const action = actionTarget?.dataset?.action;

            if (action === 'delete') {
                e.preventDefault();
                e.stopPropagation();
                deleteRoute(Number(actionTarget.dataset.index));
                return;
            }

            if (action === 'reroute') {
                e.preventDefault();
                e.stopPropagation();
                loadRoute(index);
                const routingTabBtn = document.getElementById('routingTabBtn');
                if (routingTabBtn) routingTabBtn.click();
                await computeRoute();
                return;
            }

            if (!action) {
                loadRoute(index);
            }
        });

        row.appendChild(btn);
        container.appendChild(row);
    });
}

async function saveRoute() {
    if (routePoints.length === 0) return alert(t('Aucun waypoint à sauvegarder', 'No hay waypoints para guardar'));
    const nameInput = document.getElementById('routeNameInput');
    const rawName = (nameInput?.value || '').trim();
    if (!rawName) {
        return alert(t('Le nom de la route est obligatoire', 'El nombre de la ruta es obligatorio'));
    }

    const saved = [...getSavedRoutes()];
    const canonicalName = value => String(value || '').trim().toLowerCase();
    const name = rawName;
    const nameKey = canonicalName(name);
    const targetIndexByName = saved.findIndex(route => canonicalName(route?.name) === nameKey);

    // Règle métier: update si le nom existe déjà, sinon insert d'une nouvelle route.
    const shouldUpdateExisting = targetIndexByName >= 0;
    const targetIndex = shouldUpdateExisting ? targetIndexByName : -1;

    // Calculate distance from computed route or direct waypoint distance
    let totalDistanceNm = undefined;
    if (lastComputedReportData?.metrics?.totalDistanceNm) {
        totalDistanceNm = Number(lastComputedReportData.metrics.totalDistanceNm);
    } else if (routePoints.length > 1) {
        // Calculate direct distance between waypoints
        totalDistanceNm = 0;
        for (let i = 0; i < routePoints.length - 1; i++) {
            const p1 = routePoints[i];
            const p2 = routePoints[i + 1];
            totalDistanceNm += distanceNm(
                Number(p1.lat),
                Number(p1.lng ?? p1.lon),
                Number(p2.lat),
                Number(p2.lng ?? p2.lon)
            );
        }
    }

    const payload = {
        name,
        date: departureDate,
        time: departureTime,
        tackingTimeHours,
        points: routePoints.map(p => ({ lat: p.lat, lon: p.lng ?? p.lon })),
        createdAt: shouldUpdateExisting
            ? String(saved[targetIndex]?.createdAt || '')
            : new Date().toISOString(),
        updatedAt: new Date().toISOString()
    };

    // Add distance if calculated
    if (Number.isFinite(totalDistanceNm)) {
        payload.totalDistanceNm = totalDistanceNm;
    }

    if (shouldUpdateExisting) {
        const withCreator = ensureCreatorOnEditedRecord(payload);
        saved[targetIndex] = {
            ...saved[targetIndex],
            ...withCreator
        };
        currentLoadedRouteIndex = targetIndex;
    } else {
        saved.push(addCreatorToNewRecord(payload));
        currentLoadedRouteIndex = saved.length - 1;
    }

    setSavedRoutes(saved);
    routesCloudDirty = true;
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

    alert(shouldUpdateExisting
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

    // Auto-zoom on loaded route
    if (routePoints.length > 0) {
        const bounds = L.latLngBounds(routePoints);
        if (bounds.isValid()) {
            map.fitBounds(bounds, {
                padding: [50, 50],
                maxZoom: 12,
                animate: true,
                duration: 0.5
            });
        } else {
            map.setView(routePoints[0], Math.max(map.getZoom(), 10));
        }
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
    routesCloudDirty = true;
    updateCloudDataSourceStatus('local (non synchronisé)', saved.length, waypointPhotoEntries.length);

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