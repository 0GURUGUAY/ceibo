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
let standardTileLayer;
let satelliteTileLayer;
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
const CLOUD_TABLE_NAME = 'ceibo_route_collections';
const CLOUD_AUTO_PULL_INTERVAL_MS = 45000;
let savedRoutesCache = [];
let cloudClient = null;
let cloudConfig = null;
let cloudConnected = false;
let cloudAutoPullTimer = null;
let cloudAutoPullInFlight = false;
let overpassLastRequestAt = 0;
const overpassQueryCache = new Map();
const overpassEndpointCooldownUntil = new Map();
let overpassPreferredEndpoint = OVERPASS_URLS[0];
const MAP_STYLE_STORAGE_KEY = 'ceiboMapStyle';
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

let landGeometry = null;

function normalizeHourTime(timeValue) {
    const hour = String(parseInt(String(timeValue || '12:00').split(':')[0], 10) || 12).padStart(2, '0');
    return `${hour}:00`;
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
    const year = dateObj.getFullYear();
    const month = String(dateObj.getMonth() + 1).padStart(2, '0');
    const day = String(dateObj.getDate()).padStart(2, '0');
    const hour = String(dateObj.getHours()).padStart(2, '0');
    return `${year}-${month}-${day}T${hour}:00`;
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
        if (saveBtn) saveBtn.textContent = 'Ajouter ce waypoint photo';
        if (cancelBtn) cancelBtn.style.display = 'none';
        return;
    }

    editingWaypointPhotoId = entry.id;
    if (saveBtn) saveBtn.textContent = 'Mettre à jour ce waypoint photo';
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
    const title = entry.placeName ? escapeHtml(entry.placeName) : 'Mouillage noté';
    const comment = entry.comment ? `<div style="margin-top:6px;">${escapeHtml(entry.comment)}</div>` : '';
    const bottom = entry.bottomType ? `<div>Fond: ${escapeHtml(entry.bottomType)}</div>` : '';
    const image = entry.imageDataUrl
        ? `<img src="${entry.imageDataUrl}" alt="Photo mouillage" style="margin-top:6px; width:100%; max-width:180px; border-radius:8px;">`
        : '';

    return `<strong>${title}</strong><br>` +
        `${entry.lat.toFixed(5)}, ${entry.lng.toFixed(5)}<br>` +
        `Global: ${starsLabel(entry.rating)}<br>` +
        `Propreté: ${starsLabel(entry.cleanliness)} · Protection: ${escapeHtml(formatProtectionList(entry.protection))} · Profondeur: ${entry.depthMeters} m` +
        `${bottom}` +
        `${comment}` +
        `${weatherHtml}` +
        `${image}`;
}

async function enrichWaypointPhotoPopupWithCurrentWeather(marker, entry) {
    if (!marker || !entry) return;

    const loadingWeatherHtml = '<div style="margin-top:6px;"><strong>Météo actuelle</strong><br>Chargement...</div>';
    marker.setPopupContent(buildWaypointPhotoPopupContent(entry, loadingWeatherHtml));

    try {
        const weather = await getCurrentWeatherAtWaypoint(entry.lat, entry.lng);
        const nowLabel = formatWeekdayHourUtc(new Date());
        const weatherLine = formatWeatherTooltipContent(weather, nowLabel).replace('<strong>Météo</strong><br>', '');
        const weatherHtml = `<div style="margin-top:6px;"><strong>Météo actuelle</strong><br>${weatherLine}</div>`;
        marker.setPopupContent(buildWaypointPhotoPopupContent(entry, weatherHtml));
    } catch (error) {
        const weatherHtml = '<div style="margin-top:6px;"><strong>Météo actuelle</strong><br>Indisponible</div>';
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
        container.innerHTML = '<div class="arrival-list__item">Aucun mouillage enregistré.</div>';
        return;
    }

    container.innerHTML = waypointPhotoEntries.map(entry => {
        const title = entry.placeName ? escapeHtml(entry.placeName) : 'Mouillage sans nom';
        const bottom = entry.bottomType ? `<div>Fond: ${escapeHtml(entry.bottomType)}</div>` : '';
        const comment = entry.comment ? `<div style="margin-top:4px;">${escapeHtml(entry.comment)}</div>` : '';
        const image = entry.imageDataUrl ? `<img class="waypoint-photo-card__img" src="${entry.imageDataUrl}" alt="Photo mouillage">` : '';

        return `<div class="waypoint-photo-card">
            <div><strong>${title}</strong></div>
            <div><strong>${starsLabel(entry.rating)}</strong> · ${entry.lat.toFixed(4)}, ${entry.lng.toFixed(4)}</div>
            <div>Propreté ${starsLabel(entry.cleanliness)} · Protection ${escapeHtml(formatProtectionList(entry.protection))} · Profondeur ${entry.depthMeters} m</div>
            ${bottom}
            ${comment}
            ${image}
            <div class="button-row">
                <button type="button" class="js-waypoint-photo-go" data-id="${escapeHtml(entry.id)}" style="flex:1;">Y aller</button>
                <button type="button" class="js-waypoint-photo-center" data-id="${escapeHtml(entry.id)}" style="flex:1;">Voir sur carte</button>
                <button type="button" class="js-waypoint-photo-edit" data-id="${escapeHtml(entry.id)}" style="flex:1;">Modifier</button>
                <button type="button" class="js-waypoint-photo-delete" data-id="${escapeHtml(entry.id)}" style="flex:1;">Supprimer</button>
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
        if (status) status.textContent = 'Ce fichier n\'est pas une image.';
        pendingWaypointPhotoDraft = null;
        return;
    }

    pendingWaypointPhotoDraft = {
        ...(pendingWaypointPhotoDraft || {}),
        file
    };

    waypointPhotoInputProcessing = true;

    try {
        if (status) status.textContent = 'Lecture des coordonnées GPS...';

        const coords = await extractGpsCoordinatesFromPhoto(file);
        if (!coords) {
            if (editingWaypointPhotoId) {
                const entry = waypointPhotoEntries.find(item => item.id === editingWaypointPhotoId);
                if (entry) {
                    pendingWaypointPhotoDraft = { file, lat: entry.lat, lng: entry.lng };
                    if (status) status.textContent = `Pas de GPS dans la nouvelle photo: coordonnées conservées (${entry.lat.toFixed(5)}, ${entry.lng.toFixed(5)}).`;
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
                    status.textContent = 'Pas de GPS EXIF: position manuelle activée sur la mini-carte (déplace le marqueur puis sauvegarde).';
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
        if (status) status.textContent = 'Erreur de lecture image. Essaie une autre photo (JPG/PNG).';
    } finally {
        waypointPhotoInputProcessing = false;
    }
}

async function saveWaypointPhotoEntry() {
    if (waypointPhotoInputProcessing) {
        alert('Photo en cours de traitement, attends 1-2 secondes puis réessaie.');
        return;
    }

    const isEdit = Number.isFinite(waypointPhotoEntries.findIndex(entry => entry.id === editingWaypointPhotoId))
        && waypointPhotoEntries.findIndex(entry => entry.id === editingWaypointPhotoId) !== -1;

    if (!pendingWaypointPhotoDraft?.file && !isEdit) {
        alert('Choisis une photo avec coordonnées GPS intégrées.');
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
            alert('Waypoint à modifier introuvable.');
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
            alert('Enregistrement impossible: stockage local saturé. Essaie une image plus légère ou supprime des anciennes photos.');
            return;
        }

        renderWaypointPhotoList();
        syncWaypointPhotoMarkersInView();
        cancelWaypointPhotoEdit();
        alert('Waypoint photo modifié.');
        return;
    }

    if (!Number.isFinite(pendingWaypointPhotoDraft?.lat) || !Number.isFinite(pendingWaypointPhotoDraft?.lng)) {
        alert('Coordonnées GPS manquantes pour ce waypoint photo.');
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
        alert('Enregistrement impossible: stockage local saturé. Essaie une image plus légère ou supprime des anciennes photos.');
        return;
    }

    renderWaypointPhotoList();
    syncWaypointPhotoMarkersInView();

    resetWaypointPhotoFormValues();
    clearWaypointPhotoDraft();
    alert('Waypoint photo enregistré.');
    } catch (error) {
        alert('Impossible d\'enregistrer la photo. Essaie une image plus légère (JPG), puis recommence.');
    }
}

function updateSelectedWaypointInfo() {
    const info = document.getElementById('selectedWpInfo');
    if (!info) return;

    if (!Number.isInteger(selectedUserWaypointIndex) || selectedUserWaypointIndex < 0 || selectedUserWaypointIndex >= markers.length) {
        info.textContent = 'WP sélectionné: aucun · clic sur WP pour sélectionner · clic droit pour supprimer';
        return;
    }

    const marker = markers[selectedUserWaypointIndex];
    const latlng = marker?.getLatLng?.();
    if (!latlng) {
        info.textContent = 'WP sélectionné: aucun · clic sur WP pour sélectionner · clic droit pour supprimer';
        return;
    }

    info.textContent = `WP sélectionné: ${selectedUserWaypointIndex + 1} (${latlng.lat.toFixed(4)}, ${latlng.lng.toFixed(4)})`;
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
        container.innerHTML = '<div class="arrival-card">Aucun mouillage recommandé trouvé.</div>';
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
                <div>Vent ETA: ${wind} · Houle ETA: ${wave} · Confiance: ${escapeHtml(item.confidence)}</div>
                <button type="button" class="apply-anchorage-btn" data-anch-index="${index}">Utiliser comme WP final</button>
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
        alert('Ajoute au moins 2 waypoints pour analyser la zone d\'arrivée.');
        return;
    }

    const button = document.getElementById('analyzeArrivalBtn');
    const summary = document.getElementById('arrivalSummary');
    if (button) {
        button.disabled = true;
        button.textContent = 'Analyse en cours...';
    }
    if (summary) summary.textContent = 'Analyse mouillage: récupération des données...';

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
        renderNearbyList('nearbyRestaurants', restaurants, 'Aucun restaurant proche trouvé');
        renderNearbyList('nearbyShops', shops, 'Aucun magasin proche trouvé');

        if (summary) {
            if (recommendations.length) {
                summary.innerHTML = `<strong>Top mouillage:</strong> ${escapeHtml(recommendations[0].name)} · ${recommendations[0].distanceNm.toFixed(2)} nm de l'arrivée`;
            } else {
                summary.textContent = 'Analyse mouillage: aucun mouillage adapté trouvé à proximité.';
            }
        }
    } catch (_error) {
        if (summary) summary.textContent = 'Analyse mouillage: erreur de récupération des données.';
    } finally {
        if (button) {
            button.disabled = false;
            button.textContent = 'Conseiller mouillage à l\'arrivée';
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

async function estimateRouteForDeparture(departureDateTime) {
    if (!Array.isArray(routePoints) || routePoints.length < 2) return null;

    let totalTimeHours = 0;
    let maxWind = 0;
    let maxGust = 0;
    let minWind = Number.POSITIVE_INFINITY;
    let hasMotorSegment = false;
    let motorTimeHours = 0;

    for (let i = 0; i < routePoints.length - 1; i++) {
        const start = { lat: routePoints[i].lat, lon: routePoints[i].lng };
        const end = { lat: routePoints[i + 1].lat, lon: routePoints[i + 1].lng };
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
        routePoints[0].lat,
        routePoints[0].lng,
        departureSlot.date,
        departureSlot.hour
    );

    const arrivalWeather = await getWeatherAtDateHour(
        routePoints[routePoints.length - 1].lat,
        routePoints[routePoints.length - 1].lng,
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
        isNoMotor: windFloorOk && !hasMotorSegment
    };
}

function renderDepartureSuggestion(result) {
    const container = document.getElementById('departureSuggestionInfo');
    if (!container) return;

    if (!result) {
        lastDepartureSuggestion = null;
        container.textContent = 'Suggestion départ: aucune fenêtre météo favorable trouvée.';
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
        `<strong>Départ conseillé:</strong> ${formatWeekdayHourUtc(result.departureDateTime)}<br>` +
        `Arrivée estimée: ${formatWeekdayHourUtc(result.arrivalDateTime)} · ${formatDurationHours(result.totalTimeHours)}<br>` +
        `Vent départ: ${depWind} · Vent arrivée: ${arrWind} · Vent min trajet: ${minWind}<br>` +
        `Vent maxi trajet: ${maxWind} · Rafale maxi trajet: ${maxGust}<br>` +
        `Moteur estimé: ${formatDurationHours(motorTime)} (${motorShare}%)`;
}

async function suggestBestDeparture() {
    if (routePoints.length < 2) {
        alert('Ajoute au moins 2 waypoints pour analyser un départ.');
        return;
    }

    const button = document.getElementById('suggestDepartureBtn');
    if (button) {
        button.disabled = true;
        button.textContent = 'Analyse météo...';
    }

    try {
        const baseDateTime = new Date(`${departureDate}T${departureTime}:00`);
        if (Number.isNaN(baseDateTime.getTime())) throw new Error('invalid-departure');

        const candidates = [];
        const stepHours = 3;
        const maxCandidates = Math.min(32, Math.max(8, forecastWindowDays * 8));

        for (let i = 0; i < maxCandidates; i++) {
            const candidateDate = new Date(baseDateTime.getTime() + i * stepHours * 3600 * 1000);
            if ((candidateDate.getTime() - baseDateTime.getTime()) > forecastWindowDays * 24 * 3600 * 1000) break;
            const estimate = await estimateRouteForDeparture(candidateDate);
            if (estimate) candidates.push(estimate);
        }

        if (candidates.length === 0) {
            renderDepartureSuggestion(null);
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
                container.textContent = 'Suggestion départ: aucune fenêtre météo ≤ 20 kn trouvée.';
            }
            return;
        }

        renderDepartureSuggestion(best);
        applyLastDepartureSuggestion();
    } catch (_error) {
        alert('Impossible de calculer une suggestion de départ pour le moment.');
    } finally {
        if (button) {
            button.disabled = false;
            button.textContent = 'Conseiller départ météo (≤ 20 kn)';
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
            <text x="${Math.min(width - 90, start.x + 10).toFixed(1)}" y="${Math.max(18, start.y - 10).toFixed(1)}" fill="#bde9ff" font-size="13" font-family="Arial">Départ</text>
            <text x="${Math.min(width - 90, end.x + 10).toFixed(1)}" y="${Math.max(18, end.y - 10).toFixed(1)}" fill="#ffc8ce" font-size="13" font-family="Arial">Arrivée</text>
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
    const routeName = escapeHtml(data?.routeName || 'Route');
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
        ? `<img class="map-image" src="${mapImageDataUrl}" alt="Carte de navigation" />`
        : '<div class="map-placeholder">Carte indisponible</div>';

    const vectorSection = vectorImageDataUrl
        ? `<img class="map-image" src="${vectorImageDataUrl}" alt="Tracé 2D" />`
        : '<div class="map-placeholder">Tracé 2D indisponible</div>';

    return `
    <div class="pdf-report">
        <header class="hero">
            <div>
                <h1>Carnet de Voyage</h1>
                <h2>${routeName}</h2>
                <p>Généré le ${computedAt}</p>
            </div>
            <div class="hero-meta">
                <div><strong>Départ</strong><span>${departure}</span></div>
                <div><strong>Arrivée</strong><span>${arrival}</span></div>
                <div><strong>Météo MAJ</strong><span>${weatherUpdatedAt}</span></div>
            </div>
        </header>

        <section class="cards">
            <article><span>Distance</span><strong>${escapeHtml(metrics.totalDistanceNm)} nm</strong></article>
            <article><span>Durée</span><strong>${escapeHtml(metrics.totalTimeLabel)}</strong></article>
            <article><span>Segments</span><strong>${escapeHtml(metrics.segmentCount)}</strong></article>
            <article><span>WP auto</span><strong>${escapeHtml(metrics.generatedWaypointCount)}</strong></article>
        </section>

        <section>
            <h3>Carte de navigation</h3>
            ${mapSection}
        </section>

        <section>
            <h3>Tracé de navigation (2D)</h3>
            ${vectorSection}
        </section>

        <section>
            <h3>Segments</h3>
            <table>
                <thead>
                    <tr><th>WP</th><th>Seg</th><th>Nom</th><th>Départ</th><th>Arrivée</th><th>Cap</th><th>Dist</th><th>Temps</th><th>Vit</th><th>Voiles</th></tr>
                </thead>
                <tbody>${segmentRows}</tbody>
            </table>
        </section>

        <section>
            <h3>Météo aux waypoints</h3>
            <table>
                <thead>
                    <tr><th>WP</th><th>Passage</th><th>Vent</th><th>Dir</th><th>Pression</th><th>Houle</th><th>Résumé</th></tr>
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
        alert('Calcule une route avant d\'exporter le rapport PDF.');
        return;
    }

    if (!window?.jspdf?.jsPDF) {
        alert('jsPDF non disponible dans le navigateur.');
        return;
    }

    const button = document.getElementById('exportVoyagePdfBtn');
    if (button) {
        button.disabled = true;
        button.textContent = 'Génération PDF...';
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
        alert('Impossible de générer le PDF.');
    } finally {
        if (button) {
            button.disabled = false;
            button.textContent = 'Exporter rapport PDF';
        }
    }
}

function getSeaComfortLevel(weather) {
    const waveHeight = weather?.waveHeight;
    const windSpeed = weather?.windSpeed;

    if (Number.isFinite(waveHeight)) {
        if (waveHeight < 0.7) return 'Confort: calme';
        if (waveHeight < 1.5) return 'Confort: modéré';
        if (waveHeight < 2.5) return 'Confort: agité';
        return 'Confort: difficile';
    }

    if (Number.isFinite(windSpeed)) {
        if (windSpeed < 10) return 'Confort: calme';
        if (windSpeed < 18) return 'Confort: modéré';
        if (windSpeed < 25) return 'Confort: agité';
        return 'Confort: difficile';
    }

    return 'Confort: N/A';
}

function getSailRecommendation({ isMotorSegment, tws, twa, sailModeValue }) {
    if (isMotorSegment) return 'Moteur';

    const prudentOffset = sailModeValue === 'prudent' ? -2 : 0;
    const perfOffset = sailModeValue === 'performance' ? 2 : 0;

    if (tws >= (22 + prudentOffset)) return 'GV 2 ris + trinquette';
    if (tws >= (16 + prudentOffset)) {
        if (twa > 130 && sailModeValue === 'performance') return 'GV 1 ris + spi';
        return 'GV 1 ris + génois réduit';
    }

    if (twa < 60) return sailModeValue === 'prudent' ? 'GV pleine + génois réduit' : 'GV pleine + génois';
    if (twa < 115) return 'GV pleine + génois';
    if (twa < 145) return sailModeValue === 'prudent' ? 'GV + génois tangonné' : 'GV + gennaker';

    if (sailModeValue === 'performance' && tws < (18 + perfOffset)) return 'GV + spi';
    return 'GV + génois tangonné';
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
    if (isMotorSegment) return 'Vent faible (< 5 kn) : passage au moteur à 7 kn.';

    const twaText = Number.isFinite(twa) ? `${Math.round(twa)}°` : 'N/A';
    const twsText = Number.isFinite(tws) ? `${tws.toFixed(1)} kn` : 'N/A';
    const modeLabel = sailModeValue === 'prudent' ? 'Prudent' : (sailModeValue === 'performance' ? 'Performance' : 'Auto');

    return `Mode ${modeLabel} · TWS ${twsText} · TWA ${twaText} → ${sailSetup}`;
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
    info.textContent = `Mesure: ${total.toFixed(2)} nm`;
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
        btn.textContent = `Mesure NM: ${measureModeEnabled ? 'ON' : 'OFF'}`;
        btn.classList.toggle('active', measureModeEnabled);
    }

    if (map) {
        map.getContainer().style.cursor = measureModeEnabled ? 'crosshair' : '';
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

// =====================
// INIT MAP
// =====================

document.addEventListener('DOMContentLoaded', async function() {
    map = L.map('map').setView([41.3851, 2.1734], 8);

    standardTileLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '© OpenStreetMap',
        crossOrigin: true
    });

    satelliteTileLayer = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
        attribution: 'Tiles © Esri',
        crossOrigin: true
    });

    baseLayerControl = L.control.layers(
        {
            'Standard': standardTileLayer,
            'Satellite': satelliteTileLayer
        },
        {},
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
        if (measureModeEnabled) {
            addMeasurePoint(e.latlng);
            return;
        }

        addUserWaypoint(e.latlng);
    });

    map.on('moveend zoomend', syncWaypointPhotoMarkersInView);

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
        const style = e.name === 'Satellite' ? 'satellite' : 'standard';
        activeBaseLayer = style === 'satellite' ? satelliteTileLayer : standardTileLayer;
        localStorage.setItem(MAP_STYLE_STORAGE_KEY, style);
    });

    const routingTabBtn = document.getElementById('routingTabBtn');
    const routesTabBtn = document.getElementById('routesTabBtn');
    const cloudTabBtn = document.getElementById('cloudTabBtn');
    const arrivalTabBtn = document.getElementById('arrivalTabBtn');
    const waypointTabBtn = document.getElementById('waypointTabBtn');
    const routingTab = document.getElementById('routingTab');
    const routesTab = document.getElementById('routesTab');
    const cloudTab = document.getElementById('cloudTab');
    const arrivalTab = document.getElementById('arrivalTab');
    const waypointTab = document.getElementById('waypointTab');

    function activateTab(tabName) {
        const isRouting = tabName === 'routing';
        const isRoutes = tabName === 'routes';
        const isCloud = tabName === 'cloud';
        const isArrival = tabName === 'arrival';
        const isWaypoint = tabName === 'waypoint';

        routingTabBtn.classList.toggle('active', isRouting);
        routesTabBtn.classList.toggle('active', isRoutes);
        cloudTabBtn.classList.toggle('active', isCloud);
        arrivalTabBtn.classList.toggle('active', isArrival);
        waypointTabBtn.classList.toggle('active', isWaypoint);

        routingTab.classList.toggle('active', isRouting);
        routesTab.classList.toggle('active', isRoutes);
        cloudTab.classList.toggle('active', isCloud);
        arrivalTab.classList.toggle('active', isArrival);
        waypointTab.classList.toggle('active', isWaypoint);
    }

    routingTabBtn.addEventListener('click', () => activateTab('routing'));
    routesTabBtn.addEventListener('click', () => activateTab('routes'));
    cloudTabBtn.addEventListener('click', () => activateTab('cloud'));
    arrivalTabBtn.addEventListener('click', () => activateTab('arrival'));
    waypointTabBtn.addEventListener('click', () => activateTab('waypoint'));

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

    const saveWaypointPhotoBtn = document.getElementById('saveWaypointPhotoBtn');
    if (saveWaypointPhotoBtn) {
        saveWaypointPhotoBtn.addEventListener('click', saveWaypointPhotoEntry);
    }

    const cancelWaypointPhotoEditBtn = document.getElementById('cancelWaypointPhotoEditBtn');
    if (cancelWaypointPhotoEditBtn) {
        cancelWaypointPhotoEditBtn.addEventListener('click', cancelWaypointPhotoEdit);
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
            suggestionBox.textContent = 'Suggestion départ: en attente';
            suggestionBox.classList.remove('suggestion-clickable');
        }
        const arrivalSummary = document.getElementById('arrivalSummary');
        if (arrivalSummary) arrivalSummary.textContent = 'Analyse mouillage: en attente';
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

    const cloudConnectBtn = document.getElementById('cloudConnectBtn');
    if (cloudConnectBtn) {
        cloudConnectBtn.addEventListener('click', async () => {
            if (!isCloudPasswordValid()) {
                setCloudStatus('Mot de passe cloud invalide', true);
                return;
            }
            const config = readCloudConfigFromForm();
            await connectCloud(config);
        });
    }

    const cloudPasswordInput = document.getElementById('cloudPasswordInput');
    if (cloudPasswordInput) {
        cloudPasswordInput.addEventListener('keydown', async (e) => {
            if (e.key !== 'Enter') return;
            e.preventDefault();
            if (!isCloudPasswordValid()) {
                setCloudStatus('Mot de passe cloud invalide', true);
                return;
            }
            const config = readCloudConfigFromForm();
            await connectCloud(config);
        });
    }

    const cloudRefreshBtn = document.getElementById('cloudRefreshBtn');
    if (cloudRefreshBtn) {
        cloudRefreshBtn.addEventListener('click', async () => {
            if (!isCloudReady()) {
                setCloudStatus('Cloud non connecté (utilise "Connecter cloud")', true);
                return;
            }

            try {
                await autoPullRoutesFromCloud('manual');
            } catch (error) {
                setCloudStatus(`Rafraîchissement cloud impossible: ${formatCloudError(error)}`, true);
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

    loadWaypointPhotoEntries();
    resetWaypointPhotoFormValues();
    setWaypointPhotoEditMode(null);
    renderWaypointPhotoList();
    syncWaypointPhotoMarkersInView();

    setSavedRoutes(loadRoutesFromLocalStorage());
    refreshSavedList();

    const storedCloudConfig = loadCloudConfigFromStorage();
    updateCloudFormFromConfig(storedCloudConfig);
    if (storedCloudConfig) {
        if (isCloudPasswordValid()) {
            await connectCloud(storedCloudConfig, { silent: true });
        } else {
            setCloudStatus('Entre le mot de passe cloud puis clique "Connecter cloud"');
        }
    } else {
        const hiddenConfig = readCloudConfigFromForm();
        if (hiddenConfig?.url && hiddenConfig?.anonKey && hiddenConfig?.projectKey) {
            setCloudStatus('Entre le mot de passe cloud puis clique "Connecter cloud"');
        } else {
            setCloudStatus('Mode local (pas de cloud configuré)');
        }
    }
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
        alert('Date/heure de départ invalide');
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
            activeDisplaySegment.bearing = Math.round(getBearing(activeDisplaySegment.startLatLng, endPoint));

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

    let segmentsHtml = '<table id="segmentsTable" style="width:100%; border-collapse:collapse; margin-top:8px; font-size:8px;"><tr style="border-bottom:1px solid #ccc;"><th style="text-align:left; padding:2px; width:22px;">WP</th><th style="text-align:left; padding:2px; width:14px;">Seg</th><th style="text-align:right; padding:2px;">Départ</th><th style="text-align:right; padding:2px;">Arrivée</th><th style="text-align:right; padding:2px;">Cap</th><th style="text-align:right; padding:2px;">Dist.</th><th style="text-align:right; padding:2px;">Temps</th><th style="text-align:right; padding:2px;">Vit.</th><th style="text-align:right; padding:2px;">V.V</th><th style="text-align:right; padding:2px;">D.V</th><th style="text-align:left; padding:2px;">Voiles</th></tr>';
    
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
            <strong>Segments: ${routePoints.length - 1}</strong><br>
            <strong>Distance totale: ${totalDistance.toFixed(2)} nm</strong><br>
            <strong>Temps total: ${totalTime.toFixed(2)} h</strong><br>
            <strong>WP auto générés: ${generatedAutoWaypointCount}</strong><br>
            <strong>Météo (dernière MAJ): ${weatherUpdatedAt}</strong><br>
            <strong>Évolution pression: ${pressureSummary}</strong>
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
    const weekday = dateObj.toLocaleDateString('fr-FR', { weekday: 'short' }).replace('.', '');
    const hour = String(dateObj.getHours()).padStart(2, '0');
    return `${weekday} ${hour}h`;
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

    if (pressureSeries.length < 2) return 'Données insuffisantes';

    const firstPressure = pressureSeries[0];
    const lastPressure = pressureSeries[pressureSeries.length - 1];
    const delta = lastPressure - firstPressure;
    const roundedDelta = `${delta >= 0 ? '+' : ''}${delta.toFixed(1)} hPa`;

    let trend = 'stable';
    if (delta > 0.8) trend = 'hausse';
    if (delta < -0.8) trend = 'baisse';

    return `${firstPressure.toFixed(0)} → ${lastPressure.toFixed(0)} hPa (${roundedDelta}, ${trend})`;
}

function weatherCodeToLabel(code) {
    const labels = {
        0: 'Ciel dégagé',
        1: 'Peu nuageux',
        2: 'Partiellement nuageux',
        3: 'Couvert',
        45: 'Brouillard',
        48: 'Brouillard givrant',
        51: 'Bruine légère',
        53: 'Bruine modérée',
        55: 'Bruine forte',
        61: 'Pluie faible',
        63: 'Pluie modérée',
        65: 'Pluie forte',
        71: 'Neige faible',
        73: 'Neige modérée',
        75: 'Neige forte',
        80: 'Averses faibles',
        81: 'Averses modérées',
        82: 'Averses fortes',
        95: 'Orage'
    };

    return labels[code] || 'Conditions variables';
}

function degreesToCardinalFr(degrees) {
    if (!Number.isFinite(degrees)) return 'N/A';
    const directions = ['Nord', 'Nord-Est', 'Est', 'Sud-Est', 'Sud', 'Sud-Ouest', 'Ouest', 'Nord-Ouest'];
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

    return `<strong>Météo</strong><br>${summary}<br>Temp: ${temp}<br>Vent: ${windSpeed} (${windDirection}, ${windCardinal})<br>Rafales: ${windGust}<br>Pluie: ${precipitation}<br>Pression: ${pressure}<br>Houle: ${waveHeight} · ${wavePeriod} · ${waveDirection} (${waveCardinal})<br>${seaComfort}<br>Passage WP: ${passageRef}`;
}

function formatUtcDateTime(isoString) {
    if (!isoString) return 'N/A';
    const date = new Date(isoString);
    if (Number.isNaN(date.getTime())) return 'N/A';

    return `${date.toLocaleDateString('fr-FR')} ${date.toLocaleTimeString('fr-FR', {
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
        `Température: ${temp}<br>` +
        `Vent: ${windSpeed} — ${windDirectionDeg} (${windDirectionCardinal})<br>` +
        `Rafales: ${windGust}<br>` +
        `Pluie: ${precipitation}<br>` +
        `Pression: ${pressure}<br>` +
        `Houle: ${waveHeight} · ${wavePeriod} · ${waveDirectionDeg} (${waveDirectionCardinal})<br>` +
        `${seaComfort}<br>` +
        `Passage WP: ${passageRef}`;
}

function formatWaypointPopupContent(marker, weather, referenceLabel) {
    const current = marker.getLatLng();
    const index = markers.indexOf(marker);
    const waypointLabel = marker?._ceiboLabel || (index !== -1 ? `Waypoint ${index + 1}` : 'Waypoint');

    return `<strong>${waypointLabel}</strong><br>${current.lat.toFixed(4)}, ${current.lng.toFixed(4)}<br>${formatWeatherTooltipContent(weather, referenceLabel).replace('<strong>Météo</strong><br>', '')}`;
}

async function openWaypointWeatherPopup(marker) {
    const markerLabel = marker?._ceiboLabel || 'Waypoint';
    marker.bindPopup(`<strong>${markerLabel}</strong><br>Chargement météo...`, { maxWidth: 340, autoPan: false });
    marker.openPopup();

    try {
        const result = await getWeatherForMarker(marker);
        marker.setPopupContent(formatWaypointPopupContent(marker, result.weather, result.referenceLabel));
        marker.openPopup();
    } catch (error) {
        marker.setPopupContent('<strong>Waypoint</strong><br>Météo indisponible');
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
        marker.bindTooltip('Chargement météo...', { direction: 'top', opacity: 0.95 });
        marker.openTooltip();

        try {
            const result = await getWeatherForMarker(marker);
            marker.setTooltipContent(formatWeatherTooltipContent(result.weather, result.referenceLabel));
            marker.openTooltip();
        } catch (error) {
            marker.setTooltipContent('Météo indisponible');
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
                weatherContainer.innerHTML = '<strong>Waypoint</strong><br>Météo indisponible';
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
        marker.bindTooltip('Chargement météo...', { direction: 'top', opacity: 0.95 });
        marker.openTooltip();

        try {
            const result = await getWeatherForMarker(marker);
            marker.setTooltipContent(formatWeatherTooltipContent(result.weather, result.referenceLabel));
            marker.openTooltip();
        } catch (error) {
            marker.setTooltipContent('Météo indisponible');
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
                weatherContainer.innerHTML = `<strong>${label}</strong><br>Météo indisponible`;
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

        arrowLayer.bindTooltip(`Vent de ${sourceCardinal} · ${windSpeedLabel}`, {
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
            `<strong>Point d'arrivée</strong><br>` +
            `<strong>Météo actuelle</strong><br>${currentLine.replace('<strong>Météo</strong><br>', '')}<br><br>` +
            `<strong>Météo prévue (${arrivalRef})</strong><br>${arrivalLine.replace('<strong>Météo</strong><br>', '')}`;

        arrivalMarker.bindPopup(popupContent, { maxWidth: 320 });
        arrivalMarker.openPopup();
    } catch (error) {
        arrivalMarker.bindPopup('<strong>Point d\'arrivée</strong><br>Météo indisponible', { maxWidth: 300 });
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
                `<strong>Segment ${seg.segmentNumber}</strong><br>` +
                `Départ: ${seg.departureHour} UTC<br>` +
                `Réglage voiles: ${seg.sailSetup}<br>` +
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
        '<strong>Légende vent (route)</strong>' +
        '<div class="wind-legend-row"><span class="wind-legend-swatch" style="background:#ff4fa3"></span><span>Moteur (&lt; 5 kn) · 7 kn</span></div>' +
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
    return cloudConnected && !!cloudClient && !!cloudConfig?.projectKey;
}

async function autoPullRoutesFromCloud(trigger = 'auto') {
    if (!isCloudReady() || cloudAutoPullInFlight) return false;
    cloudAutoPullInFlight = true;

    try {
        const routes = await pullRoutesFromCloud();
        refreshSavedList();
        if (trigger !== 'silent') {
            setCloudStatus(`Cloud synchro auto · ${routes.length} route(s)`);
        }
        return true;
    } catch (error) {
        setCloudStatus(`Synchro auto cloud impossible: ${formatCloudError(error)}`, true);
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

    const { data, error } = await cloudClient
        .from(CLOUD_TABLE_NAME)
        .select('routes')
        .eq('project_key', cloudConfig.projectKey)
        .maybeSingle();

    if (error) throw error;

    const rawPayload = data?.routes;
    let rawRoutes = [];
    let rawWaypointPhotos = null;

    if (Array.isArray(rawPayload)) {
        rawRoutes = rawPayload;
    } else if (rawPayload && typeof rawPayload === 'object') {
        rawRoutes = Array.isArray(rawPayload.routes) ? rawPayload.routes : [];
        rawWaypointPhotos = Array.isArray(rawPayload.waypointPhotos) ? rawPayload.waypointPhotos : [];
    }

    const cloudRoutes = rawRoutes.map((route, index) => sanitizeSavedRoute(route, index));
    setSavedRoutes(cloudRoutes);

    if (Array.isArray(rawWaypointPhotos)) {
        setWaypointPhotoEntries(rawWaypointPhotos, { persistLocal: true, refreshUi: true });
    }

    return cloudRoutes;
}

async function pushRoutesToCloud() {
    if (!isCloudReady()) return false;
    const payload = {
        version: 2,
        routes: getSavedRoutes(),
        waypointPhotos: waypointPhotoEntries
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

function getCloudPasswordPreset() {
    return String(document.getElementById('cloudPasswordPreset')?.value || '').trim();
}

function getCloudEnteredPassword() {
    return String(document.getElementById('cloudPasswordInput')?.value || '').trim();
}

function isCloudPasswordValid() {
    const preset = getCloudPasswordPreset();
    if (!preset) return true;
    return getCloudEnteredPassword() === preset;
}

async function connectCloud(config, { silent = false } = {}) {
    if (!config?.url || !config?.anonKey || !config?.projectKey) {
        stopCloudAutoSync();
        cloudClient = null;
        cloudConfig = null;
        cloudConnected = false;
        if (!silent) setCloudStatus('Mode local (paramètres cloud incomplets)');
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
            cloudClient = createClient(config.url, config.anonKey, {
                auth: {
                    persistSession: false,
                    autoRefreshToken: false,
                    detectSessionInUrl: false,
                    storageKey: `ceibo-supabase-${config.projectKey}`
                }
            });
        }

        cloudConfig = config;
        const routes = await pullRoutesFromCloud();
        saveCloudConfigToStorage(config);
        cloudConnected = true;
        startCloudAutoSync();
        refreshSavedList();
        setCloudStatus(`Cloud connecté · ${routes.length} route(s) partagée(s)`);
        return true;
    } catch (error) {
        stopCloudAutoSync();
        cloudClient = null;
        cloudConfig = null;
        cloudConnected = false;
        setCloudStatus(`Connexion cloud impossible: ${formatCloudError(error)}`, true);
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
    if (routePoints.length === 0) return alert('Aucun waypoint à sauvegarder');
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

    if (isCloudReady()) {
        try {
            await pushRoutesToCloud();
            setCloudStatus(`Cloud synchronisé · ${saved.length} route(s)`);
        } catch (error) {
            setCloudStatus(`Sauvegarde locale OK, synchro cloud échouée: ${formatCloudError(error)}`, true);
        }
    }

    refreshSavedList();

    const sel = document.getElementById('savedRoutesSelect');
    if (sel && currentLoadedRouteIndex >= 0 && currentLoadedRouteIndex < saved.length) {
        sel.selectedIndex = currentLoadedRouteIndex;
    }

    alert(canUpdateLoadedRoute ? `Route mise à jour: ${name}` : `Route sauvegardée: ${name}`);
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
        suggestionBox.textContent = 'Suggestion départ: en attente';
        suggestionBox.classList.remove('suggestion-clickable');
    }
    const arrivalSummary = document.getElementById('arrivalSummary');
    if (arrivalSummary) arrivalSummary.textContent = 'Analyse mouillage: en attente';
    const anchorageContainer = document.getElementById('anchorageRecommendations');
    if (anchorageContainer) anchorageContainer.innerHTML = '';
    const restaurants = document.getElementById('nearbyRestaurants');
    if (restaurants) restaurants.innerHTML = '';
    const shops = document.getElementById('nearbyShops');
    if (shops) shops.innerHTML = '';
    updateSelectedWaypointInfo();
}

function loadRoute(index) {
    const saved = getSavedRoutes();
    if (!saved || !saved[index]) return alert('Aucune route sélectionnée');
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
            .then(() => setCloudStatus(`Cloud synchronisé · ${saved.length} route(s)`))
            .catch(error => setCloudStatus(`Suppression locale OK, synchro cloud échouée: ${formatCloudError(error)}`, true))
            .finally(finalize);
        return;
    }

    finalize();
}

function exportRoute(index) {
    const saved = getSavedRoutes();
    if (!saved || !saved[index]) return alert('Aucune route sélectionnée');
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
    if (!saved || !saved[index]) return alert('Aucune route sélectionnée');

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
                    setCloudStatus(`Cloud synchronisé · ${saved.length} route(s)`);
                } catch (error) {
                    setCloudStatus(`Import local OK, synchro cloud échouée: ${formatCloudError(error)}`, true);
                }
            }
            refreshSavedList();
            alert('Import OK');
        } catch (err) { alert('Fichier JSON/GPX invalide'); }
    };
    reader.readAsText(file);
}