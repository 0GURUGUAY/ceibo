import { routeSegment, distanceNm, getBearing, computeTWA } from './polarRouter.js';

let map;
let standardTileLayer;
let satelliteTileLayer;
let activeBaseLayer;
let baseLayerControl;
let routePoints = [];
let markers = [];
let routeLayer = null;
let windLayer = null;
let departureDate = new Date().toISOString().split('T')[0];
let departureTime = "12:00";
let tackingTimeHours = 0.5;
let sailMode = 'auto';
const weatherCache = new Map();
let lastWeatherUpdateAt = null;
let waypointWindDirectionLayers = [];
let waveDirectionSegmentLayers = [];
let waypointPassageSlots = new Map();
let lastRouteBounds = null;
const MOTOR_WIND_THRESHOLD_KN = 5;
const MOTOR_SPEED_KN = 7;
const MAP_STYLE_STORAGE_KEY = 'ceiboMapStyle';
const STRONG_WAVE_THRESHOLD_M = 1.8;

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

// =====================
// INIT MAP
// =====================

document.addEventListener('DOMContentLoaded', function() {
    map = L.map('map').setView([41.3851, 2.1734], 8);

    standardTileLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '© OpenStreetMap'
    });

    satelliteTileLayer = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
        attribution: 'Tiles © Esri'
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

    // Setup event listeners after map is ready
    map.on('click', function(e) {
        routePoints.push(e.latlng);
        const marker = createWaypointMarker(e.latlng);
        markers.push(marker);
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
        const style = e.name === 'Satellite' ? 'satellite' : 'standard';
        activeBaseLayer = style === 'satellite' ? satelliteTileLayer : standardTileLayer;
        localStorage.setItem(MAP_STYLE_STORAGE_KEY, style);
    });

    const routingTabBtn = document.getElementById('routingTabBtn');
    const routesTabBtn = document.getElementById('routesTabBtn');
    const routingTab = document.getElementById('routingTab');
    const routesTab = document.getElementById('routesTab');

    function activateTab(tabName) {
        const isRouting = tabName === 'routing';
        routingTabBtn.classList.toggle('active', isRouting);
        routesTabBtn.classList.toggle('active', !isRouting);
        routingTab.classList.toggle('active', isRouting);
        routesTab.classList.toggle('active', !isRouting);
    }

    routingTabBtn.addEventListener('click', () => activateTab('routing'));
    routesTabBtn.addEventListener('click', () => activateTab('routes'));

    document.getElementById("tackingTimeInput").value = tackingTimeHours;
    document.getElementById("tackingTimeInput").addEventListener("change", function(e) {
        tackingTimeHours = parseFloat(e.target.value);
    });

    document.getElementById("sailModeSelect").value = sailMode;
    document.getElementById("sailModeSelect").addEventListener("change", function(e) {
        sailMode = e.target.value;
    });

    document.getElementById("computeBtn").addEventListener("click", computeRoute);
    document.getElementById("recenterBtn").addEventListener("click", recenterOnRoute);
    document.getElementById("resetBtn").addEventListener("click", () => {

        routePoints = [];
        markers = [];

        if (routeLayer) map.removeLayer(routeLayer);
        if (windLayer) map.removeLayer(windLayer);

        map.eachLayer(layer => {
            if (layer instanceof L.Marker) map.removeLayer(layer);
        });
        clearWaypointWindDirectionLayers();
        clearWaveDirectionSegmentLayers();
        waypointPassageSlots.clear();
        lastRouteBounds = null;

        document.getElementById("info").innerHTML = "";
        const weatherContainer = document.getElementById('waypointWeatherInfo');
        if (weatherContainer) weatherContainer.innerHTML = '';
        const windLegend = document.getElementById('windSpeedLegend');
        if (windLegend) windLegend.innerHTML = '';
    });

    // Saved routes UI
    document.getElementById('saveRouteBtn').addEventListener('click', saveRoute);
    document.getElementById('loadRouteBtn').addEventListener('click', () => {
        const sel = document.getElementById('savedRoutesSelect');
        loadRoute(sel.selectedIndex);
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
    document.getElementById('importRouteInput').addEventListener('change', handleImport);

    refreshSavedList();
});

// =====================
// COMPUTE ROUTE
// =====================

async function computeRoute() {

    if (routePoints.length < 2) return;

    clearWaypointWindDirectionLayers();
    clearWaveDirectionSegmentLayers();
    waypointPassageSlots.clear();

    let fullRoute = [];
    let totalTime = 0;
    let totalDistance = 0;
    let segmentsInfo = [];
    const waypointPassageWeather = [];
    const routeSegmentsForDraw = [];

    const departureDateTime = new Date(`${departureDate}T${departureTime}:00`);
    if (Number.isNaN(departureDateTime.getTime())) {
        alert('Date/heure de départ invalide');
        return;
    }

    for (let i = 0; i < routePoints.length - 1; i++) {

        const passageDateTime = new Date(departureDateTime.getTime() + totalTime * 3600 * 1000);
        const passageSlot = toDateAndHourUtc(passageDateTime);
        const weatherAtPassage = await getWeatherAtDateHour(
            routePoints[i].lat,
            routePoints[i].lng,
            passageSlot.date,
            passageSlot.hour
        );

        const wind = {
            speed: Number.isFinite(weatherAtPassage.windSpeed) ? weatherAtPassage.windSpeed : 10,
            direction: Number.isFinite(weatherAtPassage.windDirection) ? weatherAtPassage.windDirection : 0
        };

        waypointPassageWeather.push({
            waypointIndex: i,
            weather: weatherAtPassage,
            slot: passageSlot
        });
        waypointPassageSlots.set(i, passageSlot);

        drawWind(routePoints[i].lat, routePoints[i].lng, wind.direction);

        const startPoint = {lat: routePoints[i].lat, lon: routePoints[i].lng};
        const endPoint = {lat: routePoints[i+1].lat, lon: routePoints[i+1].lng};
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

        const segment = isMotorSegment
            ? rawSegment
            : {
                ...rawSegment,
                speed: rawSegment.speed * sailFactor,
                timeHours: rawSegment.timeHours / sailFactor
            };

        const segmentLatLngs = buildSegmentLatLngs(routePoints[i], routePoints[i + 1], segment);
        routeSegmentsForDraw.push({
            latlngs: segmentLatLngs,
            windSpeed: wind.speed,
            mode: segment.type,
            waveHeight: weatherAtPassage.waveHeight,
            waveDirection: weatherAtPassage.waveDirection,
            segmentNumber: i + 1,
            sailSetup,
            sailComment,
            departureHour: passageSlot.hour
        });

        fullRoute = fullRoute.concat(segment.points);
        totalTime += segment.timeHours;
        totalDistance += segment.distance;
        
        segmentsInfo.push({
            number: i + 1,
            startWaypointIndex: i,
            departureLabel: formatWeekdayHourUtc(passageDateTime),
            distance: segment.distance.toFixed(2),
            time: segment.timeHours.toFixed(2),
            speed: segment.speed.toFixed(2),
            bearing: Math.round(segment.bearing),
            windSpeed: wind.speed.toFixed(1),
            windDirection: Math.round(wind.direction),
            type: segment.type,
            sailSetup,
            sailComment,
            departureHour: passageSlot.hour
        });
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

    let segmentsHtml = '<table id="segmentsTable" style="width:100%; border-collapse:collapse; margin-top:8px; font-size:8px;"><tr style="border-bottom:1px solid #ccc;"><th style="text-align:left; padding:2px; width:14px;">Seg</th><th style="text-align:right; padding:2px;">Départ</th><th style="text-align:right; padding:2px;">Cap</th><th style="text-align:right; padding:2px;">Dist.</th><th style="text-align:right; padding:2px;">Temps</th><th style="text-align:right; padding:2px;">Vit.</th><th style="text-align:right; padding:2px;">V.V</th><th style="text-align:right; padding:2px;">D.V</th><th style="text-align:left; padding:2px;">Voiles</th></tr>';
    
    segmentsInfo.forEach(seg => {
        segmentsHtml += `<tr class="segment-row" data-wp-index="${seg.startWaypointIndex}" style="border-bottom:1px solid #eee; cursor:pointer;"><td style="padding:2px; width:14px;">${seg.number}</td><td style="text-align:right; padding:2px;">${seg.departureLabel}</td><td style="text-align:right; padding:2px;">${seg.bearing}°</td><td style="text-align:right; padding:2px;">${seg.distance} nm</td><td style="text-align:right; padding:2px;">${seg.time} h</td><td style="text-align:right; padding:2px;">${seg.speed} kn</td><td style="text-align:right; padding:2px;">${seg.windSpeed} kn</td><td style="text-align:right; padding:2px;">${seg.windDirection}°</td><td style="padding:2px;">${seg.sailSetup}</td></tr>`;
    });
    
    segmentsHtml += '</table>';

    const pressureSummary = buildPressureEvolutionSummary(waypointPassageWeather);
    const weatherUpdatedAt = formatUtcDateTime(lastWeatherUpdateAt);

    document.getElementById("info").innerHTML =
        `<div class="segment-summary">
            <strong>Segments: ${routePoints.length - 1}</strong><br>
            <strong>Distance totale: ${totalDistance.toFixed(2)} nm</strong><br>
            <strong>Temps total: ${totalTime.toFixed(2)} h</strong><br>
            <strong>Météo (dernière MAJ): ${weatherUpdatedAt}</strong><br>
            <strong>Évolution pression: ${pressureSummary}</strong>
        </div>` +
        segmentsHtml;

    const segmentRows = document.querySelectorAll('#segmentsTable .segment-row');
    segmentRows.forEach(row => {
        row.addEventListener('click', async () => {
            const wpIndex = Number(row.getAttribute('data-wp-index'));
            const marker = markers[wpIndex];
            if (!marker) return;
            const latlng = marker.getLatLng();
            map.panTo(latlng, { animate: true, duration: 0.45 });
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

async function getWeatherAtDateHour(lat, lon, date, hour) {
    const cacheKey = getWeatherCacheKeyAtDateHour(lat, lon, date, hour);
    if (weatherCache.has(cacheKey)) return weatherCache.get(cacheKey);

    const forecastUrl = `https://api.open-meteo.com/v1/forecast?latitude=${lat}&longitude=${lon}&start_date=${date}&end_date=${date}&hourly=temperature_2m,windspeed_10m,winddirection_10m,windgusts_10m,precipitation,weather_code,surface_pressure&windspeed_unit=kn&timezone=UTC`;
    const marineUrl = `https://marine-api.open-meteo.com/v1/marine?latitude=${lat}&longitude=${lon}&start_date=${date}&end_date=${date}&hourly=wave_height,wave_direction,wave_period&timezone=UTC`;

    const [data, marineData] = await Promise.all([
        fetch(forecastUrl).then(response => response.json()),
        fetch(marineUrl).then(response => response.json()).catch(() => null)
    ]);

    const hourIndex = parseInt(hour.split(':')[0], 10);

    const fetchedAt = new Date().toISOString();

    const weather = {
        temperature: data?.hourly?.temperature_2m?.[hourIndex],
        windSpeed: data?.hourly?.windspeed_10m?.[hourIndex],
        windGust: data?.hourly?.windgusts_10m?.[hourIndex],
        windDirection: data?.hourly?.winddirection_10m?.[hourIndex],
        precipitation: data?.hourly?.precipitation?.[hourIndex],
        pressure: data?.hourly?.surface_pressure?.[hourIndex],
        waveHeight: marineData?.hourly?.wave_height?.[hourIndex],
        waveDirection: marineData?.hourly?.wave_direction?.[hourIndex],
        wavePeriod: marineData?.hourly?.wave_period?.[hourIndex],
        weatherCode: data?.hourly?.weather_code?.[hourIndex],
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
    const waypointLabel = index !== -1 ? `Waypoint ${index + 1}` : 'Waypoint';

    return `<strong>${waypointLabel}</strong><br>${current.lat.toFixed(4)}, ${current.lng.toFixed(4)}<br>${formatWeatherTooltipContent(weather, referenceLabel).replace('<strong>Météo</strong><br>', '')}`;
}

async function openWaypointWeatherPopup(marker) {
    marker.bindPopup('Chargement météo...', { maxWidth: 340, autoPan: false });
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

function getSavedRoutes() {
    const raw = localStorage.getItem('savedRoutes');
    return raw ? JSON.parse(raw) : [];
}

function setSavedRoutes(list) {
    localStorage.setItem('savedRoutes', JSON.stringify(list));
}

function refreshSavedList() {
    const sel = document.getElementById('savedRoutesSelect');
    if (!sel) return;
    sel.innerHTML = '';
    const saved = getSavedRoutes();
    saved.forEach((r, i) => {
        const opt = document.createElement('option');
        opt.value = i;
        opt.text = `${r.name} (${r.date} ${r.time})`;
        sel.appendChild(opt);
    });
}

function saveRoute() {
    if (routePoints.length === 0) return alert('Aucun waypoint à sauvegarder');
    const nameInput = document.getElementById('routeNameInput');
    const name = (nameInput && nameInput.value) ? nameInput.value : `Route ${new Date().toLocaleString()}`;

    const saved = getSavedRoutes();
    saved.push({
        name,
        date: departureDate,
        time: departureTime,
        tackingTimeHours,
        points: routePoints.map(p => ({ lat: p.lat, lon: p.lng ?? p.lon })) ,
        createdAt: new Date().toISOString()
    });
    setSavedRoutes(saved);
    refreshSavedList();
}

function clearCurrentRoute() {
    // remove markers
    markers.forEach(m => map.removeLayer(m));
    markers = [];
    routePoints = [];
    waypointPassageSlots.clear();
    lastRouteBounds = null;
    clearWaypointWindDirectionLayers();
    clearWaveDirectionSegmentLayers();
    if (routeLayer) { map.removeLayer(routeLayer); routeLayer = null; }
    const weatherContainer = document.getElementById('waypointWeatherInfo');
    if (weatherContainer) weatherContainer.innerHTML = '';
    const windLegend = document.getElementById('windSpeedLegend');
    if (windLegend) windLegend.innerHTML = '';
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

    // restore UI values (keep current selected date)
    departureTime = normalizeHourTime(r.time);
    updateDepartureDateTimeInput();
    document.getElementById('tackingTimeInput').value = r.tackingTimeHours;
    tackingTimeHours = r.tackingTimeHours;

    // draw polyline between waypoints
    drawRoute(routePoints);
}

function deleteRoute(index) {
    const saved = getSavedRoutes();
    if (!saved || !saved[index]) return;
    saved.splice(index, 1);
    setSavedRoutes(saved);
    refreshSavedList();
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
    reader.onload = function(evt) {
        try {
            const content = String(evt.target.result || '');
            const saved = getSavedRoutes();

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
            refreshSavedList();
            alert('Import OK');
        } catch (err) { alert('Fichier JSON/GPX invalide'); }
    };
    reader.readAsText(file);
}