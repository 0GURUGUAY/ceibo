import { routeSegment } from './polarRouter.js';

let map;
let routePoints = [];
let markers = [];
let routeLayer = null;
let windLayer = null;
let departureDate = new Date().toISOString().split('T')[0];
let departureTime = "12:00";
let tackingTimeHours = 0.5;
const weatherCache = new Map();
let lastWeatherUpdateAt = null;
let waypointWindDirectionLayers = [];

// =====================
// INIT MAP
// =====================

document.addEventListener('DOMContentLoaded', function() {
    map = L.map('map').setView([41.3851, 2.1734], 8);

    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '© OpenStreetMap'
    }).addTo(map);

    // Setup event listeners after map is ready
    map.on('click', function(e) {
        routePoints.push(e.latlng);
        const marker = createWaypointMarker(e.latlng);
        markers.push(marker);
    });

    // =====================
    // DATE & TIME INPUT
    // =====================

    document.getElementById("dateInput").value = departureDate;
    document.getElementById("dateInput").addEventListener("change", function(e) {
        departureDate = e.target.value;
    });

    document.getElementById("timeInput").value = departureTime;
    document.getElementById("timeInput").addEventListener("change", function(e) {
        departureTime = e.target.value;
    });

    document.getElementById("tackingTimeInput").value = tackingTimeHours;
    document.getElementById("tackingTimeInput").addEventListener("change", function(e) {
        tackingTimeHours = parseFloat(e.target.value);
    });

    document.getElementById("computeBtn").addEventListener("click", computeRoute);
    document.getElementById("resetBtn").addEventListener("click", () => {

        routePoints = [];
        markers = [];

        if (routeLayer) map.removeLayer(routeLayer);
        if (windLayer) map.removeLayer(windLayer);

        map.eachLayer(layer => {
            if (layer instanceof L.Marker) map.removeLayer(layer);
        });
        clearWaypointWindDirectionLayers();

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

    let fullRoute = [];
    let totalTime = 0;
    let totalDistance = 0;
    let segmentsInfo = [];
    const waypointPassageWeather = [];
    const routeSegmentsForDraw = [];

    const departureDateTime = new Date(`${departureDate}T${departureTime}:00Z`);
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

        drawWind(routePoints[i].lat, routePoints[i].lng, wind.direction);

        const segment = routeSegment(
            {lat: routePoints[i].lat, lon: routePoints[i].lng},
            {lat: routePoints[i+1].lat, lon: routePoints[i+1].lng},
            wind.direction,
            wind.speed,
            tackingTimeHours
        );

        const segmentLatLngs = buildSegmentLatLngs(routePoints[i], routePoints[i + 1], segment);
        routeSegmentsForDraw.push({
            latlngs: segmentLatLngs,
            windSpeed: wind.speed
        });

        fullRoute = fullRoute.concat(segment.points);
        totalTime += segment.timeHours;
        totalDistance += segment.distance;
        
        segmentsInfo.push({
            number: i + 1,
            distance: segment.distance.toFixed(2),
            time: segment.timeHours.toFixed(2),
            speed: segment.speed.toFixed(2),
            bearing: Math.round(segment.bearing),
            windSpeed: wind.speed.toFixed(1),
            windDirection: Math.round(wind.direction),
            type: segment.type
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

    drawWaypointWindDirections(waypointPassageWeather);

    drawRouteByWindSpeed(routeSegmentsForDraw);

    let segmentsHtml = '<table style="width:100%; border-collapse:collapse; margin-top:10px;"><tr style="border-bottom:1px solid #ccc;"><th style="text-align:left; padding:5px;">Seg</th><th style="text-align:right; padding:5px;">Cap</th><th style="text-align:right; padding:5px;">Distance</th><th style="text-align:right; padding:5px;">Temps</th><th style="text-align:right; padding:5px;">Vit.</th><th style="text-align:right; padding:5px;">V.V</th><th style="text-align:right; padding:5px;">D.V</th></tr>';
    
    segmentsInfo.forEach(seg => {
        segmentsHtml += `<tr style="border-bottom:1px solid #eee;"><td style="padding:5px;">${seg.number}</td><td style="text-align:right; padding:5px;">${seg.bearing}°</td><td style="text-align:right; padding:5px;">${seg.distance} nm</td><td style="text-align:right; padding:5px;">${seg.time} h</td><td style="text-align:right; padding:5px;">${seg.speed} kn</td><td style="text-align:right; padding:5px;">${seg.windSpeed} kn</td><td style="text-align:right; padding:5px;">${seg.windDirection}°</td></tr>`;
    });
    
    segmentsHtml += '</table>';

    const pressureSummary = buildPressureEvolutionSummary(waypointPassageWeather);
    const weatherUpdatedAt = formatUtcDateTime(lastWeatherUpdateAt);

    document.getElementById("info").innerHTML =
        `<strong>Segments: ${routePoints.length - 1}</strong><br>
        <strong>Distance totale: ${totalDistance.toFixed(2)} nm</strong><br>
        <strong>Temps total: ${totalTime.toFixed(2)} h</strong><br>
        <strong>Météo (dernière MAJ): ${weatherUpdatedAt}</strong><br>
        <strong>Évolution pression: ${pressureSummary}</strong>` +
        segmentsHtml;

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

    const url = `https://api.open-meteo.com/v1/forecast?latitude=${lat}&longitude=${lon}&start_date=${date}&end_date=${date}&hourly=temperature_2m,windspeed_10m,winddirection_10m,precipitation,weather_code,surface_pressure&windspeed_unit=kn&timezone=UTC`;
    const response = await fetch(url);
    const data = await response.json();

    const hourIndex = parseInt(hour.split(':')[0], 10);

    const fetchedAt = new Date().toISOString();

    const weather = {
        temperature: data?.hourly?.temperature_2m?.[hourIndex],
        windSpeed: data?.hourly?.windspeed_10m?.[hourIndex],
        windDirection: data?.hourly?.winddirection_10m?.[hourIndex],
        precipitation: data?.hourly?.precipitation?.[hourIndex],
        pressure: data?.hourly?.surface_pressure?.[hourIndex],
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

async function getCurrentWeatherAtWaypoint(lat, lon) {
    const now = new Date();
    const { date, hour } = toDateAndHourUtc(now);
    return getWeatherAtDateHour(lat, lon, date, hour);
}

function formatWeatherTooltipContent(weather) {
    const temp = Number.isFinite(weather.temperature) ? `${weather.temperature.toFixed(1)}°C` : 'N/A';
    const windSpeed = Number.isFinite(weather.windSpeed) ? `${weather.windSpeed.toFixed(1)} kn` : 'N/A';
    const windDirection = Number.isFinite(weather.windDirection) ? `${Math.round(weather.windDirection)}°` : 'N/A';
    const windCardinal = degreesToCardinalFr(weather.windDirection);
    const precipitation = Number.isFinite(weather.precipitation) ? `${weather.precipitation.toFixed(1)} mm` : 'N/A';
    const pressure = Number.isFinite(weather.pressure) ? `${weather.pressure.toFixed(0)} hPa` : 'N/A';
    const updatedAt = formatUtcDateTime(weather.updatedAt);
    const summary = weatherCodeToLabel(weather.weatherCode);

    return `<strong>Météo</strong><br>${summary}<br>Temp: ${temp}<br>Vent: ${windSpeed} (${windDirection}, ${windCardinal})<br>Pluie: ${precipitation}<br>Pression: ${pressure}<br>Maj: ${updatedAt}`;
}

function formatUtcDateTime(isoString) {
    if (!isoString) return 'N/A';
    const date = new Date(isoString);
    if (Number.isNaN(date.getTime())) return 'N/A';

    return `${date.toLocaleDateString('fr-FR', { timeZone: 'UTC' })} ${date.toLocaleTimeString('fr-FR', {
        timeZone: 'UTC',
        hour: '2-digit',
        minute: '2-digit'
    })} UTC`;
}

function renderWaypointWeatherInfo(marker, weather) {
    const weatherContainer = document.getElementById('waypointWeatherInfo');
    if (!weatherContainer) return;

    const current = marker.getLatLng();
    const index = markers.indexOf(marker);
    const waypointLabel = index !== -1 ? `Waypoint ${index + 1}` : 'Waypoint';

    const temp = Number.isFinite(weather.temperature) ? `${weather.temperature.toFixed(1)}°C` : 'N/A';
    const windSpeed = Number.isFinite(weather.windSpeed) ? `${weather.windSpeed.toFixed(1)} kn` : 'N/A';
    const windDirectionDeg = Number.isFinite(weather.windDirection) ? `${Math.round(weather.windDirection)}°` : 'N/A';
    const windDirectionCardinal = degreesToCardinalFr(weather.windDirection);
    const precipitation = Number.isFinite(weather.precipitation) ? `${weather.precipitation.toFixed(1)} mm` : 'N/A';
    const pressure = Number.isFinite(weather.pressure) ? `${weather.pressure.toFixed(0)} hPa` : 'N/A';
    const updatedAt = formatUtcDateTime(weather.updatedAt);
    const summary = weatherCodeToLabel(weather.weatherCode);

    weatherContainer.innerHTML =
        `<strong>${waypointLabel}</strong><br>` +
        `${current.lat.toFixed(4)}, ${current.lng.toFixed(4)}<br>` +
        `<strong>${summary}</strong><br>` +
        `Température: ${temp}<br>` +
        `Vent: ${windSpeed} — ${windDirectionDeg} (${windDirectionCardinal})<br>` +
        `Pluie: ${precipitation}<br>` +
        `Pression: ${pressure}<br>` +
        `Prévision: ${departureDate} ${departureTime} UTC<br>` +
        `Dernière mise à jour: ${updatedAt}`;
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
        const current = marker.getLatLng();
        marker.bindTooltip('Chargement météo...', { direction: 'top', opacity: 0.95 });
        marker.openTooltip();

        try {
            const weather = await getWeatherAtWaypoint(current.lat, current.lng);
            marker.setTooltipContent(formatWeatherTooltipContent(weather));
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
            const current = marker.getLatLng();
            const weather = await getWeatherAtWaypoint(current.lat, current.lng);
            renderWaypointWeatherInfo(marker, weather);
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
    const departureDateTime = new Date(`${departureDate}T${departureTime}:00Z`);

    if (Number.isNaN(departureDateTime.getTime())) return;

    const arrivalDateTime = new Date(departureDateTime.getTime() + totalTimeHours * 3600 * 1000);
    const arrivalSlot = toDateAndHourUtc(arrivalDateTime);

    try {
        const [currentWeather, arrivalWeather] = await Promise.all([
            getCurrentWeatherAtWaypoint(destination.lat, destination.lng),
            getWeatherAtDateHour(destination.lat, destination.lng, arrivalSlot.date, arrivalSlot.hour)
        ]);

        const currentLine = formatWeatherTooltipContent(currentWeather);
        const arrivalLine = formatWeatherTooltipContent(arrivalWeather);

        const popupContent =
            `<strong>Point d'arrivée</strong><br>` +
            `<strong>Météo actuelle</strong><br>${currentLine.replace('<strong>Météo</strong><br>', '')}<br><br>` +
            `<strong>Météo prévue (${arrivalSlot.date} ${arrivalSlot.hour} UTC)</strong><br>${arrivalLine.replace('<strong>Météo</strong><br>', '')}`;

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
            latlngs.push([lat, lon]);
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

function drawRouteByWindSpeed(routeSegments) {
    if (routeLayer) map.removeLayer(routeLayer);

    const polylines = routeSegments
        .filter(seg => Array.isArray(seg.latlngs) && seg.latlngs.length >= 2)
        .map(seg => L.polyline(seg.latlngs, {
            color: getWindSpeedColor(seg.windSpeed),
            weight: 4,
            opacity: 0.95
        }));

    routeLayer = L.layerGroup(polylines).addTo(map);
}

function renderWindSpeedLegend() {
    const legend = document.getElementById('windSpeedLegend');
    if (!legend) return;

    legend.innerHTML =
        '<strong>Légende vent (route)</strong>' +
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
    clearWaypointWindDirectionLayers();
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

    r.points.forEach(pt => {
        const lat = pt.lat;
        const lon = pt.lon;
        const marker = createWaypointMarker([lat, lon]);
        markers.push(marker);
        routePoints.push(marker.getLatLng());
    });

    // restore UI values (keep current selected date)
    document.getElementById('timeInput').value = r.time;
    departureTime = r.time;
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