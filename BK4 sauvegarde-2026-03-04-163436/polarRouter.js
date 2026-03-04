// ===============================
// DUFOUR 56 POLAR ROUTER ENGINE
// ===============================

// ---- POLAR DATA ----
const POLAR = {
  2: { 52:1.2, 60:1.5, 75:1.7, 90:1.8, 110:1.6, 120:1.5, 135:1.3, 150:1.1, 165:0.9 },
  4: { 52:2.8, 60:3.5, 75:4.0, 90:4.2, 110:3.9, 120:3.7, 135:3.2, 150:2.8, 165:2.3 },
  6: { 52:6.1, 60:6.8, 75:7.4, 90:7.6, 110:7.3, 120:7.1, 135:6.4, 150:5.8, 165:5.2 },
  8: { 45:6.3, 52:7.1, 60:7.8, 75:8.6, 90:8.9, 110:8.5, 120:8.3, 135:7.6, 150:6.9, 165:6.2 },
  10:{ 42:6.9, 52:8.0, 60:8.8, 75:9.7, 90:10.1, 110:9.6, 120:9.4, 135:8.7, 150:7.8, 165:7.0 },
  12:{ 40:7.3, 52:8.7, 60:9.6, 75:10.5, 90:10.9, 110:10.3, 120:10.0, 135:9.3, 150:8.4, 165:7.5 },
  14:{ 40:7.7, 52:9.2, 60:10.2, 75:11.2, 90:11.6, 110:10.9, 120:10.5, 135:9.8, 150:8.9, 165:8.0 },
  16:{ 40:8.0, 52:9.6, 60:10.7, 75:11.8, 90:12.2, 110:11.4, 120:11.0, 135:10.2, 150:9.2, 165:8.2 },
  20:{ 40:8.3, 52:10.1, 60:11.3, 75:12.4, 90:12.9, 110:12.0, 120:11.6, 135:10.7, 150:9.6, 165:8.5 },
  24:{ 40:8.5, 52:10.4, 60:11.7, 75:12.9, 90:13.5, 110:12.5, 120:12.0, 135:11.0, 150:9.9, 165:8.7 },
  28:{ 40:8.6, 52:10.6, 60:11.9, 75:13.1, 90:13.8, 110:12.7, 120:12.2, 135:11.1, 150:10.0, 165:8.8 },
  30:{ 40:8.7, 52:10.7, 60:12.0, 75:13.2, 90:13.9, 110:12.8, 120:12.3, 135:11.2, 150:10.1, 165:8.9 }
};

// ---- UTILITIES ----
function lerp(x, x0, x1, y0, y1) {
  if (x1 === x0) return y0;
  return y0 + ( (x - x0) * (y1 - y0) ) / (x1 - x0);
}

function getBoundingValues(value, keys) {
  const sorted = keys.map(Number).sort((a,b)=>a-b);
  let lower = sorted[0];
  let upper = sorted[sorted.length -1];

  for (let i=0;i<sorted.length;i++){
    if (sorted[i] <= value) lower = sorted[i];
    if (sorted[i] >= value){ upper = sorted[i]; break; }
  }
  return [lower, upper];
}

// ---- POLAR LOOKUP WITH BILINEAR INTERPOLATION ----
export function polarLookup(tws, twa) {

  if (twa > 180) twa = 360 - twa;

  const twsKeys = Object.keys(POLAR);
  const [twsLow, twsHigh] = getBoundingValues(tws, twsKeys);

  const angleKeysLow = Object.keys(POLAR[twsLow]);
  const angleKeysHigh = Object.keys(POLAR[twsHigh]);

  const [twaLowL, twaHighL] = getBoundingValues(twa, angleKeysLow);
  const [twaLowH, twaHighH] = getBoundingValues(twa, angleKeysHigh);

  const vLL = POLAR[twsLow][twaLowL] ?? 0;
  const vLH = POLAR[twsLow][twaHighL] ?? vLL;
  const vHL = POLAR[twsHigh][twaLowH] ?? 0;
  const vHH = POLAR[twsHigh][twaHighH] ?? vHL;

  const vLow = lerp(twa, twaLowL, twaHighL, vLL, vLH);
  const vHigh = lerp(twa, twaLowH, twaHighH, vHL, vHH);

  return lerp(tws, twsLow, twsHigh, vLow, vHigh);
}

// ---- COMPUTE TWA ----
export function computeTWA(course, windDir) {
  let angle = Math.abs(course - windDir);
  if (angle > 180) angle = 360 - angle;
  return angle;
}

// ---- BEST UPWIND ANGLE (VMG MAX) ----
export function findBestUpwindAngle(tws) {

  let bestAngle = 45;
  let bestVMG = 0;

  for (let angle = 35; angle <= 60; angle += 1) {
    const speed = polarLookup(tws, angle);
    const vmg = speed * Math.cos(angle * Math.PI/180);

    if (vmg > bestVMG) {
      bestVMG = vmg;
      bestAngle = angle;
    }
  }

  return bestAngle;
}

// ---- DISTANCE (NM) ----
export function distanceNm(lat1, lon1, lat2, lon2) {
  const R = 3440.065; // nautical miles
  const toRad = deg => deg * Math.PI/180;

  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);

  const a =
    Math.sin(dLat/2)**2 +
    Math.cos(toRad(lat1)) *
    Math.cos(toRad(lat2)) *
    Math.sin(dLon/2)**2;

  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
  return R * c;
}

// ---- MOVE POINT ----
export function movePoint(lat, lon, bearing, distanceNm) {

  const R = 3440.065;
  const toRad = deg => deg * Math.PI/180;
  const toDeg = rad => rad * 180/Math.PI;

  const brng = toRad(bearing);
  const d = distanceNm / R;

  const lat1 = toRad(lat);
  const lon1 = toRad(lon);

  const lat2 = Math.asin(
    Math.sin(lat1)*Math.cos(d) +
    Math.cos(lat1)*Math.sin(d)*Math.cos(brng)
  );

  const lon2 = lon1 + Math.atan2(
    Math.sin(brng)*Math.sin(d)*Math.cos(lat1),
    Math.cos(d)-Math.sin(lat1)*Math.sin(lat2)
  );

  return {
    lat: toDeg(lat2),
    lon: toDeg(lon2)
  };
}

// ---- MAIN ROUTING FUNCTION ----
export function routeSegment(start, end, windDir, windSpeed, legTimeHours = 0.5) {

  const resultPoints = [];
  const totalDistance = distanceNm(start.lat, start.lon, end.lat, end.lon);

  const bearing = getBearing(start, end);
  const twa = computeTWA(bearing, windDir);

  if (twa >= 40) {

    const boatSpeed = polarLookup(windSpeed, twa);
    const time = totalDistance / boatSpeed;

    return {
      type: "direct",
      distance: totalDistance,
      speed: boatSpeed,
      bearing: bearing,
      timeHours: time,
      points: [start, end]
    };

  } else {

    const optimalAngle = findBestUpwindAngle(windSpeed);
    const boatSpeed = polarLookup(windSpeed, optimalAngle);

    const legDistance = boatSpeed * legTimeHours;

    let current = start;
    let tack = 1;
    let sailed = 0;

    while (sailed < totalDistance) {

      const heading = bearing + tack * optimalAngle;
      const next = movePoint(current.lat, current.lon, heading, legDistance);

      resultPoints.push(next);

      current = next;
      sailed += legDistance * Math.cos(optimalAngle * Math.PI/180);
      tack *= -1;
    }

    return {
      type: "tacking",
      distance: totalDistance,
      speed: boatSpeed,
      bearing: bearing,
      timeHours: sailed / boatSpeed,
      points: resultPoints
    };
  }
}

// ---- BEARING ----
export function getBearing(start, end) {

  const toRad = deg => deg * Math.PI/180;
  const toDeg = rad => rad * 180/Math.PI;

  const lat1 = toRad(start.lat);
  const lat2 = toRad(end.lat);
  const dLon = toRad(end.lon - start.lon);

  const y = Math.sin(dLon) * Math.cos(lat2);
  const x =
    Math.cos(lat1)*Math.sin(lat2) -
    Math.sin(lat1)*Math.cos(lat2)*Math.cos(dLon);

  return (toDeg(Math.atan2(y,x)) + 360) % 360;
}