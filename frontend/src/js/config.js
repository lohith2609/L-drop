// js/config.js (FINAL, CORRECTED VERSION)

function normalizeConfiguredUrl(rawUrl) {
    if (!rawUrl) {
        return rawUrl;
    }

    try {
        const parsed = new URL(rawUrl);
        const isLoopbackHost =
            parsed.hostname === 'localhost' ||
            parsed.hostname === '127.0.0.1' ||
            parsed.hostname === '0.0.0.0';
        const isLanPage =
            window.location.hostname !== 'localhost' &&
            window.location.hostname !== '127.0.0.1';

        if (isLoopbackHost && isLanPage) {
            parsed.hostname = window.location.hostname;
            return parsed.toString().replace(/\/$/, '');
        }
    } catch {
        return rawUrl;
    }

    return rawUrl;
}

function getWebSocketUrl() {
    if (import.meta.env?.VITE_WEBSOCKET_URL) {
        return import.meta.env.VITE_WEBSOCKET_URL;
    }

    if (import.meta.env?.VITE_USE_LOCAL_BACKEND === 'true') {
        return `ws://${window.location.hostname}:8080`;
    }

    const isLocalhost =
        typeof window !== 'undefined' &&
        (window.location.hostname === 'localhost' ||
            window.location.hostname === '127.0.0.1' ||
            window.location.hostname.endsWith('.local'));

    if (isLocalhost) {
        return `ws://${window.location.hostname}:8080`;
    }

    return 'wss://dropsilk-backend.onrender.com';
}

function getApiBaseUrl() {
    const configuredBaseUrl = import.meta.env?.VITE_API_BASE_URL;
    if (configuredBaseUrl) {
        return normalizeConfiguredUrl(configuredBaseUrl);
    }

    if (import.meta.env?.VITE_USE_LOCAL_BACKEND === 'true') {
        return `http://${window.location.hostname}:8080`;
    }

    // Relative path routes through Vite dev proxy locally and Vercel rewrites in production, eliminating CORS errors
    return '';
}

export const WEBSOCKET_URL = getWebSocketUrl();
// ICE_SERVERS are now fetched dynamically from the backend in webrtc.js to support TURN.
export const HIGH_WATER_MARK = 1024 * 1024; // buffer size for data channel
// reCAPTCHA: read from Vite env; do not hardcode in source.
export const RECAPTCHA_SITE_KEY = (import.meta.env?.VITE_RECAPTCHA_SITE_KEY) || '';
export const API_BASE_URL = getApiBaseUrl();
export const OPFS_THRESHOLD = 256 * 1024 * 1024; // 256 MB