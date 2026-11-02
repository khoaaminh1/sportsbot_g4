// Where the page finds the SportsBot API.
// Empty: same origin (the backend serves this page at /app/).
// When the page is hosted elsewhere (for example on Vercel), set the public API address,
// e.g. "https://sportsbot-api.example.com" (a Cloudflare Tunnel to the backend).
window.SPORTSBOT_API = window.SPORTSBOT_API || "";
